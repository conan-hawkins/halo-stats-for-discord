"""Measure what HaloStatsRateLimiter actually does under the deployed shape.

Run it after any change to src/api/rate_limiters.py. It needs no network and
no credentials.

    PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe tools/rl_repro.py

It differs from tests/test_rate_limiter_stress.py in the two ways that make the
limiter's behaviour observable at all:

  1. it uses the DEPLOYED rate of 3 req/s per account, not 1000. At 1000 the
     minimum interval is 1ms and nothing it is meant to enforce is visible.
  2. it HOLDS each slot for a simulated request duration, so in-flight
     concurrency is a real quantity rather than always being zero.

History, measured on 2026-08-11 before the fix:
    peak in-flight 55 against a cap of 25; 25 requests issued in 0.347s
    instead of ~1.33s; 20 of those 25 issued within the same 0.1ms, all on
    account 0. The limiter did not merely fail to space requests, it
    synchronised them into a herd.
"""
import asyncio
import sys
import time

sys.path.insert(0, ".")

from src.api.rate_limiters import HaloStatsRateLimiter

ACCOUNTS = 5
RATE = 3                  # req/s per account, as deployed
REQUEST_SECONDS = 0.25    # pretend the Halo call takes 250ms

# asyncio.sleep can only overshoot, never undershoot, and the Windows timer
# granularity is ~15ms. An observed gap is therefore the scheduled gap plus the
# wake jitter of the LATER call minus that of the earlier one, which can read
# slightly short even when the schedule is exact. Only flag gaps that miss by
# more than the scheduler could account for.
JITTER_TOLERANCE = 0.050


def rule(title):
    print(f"\n{title}\n{'-' * len(title)}")


async def measure(concurrency, hold_slot, request_seconds=REQUEST_SECONDS):
    """Drive `concurrency` callers. hold_slot=True uses the slot() context
    manager (the correct API); False calls wait_if_needed() directly."""
    rl = HaloStatsRateLimiter(requests_per_second_per_account=RATE)
    rl.set_num_accounts(ACCOUNTS)

    t0 = time.monotonic()
    inflight = 0
    peak = 0
    events = []

    async def caller(_):
        nonlocal inflight, peak
        if hold_slot:
            async with rl.slot() as account:
                inflight += 1
                peak = max(peak, inflight)
                events.append((time.monotonic() - t0, account))
                await asyncio.sleep(request_seconds)
                inflight -= 1
        else:
            account = await rl.wait_if_needed()
            inflight += 1
            peak = max(peak, inflight)
            events.append((time.monotonic() - t0, account))
            await asyncio.sleep(request_seconds)
            inflight -= 1

    await asyncio.gather(*(caller(i) for i in range(concurrency)))
    events.sort()
    return peak, events


def report_pacing(events):
    min_interval = 1.0 / RATE
    per_account = {}
    for at, acct in events:
        per_account.setdefault(acct, []).append(at)

    total_violations = 0
    for acct in sorted(per_account):
        times = per_account[acct]
        gaps = [b - a for a, b in zip(times, times[1:])]
        bad = [g for g in gaps if g < min_interval - JITTER_TOLERANCE]
        total_violations += len(bad)
        print(f"  account {acct}: {len(times):>2} requests, "
              f"{len(bad)} gap(s) below {min_interval:.3f}s "
              f"(tolerance {JITTER_TOLERANCE * 1000:.0f}ms)")
        print(f"    at {[round(t, 4) for t in times]}")

    # A herd is the signature defect: many requests sharing one instant.
    instants = {}
    for at, _ in events:
        instants[round(at, 3)] = instants.get(round(at, 3), 0) + 1
    biggest = max(instants.values())

    return total_violations, biggest


async def main():
    min_interval = 1.0 / RATE
    print(f"deployed shape: {ACCOUNTS} accounts x {RATE} req/s "
          f"= {ACCOUNTS * RATE} req/s, {ACCOUNTS * 5} max concurrent")
    print(f"per-account minimum interval: {min_interval:.4f}s")

    # ---- pacing, via the correct API -------------------------------------
    rule("A. pacing and spread (25 callers via slot())")
    peak, events = await measure(25, hold_slot=True)
    violations, biggest = report_pacing(events)

    span = max(t for t, _ in events)
    # 25 requests over 5 accounts = 5 each; first at 0, last at 4 intervals.
    expected_span = (25 / ACCOUNTS - 1) * min_interval
    print(f"\n  pacing violations       : {violations}   (want 0)")
    # ACCOUNTS sends may legitimately share an instant - one per account. More
    # than that is the herd signature.
    print(f"  most requests at one ms : {biggest}   "
          f"(want <= {ACCOUNTS}, one per account; was 20)")
    print(f"  span, first to last     : {span:.3f}s   (want ~{expected_span:.2f}s)")
    print(f"  peak in-flight          : {peak}")

    # ---- concurrency cap --------------------------------------------------
    # Correct pacing alone keeps in-flight near rate x duration, so the cap only
    # bites when requests are slow. 3s at 15 req/s wants ~45 in flight; the cap
    # is 25. A slow upstream is exactly when the cap matters.
    cap = ACCOUNTS * 5
    slow = 3.0

    rule(f"B. concurrency cap - 120 callers, {slow:.0f}s requests, via slot()")
    peak_slot, _ = await measure(120, hold_slot=True, request_seconds=slow)
    print(f"  peak in-flight : {peak_slot}   (cap {cap})")
    print(f"  verdict        : "
          f"{'HELD' if peak_slot <= cap else 'EXCEEDED - permit not held across the request'}")

    rule(f"C. control - same load via wait_if_needed() alone")
    peak_bare, _ = await measure(120, hold_slot=False, request_seconds=slow)
    print(f"  peak in-flight : {peak_bare}   (cap {cap})")
    print("  wait_if_needed() paces but deliberately does not bound concurrency.")
    print("  This is the old behaviour, and the gap between B and C is the fix.")


asyncio.run(main())
