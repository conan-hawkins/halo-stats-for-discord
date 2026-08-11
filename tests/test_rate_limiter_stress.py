import asyncio

import pytest

from src.api.rate_limiters import HaloStatsRateLimiter, XboxProfileRateLimiter


@pytest.mark.asyncio
async def test_halo_rate_limiter_handles_many_concurrent_waits():
    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(3)

    async def task():
        return await limiter.wait_if_needed()

    results = await asyncio.gather(*[task() for _ in range(60)])

    assert len(results) == 60
    assert all(r in {0, 1, 2} for r in results)


@pytest.mark.asyncio
async def test_xbox_rate_limiter_handles_many_acquire_release_cycles():
    limiter = XboxProfileRateLimiter()
    limiter.set_num_accounts(4)

    async def task():
        idx = await limiter.acquire()
        limiter.release()
        return idx

    results = await asyncio.gather(*[task() for _ in range(40)])

    assert len(results) == 40
    assert all(0 <= r < 4 for r in results)


# ---------------------------------------------------------------------------
# Regression tests for the 2026-08-11 defects. Both need the DEPLOYED rate of
# 3 req/s (not 1000) and a held slot; without either, neither defect is
# observable, which is why the tests above missed them for so long.
# ---------------------------------------------------------------------------

async def _drive(limiter, callers, request_seconds, hold_slot=True):
    """Run `callers` concurrently, recording issue times and peak concurrency."""
    loop_start = asyncio.get_running_loop().time()
    state = {"inflight": 0, "peak": 0}
    events = []

    async def one():
        if hold_slot:
            async with limiter.slot() as account:
                await _occupy(state, events, account, loop_start, request_seconds)
        else:
            account = await limiter.wait_if_needed()
            await _occupy(state, events, account, loop_start, request_seconds)

    await asyncio.gather(*(one() for _ in range(callers)))
    events.sort()
    return state["peak"], events


async def _occupy(state, events, account, loop_start, request_seconds):
    state["inflight"] += 1
    state["peak"] = max(state["peak"], state["inflight"])
    events.append((asyncio.get_running_loop().time() - loop_start, account))
    await asyncio.sleep(request_seconds)
    state["inflight"] -= 1


@pytest.mark.asyncio
async def test_slot_bounds_in_flight_requests():
    """The permit must span the request, not just the setup.

    Previously the semaphore was released inside wait_if_needed(), before the
    caller had issued anything, so it bounded nothing: 60 callers against a cap
    of 25 measured a peak of 55 in flight.
    """
    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(2)          # cap = 10
    peak, _ = await _drive(limiter, callers=40, request_seconds=0.05)
    assert peak <= 10, f"cap 10 exceeded: {peak} concurrent"


@pytest.mark.asyncio
async def test_concurrent_claims_do_not_form_a_herd():
    """Callers must get distinct send slots, not all wake from one shared sleep.

    The claim used to be read in one locked block and written in a later one
    with the sleep in between, so everyone read the same stale timestamp and
    fired together: 20 of 25 within the same 0.1 ms, all on one account.
    """
    # Drives wait_if_needed() directly rather than slot(): pacing is a property
    # of the claim itself, so this exercises the defect rather than the new API,
    # and fails on the old implementation with a real assertion.
    limiter = HaloStatsRateLimiter(requests_per_second_per_account=20)  # 0.05s
    limiter.set_num_accounts(4)
    _, events = await _drive(limiter, callers=20, request_seconds=0.0,
                             hold_slot=False)

    # At most one request per account may share any single instant.
    from collections import Counter
    per_instant = Counter(round(at, 3) for at, _ in events)
    assert max(per_instant.values()) <= 4, (
        f"herd: {max(per_instant.values())} requests in one millisecond"
    )

    # And the load must spread rather than pile onto whichever account was
    # nominally "most idle" at t=0.
    per_account = Counter(acct for _, acct in events)
    assert len(per_account) == 4, f"only used accounts {sorted(per_account)}"
    assert max(per_account.values()) <= 6, f"uneven spread: {dict(per_account)}"


@pytest.mark.asyncio
async def test_nested_slot_in_same_task_does_not_deadlock():
    """fetch_match_page retries by recursing from inside its own slot.

    Without per-task re-entrancy, 8 pages recursing 4 deep would need 32
    permits, block forever on 8, and hang the crawl.
    """
    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(2)          # cap = 10

    async def recurse(depth=0):
        async with limiter.slot():
            if depth < 4:
                return await recurse(depth + 1)
            return depth

    results = await asyncio.wait_for(
        asyncio.gather(*(recurse() for _ in range(8))), timeout=10
    )
    assert results == [4] * 8
    assert limiter._permit_holders == set(), "permit holders leaked"
    assert limiter._semaphore._value == 10, "permits not returned"


@pytest.mark.asyncio
async def test_slot_releases_permit_when_body_raises():
    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(1)          # cap = 5

    for _ in range(12):
        with pytest.raises(ValueError):
            async with limiter.slot():
                raise ValueError("boom")

    assert limiter._semaphore._value == 5, "a raising body leaked a permit"
    assert limiter._permit_holders == set()


@pytest.mark.asyncio
async def test_buckets_pace_independently():
    """Endpoints with different limits must not throttle each other.

    Halo limits matches-list and match-stats separately: match-stats served
    60/60 clean at 50 req/s, while matches-list 429'd half its requests at 30
    and peaked around 15. A single global rate has to satisfy the tighter of
    the two, which throttles the endpoint that is ~87% of a crawl to protect
    the one that is ~13%.
    """
    from src.api.rate_limiters import BUCKET_MATCH_LIST, BUCKET_MATCH_STATS

    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1)
    limiter.set_num_accounts(1)                       # one account, so pacing is visible
    limiter.set_bucket_rate(BUCKET_MATCH_LIST, 1)     # 1.0s apart
    limiter.set_bucket_rate(BUCKET_MATCH_STATS, 20)   # 0.05s apart

    assert limiter.min_interval_for(BUCKET_MATCH_LIST) == 1.0
    assert limiter.min_interval_for(BUCKET_MATCH_STATS) == 0.05

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    # Five fast-bucket claims must NOT be held up by the slow bucket's pace.
    await asyncio.gather(*(limiter.wait_if_needed(bucket=BUCKET_MATCH_STATS)
                           for _ in range(5)))
    fast_elapsed = loop.time() - t0
    assert fast_elapsed < 0.5, (
        f"stats bucket paced at the list bucket's rate: {fast_elapsed:.2f}s"
    )

    # And the slow bucket still is slow - the split did not just remove pacing.
    t1 = loop.time()
    await asyncio.gather(*(limiter.wait_if_needed(bucket=BUCKET_MATCH_LIST)
                           for _ in range(3)))
    slow_elapsed = loop.time() - t1
    assert slow_elapsed >= 1.8, f"list bucket lost its pacing: {slow_elapsed:.2f}s"


@pytest.mark.asyncio
async def test_bucket_backoff_is_shared_per_account():
    """A 429 benches the ACCOUNT, not just the bucket.

    The token is what Halo rate-limits, so a refusal on one endpoint is
    evidence about the account as a whole. Only pacing is split.
    """
    from src.api.rate_limiters import BUCKET_MATCH_LIST, BUCKET_MATCH_STATS

    limiter = HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(2)
    limiter.set_backoff(seconds=30, account_index=0)

    # Even in the other bucket, account 0 is avoided while backed off.
    picks = {await limiter.wait_if_needed(bucket=BUCKET_MATCH_STATS) for _ in range(6)}
    assert picks == {1}, f"backed-off account used in another bucket: {picks}"
    picks = {await limiter.wait_if_needed(bucket=BUCKET_MATCH_LIST) for _ in range(6)}
    assert picks == {1}
