r"""One-line, greppable phase timings for a player refresh.

Pure instrumentation: every probe is a timestamp plus a print, and nothing here
feeds a decision. It exists to answer one question with measurements instead of
inference - where the wall time of a slow player lookup actually goes - because
the cost model this code was tuned against no longer describes the machine. The
figures the design leans on (~17ms per match on a cold page cache, 16.4s for a
696-match player) were measured on a spinning disk; the DB is on an SSD now.

Four probes, one line each:

    phase=refresh        queue_wait | fetch     - internal_api._do_refresh
    phase=cache_load     seconds                - the index-only cached-match read
    phase=match_details  seconds | rate         - the rolling match-stats queue
    phase=db_write       seconds                - the per-player batch commit

`queue_wait` is the one the hypothesis turns on. REFRESH_MAX_CONCURRENCY is 2
and the slot is held for the WHOLE fetch, so a cheap incremental refresh can
queue behind a full crawl running for minutes. Near-zero kills that theory;
tens of seconds confirms head-of-line blocking is the cost, not throughput.

Read a run straight out of the container:

    docker compose logs bot | grep '\[TIMING\]'
    docker compose logs bot | grep 'phase=refresh' | grep -o 'queue_wait=[0-9.]*'

Set HALO_TIMING=0 in the bot's environment to silence it without a redeploy.
"""

import os

# Default ON: this was added to collect a sample, and a flag defaulting off
# collects nothing. Flip it in compose and restart once the numbers are in.
TIMING_ENABLED = os.getenv("HALO_TIMING", "1").strip().lower() not in ("0", "false", "no", "")


def log_timing(phase: str, **fields) -> None:
    """Emit one `[TIMING] phase=... k=v` line.

    Never raises. Instrumentation added to diagnose a latency problem must not
    be able to become a correctness problem: any formatting error here is
    swallowed rather than propagated into a refresh that would otherwise have
    succeeded. Floats print to 2dp, everything else verbatim.
    """
    if not TIMING_ENABLED:
        return
    try:
        parts = " ".join(
            f"{k}={v:.2f}" if isinstance(v, float) else f"{k}={v}"
            for k, v in fields.items()
        )
        print(f"[TIMING] phase={phase} {parts}", flush=True)
    except Exception:
        pass
