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
