import pytest

from src.api import rate_limiters


def test_halo_rate_limiter_min_interval():
    limiter = rate_limiters.HaloStatsRateLimiter(requests_per_second_per_account=4)
    assert limiter.min_interval_per_account == 0.25


def test_halo_rate_limiter_set_num_accounts_creates_semaphore():
    limiter = rate_limiters.HaloStatsRateLimiter()
    limiter.set_num_accounts(3)

    assert limiter.num_accounts == 3
    assert limiter._semaphore is not None


def test_halo_rate_limiter_get_best_account_skips_backoff(monkeypatch):
    limiter = rate_limiters.HaloStatsRateLimiter()
    limiter.set_num_accounts(2)

    monkeypatch.setattr(rate_limiters.time, "time", lambda: 100.0)
    limiter._account_backoff[0] = 120.0
    limiter._account_backoff[1] = 0.0

    assert limiter.get_best_account() == 1


@pytest.mark.asyncio
async def test_halo_rate_limiter_wait_if_needed_returns_account():
    limiter = rate_limiters.HaloStatsRateLimiter(requests_per_second_per_account=1000)
    limiter.set_num_accounts(2)

    selected = await limiter.wait_if_needed()

    assert selected in {0, 1}
    assert limiter._account_last_request[selected] > 0


def test_xbox_rate_limiter_set_num_accounts():
    limiter = rate_limiters.XboxProfileRateLimiter()
    limiter.set_num_accounts(4)

    assert limiter.num_accounts == 4
    assert limiter._semaphore is not None


def test_xbox_rate_limiter_round_robin_and_backoff(monkeypatch):
    limiter = rate_limiters.XboxProfileRateLimiter()
    limiter.set_num_accounts(2)

    monkeypatch.setattr(rate_limiters.time, "time", lambda: 100.0)

    assert limiter.get_best_account() == 0
    limiter.set_backoff(0, 20)
    assert limiter.get_best_account() == 1


@pytest.mark.asyncio
async def test_xbox_rate_limiter_acquire_respects_backoff(monkeypatch):
    limiter = rate_limiters.XboxProfileRateLimiter()
    limiter.set_num_accounts(1)

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(rate_limiters.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(rate_limiters.time, "time", lambda: 100.0)
    limiter._account_backoff[0] = 103.5

    account = await limiter.acquire(0)
    limiter.release()

    assert account == 0
    assert sleep_calls == [3.5]
