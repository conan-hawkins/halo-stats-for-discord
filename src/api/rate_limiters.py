"""
Rate Limiters for Halo Infinite API

Provides per-account rate limiting to prevent 429 errors when
accessing Xbox Profile and Halo Stats APIs.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional


class XboxProfileRateLimiter:
    """
    Simple concurrency limiter for Xbox Live Profile API calls.
    
    Uses exponential backoff on 429 errors rather than pre-emptive rate limiting.
    Tracks per-account backoff times when rate limited.
    
    Attributes:
        num_accounts (int): Number of Xbox accounts available
        _semaphore: Controls concurrent requests
        _account_backoff (dict): Per-account backoff timestamps
    """
    
    def __init__(self):
        """Initialize the rate limiter."""
        self.num_accounts = 1
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._current_account_index = 0
        self._account_backoff: Dict[int, float] = {}  # account_index -> backoff_until timestamp
        self.lock = asyncio.Lock()
    
    def set_num_accounts(self, num_accounts: int) -> None:
        """
        Update the number of accounts for concurrency scaling.
        
        Args:
            num_accounts: Number of authenticated Xbox accounts available
        """
        self.num_accounts = max(1, num_accounts)
        # Allow 2 concurrent requests per account
        max_concurrent = self.num_accounts * 2
        self._semaphore = asyncio.Semaphore(max_concurrent)
        print(f"📱 Xbox rate limiter: {self.num_accounts} accounts, {max_concurrent} max concurrent")
    
    def get_best_account(self) -> int:
        """
        Get the account index that is not in backoff.
        Uses round-robin among available accounts.
        
        Returns:
            Account index (0 to num_accounts-1)
        """
        now = time.time()
        
        # Try round-robin starting from current index
        for _ in range(self.num_accounts):
            idx = self._current_account_index % self.num_accounts
            self._current_account_index += 1
            
            # Check if this account is in backoff
            backoff_until = self._account_backoff.get(idx, 0)
            if now >= backoff_until:
                return idx
        
        # All accounts in backoff, return the one with shortest wait
        min_wait_idx = 0
        min_wait_time = float('inf')
        for idx in range(self.num_accounts):
            backoff_until = self._account_backoff.get(idx, 0)
            wait_time = backoff_until - now
            if wait_time < min_wait_time:
                min_wait_time = wait_time
                min_wait_idx = idx
        
        return min_wait_idx
    
    def set_backoff(self, *, account_index: int, seconds: float) -> None:
        """
        Set backoff time for an account after receiving 429.

        KEYWORD-ONLY, deliberately. This class and HaloStatsRateLimiter used to
        take these two arguments in opposite orders - (account_index, seconds)
        here, (seconds, account_index) there. Both are numbers, so transposing
        them raised nothing: you would set a 0-second backoff on account 30 and
        the only symptom would be a limiter that quietly stopped backing off.
        Naming them at every call site makes the order irrelevant.

        Args:
            account_index: Account to set backoff for
            seconds: Seconds to wait before retrying
        """
        self._account_backoff[account_index] = time.time() + seconds
        print(f"⏳ Account {account_index + 1} rate limited, backoff {seconds:.0f}s")
    
    async def acquire(self, account_index: Optional[int] = None) -> int:
        """
        Acquire a slot for making a request.
        
        Args:
            account_index: Optional specific account to use.
        
        Returns:
            The account index to use for this request.
        """
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(2)
        
        await self._semaphore.acquire()

        try:
            wait_time = 0.0
            selected_index = 0

            async with self.lock:
                if account_index is None:
                    selected_index = self.get_best_account()
                else:
                    selected_index = int(account_index) % max(1, self.num_accounts)

                now = time.time()
                backoff_until = self._account_backoff.get(selected_index, 0)
                wait_time = max(0.0, backoff_until - now)

            if wait_time > 0:
                print(f"⏳ Waiting {wait_time:.1f}s for account {selected_index + 1} backoff...")
                await asyncio.sleep(wait_time)

            return selected_index
        except BaseException:
            self.release()
            raise
    
    def release(self) -> None:
        """Release a request slot."""
        if self._semaphore:
            self._semaphore.release()


class HaloStatsRateLimiter:
    """
    Per-account rate limiter for Halo Stats API calls.
    
    Implements per-account rate limiting to prevent 429 errors. Each account
    has its own rate limit window, allowing parallel requests across different
    accounts while respecting individual account limits.
    
    Two separate limits, enforced by two separate mechanisms:

      RATE  - `wait_if_needed()` claims a per-account send slot. Claiming writes
              the reservation back under the lock BEFORE sleeping, so N callers
              get N distinct send times instead of all waiting for the same one.
      COUNT - `slot()` holds a semaphore permit for the whole request. This is
              the only correct way to bound in-flight requests; a permit taken
              and dropped inside `wait_if_needed()` bounds nothing, because the
              request has not happened yet when it is dropped.

    Callers issuing a request MUST use `slot()`. `wait_if_needed()` on its own
    only paces - see its docstring.

    Attributes:
        base_rate (int): Base requests per second per account
        num_accounts (int): Number of authenticated accounts
        _semaphore (asyncio.Semaphore): Bounds in-flight requests, via slot()
        _next_free (dict): Per-account monotonic time the next send may go out
        _account_last_request (dict): Per-account last claim time (observability)
        _account_backoff (dict): Per-account backoff timestamps (after 429)
    """
    def __init__(self, requests_per_second_per_account: int = 10):
        """
        Initialize the rate limiter.

        Args:
            requests_per_second_per_account: Base rate limit per account
        """
        self.base_rate = requests_per_second_per_account
        self.num_accounts = 1  # Will be updated when accounts are loaded
        self._semaphore: Optional[asyncio.Semaphore] = None  # Created when accounts are set
        # Monotonic timestamp of the next permitted send per account. This, not
        # _account_last_request, is what actually paces: it is advanced at CLAIM
        # time, so a caller that is still sleeping has already consumed its slot
        # and the next caller is handed a later one.
        self._next_free: Dict[int, float] = {}
        self._account_last_request: Dict[int, float] = {}  # Per-account last request time
        self._account_backoff: Dict[int, float] = {}  # Per-account backoff until timestamp
        self.lock = asyncio.Lock()
        self._global_backoff_until = 0.0  # Global backoff (all accounts hit limit)
        self._current_account_index = 0  # For selecting accounts
        # Tasks currently holding a semaphore permit, so a nested slot() in the
        # same task reuses it instead of deadlocking on a second one.
        self._permit_holders: set = set()

    def set_num_accounts(self, num_accounts: int) -> None:
        """
        Update the number of accounts for rate limit scaling.
        
        Args:
            num_accounts: Number of authenticated accounts available
        """
        self.num_accounts = max(1, num_accounts)
        # Allow 5 concurrent requests per account to avoid overwhelming API
        # Being conservative to prevent 429 errors and potential bans
        max_concurrent = self.num_accounts * 5
        self._semaphore = asyncio.Semaphore(max_concurrent)
        # Initialize per-account tracking
        for i in range(self.num_accounts):
            if i not in self._account_last_request:
                self._account_last_request[i] = 0.0
            if i not in self._account_backoff:
                self._account_backoff[i] = 0.0
            if i not in self._next_free:
                self._next_free[i] = 0.0
        print(f"📊 Rate limiter updated: {self.num_accounts} accounts = {max_concurrent} max concurrent requests")
    
    @property
    def min_interval_per_account(self) -> float:
        """
        Calculate minimum time between requests FOR THE SAME ACCOUNT.
        
        Returns:
            Minimum interval in seconds between requests for one account
        """
        # Each account can do base_rate requests per second
        # e.g., 8 req/sec = 0.125s between requests per account
        return 1.0 / self.base_rate
    
    def get_best_account(self) -> int:
        """
        Get the account index with the longest time since last request.
        This helps distribute load and avoid hitting rate limits.
        
        Returns:
            Account index (0 to num_accounts-1)
        """
        now = time.time()
        best_account = 0
        longest_idle = -1
        
        for i in range(self.num_accounts):
            # Skip accounts in backoff
            if now < self._account_backoff.get(i, 0):
                continue
            
            idle_time = now - self._account_last_request.get(i, 0)
            if idle_time > longest_idle:
                longest_idle = idle_time
                best_account = i
        
        return best_account
    
    async def wait_if_needed(self, account_index: Optional[int] = None) -> int:
        """
        Claim this account's next send slot, waiting until it comes due.

        PACING ONLY - this does not bound concurrency and never touches the
        semaphore. A caller that is about to issue a request must go through
        `slot()`, which holds a permit across the request itself.

        The claim is written back under the lock BEFORE the wait, which is the
        whole point. The previous implementation read the account's last-send
        time in one locked block and stamped it in a LATER one, with the sleep
        in between - so every caller that arrived before the first one stamped
        read the same stale timestamp, computed the same delay, and woke up
        together. Measured: 20 of 25 callers issued within the same 0.1ms, all
        on one account. Reserving up front means N callers get N distinct slots.

        The sleep stays OUTSIDE the lock: holding it across an await would
        serialise every other caller behind this one's cooldown.

        Args:
            account_index: Preferred account. Honoured when it is not in
                backoff; otherwise the soonest-available account is used.

        Returns:
            The account index to use for this request.
        """
        send_at, selected_index = await self._claim_send_slot(account_index)

        delay = send_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

        return selected_index

    async def _claim_send_slot(self, account_index: Optional[int] = None):
        """Reserve the next send slot atomically. Returns (send_at, account).

        `send_at` is a monotonic deadline. Backoffs are wall-clock (set_backoff
        uses time.time(), and callers/tests depend on that), so they are
        converted to a remaining duration here and folded into the monotonic
        timeline. Durations must never be measured on the wall clock: an NTP
        step would otherwise skew every pending wait.
        """
        async with self.lock:
            now_wall = time.time()
            now_mono = time.monotonic()

            global_remaining = max(0.0, self._global_backoff_until - now_wall)
            selected_index = self._select_account(account_index, now_wall, now_mono)
            account_remaining = max(
                0.0, self._account_backoff.get(selected_index, 0.0) - now_wall
            )

            earliest = now_mono + max(global_remaining, account_remaining)
            send_at = max(earliest, self._next_free.get(selected_index, 0.0))

            # Consume the slot now, while still holding the lock. Everything
            # above is a read; this is the line that makes the claim exclusive.
            self._next_free[selected_index] = send_at + self.min_interval_per_account
            self._account_last_request[selected_index] = now_wall

        return send_at, selected_index

    def _select_account(
        self, account_index: Optional[int], now_wall: float, now_mono: float
    ) -> int:
        """Pick the account whose next slot comes soonest. Caller holds the lock.

        Ties cannot persist: claiming an account pushes its _next_free forward,
        so the following caller sees a different soonest account and the load
        spreads by construction. The old "longest idle wins, strict >" rule had
        the opposite property - immediately after a burst every account was
        equally idle, the comparison never advanced past index 0, and the whole
        herd piled onto one account.
        """
        count = max(1, self.num_accounts)

        if account_index is not None:
            preferred = int(account_index) % count
            if now_wall >= self._account_backoff.get(preferred, 0.0):
                return preferred

        best_index = 0
        best_at = None
        for idx in range(count):
            backoff_remaining = max(
                0.0, self._account_backoff.get(idx, 0.0) - now_wall
            )
            available_at = max(
                now_mono + backoff_remaining, self._next_free.get(idx, 0.0)
            )
            if best_at is None or available_at < best_at:
                best_at = available_at
                best_index = idx
        return best_index

    @asynccontextmanager
    async def slot(self, account_index: Optional[int] = None):
        """Hold a concurrency permit for the whole request. Use this to fetch.

            async with halo_stats_rate_limiter.slot() as account:
                async with session.get(url, headers=headers_for(account)) as r:
                    ...

        The permit is taken before pacing and released only after the caller's
        block finishes, so `num_accounts * 5` finally means what it says. The
        previous code acquired and released inside wait_if_needed(), which
        returns before the request is made - measured peak was 55 concurrent
        against a cap of 25.

        Re-selecting an account inside the block (a 429 retry switching
        accounts) should call `wait_if_needed()` directly: it re-paces without
        taking a second permit, so a retry loop cannot leak one.

        RE-ENTRANT PER TASK. fetch_match_page() retries by calling itself from
        inside its own slot, so a nested slot() in the same task must not take
        a second permit: 25 concurrent pages each recursing 5 deep would need
        125 permits, wait forever on 25, and deadlock the crawl. A task issues
        one request at a time, so one permit per task is the correct unit. The
        nested call still re-paces, it just reuses the permit it already holds.
        """
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(max(1, self.num_accounts) * 5)

        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None

        # Fall back to non-reentrant when there is no task identity to key on.
        reentrant = task is not None and task in self._permit_holders

        semaphore = self._semaphore
        if not reentrant:
            await semaphore.acquire()
            if task is not None:
                self._permit_holders.add(task)
        try:
            yield await self.wait_if_needed(account_index)
        finally:
            if not reentrant:
                if task is not None:
                    self._permit_holders.discard(task)
                # Release the object we acquired, not self._semaphore: a
                # concurrent set_num_accounts() swaps in a new semaphore, and
                # releasing that one would hand out a permit nobody took.
                semaphore.release()


    def set_backoff(self, *, seconds: float, account_index: Optional[int] = None) -> None:
        """
        Set a backoff period after receiving a 429 response.

        KEYWORD-ONLY - see XboxProfileRateLimiter.set_backoff. That class takes
        the same two numbers in the opposite order, and a silent transposition
        would disable backoff rather than raise.

        Args:
            seconds: Number of seconds to wait before resuming requests
            account_index: Specific account to backoff, or None for global backoff
        """
        if account_index is not None:
            self._account_backoff[account_index] = time.time() + seconds
        else:
            self._global_backoff_until = time.time() + seconds


# =============================================================================
# GLOBAL RATE LIMITER INSTANCES
# =============================================================================
xbox_profile_rate_limiter = XboxProfileRateLimiter()
halo_stats_rate_limiter = HaloStatsRateLimiter(requests_per_second_per_account=3)  # 3 req/sec per account (conservative)
