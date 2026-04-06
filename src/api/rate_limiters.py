"""
Rate Limiters for Halo Infinite API

Provides per-account rate limiting to prevent 429 errors when
accessing Xbox Profile and Halo Stats APIs.
"""

import asyncio
import time
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
    
    def set_backoff(self, account_index: int, seconds: float) -> None:
        """
        Set backoff time for an account after receiving 429.
        
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
    
    Attributes:
        base_rate (int): Base requests per second per account
        num_accounts (int): Number of authenticated accounts
        _semaphore (asyncio.Semaphore): Controls concurrent requests
        _account_last_request (dict): Per-account last request timestamps
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
        self._account_last_request: Dict[int, float] = {}  # Per-account last request time
        self._account_backoff: Dict[int, float] = {}  # Per-account backoff until timestamp
        self.lock = asyncio.Lock()
        self._global_backoff_until = 0.0  # Global backoff (all accounts hit limit)
        self._current_account_index = 0  # For selecting accounts
    
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
        Apply rate limiting before making a request.
        
        Args:
            account_index: Optional specific account to use. If None, selects best available.
        
        Returns:
            The account index that should be used for this request.
        """
        # Create default semaphore if not initialized
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(5)
        
        # Acquire semaphore (limits concurrent requests)
        await self._semaphore.acquire()
        
        try:
            selected_index: Optional[int] = None

            async with self.lock:
                now = time.time()
                global_wait = max(0.0, self._global_backoff_until - now)

            if global_wait > 0:
                print(f"⏳ Global rate limit backoff: waiting {global_wait:.1f}s...")
                await asyncio.sleep(global_wait)

            async with self.lock:
                now = time.time()

                preferred_index: Optional[int] = None
                if account_index is not None:
                    preferred_index = int(account_index) % max(1, self.num_accounts)

                if preferred_index is not None and now >= self._account_backoff.get(preferred_index, 0):
                    selected_index = preferred_index
                else:
                    selected_index = None
                    longest_idle = float('-inf')
                    for idx in range(self.num_accounts):
                        if now < self._account_backoff.get(idx, 0):
                            continue
                        idle = now - self._account_last_request.get(idx, 0)
                        if idle > longest_idle:
                            longest_idle = idle
                            selected_index = idx

                account_wait = 0.0
                if selected_index is None:
                    # All accounts are currently in backoff.
                    selected_index = min(
                        range(self.num_accounts),
                        key=lambda idx: self._account_backoff.get(idx, 0),
                    )
                    account_wait = max(0.0, self._account_backoff.get(selected_index, 0) - now)

            if account_wait > 0:
                print(f"⏳ All accounts in backoff, waiting {account_wait:.1f}s...")
                await asyncio.sleep(account_wait)

            async with self.lock:
                now = time.time()

                # Re-check selected account availability after wait and prefer alternatives if needed.
                backoff_wait = 0.0
                selected_backoff_until = self._account_backoff.get(selected_index, 0)
                if now < selected_backoff_until:
                    fallback_index = None
                    longest_idle = float('-inf')
                    for idx in range(self.num_accounts):
                        if now < self._account_backoff.get(idx, 0):
                            continue
                        idle = now - self._account_last_request.get(idx, 0)
                        if idle > longest_idle:
                            longest_idle = idle
                            fallback_index = idx
                    if fallback_index is not None:
                        selected_index = fallback_index
                    else:
                        backoff_wait = max(0.0, selected_backoff_until - now)

                # Enforce per-account pacing; if possible, switch to an account already out of cooldown.
                min_interval = self.min_interval_per_account
                last_request = self._account_last_request.get(selected_index, 0.0)
                elapsed = now - last_request

                if elapsed < min_interval:
                    replacement_index = None
                    longest_idle = float('-inf')
                    for idx in range(self.num_accounts):
                        if idx == selected_index:
                            continue
                        if now < self._account_backoff.get(idx, 0):
                            continue
                        idle = now - self._account_last_request.get(idx, 0.0)
                        if idle >= min_interval and idle > longest_idle:
                            longest_idle = idle
                            replacement_index = idx
                    if replacement_index is not None:
                        selected_index = replacement_index
                        elapsed = now - self._account_last_request.get(selected_index, 0.0)

                spacing_wait = max(backoff_wait, max(0.0, min_interval - elapsed))

            if spacing_wait > 0:
                await asyncio.sleep(spacing_wait)

            async with self.lock:
                self._account_last_request[selected_index] = time.time()
                return selected_index
        finally:
            # Release semaphore after request setup
            self._semaphore.release()
    
    def set_backoff(self, seconds: float, account_index: Optional[int] = None) -> None:
        """
        Set a backoff period after receiving a 429 response.
        
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
