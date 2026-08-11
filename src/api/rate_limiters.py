"""
Rate Limiters for Halo Infinite API

Provides per-account rate limiting to prevent 429 errors when
accessing Xbox Profile and Halo Stats APIs.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

# Endpoint families paced independently. Halo limits them separately and very
# differently - see HaloStatsRateLimiter.set_bucket_rate for the measurements.
BUCKET_MATCH_LIST = "match_list"    # /matches?start=..  - the tighter one
BUCKET_MATCH_STATS = "match_stats"  # /matches/{id}/stats - the roomier one
BUCKET_PROFILE = "profile"          # profile.svc /users?xuids= - 100 ids per call
BUCKET_SKILL = "skill"              # skill.svc csrs / match skill - batched by player
DEFAULT_BUCKET = "default"

# AIMD constants for the adaptive per-bucket rate. Conservative on the way down
# and slow on the way up, because being wrong upward costs 429s and retries
# while being wrong downward only costs a little throughput.
AIMD_DECREASE_FACTOR = 0.7
AIMD_INCREASE_STEP = 0.25      # req/s/account
AIMD_INCREASE_AFTER = 40       # consecutive clean responses
AIMD_DECREASE_COOLDOWN = 2.0   # seconds; one overload = one decrease


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
        # Monotonic timestamp of the next permitted send, keyed by
        # (bucket, account). This, not _account_last_request, is what actually
        # paces: it is advanced at CLAIM time, so a caller that is still
        # sleeping has already consumed its slot and the next caller is handed
        # a later one.
        #
        # Keyed by BUCKET because Halo's endpoints do not share a limit. The
        # match-stats endpoint serves 50 req/s clean; the matches-list endpoint
        # 429s half its requests at 30 and peaks around 15. One global rate has
        # to satisfy the tighter of the two, which throttles the endpoint that
        # is 87% of a crawl to protect the one that is 13%.
        self._next_free: Dict[tuple, float] = {}
        # Per-bucket requests/sec/account; falls back to base_rate. Adaptive -
        # see set_bucket_rate and note_result.
        self._bucket_rates: Dict[str, float] = {}
        self._bucket_bounds: Dict[str, tuple] = {}
        self._bucket_successes: Dict[str, int] = {}
        self._bucket_last_decrease: Dict[str, float] = {}
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

    def set_bucket_rate(self, bucket: str, requests_per_second_per_account: float,
                        floor: Optional[float] = None,
                        ceiling: Optional[float] = None) -> None:
        """Give one endpoint family its own pace, and the room to adapt it.

        Buckets exist because Halo's endpoints are limited independently, and
        measured very differently: match-stats served 60/60 requests clean at
        15, 30 AND 50 req/s, while matches-list 429'd 50% of requests at 30.

        `requests_per_second_per_account` is a STARTING point, not a setting.
        A fixed number cannot be right here: the value that looked optimal over
        a 40-page burst (3/account) collapsed over a real 2,000-page crawl,
        where 162 of ~340 pages exhausted their retries and the crawl never
        found the end of the history. Halo's limit over a sustained crawl is
        plainly lower than over a burst, and probably not constant. So the rate
        moves within [floor, ceiling] via note_result() instead.
        """
        rate = float(requests_per_second_per_account)
        lo = float(floor) if floor is not None else rate / 4.0
        hi = float(ceiling) if ceiling is not None else rate
        self._bucket_rates[bucket] = max(lo, min(rate, hi))
        self._bucket_bounds[bucket] = (lo, hi)
        self._bucket_successes[bucket] = 0

    def note_result(self, bucket: Optional[str], rate_limited: bool) -> None:
        """Feed one response back into the bucket's pace (AIMD).

        Additive increase, multiplicative decrease - the same shape TCP uses,
        and for the same reason: the safe sustained rate is unknown, changes,
        and is only discoverable by probing upward until told to stop.

        The decrease is rate-limited itself. A single overload produces a burst
        of 429s across many in-flight requests; treating each as independent
        evidence would multiply the rate down by 0.7 twenty times over and
        stall the crawl outright.
        """
        key = bucket or DEFAULT_BUCKET
        bounds = self._bucket_bounds.get(key)
        if bounds is None:
            return                      # not an adaptive bucket
        lo, hi = bounds
        current = self._bucket_rates.get(key, hi)

        if rate_limited:
            now = time.monotonic()
            if now - self._bucket_last_decrease.get(key, 0.0) < AIMD_DECREASE_COOLDOWN:
                return
            self._bucket_last_decrease[key] = now
            self._bucket_successes[key] = 0
            new = max(lo, current * AIMD_DECREASE_FACTOR)
            if new != current:
                self._bucket_rates[key] = new
                print(f"📉 {key}: backing off to {new:.2f} req/s/account")
            return

        self._bucket_successes[key] = self._bucket_successes.get(key, 0) + 1
        if self._bucket_successes[key] >= AIMD_INCREASE_AFTER:
            self._bucket_successes[key] = 0
            new = min(hi, current + AIMD_INCREASE_STEP)
            if new != current:
                self._bucket_rates[key] = new
                print(f"📈 {key}: easing up to {new:.2f} req/s/account")

    def bucket_rate(self, bucket: Optional[str]) -> float:
        """Current adaptive rate, for logging and tests."""
        return self._bucket_rates.get(bucket or DEFAULT_BUCKET, self.base_rate)

    def min_interval_for(self, bucket: Optional[str]) -> float:
        """Minimum gap between requests on one account within one bucket."""
        rate = self._bucket_rates.get(bucket or DEFAULT_BUCKET, self.base_rate)
        return 1.0 / rate if rate > 0 else 0.0
    
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
    
    async def wait_if_needed(self, account_index: Optional[int] = None,
                             bucket: Optional[str] = None) -> int:
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
        send_at, selected_index = await self._claim_send_slot(account_index, bucket)

        delay = send_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

        return selected_index

    async def _claim_send_slot(self, account_index: Optional[int] = None,
                               bucket: Optional[str] = None):
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

            key_bucket = bucket or DEFAULT_BUCKET
            global_remaining = max(0.0, self._global_backoff_until - now_wall)
            selected_index = self._select_account(
                account_index, now_wall, now_mono, key_bucket
            )
            # Backoff stays per-ACCOUNT, not per-bucket: a 429 says that token
            # is hot, and Halo's own limits are keyed to the account. Only the
            # pacing is split.
            account_remaining = max(
                0.0, self._account_backoff.get(selected_index, 0.0) - now_wall
            )

            key = (key_bucket, selected_index)
            earliest = now_mono + max(global_remaining, account_remaining)
            send_at = max(earliest, self._next_free.get(key, 0.0))

            # Consume the slot now, while still holding the lock. Everything
            # above is a read; this is the line that makes the claim exclusive.
            self._next_free[key] = send_at + self.min_interval_for(key_bucket)
            self._account_last_request[selected_index] = now_wall

        return send_at, selected_index

    def _select_account(
        self, account_index: Optional[int], now_wall: float, now_mono: float,
        bucket: str = DEFAULT_BUCKET
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
                now_mono + backoff_remaining,
                self._next_free.get((bucket, idx), 0.0),
            )
            if best_at is None or available_at < best_at:
                best_at = available_at
                best_index = idx
        return best_index

    @asynccontextmanager
    async def slot(self, account_index: Optional[int] = None,
                   bucket: Optional[str] = None):
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
            yield await self.wait_if_needed(account_index, bucket)
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

# Base rate for anything without its own bucket. Deliberately the conservative
# of the two measured endpoints.
halo_stats_rate_limiter = HaloStatsRateLimiter(requests_per_second_per_account=3)

# Per-endpoint pacing, from measurement rather than guesswork. Halo limits these
# independently, so one global rate had to satisfy the tighter of the two and
# throttled the endpoint that is 87% of a crawl to protect the one that is 13%.
#
# matches-list, 40 pages, effective throughput by configured rate:
#     30/s -> 4.33 pages/s, 50% of requests 429
#     15/s -> 6.92 pages/s, 16% 429      <- best; retry churn dominates above it
#     10/s -> 5.70 pages/s, 29% 429
#      5/s -> 4.86 pages/s,  0% 429      <- no 429s, but we are the bottleneck
#
# match-stats, 60 requests per trial: 60/60 HTTP 200 at 15, 30 AND 50 req/s.
# Set to 30, not 50: these are real Xbox accounts and the cost of being wrong
# is losing them, so this stays inside measured-safe rather than at its edge.
# The listing rate ADAPTS. 3/account looked optimal over a 40-page burst and
# collapsed over a real 2,000-page crawl (162 of ~340 pages exhausted their
# retries), so it starts there and moves within [0.75, 3] on live evidence.
halo_stats_rate_limiter.set_bucket_rate(BUCKET_MATCH_LIST, 3, floor=0.75, ceiling=3)

# Match-stats measured clean at 15, 30 AND 50 req/s and sustained 30/s across a
# 1,154-match crawl with zero 429s, so it starts at its ceiling and only moves
# if the API starts objecting.
halo_stats_rate_limiter.set_bucket_rate(BUCKET_MATCH_STATS, 6, floor=1.5, ceiling=6)

# profile.svc /users?xuids= carries up to 100 identities per request, so one
# call here does the work of 100 on the Xbox profile endpoint. Deliberately
# slow: measured latency was 2.4s for a full 100-id batch, and at 1/s/account a
# five-account pool still resolves ~500 identities a second. There is no
# measured ceiling for this endpoint - it was never driven hard enough to 429 -
# so this starts low and adapts upward on clean responses rather than guessing
# high against real Xbox accounts.
halo_stats_rate_limiter.set_bucket_rate(BUCKET_PROFILE, 1, floor=0.25, ceiling=2)

# skill.svc batches by player within a single match or playlist, so the request
# count is driven by matches/playlists rather than by roster size. Starts at the
# same conservative rate as profile for the same reason - no measured 429
# ceiling exists for it - and adapts from there.
halo_stats_rate_limiter.set_bucket_rate(BUCKET_SKILL, 1, floor=0.25, ceiling=3)
