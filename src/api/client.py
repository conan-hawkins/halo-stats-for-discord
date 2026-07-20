"""
Halo Infinite API Client for Discord Bot
=========================================

A comprehensive async client for interacting with the Halo Waypoint API
to retrieve player statistics, match history, and related data.

Features:
    - Multi-account authentication with round-robin load balancing
    - Rate limiting with semaphore-based concurrency control
    - SQLite v2 caching for efficient data retrieval
    - Automatic token refresh with exponential backoff
    - Thread-safe file operations with portalocker

Author: Conan Hawkins
Created: 14/10/2025
"""

# =============================================================================
# IMPORTS
# =============================================================================

import aiohttp
import asyncio
import json
import os
import re
import time
import traceback
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv

# Load environment variables before module initialization
load_dotenv()

from src.auth.tokens import run_auth_flow
from src.database.cache import get_cache
from src.config import (
    TOKEN_CACHE_FILE,
    get_token_cache_path,
    XUID_CACHE_FILE,
    REQUESTS_PER_SECOND_PER_ACCOUNT,
    CORE_RANKED_PLAYLIST_IDS,
    STATS_HISTORY_FRESHNESS_TTL_SECONDS,
)

# Import from refactored modules
from src.api.rate_limiters import (
    XboxProfileRateLimiter,
    HaloStatsRateLimiter,
    xbox_profile_rate_limiter,
    halo_stats_rate_limiter,
)
from src.api.utils import (
    safe_read_json,
    safe_write_json,
    is_token_valid,
    get_token_swap_lock,
    recover_token_swap_marker,
    write_token_swap_marker,
    clear_token_swap_marker,
)
from src.api.history_sync import (
    build_boundary_probe_plan,
    decide_full_history_sync,
)
from src.api.xuid_cache import (
    _normalize_gamertag_alias_key,
    _normalize_gamertag_for_lookup,
    load_xuid_cache,
    save_xuid_cache,
)


# Sentinel returned by fetch_match_page when a page-listing request fails after
# exhausting its retries. Kept distinct from [] (a genuine empty/end-of-history
# page) and None (401, needs token refresh) so a transient failure can never be
# mistaken for "reached the end of the player's history" and silently truncate a
# crawl. It is truthy and has no len(), so call sites MUST test `is
# _PAGE_FETCH_FAILED` before any truthiness / len() check.
_PAGE_FETCH_FAILED = object()


# =============================================================================
# HALO API CLIENT
# =============================================================================


class HaloAPIClient:
    """
    Async client for interacting with the Halo Infinite API.
    
    Provides methods for:
        - Player statistics retrieval
        - Match history analysis
        - Gamertag/XUID resolution
        - Multi-account authentication management
    
    Attributes:
        stats_url (str): Base URL for Halo Stats API
        profile_url (str): Base URL for Profile API
        spartan_token (str): Current Spartan authentication token
        spartan_accounts (list): List of authenticated accounts for round-robin
        stats_cache: SQLite v2 cache instance
    """
    
    # API Endpoints (class constants)
    SETTINGS_URL = "https://settings.svc.halowaypoint.com"
    STATS_URL = "https://halostats.svc.halowaypoint.com"
    PROFILE_URL = "https://profile.svc.halowaypoint.com/users/by-gamertag"
    DISCOVERY_UGC_URL = "https://discovery-infiniteugc.svc.halowaypoint.com"
    USER_AGENT = "HaloWaypoint/2021.01.10.01"
    # Zero-network fast path for known ranked playlist asset IDs. Asset IDs
    # rotate across seasons, so this static set is only a best-effort
    # accelerator - playlist_metadata name-matching (_lookup_or_resolve_playlist_ranked)
    # is the durable mechanism, and e.g. Ranked Slayer
    # (dcb2e24e-05fb-4390-8076-32a0cdb4326e) is already caught that way. IDs
    # cross-checked against Den Delimarsky's confirmed playlist list
    # (den.dev/blog/halo-infinite-playlist-weights).
    RANKED_PLAYLIST_IDS = {
        "6e4e9372-5d49-4f87-b0a7-4489b5e96a0b",  # Ranked Arena (older-season asset id)
        "edfef3ac-9cbe-4fa2-b949-8f29deafd483",  # Ranked Arena
    }
    # Word-boundary match so a hypothetical "Unranked ..." playlist name can
    # never classify as ranked; still matches "RANKED 1V1 SHOWDOWN" and
    # "Squad Battle: Ranked" style names. Run against lowercased text.
    RANKED_NAME_RE = re.compile(r"\branked\b")
    # Substrings that mark a playlist's PublicName as PvE (co-op vs AI), which
    # should be excluded from PvP "overall"/"social" aggregates the same way
    # private/forge customs are. All known Firefight playlists (Gruntpocalypse,
    # King of the Hill, Battle for Reach, Classic/Heroic/Legendary, ...) carry
    # "firefight" in their name. Deliberately NOT "bot bootcamp" - that is
    # PvP-vs-AI matchmaking, a separate call left counted as social for now.
    PVE_PLAYLIST_NAME_HINTS = {"firefight"}
    EXPLICIT_CUSTOM_FLAG_KEYS = {
        "iscustom",
        "iscustommatch",
        "customgame",
        "custommatch",
    }
    CUSTOM_MATCH_TEXT_HINTS = {
        "custom",
        "customgame",
        "custom game",
        "forge",
        "ugc",
        "private match",
        "private_match",
    }
    CUSTOM_GAME_VARIANT_CATEGORIES = {6}
    CUSTOM_LIFECYCLE_MODES = {1}
    SOCIAL_MATCH_TEXT_HINTS = {
        "social",
        "quick play",
        "quickplay",
        "matchmade",
        "matchmaking",
    }
    
    def __init__(self):
        """Initialize the Halo API client."""
        # API endpoint URLs
        self.settings_url = self.SETTINGS_URL
        self.stats_url = self.STATS_URL
        self.profile_url = self.PROFILE_URL
        self.clearance_token: Optional[str] = None
        self.spartan_token: Optional[str] = None
        self.user_agent = self.USER_AGENT
        
        # Multi-account support for Spartan tokens (round-robin load balancing)
        self.spartan_accounts: List[Dict] = []  # List of {id, token, name}
        self.current_account_index = 0  # Round-robin index
        
        # Multi-account support for Xbox Live tokens (for friends list API)
        self.xbox_accounts: List[Dict] = []  # List of {id, token, uhs, cache_file}
        self.current_xbox_index = 0  # Round-robin index for Xbox accounts
        
        # OAuth credentials from environment variables
        self.client_id = os.getenv('client_id')
        self.client_secret = os.getenv('client_secret')
        
        # Use SQLite v2 cache instead of JSON files
        self.stats_cache = get_cache()  # Uses halo_stats_v2.db
        
        # Track token refresh attempts (prevent infinite loops)
        self._refresh_in_progress = False
        self._last_refresh_time = 0.0

        # Dedicated single-worker executor for blocking SQLite writes, so they
        # never stall the asyncio event loop / gateway heartbeat (critical on
        # HDD-backed storage where a commit can take tens of ms). One worker =>
        # one writer connection => no write-lock contention, and writes are
        # naturally serialized, which spinning disks strongly prefer.
        self._db_write_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="halo-db-writer"
        )

        # In-flight background full-history collections, keyed by xuid, so a
        # second request for the same brand-new player doesn't start a
        # duplicate collect. Different players collect concurrently.
        self._full_collect_tasks: Dict[str, asyncio.Task] = {}

        # In-flight stat requests keyed by (xuid, stat_type, matches_to_process,
        # force_full_fetch), so spammed identical commands share one fetch
        # instead of stacking API calls into 429s.
        self._stats_inflight: Dict[tuple, asyncio.Task] = {}

        # time.monotonic() of the last completed API history check, per xuid.
        # Keyed by xuid alone: every stat_type runs the identical incremental
        # history check, only the post-fetch filtering differs.
        self._history_checked_at: Dict[str, float] = {}

    async def _refresh_account_via_swap(self, account_num: int, account_cache: Dict) -> bool:
        """Refresh a secondary account by swapping its cache into the primary slot."""
        cache_file = get_token_cache_path(account_num)
        async with get_token_swap_lock():
            account1_backup = safe_read_json(TOKEN_CACHE_FILE, default={})
            if not account1_backup:
                return False

            refresh_succeeded = False
            restore_succeeded = False
            try:
                write_token_swap_marker(account1_backup, cache_file)
                safe_write_json(TOKEN_CACHE_FILE, account_cache)

                # interactive=False: the bot must never block on a browser
                # login the headless server cannot complete.
                await run_auth_flow(self.client_id, self.client_secret, use_halo=True, interactive=False)

                refreshed_cache = safe_read_json(TOKEN_CACHE_FILE, default={})
                if refreshed_cache:
                    safe_write_json(cache_file, refreshed_cache)

                new_spartan = refreshed_cache.get("spartan") if refreshed_cache else None
                new_xsts = refreshed_cache.get("xsts") if refreshed_cache else None
                new_xbox = refreshed_cache.get("xsts_xbox") if refreshed_cache else None
                refresh_succeeded = bool(
                    new_spartan and is_token_valid(new_spartan) and
                    new_xsts and is_token_valid(new_xsts) and
                    new_xbox and is_token_valid(new_xbox)
                )
            finally:
                restore_attempts = 3
                for attempt in range(restore_attempts):
                    try:
                        safe_write_json(TOKEN_CACHE_FILE, account1_backup)
                        restore_succeeded = True
                        break
                    except Exception as restore_error:
                        print(f"Restore attempt {attempt + 1}/{restore_attempts} failed for Account {account_num}: {restore_error}")
                        if attempt < restore_attempts - 1:
                            await asyncio.sleep(0.25 * (attempt + 1))

                if restore_succeeded:
                    clear_token_swap_marker()

            if not restore_succeeded:
                return False

            return refresh_succeeded

    @staticmethod
    def _oauth_refresh_alive(account_cache: Optional[Dict]) -> bool:
        """True if the account's OAuth token is still valid.

        Used to decide whether a refresh miss is recoverable. A live OAuth
        entry after a refresh attempt means the long-lived refresh token still
        works and only the non-critical derived/clearance step hiccuped - so a
        manual browser re-auth is NOT needed. Only a dead OAuth token requires
        `setup_account`.
        """
        if not account_cache:
            return False
        return is_token_valid(account_cache.get("oauth"))

    async def reload_spartan_accounts_from_cache(self) -> int:
        """Rebuild the in-memory Spartan pool from the on-disk token caches.

        No network calls. Re-reads the five token cache files and repopulates
        self.spartan_accounts with every account whose cached spartan/xsts/xbox
        tokens are still valid. The weekly proactive refresh rewrites those cache
        files but never touches this in-memory pool, so without this the pool can
        stay stuck at a single account and starve match-fetch concurrency.
        """
        async with get_token_swap_lock():
            # Account 1 lives in the primary cache slot.
            account1_cache = safe_read_json(TOKEN_CACHE_FILE, default={})
            account1_spartan = account1_cache.get("spartan") if account1_cache else None

            new_accounts: List[Dict] = []
            if account1_spartan and account1_spartan.get("token"):
                new_accounts.append({
                    'id': 'account1',
                    'token': account1_spartan.get("token"),
                    'name': 'Account 1'
                })

            # Additional accounts (2-5): include only when fully valid, matching
            # the membership gate used elsewhere in ensure_valid_tokens.
            for i in range(2, 6):
                cache_file = get_token_cache_path(i)
                cache_data = safe_read_json(cache_file, default={})
                if not cache_data:
                    continue

                spartan_info = cache_data.get("spartan")
                xsts_info = cache_data.get("xsts")
                xsts_xbox_info = cache_data.get("xsts_xbox")

                if (spartan_info and is_token_valid(spartan_info) and
                        xsts_info and is_token_valid(xsts_info) and
                        xsts_xbox_info and is_token_valid(xsts_xbox_info)):
                    new_accounts.append({
                        'id': f'account{i}',
                        'token': spartan_info.get("token"),
                        'name': f'Account {i}',
                        'cache_file': cache_file
                    })

            if not new_accounts:
                # Never wipe the pool to empty - keep whatever is already loaded.
                print("⚠️ reload_spartan_accounts_from_cache found no valid accounts; keeping existing pool")
                return 0

            # Single atomic reassignment so concurrent match-fetch readers never
            # observe a half-built list.
            self.spartan_accounts = new_accounts
            if account1_spartan and account1_spartan.get("token"):
                self.spartan_token = account1_spartan.get("token")

            halo_stats_rate_limiter.set_num_accounts(len(new_accounts))
            print(f"Reloaded {len(new_accounts)} Spartan account(s) from cache for match fetching")
            return len(new_accounts)

    # =========================================================================
    # AUTHENTICATION METHODS
    # =========================================================================
        
    async def ensure_valid_tokens(self) -> bool:
        """
        Validate and refresh authentication tokens for all accounts.
        
        Checks tokens for accounts 1-5 and refreshes any that are expired.
        Updates the rate limiter with the number of valid accounts.
        
        Returns:
            True if at least account 1 has valid tokens, False otherwise
        """
        recover_token_swap_marker()

        # Prevent concurrent refresh attempts
        if self._refresh_in_progress:
            print("Token refresh already in progress, waiting...")
            return False
        
        # Check Account 1 tokens
        cache = safe_read_json(TOKEN_CACHE_FILE, default={})
        if not cache:
            print("No token cache found for Account 1")
            print("Run: python -m src.auth.tokens")
            return False
        
        # Check ALL required tokens for Account 1
        spartan_info = cache.get("spartan")
        xsts_info = cache.get("xsts")  # Main XSTS token for Halo API
        xsts_xbox_info = cache.get("xsts_xbox")  # XSTS token for Xbox Live
        account1_spartan_info = spartan_info
        
        spartan_valid = spartan_info and is_token_valid(spartan_info)
        xsts_valid = xsts_info and is_token_valid(xsts_info)
        xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
        
        account1_valid = spartan_valid and xsts_valid and xbox_valid
        
        # Check tokens for additional accounts (2-5)
        additional_accounts = []
        for i in range(2, 6):  # Accounts 2, 3, 4, 5
            cache_file = get_token_cache_path(i)
            cache_data = safe_read_json(cache_file, default={})
            
            if cache_data:
                spartan_info = cache_data.get("spartan")
                xsts_info = cache_data.get("xsts")
                xsts_xbox_info = cache_data.get("xsts_xbox")
                
                spartan_valid = spartan_info and is_token_valid(spartan_info)
                xsts_valid = xsts_info and is_token_valid(xsts_info)
                xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
                
                if spartan_valid and xsts_valid and xbox_valid:
                    additional_accounts.append({
                        'id': f'account{i}',
                        'token': spartan_info.get("token"),
                        'name': f'Account {i}',
                        'cache_file': cache_file
                    })
        
        # Keep backwards compatibility
        cache2 = safe_read_json(get_token_cache_path(2), default={})
        account2_valid = any(acc['id'] == 'account2' for acc in additional_accounts)
        
        # If Account 1 valid, load tokens and refresh any expired additional accounts
        if account1_valid:
            # First, refresh any expired additional accounts (2-5)
            for i in range(2, 6):
                # Skip if already valid
                if any(acc['id'] == f'account{i}' for acc in additional_accounts):
                    continue
                    
                cache_file = get_token_cache_path(i)
                account_cache = safe_read_json(cache_file, default={})
                
                if not account_cache:
                    continue
                
                # This account exists but is invalid - try to refresh
                oauth_info = account_cache.get("oauth")
                refresh_success = False
                
                if oauth_info and oauth_info.get("refresh_token"):
                    print(f"Refreshing expired Account {i} tokens...")
                    try:
                        refresh_success = await self._refresh_account_via_swap(i, account_cache)
                        if refresh_success:
                            refreshed_cache = safe_read_json(cache_file, default={})
                            new_spartan = refreshed_cache.get("spartan") if refreshed_cache else None
                            additional_accounts.append({
                                'id': f'account{i}',
                                'token': new_spartan.get("token") if new_spartan else None,
                                'name': f'Account {i}',
                                'cache_file': cache_file
                            })
                            print(f"Account {i} tokens refreshed successfully")
                    except Exception as e:
                        print(f"Error refreshing Account {i}: {e}")

                if not refresh_success:
                    post_cache = safe_read_json(cache_file, default={})
                    if self._oauth_refresh_alive(post_cache):
                        print(f"Account {i}: refreshed, clearance temporarily unavailable (non-critical)")
                    else:
                        print(f"⚠️ Account {i} needs manual re-auth. Run: python -m src.auth.setup_account {i}")
            
            self.spartan_token = account1_spartan_info.get("token")
            
            # Load Spartan accounts
            self.spartan_accounts = []
            self.spartan_accounts.append({
                'id': 'account1',
                'token': account1_spartan_info.get("token"),
                'name': 'Account 1'
            })
            
            # Add all additional valid accounts (2-5)
            self.spartan_accounts.extend(additional_accounts)
            
            print(f"Loaded {len(self.spartan_accounts)} Spartan account(s) for match fetching")
            
            # Update rate limiter with number of accounts
            halo_stats_rate_limiter.set_num_accounts(len(self.spartan_accounts))
            
            # Load Xbox accounts for friends list API
            self._load_xbox_accounts()
            
            return True
        
        # Need to refresh - check cooldown (1 minute minimum between refreshes)
        time_since_last = time.time() - self._last_refresh_time
        if time_since_last < 60:
            print(f"Refresh cooldown active ({60-time_since_last:.0f}s remaining)")
            return False
        
        # Perform refresh for both accounts
        self._refresh_in_progress = True
        self._last_refresh_time = time.time()
        
        try:
            # Refresh Account 1
            if not account1_valid:
                async with get_token_swap_lock():
                    oauth_info = cache.get("oauth")
                    if not oauth_info or not oauth_info.get("refresh_token"):
                        print("No OAuth refresh token available for Account 1")
                        print("Run: python -m src.auth.tokens")
                        return False

                    print("Refreshing Account 1 tokens...")

                    # Force expiry of all tokens for Account 1
                    for key in ["spartan", "clearance", "xsts", "xsts_xbox"]:
                        if key in cache:
                            cache[key]["expires_at"] = 0
                    safe_write_json(TOKEN_CACHE_FILE, cache)

                    # Run auth flow for Account 1 (never open a browser from
                    # the running bot - fail cleanly if the refresh token dies)
                    await run_auth_flow(self.client_id, self.client_secret, use_halo=True, interactive=False)

                    # Reload and validate Account 1
                    cache = safe_read_json(TOKEN_CACHE_FILE, default={})
                    spartan_info = cache.get("spartan")
                    xsts_info = cache.get("xsts")
                    xsts_xbox_info = cache.get("xsts_xbox")

                    spartan_valid = spartan_info and is_token_valid(spartan_info)
                    xsts_valid = xsts_info and is_token_valid(xsts_info)
                    xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
                    account1_valid = spartan_valid and xsts_valid and xbox_valid
                    account1_spartan_info = spartan_info

                    if account1_valid:
                        print("Account 1 tokens refreshed successfully")
                    else:
                        print("Account 1 token refresh failed - tokens still invalid")
                        return False
            
            # Refresh additional accounts (2-5) if needed
            refreshed_account_attempts = set()
            for i in range(2, 6):
                cache_file = get_token_cache_path(i)
                account_cache = safe_read_json(cache_file, default={})
                
                if not account_cache:
                    continue
                
                # Check if this account needs refresh
                spartan_info = account_cache.get("spartan")
                xsts_info = account_cache.get("xsts")
                xsts_xbox_info = account_cache.get("xsts_xbox")
                
                spartan_valid = spartan_info and is_token_valid(spartan_info)
                xsts_valid = xsts_info and is_token_valid(xsts_info)
                xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
                account_valid = spartan_valid and xsts_valid and xbox_valid
                
                if not account_valid:
                    oauth_info = account_cache.get("oauth")
                    if oauth_info and oauth_info.get("refresh_token"):
                        refreshed_account_attempts.add(i)
                        print(f"Refreshing Account {i} tokens...")
                        try:
                            await self._refresh_account_via_swap(i, account_cache)
                        except Exception as e:
                            print(f"Error refreshing Account {i}: {e}")
                    else:
                        print(f"No OAuth refresh token for Account {i}")
                        print(f"Run: python -m src.auth.setup_account {i}")
            
            # Load tokens if Account 1 is valid (other accounts are optional)
            if account1_valid:
                self.spartan_token = account1_spartan_info.get("token")
                
                # Reload all accounts after refresh
                additional_accounts = []
                for i in range(2, 6):
                    cache_file = get_token_cache_path(i)
                    cache_data = safe_read_json(cache_file, default={})
                    
                    if cache_data:
                        spartan_info_acc = cache_data.get("spartan")
                        xsts_info_acc = cache_data.get("xsts")
                        xsts_xbox_info_acc = cache_data.get("xsts_xbox")
                        
                        spartan_valid = spartan_info_acc and is_token_valid(spartan_info_acc)
                        xsts_valid = xsts_info_acc and is_token_valid(xsts_info_acc)
                        xbox_valid = xsts_xbox_info_acc and is_token_valid(xsts_xbox_info_acc)

                        if i in refreshed_account_attempts:
                            if spartan_valid and xsts_valid and xbox_valid:
                                print(f"Account {i} tokens refreshed successfully")
                            elif self._oauth_refresh_alive(cache_data):
                                print(f"Account {i}: refreshed, clearance temporarily unavailable (non-critical)")
                            else:
                                print(f"⚠️ Account {i} needs manual re-auth. Run: python -m src.auth.setup_account {i}")
                        elif not (spartan_valid and xsts_valid and xbox_valid):
                            print(f"⚠️ Account {i} needs manual re-auth. Run: python -m src.auth.setup_account {i}")

                        if spartan_valid and xsts_valid and xbox_valid:
                            additional_accounts.append({
                                'id': f'account{i}',
                                'token': spartan_info_acc.get("token"),
                                'name': f'Account {i}',
                                'cache_file': cache_file
                            })
                
                # Load Spartan accounts
                self.spartan_accounts = []
                self.spartan_accounts.append({
                    'id': 'account1',
                    'token': account1_spartan_info.get("token"),
                    'name': 'Account 1'
                })
                
                # Add all valid additional accounts
                self.spartan_accounts.extend(additional_accounts)
                print(f"Token refresh complete - Loaded {len(self.spartan_accounts)} valid Spartan account(s)")
                
                # Load Xbox accounts for friends list API
                self._load_xbox_accounts()
                
                return True
            else:
                print("Token refresh failed for Account 1")
                return False
                
        except Exception as e:
            print(f"Token validation error: {e}")
            traceback.print_exc()
            return False
        finally:
            self._refresh_in_progress = False
    
    def get_next_spartan_token(self, account_index: Optional[int] = None) -> Optional[str]:
        """
        Get a Spartan token by account index or round-robin selection.
        
        Args:
            account_index: Specific account index to use, or None for round-robin
        
        Returns:
            Spartan token string, or None if no tokens available
        """
        if not self.spartan_accounts:
            return self.spartan_token  # Fallback to single token
        
        if account_index is not None and account_index < len(self.spartan_accounts):
            return self.spartan_accounts[account_index]['token']
        
        # Round-robin selection across all accounts (fallback)
        account = self.spartan_accounts[self.current_account_index % len(self.spartan_accounts)]
        self.current_account_index += 1
        return account['token']
    
    def _load_xbox_accounts(self) -> None:
        """
        Load Xbox Live tokens from all account cache files.
        
        Populates self.xbox_accounts with valid Xbox XSTS tokens from all
        configured accounts (1-5) for parallel friends list fetching.
        """
        self.xbox_accounts = []
        
        # Load Account 1
        cache = safe_read_json(TOKEN_CACHE_FILE, default={})
        xsts_xbox = cache.get('xsts_xbox')
        if xsts_xbox and is_token_valid(xsts_xbox):
            self.xbox_accounts.append({
                'id': 'account1',
                'token': xsts_xbox.get('token'),
                'uhs': xsts_xbox.get('uhs'),
                'cache_file': TOKEN_CACHE_FILE
            })
        
        # Load Accounts 2-5
        for i in range(2, 6):
            cache_file = get_token_cache_path(i)
            cache_data = safe_read_json(cache_file, default={})
            xsts_xbox = cache_data.get('xsts_xbox')
            if xsts_xbox and is_token_valid(xsts_xbox):
                self.xbox_accounts.append({
                    'id': f'account{i}',
                    'token': xsts_xbox.get('token'),
                    'uhs': xsts_xbox.get('uhs'),
                    'cache_file': cache_file
                })
        
        if self.xbox_accounts:
            print(f"📱 Loaded {len(self.xbox_accounts)} Xbox account(s) for friends list")
            xbox_profile_rate_limiter.set_num_accounts(len(self.xbox_accounts))
    
    def get_xbox_account(self, account_index: Optional[int] = None) -> Optional[Dict]:
        """
        Get Xbox account credentials by index or round-robin selection.
        
        Args:
            account_index: Specific account index, or None for round-robin
        
        Returns:
            Dict with 'token' and 'uhs' keys, or None if no accounts available
        """
        if not self.xbox_accounts:
            return None
        
        if account_index is not None and account_index < len(self.xbox_accounts):
            return self.xbox_accounts[account_index]
        
        # Round-robin selection
        account = self.xbox_accounts[self.current_xbox_index % len(self.xbox_accounts)]
        self.current_xbox_index += 1
        return account

    # =========================================================================
    # TOKEN MANAGEMENT
    # =========================================================================
    
    async def get_clearance_token(self) -> bool:
        """
        Load and validate the clearance token from cache.
        
        Returns:
            True if valid token loaded, False otherwise
        """
        try:
            cache_file = TOKEN_CACHE_FILE

            if not os.path.exists(cache_file):
                print(f"ERROR: Token cache file '{cache_file}' not found")
                print("Run: python -m src.auth.tokens")
                return False

            # Hold the swap lock while reading so this can't observe another
            # account's cache mid-swap (see _refresh_account_via_swap /
            # proactive_token_refresh, which temporarily overwrite
            # TOKEN_CACHE_FILE with account 2-5's data while refreshing it).
            async with get_token_swap_lock():
                cache = safe_read_json(cache_file, default={})
                if not cache:
                    print("ERROR: Failed to parse token cache")
                    return False

                # Validate spartan and Xbox XSTS tokens
                spartan_info = cache.get("spartan")
                xsts_xbox_info = cache.get("xsts_xbox")

                if is_token_valid(spartan_info) and is_token_valid(xsts_xbox_info):
                    self.spartan_token = spartan_info.get("token")
                    if self.spartan_token:
                        expires = time.ctime(spartan_info.get('expires_at', 0))
                        print(f"Loaded valid Spartan token (expires: {expires})")
                        return True

            print("Tokens expired or invalid - need refresh")
            return False
            
        except Exception as e:
            print(f"EXCEPTION in get_clearance_token: {e}")
            traceback.print_exc()
            return False
    
    # =========================================================================
    # GAMERTAG / XUID RESOLUTION
    # =========================================================================
    
    async def resolve_gamertag_to_xuid(self, gamertag: str) -> Optional[str]:
        """
        Convert a gamertag to XUID using Xbox Live Profile API.
        
        Uses cache first to minimize API calls. Rate limited to respect
        Xbox Profile API limits.
        
        Args:
            gamertag: Xbox gamertag to resolve
        
        Returns:
            XUID string if found, None otherwise
        """
        try:
            gamertag = (gamertag or "").strip()
            if not gamertag:
                return None

            # Check cache first (reverse lookup: gamertag => XUID)
            xuid_cache = load_xuid_cache()
            normalized_query = _normalize_gamertag_for_lookup(gamertag)
            alias_query = _normalize_gamertag_alias_key(gamertag)

            strict_hits: List[str] = []
            alias_hits: List[str] = []
            for xuid, cached_gamertag in xuid_cache.items():
                normalized_cached = _normalize_gamertag_for_lookup(cached_gamertag)
                if normalized_cached == normalized_query:
                    strict_hits.append(str(xuid))
                    continue

                if alias_query and _normalize_gamertag_alias_key(cached_gamertag) == alias_query:
                    alias_hits.append(str(xuid))

            if len(strict_hits) == 1:
                xuid = strict_hits[0]
                print(f"Cache hit: '{gamertag}' -> XUID: {xuid}")
                return xuid

            if len(strict_hits) > 1:
                print(f"Ambiguous strict cache match for '{gamertag}' ({len(strict_hits)} candidates); falling back to API")

            # Alias matching removes spaces; only trust unique candidates.
            if len(alias_hits) == 1:
                xuid = alias_hits[0]
                print(f"Cache alias hit: '{gamertag}' -> XUID: {xuid}")
                return xuid

            if len(alias_hits) > 1:
                print(f"Ambiguous alias cache match for '{gamertag}' ({len(alias_hits)} candidates); falling back to API")
            
            # Cache miss - need to resolve via API
            print(f"Cache miss for '{gamertag}', resolving via API...")
            
            # Acquire rate limiter slot
            await xbox_profile_rate_limiter.acquire()
            
            try:
                # Load the Xbox Live XSTS token from cache
                cache_file = TOKEN_CACHE_FILE
                if not os.path.exists(cache_file):
                    print(f"Token cache not found")
                    return None
                
                with open(cache_file, 'r') as f:
                    cache = json.load(f)
                
                # Get Xbox Live XSTS token (not Halo XSTS)
                xsts_xbox = cache.get('xsts_xbox')
                if not xsts_xbox:
                    print(f"Xbox Live XSTS token not found in cache")
                    print(f"Run python -m src.auth.tokens to authenticate with Xbox Live profile access")
                    return None
                
                xbox_token = xsts_xbox.get('token')
                uhs = xsts_xbox.get('uhs')
                
                if not xbox_token or not uhs:
                    print(f"Xbox Live XSTS token or UHS missing")
                    return None
                
                # Use Xbox Live Profile API with GET and gt() in URL path
                # This is the official GDK documented approach for gamertag lookups
                profile_url = f'https://profile.xboxlive.com/users/gt({gamertag})/profile/settings?settings=Gamertag'
                
                headers = {
                    'Authorization': f'XBL3.0 x={uhs};{xbox_token}',
                    'x-xbl-contract-version': '2',
                    'Accept': 'application/json'
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(profile_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Extract XUID from response
                            if 'profileUsers' in data and len(data['profileUsers']) > 0:
                                user = data['profileUsers'][0]
                                xuid = user.get('id')
                                
                                if xuid:
                                    canonical_gamertag = None
                                    settings = user.get('settings', [])
                                    if isinstance(settings, list):
                                        for setting in settings:
                                            if not isinstance(setting, dict):
                                                continue
                                            if setting.get('id') != 'Gamertag':
                                                continue

                                            candidate = setting.get('value')
                                            if isinstance(candidate, str):
                                                candidate = candidate.strip()
                                                if candidate:
                                                    canonical_gamertag = candidate
                                                    break

                                    if not canonical_gamertag:
                                        canonical_gamertag = gamertag

                                    print(f"Resolved '{gamertag}' to XUID: {xuid}")
                                    # Save to cache for future lookups
                                    xuid_cache[str(xuid)] = canonical_gamertag
                                    save_xuid_cache(xuid_cache)
                                    return str(xuid)
                                else:
                                    print(f"No XUID found in profile response for '{gamertag}'")
                                    return None
                            else:
                                print(f"No profile data returned for '{gamertag}'")
                                return None
                        elif response.status == 401:
                            error_text = await response.text()
                            print(f"Unauthorized (401) - Xbox Live XSTS token is invalid")
                            print(f"Error: {error_text[:200]}")
                            return None
                        elif response.status == 404:
                            print(f"Gamertag '{gamertag}' not found")
                            return None
                        else:
                            error_text = await response.text()
                            print(f"Profile API returned status {response.status}")
                            print(f"Error: {error_text[:200]}")
                            return None
            finally:
                xbox_profile_rate_limiter.release()
            
        except Exception as e:
            print(f"Error resolving gamertag: {e}")
            traceback.print_exc()
            return None
        
        return None

    async def resolve_xuid_to_gamertag(self, xuid: str) -> Optional[str]:
        """
        Convert an XUID to gamertag using Xbox Live Profile API.

        Uses cache first to minimize API calls and writes resolved values back
        to the legacy XUID cache format.

        Args:
            xuid: Xbox User ID to resolve

        Returns:
            Gamertag string if found, None otherwise
        """
        xuid_str = str(xuid)

        try:
            xuid_cache = load_xuid_cache()
            cached_gamertag = xuid_cache.get(xuid_str)
            if isinstance(cached_gamertag, str) and cached_gamertag.strip():
                return cached_gamertag

            await xbox_profile_rate_limiter.acquire()

            try:
                cache_file = TOKEN_CACHE_FILE
                if not os.path.exists(cache_file):
                    print("Token cache not found")
                    return None

                cache = safe_read_json(cache_file, default={})
                if not cache:
                    print("Failed to parse token cache")
                    return None

                xsts_xbox = cache.get("xsts_xbox")
                if not xsts_xbox:
                    print("Xbox Live XSTS token not found in cache")
                    return None

                xbox_token = xsts_xbox.get("token")
                uhs = xsts_xbox.get("uhs")
                if not xbox_token or not uhs:
                    print("Xbox Live XSTS token or UHS missing")
                    return None

                profile_url = f"https://profile.xboxlive.com/users/xuid({xuid_str})/profile/settings?settings=Gamertag"
                headers = {
                    "Authorization": f"XBL3.0 x={uhs};{xbox_token}",
                    "x-xbl-contract-version": "2",
                    "Accept": "application/json",
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(profile_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            profile_users = data.get("profileUsers", [])
                            if not profile_users:
                                return None

                            settings = profile_users[0].get("settings", [])
                            for setting in settings:
                                if setting.get("id") == "Gamertag":
                                    gamertag = setting.get("value")
                                    if isinstance(gamertag, str) and gamertag.strip():
                                        xuid_cache[xuid_str] = gamertag
                                        save_xuid_cache(xuid_cache)
                                        return gamertag
                            return None

                        if response.status == 404:
                            return None

                        if response.status == 401:
                            error_text = await response.text()
                            print("Unauthorized (401) - Xbox Live XSTS token is invalid")
                            print(f"Error: {error_text[:200]}")
                            return None

                        error_text = await response.text()
                        print(f"Profile API returned status {response.status}")
                        print(f"Error: {error_text[:200]}")
                        return None
            finally:
                xbox_profile_rate_limiter.release()

        except Exception as e:
            print(f"Error resolving XUID to gamertag: {e}")
            traceback.print_exc()
            return None

        return None
    
    # =========================================================================
    # XBOX FRIENDS LIST
    # =========================================================================
    
    async def get_friends_list(
        self, 
        xuid: str, 
        _xuid_cache: Dict = None, 
        _cache_stats: Dict = None,
        _account_index: Optional[int] = None,
        max_retries: int = 5
    ) -> Dict:
        """
        Get a player's Xbox friends list using the People Hub API.
        
        Uses exponential backoff on 429 rate limit errors.
        
        Args:
            xuid: Xbox User ID to get friends for
            _xuid_cache: Optional shared cache dict (for batch operations)
            _cache_stats: Optional dict to track {'new_entries': count} (for batch operations)
            _account_index: Optional specific Xbox account to use (for parallel requests)
            max_retries: Maximum retry attempts on rate limit (default 5)
        
        Returns:
            Dict with 'friends' (list), 'is_private' (bool), and 'error' (str or None)
        """
        # Use People Hub API to get friends list
        friends_url = f'https://peoplehub.xboxlive.com/users/xuid({xuid})/people/social/decoration/preferredcolor,detail'
        
        # Use shared cache if provided (for batch operations), otherwise load fresh
        use_shared_cache = _xuid_cache is not None
        xuid_cache = _xuid_cache if use_shared_cache else load_xuid_cache()

        fallback_xbox_token = None
        fallback_uhs = None
        if not self.xbox_accounts:
            # Fallback to loading from cache file (single account mode)
            cache_file = TOKEN_CACHE_FILE
            if not os.path.exists(cache_file):
                print("Token cache not found")
                return {'friends': [], 'is_private': False, 'error': 'no_cache'}

            with open(cache_file, 'r') as f:
                cache = json.load(f)

            xsts_xbox = cache.get('xsts_xbox')
            if not xsts_xbox:
                print("Xbox Live XSTS token not found in cache")
                return {'friends': [], 'is_private': False, 'error': 'no_token'}

            fallback_xbox_token = xsts_xbox.get('token')
            fallback_uhs = xsts_xbox.get('uhs')
        
        # Retry loop with exponential backoff
        for attempt in range(max_retries):
            account_idx = 0
            release_rate_limiter = False
            try:
                if self.xbox_accounts:
                    # On first attempt honor caller preference; retries can rebalance to other accounts.
                    requested_account = _account_index if attempt == 0 else None
                    account_idx = await xbox_profile_rate_limiter.acquire(requested_account)
                    release_rate_limiter = True

                    account = self.xbox_accounts[account_idx] if account_idx < len(self.xbox_accounts) else self.xbox_accounts[0]
                    xbox_token = account.get('token')
                    uhs = account.get('uhs')
                else:
                    xbox_token = fallback_xbox_token
                    uhs = fallback_uhs

                if not xbox_token or not uhs:
                    print("Xbox Live XSTS token or UHS missing")
                    return {'friends': [], 'is_private': False, 'error': 'missing_token'}

                headers = {
                    'Authorization': f'XBL3.0 x={uhs};{xbox_token}',
                    'x-xbl-contract-version': '5',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US'
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(friends_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            people = data.get('people', [])
                            
                            friends = []
                            new_entries = 0
                            
                            for person in people:
                                friend_xuid = person.get('xuid')
                                gamertag = person.get('gamertag')
                                display_name = person.get('displayName', gamertag)
                                is_following_caller = person.get('isFollowingCaller', False)
                                is_followed_by_caller = person.get('isFollowedByCaller', True)
                                
                                if friend_xuid and gamertag:
                                    # Save to XUID cache if not already there
                                    xuid_str = str(friend_xuid)
                                    if xuid_str not in xuid_cache:
                                        xuid_cache[xuid_str] = gamertag
                                        new_entries += 1
                                    
                                    friends.append({
                                        'xuid': xuid_str,
                                        'gamertag': gamertag,
                                        'display_name': display_name,
                                        'is_mutual': is_following_caller and is_followed_by_caller
                                    })
                            
                            # Track stats for batch operations, or save immediately for single calls
                            if use_shared_cache and _cache_stats is not None:
                                _cache_stats['new_entries'] = _cache_stats.get('new_entries', 0) + new_entries
                            elif new_entries > 0:
                                save_xuid_cache(xuid_cache)
                                print(f"💾 Saved {new_entries} new entries to XUID cache")
                            
                            print(f"Found {len(friends)} friends for XUID {xuid}")
                            return {'friends': friends, 'is_private': False, 'error': None}
                        
                        elif response.status == 429:
                            # Rate limited - parse response for backoff info
                            current_requests = 0
                            max_requests = 0
                            try:
                                error_data = await response.json()
                                period_seconds = error_data.get('periodInSeconds', 300)
                                current_requests = error_data.get('currentRequests', 0)
                                max_requests = error_data.get('maxRequests', 30)
                                
                                # Calculate wait time with exponential backoff
                                # Base: wait for period to reset, with exponential multiplier
                                base_wait = min(period_seconds / 2, 60)  # Cap base at 60s
                                backoff_wait = base_wait * (2 ** attempt) + (attempt * 5)
                                backoff_wait = min(backoff_wait, period_seconds)  # Don't exceed period

                            except Exception:
                                # Couldn't parse - use default exponential backoff
                                backoff_wait = 30 * (2 ** attempt)

                            if self.xbox_accounts:
                                xbox_profile_rate_limiter.set_backoff(account_idx, backoff_wait)
                                print(f"⚠️ Rate limited (429) for XUID {xuid} on account {account_idx + 1} - {current_requests}/{max_requests} requests")
                                print(f"   Attempt {attempt + 1}/{max_retries}, backoff {backoff_wait:.0f}s (limiter-managed)")

                                if attempt < max_retries - 1:
                                    # Yield briefly; limiter state controls real wait/rebalance behavior.
                                    await asyncio.sleep(0)
                                    continue
                            else:
                                print(f"⚠️ Rate limited (429), waiting {backoff_wait:.0f}s (attempt {attempt + 1}/{max_retries})")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(min(backoff_wait, 30.0))
                                    continue

                            print(f"❌ Failed to get friends list for XUID {xuid} after {max_retries} retries")
                            return {'friends': [], 'is_private': False, 'error': 'max_retries'}
                        
                        elif response.status == 401:
                            print(f"Unauthorized (401) - Xbox Live XSTS token invalid for friends list")
                            return {'friends': [], 'is_private': False, 'error': 'unauthorized'}
                        
                        elif response.status == 403:
                            print(f"Forbidden (403) - Friends list is private for XUID {xuid}")
                            return {'friends': [], 'is_private': True, 'error': None}
                        
                        else:
                            error_text = await response.text()
                            print(f"People Hub API returned status {response.status}: {error_text[:200]}")
                            return {'friends': [], 'is_private': False, 'error': f'status_{response.status}'}
                            
            except Exception as e:
                print(f"Error getting friends list (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    # Keep retries responsive; limiter and per-account backoff handle pacing.
                    await asyncio.sleep(0)
                    continue
                traceback.print_exc()
                return {'friends': [], 'is_private': False, 'error': 'exception'}
            finally:
                if release_rate_limiter:
                    xbox_profile_rate_limiter.release()

        # Exhausted retries
        print(f"❌ Failed to get friends list for XUID {xuid} after {max_retries} retries")
        return {'friends': [], 'is_private': False, 'error': 'max_retries'}

    async def get_friends_of_friends(
        self,
        gamertag: str,
        max_depth: int = 2,
        progress_callback: callable = None,
        concurrency: int = None
    ) -> Dict:
        """
        Get a player's friends and friends-of-friends (2nd degree connections).

        Uses concurrent requests across multiple Xbox accounts for faster fetching.

        Args:
            gamertag: Xbox gamertag to start from
            max_depth: How many levels deep to go (1=friends only, 2=friends of friends)
            progress_callback: Optional async callback(current, total, stage, fof_count) for progress updates
            concurrency: Max concurrent requests (defaults to number of Xbox accounts, capped at 5)

        Returns:
            Dictionary with:
                - 'target': The target player info
                - 'friends': List of direct friends
                - 'friends_of_friends': List of 2nd degree connections (if depth=2)
                - 'all_unique': Set of all unique XUIDs found
                - 'error': Error message if any

        Note:
            Xbox People Hub API has strict rate limits (30 req / 5 min per account).
            Uses exponential backoff with retries on 429 errors.
        """
        result = {
            'target': None,
            'friends': [],
            'friends_of_friends': [],
            'all_unique': set(),
            'error': None,
            'new_cache_entries': 0,
            'private_friends': []  # Friends with private friends lists
        }

        # Load XUID cache once for the entire operation (batch mode)
        xuid_cache = load_xuid_cache()
        cache_stats = {'new_entries': 0}
        cache_lock = asyncio.Lock()  # For thread-safe cache updates
        private_friends_lock = asyncio.Lock()  # For thread-safe private friends tracking

        # Set concurrency - keep bounded while allowing all configured accounts by default.
        if concurrency is None:
            num_accounts = len(self.xbox_accounts) if self.xbox_accounts else 1
            concurrency = min(num_accounts, 5)

        # Resolve target gamertag to XUID
        print(f"Resolving gamertag '{gamertag}' to XUID...")
        target_xuid = await self.resolve_gamertag_to_xuid(gamertag)
        
        if not target_xuid:
            result['error'] = f"Could not resolve gamertag '{gamertag}'"
            return result
        
        result['target'] = {'xuid': target_xuid, 'gamertag': gamertag}
        result['all_unique'].add(target_xuid)
        
        # Get direct friends (level 1) - using shared cache
        print(f"Fetching friends for {gamertag} (XUID: {target_xuid})...")
        direct_result = await self.get_friends_list(target_xuid, _xuid_cache=xuid_cache, _cache_stats=cache_stats)
        direct_friends = direct_result.get('friends', [])
        
        if not direct_friends:
            print(f"No friends found or friends list is private for {gamertag}")
            result['error'] = f"Could not access friends list for '{gamertag}' (may be private)"
            return result
        
        result['friends'] = direct_friends
        for friend in direct_friends:
            result['all_unique'].add(friend['xuid'])
        
        print(f"Found {len(direct_friends)} direct friends")
        
        # Notify progress after getting direct friends
        if progress_callback:
            await progress_callback(0, len(direct_friends), 'friends_found', 0)
        
        # Get friends of friends (level 2) if requested
        if max_depth >= 2:
            print(f"Fetching friends-of-friends with {concurrency} concurrent requests...")
            fof_set = set()  # Track unique 2nd degree connections
            processed_count = 0
            total_friends = len(direct_friends)
            
            async def fetch_friend_list(friend: Dict, account_idx: int) -> Dict:
                """Fetch friends for a single friend using specified account."""
                nonlocal processed_count
                friend_xuid = friend['xuid']
                friend_gt = friend['gamertag']
                
                fof_result = await self.get_friends_list(
                    friend_xuid, 
                    _xuid_cache=xuid_cache, 
                    _cache_stats=cache_stats,
                    _account_index=account_idx
                )
                
                fof_list = fof_result.get('friends', [])
                is_private = fof_result.get('is_private', False)
                
                # Thread-safe update of processed count and private tracking
                async with cache_lock:
                    processed_count += 1
                
                if is_private:
                    async with private_friends_lock:
                        result['private_friends'].append({'xuid': friend_xuid, 'gamertag': friend_gt})
                
                return {'fof_list': [(fof, friend_gt) for fof in fof_list], 'is_private': is_private, 'friend': friend}
            
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(concurrency)
            account_slots = len(self.xbox_accounts) if self.xbox_accounts else 1
            
            async def rate_limited_fetch(friend: Dict, idx: int) -> Dict:
                """Wrapper to apply concurrency limit."""
                async with semaphore:
                    account_idx = idx % account_slots  # Distribute across all available accounts
                    return await fetch_friend_list(friend, account_idx)
            
            # Progress update task
            async def update_progress():
                """Periodically update progress while fetching."""
                last_count = 0
                while processed_count < total_friends:
                    if progress_callback and processed_count > last_count:
                        await progress_callback(
                            processed_count, 
                            total_friends, 
                            'checking_fof', 
                            len(result['friends_of_friends'])
                        )
                        last_count = processed_count
                    await asyncio.sleep(1)
            
            # Start progress updater
            progress_task = asyncio.create_task(update_progress())
            
            try:
                # Launch all requests concurrently (semaphore limits active ones)
                print(f"  Launching {total_friends} concurrent friend list fetches...")
                tasks = [
                    rate_limited_fetch(friend, i) 
                    for i, friend in enumerate(direct_friends)
                ]
                
                # Gather all results
                all_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for fetch_result in all_results:
                    if isinstance(fetch_result, Exception):
                        print(f"  Error in concurrent fetch: {fetch_result}")
                        continue
                    
                    for fof, via_gamertag in fetch_result.get('fof_list', []):
                        fof_xuid = fof['xuid']
                        # Only add if not already in our sets
                        if fof_xuid not in result['all_unique'] and fof_xuid not in fof_set:
                            fof_set.add(fof_xuid)
                            fof['via'] = via_gamertag  # Track who connected us
                            result['friends_of_friends'].append(fof)
                
            finally:
                # Cancel progress task
                progress_task.cancel()
                try:
                    await progress_task
                except (asyncio.CancelledError, RuntimeError):
                    pass
            
            # Add all 2nd degree to unique set
            for fof in result['friends_of_friends']:
                result['all_unique'].add(fof['xuid'])
            
            # Final progress callback
            if progress_callback:
                await progress_callback(total_friends, total_friends, 'checking_fof', len(result['friends_of_friends']))
            
            print(f"Found {len(result['friends_of_friends'])} unique friends-of-friends")
        
        # Save cache if we added new entries (batch save at end)
        if cache_stats['new_entries'] > 0:
            save_xuid_cache(xuid_cache)
            print(f"💾 Saved {cache_stats['new_entries']} new entries to XUID cache")
        
        result['new_cache_entries'] = cache_stats['new_entries']
        print(f"Total unique players found: {len(result['all_unique'])}")
        return result

    # =========================================================================
    # STATS RETRIEVAL
    # =========================================================================
    
    async def get_player_stats(
        self,
        gamertag: str,
        stat_type: str = "overall",
        matches_to_process: int = 10,
        force_full_fetch: bool = False,
        xuid: Optional[str] = None,
    ) -> Dict:
        """
        Get comprehensive player statistics from Halo API.

        Args:
            gamertag: Player's Xbox gamertag
            stat_type: Type of stats ("overall", "ranked", "core_ranked",
                "rotational_ranked", "social")
            matches_to_process: Number of matches to analyze
            force_full_fetch: If True, bypass cache and fetch full history from API
            xuid: Pre-resolved XUID, if the caller already has one (skips the
                gamertag->XUID lookup, e.g. when it already checked
                check_player_cached for the same player just before this call)

        Returns:
            Dictionary containing stats or error information
        """
        if not self.clearance_token:
            if not await self.get_clearance_token():
                return {"error": 4, "message": "Failed to authenticate with Halo API"}

        try:
            if not xuid:
                print(f"Resolving gamertag '{gamertag}' to XUID...")
                xuid = await self.resolve_gamertag_to_xuid(gamertag)

            if not xuid:
                return {"error": 2, "message": f"Could not resolve gamertag '{gamertag}' to XUID"}

            print(f"Using XUID: {xuid}")

            # Fast path: read precomputed per-mode stats instead of pulling
            # full match history (with its per-match medal lookups) and
            # recomputing aggregates on demand. Skipped for forced/full-history
            # fetches, which need calculate_comprehensive_stats' live-fetch logic.
            full_history_requested = matches_to_process is None or matches_to_process >= 999999
            if not force_full_fetch and not full_history_requested:
                summary = self.stats_cache.get_player_mode_summary(xuid, stat_type)
                if summary is not None:
                    print(f"Using precomputed player_mode_stats summary for {gamertag}")
                    stats_result = {
                        'error': 0,
                        'stats': summary,
                        'matches_processed': summary['games_played'],
                        'new_matches': 0,
                    }
                    return self.parse_stats(stats_result, stat_type, gamertag)

            # Coalesce identical concurrent requests: a spammed command joins
            # the in-flight fetch instead of running its own. Each Discord
            # invocation still edits its own loading embed with the shared
            # result. Callers only read the result dict, so sharing is safe.
            key = (xuid, stat_type, matches_to_process, bool(force_full_fetch))
            existing = self._stats_inflight.get(key)
            if existing is not None and not existing.done():
                print(f"[COALESCE] Joining in-flight {stat_type} request for {gamertag or xuid}")
                return await existing

            task = asyncio.create_task(self._fetch_player_stats(
                xuid, gamertag, stat_type, matches_to_process, force_full_fetch
            ))
            self._stats_inflight[key] = task
            try:
                return await task
            finally:
                if self._stats_inflight.get(key) is task:
                    self._stats_inflight.pop(key, None)

        except Exception as e:
            print(f"EXCEPTION in get_player_stats: {e}")
            traceback.print_exc()
            return {"error": 4, "message": f"API request failed: {str(e)}"}

    async def _fetch_player_stats(
        self,
        xuid: str,
        gamertag: str,
        stat_type: str,
        matches_to_process: Optional[int],
        force_full_fetch: bool,
    ) -> Dict:
        """
        The expensive part of get_player_stats, run as a shared task so
        identical concurrent requests coalesce onto one fetch. Never raises:
        errors come back as {"error": ...} dicts so every awaiter sees the
        same result shape.
        """
        try:
            # Set up headers with whatever token we have
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json"
            }
            
            # Try to get clearance token from cache if we don't have it loaded
            if not self.clearance_token:
                try:
                    cache_file = TOKEN_CACHE_FILE
                    if os.path.exists(cache_file):
                        # Same swap-lock protection as get_clearance_token() above,
                        # so this can't read another account's cache mid-swap either.
                        async with get_token_swap_lock():
                            cache = safe_read_json(cache_file, default={})
                        clearance = cache.get('clearance', {})
                        if isinstance(clearance, dict) and clearance.get('FlightConfigurationId'):
                            self.clearance_token = clearance['FlightConfigurationId']
                            print(f"Loaded Clearance from cache: {self.clearance_token}")
                except:
                    pass
            
            # Add Spartan authentication
            if self.spartan_token:
                # Extract token string if it's stored as a dict
                spartan_token = self.spartan_token
                if isinstance(spartan_token, dict) and 'token' in spartan_token:
                    spartan_token = spartan_token['token']
                
                headers["Authorization"] = f"Spartan {spartan_token}"
                headers["x-343-authorization-spartan"] = spartan_token
            
            if self.clearance_token and self.clearance_token != "skip":
                headers["x-343-authorization-clearance"] = self.clearance_token
            
            # Calculate stats from match history with caching
            stats_result = await self.calculate_comprehensive_stats(
                xuid, stat_type, gamertag=gamertag,
                matches_to_process=matches_to_process,
                force_full_fetch=force_full_fetch,
            )
            
            if stats_result.get('error') == 0:
                print(f"Stats calculated: {stats_result.get('matches_processed', 0)} matches "
                      f"({stats_result.get('new_matches', 0)} new)")
                return self.parse_stats(stats_result, stat_type, gamertag)

            return stats_result

        except Exception as e:
            print(f"EXCEPTION in _fetch_player_stats: {e}")
            traceback.print_exc()
            return {"error": 4, "message": f"API request failed: {str(e)}"}

    def start_background_full_collect(
        self,
        xuid: str,
        gamertag: str,
        on_complete: Optional[Callable[[Dict], Awaitable[None]]] = None,
    ) -> None:
        """
        Kick off a background full-history collect for a brand-new player.

        Deduped per-xuid: a second call for the same xuid while a collect is
        still running is a no-op. Different players' collects run
        concurrently. `on_complete`, if given, is awaited with the raw
        calculate_comprehensive_stats result dict once the collect finishes
        (or a {"error": ...} dict on failure) - kept caller-supplied so this
        API-layer class stays free of any Discord-specific embed building.
        """
        existing = self._full_collect_tasks.get(xuid)
        if existing and not existing.done():
            return

        async def _run():
            try:
                result = await self.calculate_comprehensive_stats(
                    xuid, "overall", gamertag=gamertag,
                    matches_to_process=None, force_full_fetch=False,
                )
            except Exception as e:
                print(f"Background full collect failed for {gamertag or xuid}: {e}")
                result = {"error": 4, "message": str(e)}
            finally:
                self._full_collect_tasks.pop(xuid, None)
            if on_complete:
                await on_complete(result)

        self._full_collect_tasks[xuid] = asyncio.create_task(_run())

    # =========================================================================
    # CACHE MANAGEMENT
    # =========================================================================
    
    def load_cached_stats(self, xuid: str, stat_type: str, gamertag: str = None) -> Optional[Dict]:
        """
        Load cached player stats from SQLite database.
        
        Args:
            xuid: Player XUID
            stat_type: Type of stats ("overall", "ranked", "social")
            gamertag: Player gamertag (for logging)
        
        Returns:
            Cached stats dictionary or None
        """
        try:
            print(f"[CLIENT] Attempting to load cache for xuid={xuid}, gamertag={gamertag}, stat_type={stat_type}")
            cached_data = self.stats_cache.load_player_stats(xuid, stat_type, gamertag)
            if cached_data:
                match_count = len(cached_data.get('processed_matches', []))
                print(f"[CLIENT] Loaded cached stats for {gamertag or xuid}: {match_count} matches")
                return cached_data
            else:
                print(f"[CLIENT] No cached data found for {gamertag or xuid}")
        except Exception as e:
            print(f"Error loading cache: {e}")
            import traceback
            traceback.print_exc()
        return None
    
    def get_cached_match_ids(self, xuid: str, stat_type: str = "overall") -> Set[str]:
        """
        Get set of match IDs already cached for a player.
        
        Args:
            xuid: Player XUID
            stat_type: Type of stats
        
        Returns:
            Set of cached match IDs
        """
        try:
            return self.stats_cache.get_cached_match_ids(xuid, stat_type)
        except Exception as e:
            print(f"Error getting cached match IDs: {e}")
            return set()
    
    def is_cache_fresh(self, cached_data: Optional[Dict], max_age_minutes: int = 30) -> bool:
        """
        Check if cached data is fresh enough to use.
        
        Args:
            cached_data: Cached stats dictionary
            max_age_minutes: Maximum acceptable age in minutes
        
        Returns:
            True if cache is fresh, False otherwise
        """
        if not cached_data:
            return False
        
        try:
            last_update = cached_data.get('last_update')
            if not last_update:
                return False
            
            cache_time = datetime.fromisoformat(last_update)
            age = datetime.now() - cache_time
            
            is_fresh = age < timedelta(minutes=max_age_minutes)
            if is_fresh:
                print(f"Cache is fresh ({age.total_seconds() / 60:.1f} minutes old)")
            return is_fresh
        except Exception as e:
            print(f"Error checking cache freshness: {e}")
            return False
    
    def save_stats_cache(self, xuid: str, stat_type: str, stats_data: Dict, gamertag: str = None) -> None:
        """
        Save player stats to SQLite database.
        
        Args:
            xuid: Player XUID
            stat_type: Type of stats
            stats_data: Stats dictionary to save
            gamertag: Player gamertag
        """
        try:
            success = self.stats_cache.save_player_stats(xuid, stat_type, stats_data, gamertag)
            if success:
                print(f"Saved stats to database for {gamertag or xuid}")
            else:
                print(f"Failed to save stats to database for {gamertag or xuid}")
        except Exception as e:
            print(f"Error saving cache: {e}")
            traceback.print_exc()

    def _persist_match_with_participants(self, stats_db, match_id, match_data, participants) -> bool:
        """Blocking helper: write one match + its participants. Runs on the
        dedicated DB-writer thread via run_in_executor so it never blocks the
        event loop. Each write self-commits (this seed-backfill path is not the
        hot write path - the per-player crawl save in save_player_stats is, and
        that one is batched into a single transaction)."""
        wrote_match = bool(stats_db.insert_match(match_data))
        wrote_participants = bool(stats_db.insert_match_participants(match_id, participants))
        return wrote_match and wrote_participants

    async def backfill_seed_match_participants(
        self,
        seed_xuid: str,
        seed_gamertag: str = None,
        limit_matches: int = None,
        min_participants: int = 2,
    ) -> Dict:
        """Repair missing participant rosters for verified seed matches."""
        normalized_seed = str(seed_xuid or "").strip()
        result = {
            'ok': False,
            'verified_matches': 0,
            'complete_matches_before': 0,
            'incomplete_matches_before': 0,
            'attempted_backfills': 0,
            'successful_backfills': 0,
            'failed_backfills': 0,
            'complete_matches_after': 0,
            'incomplete_matches_after': 0,
            'failed_match_ids': [],
            'message': '',
        }

        if not normalized_seed:
            result['message'] = "Seed XUID is required for participant backfill."
            return result

        stats_cache = getattr(self, 'stats_cache', None)
        stats_db = getattr(stats_cache, 'db', None)
        if not stats_db:
            result['message'] = "Participant database is unavailable."
            return result

        def get_verified_seed_match_ids() -> List[str]:
            if stats_cache and hasattr(stats_cache, 'get_seed_verified_match_ids'):
                return stats_cache.get_seed_verified_match_ids(normalized_seed, limit_matches=limit_matches) or []
            if hasattr(stats_db, 'get_seed_verified_match_ids'):
                return stats_db.get_seed_verified_match_ids(normalized_seed, limit_matches=limit_matches) or []
            return []

        def get_participant_coverage(match_ids: List[str]) -> Dict[str, Dict]:
            if not match_ids:
                return {}
            if stats_cache and hasattr(stats_cache, 'get_participant_coverage_for_matches'):
                return stats_cache.get_participant_coverage_for_matches(match_ids, normalized_seed) or {}
            if hasattr(stats_db, 'get_participant_coverage_for_matches'):
                return stats_db.get_participant_coverage_for_matches(match_ids, normalized_seed) or {}
            return {}

        try:
            verified_match_ids = get_verified_seed_match_ids()
            result['verified_matches'] = len(verified_match_ids)
            if not verified_match_ids:
                result['ok'] = True
                result['message'] = "No verified seed matches found for participant backfill."
                return result

            min_required_participants = max(2, int(min_participants or 2))
            coverage_before = get_participant_coverage(verified_match_ids)

            incomplete_match_ids: List[str] = []
            complete_before = 0
            for match_id in verified_match_ids:
                coverage_row = coverage_before.get(match_id) or {}
                participant_count = int(coverage_row.get('participant_count') or 0)
                seed_present = bool(coverage_row.get('seed_present'))
                if seed_present and participant_count >= min_required_participants:
                    complete_before += 1
                else:
                    incomplete_match_ids.append(match_id)

            result['complete_matches_before'] = complete_before
            result['incomplete_matches_before'] = len(incomplete_match_ids)
            result['attempted_backfills'] = len(incomplete_match_ids)

            if not incomplete_match_ids:
                result['ok'] = True
                result['complete_matches_after'] = complete_before
                result['incomplete_matches_after'] = 0
                result['message'] = "Seed participant coverage already complete."
                return result

            connector = aiohttp.TCPConnector(
                limit=30,
                limit_per_host=20,
                ttl_dns_cache=300,
                force_close=False,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_connect=30, sock_read=120)

            successful_backfills = 0
            failed_match_ids: List[str] = []

            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                num_accounts = len(self.spartan_accounts) if self.spartan_accounts else 1
                max_in_flight = min(max(5, num_accounts * 4), 20)

                async def fetch_match_detail(match_id: str):
                    match_data = await self.get_match_stats_for_match(match_id, normalized_seed, session)
                    return match_id, match_data

                pending = set()
                match_iter = iter(incomplete_match_ids)

                for _ in range(min(max_in_flight, len(incomplete_match_ids))):
                    try:
                        match_id = next(match_iter)
                    except StopIteration:
                        break
                    pending.add(asyncio.create_task(fetch_match_detail(match_id)))

                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            match_id, match_data = task.result()
                        except Exception:
                            failed_match_ids.append('unknown')
                            continue

                        participants = (match_data or {}).get('all_participants') or []
                        if not match_data or not participants:
                            failed_match_ids.append(match_id)
                        else:
                            wrote = await asyncio.get_running_loop().run_in_executor(
                                self._db_write_executor,
                                self._persist_match_with_participants,
                                stats_db, match_id, match_data, participants,
                            )
                            if wrote:
                                successful_backfills += 1
                            else:
                                failed_match_ids.append(match_id)

                        try:
                            next_match_id = next(match_iter)
                        except StopIteration:
                            continue
                        pending.add(asyncio.create_task(fetch_match_detail(next_match_id)))

            coverage_after = get_participant_coverage(verified_match_ids)
            complete_after = 0
            for match_id in verified_match_ids:
                coverage_row = coverage_after.get(match_id) or {}
                participant_count = int(coverage_row.get('participant_count') or 0)
                seed_present = bool(coverage_row.get('seed_present'))
                if seed_present and participant_count >= min_required_participants:
                    complete_after += 1

            result['ok'] = True
            result['successful_backfills'] = successful_backfills
            result['failed_backfills'] = len(failed_match_ids)
            result['failed_match_ids'] = failed_match_ids[:50]
            result['complete_matches_after'] = complete_after
            result['incomplete_matches_after'] = max(0, len(verified_match_ids) - complete_after)

            if failed_match_ids:
                result['message'] = (
                    f"Backfilled {successful_backfills}/{len(incomplete_match_ids)} incomplete seed matches "
                    f"for {seed_gamertag or normalized_seed}; {len(failed_match_ids)} failed."
                )
            else:
                result['message'] = (
                    f"Backfilled all {len(incomplete_match_ids)} incomplete seed matches "
                    f"for {seed_gamertag or normalized_seed}."
                )
            return result

        except Exception as exc:
            result['message'] = f"Seed participant backfill failed: {exc}"
            return result
    
    def _calculate_stats_from_matches(self, matches: List[Dict], stat_type: str) -> Dict:
        """
        Calculate aggregate stats from a list of matches.
        
        Stats are calculated on-demand rather than stored to save database space.
        
        Args:
            matches: List of match dictionaries with kills, deaths, assists, outcome, is_ranked
            stat_type: "overall", "ranked", "core_ranked", "rotational_ranked",
                or "social" to filter matches

        Returns:
            Dictionary containing calculated stats (kd_ratio, avg_kda, win_rate, etc.)
        """
        # Filter matches based on stat_type. Custom/private matches
        # (match_category == 'custom') are excluded from "social" and
        # "overall" - they stay in the DB but never count toward aggregates.
        # "core_ranked"/"rotational_ranked" split "ranked" by playlist:
        # core = playlists in CORE_RANKED_PLAYLIST_IDS (permanent trio plus
        # launch-era Ranked Arena), rotational = every other CSR playlist.
        if stat_type == "ranked":
            filtered_matches = [m for m in matches if m.get('is_ranked', False)]
        elif stat_type == "core_ranked":
            filtered_matches = [
                m for m in matches
                if m.get('is_ranked', False)
                and (m.get('playlist_id') or '').strip().lower() in CORE_RANKED_PLAYLIST_IDS
            ]
        elif stat_type == "rotational_ranked":
            filtered_matches = [
                m for m in matches
                if m.get('is_ranked', False)
                and (m.get('playlist_id') or '').strip().lower() not in CORE_RANKED_PLAYLIST_IDS
            ]
        elif stat_type == "social":
            filtered_matches = [
                m for m in matches
                if not m.get('is_ranked', False) and m.get('match_category') != 'custom'
            ]
        else:
            filtered_matches = [m for m in matches if m.get('match_category') != 'custom']
        
        # Calculate aggregate stats
        total_kills = sum(m.get('kills', 0) for m in filtered_matches)
        total_deaths = sum(m.get('deaths', 0) for m in filtered_matches)
        total_assists = sum(m.get('assists', 0) for m in filtered_matches)
        
        wins = sum(1 for m in filtered_matches if m.get('outcome') == 2)
        losses = sum(1 for m in filtered_matches if m.get('outcome') == 3)
        ties = sum(1 for m in filtered_matches if m.get('outcome') == 1)
        dnf = sum(1 for m in filtered_matches if m.get('outcome') == 4)
        
        games_played = len(filtered_matches)
        kd_ratio = round(total_kills / total_deaths if total_deaths > 0 else total_kills, 2)
        kda = round((total_kills + (total_assists / 3)) - total_deaths, 2)
        avg_kda = round(kda / games_played if games_played > 0 else 0, 2)
        win_rate = f"{round(wins / games_played * 100 if games_played > 0 else 0, 1)}%"
        
        return {
            'total_kills': total_kills,
            'total_deaths': total_deaths,
            'total_assists': total_assists,
            'wins': wins,
            'losses': losses,
            'ties': ties,
            'dnf': dnf,
            'games_played': games_played,
            'kd_ratio': kd_ratio,
            'kda': kda,
            'avg_kda': avg_kda,
            'win_rate': win_rate
        }
    
    # =========================================================================
    # MATCH STATS RETRIEVAL
    # =========================================================================

    @staticmethod
    def _extract_csr_and_tier(player_payload: Dict) -> Tuple[Optional[float], Optional[str]]:
        """Strict extraction: only explicit CSR/Tier fields are accepted."""
        if not isinstance(player_payload, dict):
            return None, None

        tier_keywords = {'bronze', 'silver', 'gold', 'platinum', 'diamond', 'onyx', 'unranked'}
        csr_value = None
        tier_value = None

        def walk(obj):
            nonlocal csr_value, tier_value
            if isinstance(obj, dict):
                for key, value in obj.items():
                    key_l = str(key).lower()

                    if csr_value is None and 'csr' in key_l and isinstance(value, (int, float)) and not isinstance(value, bool):
                        val = float(value)
                        if 0 <= val <= 5000:
                            csr_value = val

                    if tier_value is None and 'tier' in key_l and isinstance(value, str):
                        normalized = value.strip().lower()
                        if any(token in normalized for token in tier_keywords):
                            tier_value = value.strip()

                    if isinstance(value, (dict, list)):
                        walk(value)
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, (dict, list)):
                        walk(item)

        walk(player_payload)
        return csr_value, tier_value

    @staticmethod
    def _coerce_boolish(value: object) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value == 1:
                return True
            if value == 0:
                return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False
        return None

    @staticmethod
    def _coerce_intish(value: object) -> Optional[int]:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float) and float(value).is_integer():
            return int(value)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized and normalized.lstrip("-").isdigit():
                try:
                    return int(normalized)
                except ValueError:
                    return None
        return None

    @classmethod
    def _extract_explicit_custom_flag(cls, match_info: Optional[Dict]) -> Optional[bool]:
        if not isinstance(match_info, dict):
            return None

        stack: List[object] = [match_info]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                for raw_key, raw_value in current.items():
                    key = str(raw_key).strip().lower().replace("_", "")
                    if key in cls.EXPLICIT_CUSTOM_FLAG_KEYS:
                        parsed = cls._coerce_boolish(raw_value)
                        if parsed is not None:
                            return parsed
                    if isinstance(raw_value, (dict, list)):
                        stack.append(raw_value)
            elif isinstance(current, list):
                for item in current:
                    if isinstance(item, (dict, list)):
                        stack.append(item)

        return None

    @classmethod
    def _public_name_is_pve(cls, public_name: Optional[str]) -> bool:
        """True if a playlist's PublicName marks it as PvE (see
        PVE_PLAYLIST_NAME_HINTS). Case-insensitive substring test, mirroring
        resolve_playlist_metadata's 'ranked' name check."""
        if not public_name:
            return False
        lowered = public_name.lower()
        return any(hint in lowered for hint in cls.PVE_PLAYLIST_NAME_HINTS)

    def _classify_match_category(
        self,
        playlist_asset_id: Optional[str],
        playlist_version_id: Optional[str],
        playlist_info: Optional[Dict],
        match_info: Optional[Dict],
        metadata_is_ranked: Optional[bool] = None,
        metadata_is_pve: Optional[bool] = None,
    ) -> Tuple[str, bool, str]:
        """Classify match type for filtering and analytics compatibility.

        metadata_is_ranked comes from the playlist_metadata cache (see
        _lookup_or_resolve_playlist_ranked) - a live "ranked" substring match
        against the playlist's real PublicName, resolved via the
        discovery-infiniteugc API. RANKED_PLAYLIST_IDS is checked first as a
        zero-network fast path for already-known IDs, but Halo Infinite
        rotates playlist asset IDs across seasons, so that static set alone
        misses newer/reworked ranked playlists - metadata_is_ranked=True
        catches those. False/None means "no positive signal from the cache",
        not "confirmed not ranked" - the existing heuristics below still run.

        metadata_is_pve is the parallel PvE (Firefight) signal from the same
        cache (a PVE_PLAYLIST_NAME_HINTS substring match on PublicName). PvE
        co-op matches are bucketed as 'custom' so they're excluded from PvP
        'overall'/'social' aggregates - kept distinguishable via the
        'pve_firefight' category_source. False/None means "no PvE signal".
        """
        playlist_id = (playlist_asset_id or "").strip()
        playlist_id_lower = playlist_id.lower()

        if playlist_id_lower in self.RANKED_PLAYLIST_IDS:
            return "ranked", True, "playlist_map"

        if metadata_is_ranked is True:
            return "ranked", True, "playlist_metadata"

        # PvE/Firefight -> reuse the 'custom' bucket (excluded from every PvP
        # aggregate) but tag the source so these stay identifiable. Checked
        # before the `if playlist_id: return "social"` default below, since
        # Firefight playlists carry a real playlist_id.
        if metadata_is_pve is True:
            return "custom", False, "pve_firefight"

        playlist_name = ""
        if isinstance(playlist_info, dict):
            playlist_name = str(
                playlist_info.get('Name')
                or playlist_info.get('PlaylistName')
                or playlist_info.get('DisplayName')
                or ""
            ).strip()

        explicit_custom_flag = self._extract_explicit_custom_flag(match_info)
        if explicit_custom_flag is True:
            return "custom", False, "explicit_custom_flag"

        game_variant_category = None
        lifecycle_mode = None
        playlist_experience = None
        playlist_map_mode_pair = None

        signal_components: List[str] = []
        if isinstance(match_info, dict):
            game_variant_category = self._coerce_intish(match_info.get('GameVariantCategory'))
            lifecycle_mode = self._coerce_intish(match_info.get('LifecycleMode'))
            playlist_experience = match_info.get('PlaylistExperience')
            playlist_map_mode_pair = match_info.get('PlaylistMapModePair')

            for field in (
                'MatchType',
                'Category',
                'Mode',
                'GameMode',
                'Experience',
                'QueueType',
                'PlaylistCategory',
                'Activity',
                'GameVariantCategory',
                'LifecycleMode',
                'PlaylistExperience',
                'GameplayInteraction',
            ):
                value = str(match_info.get(field) or "").strip()
                if value:
                    signal_components.append(value)

            game_variant = match_info.get('GameVariant')
            if isinstance(game_variant, dict):
                for field in ('Name', 'AssetId', 'VersionId'):
                    value = str(game_variant.get(field) or "").strip()
                    if value:
                        signal_components.append(value)

        signal_text = " ".join(
            [
                playlist_id_lower,
                str(playlist_version_id or "").lower(),
                playlist_name.lower(),
                " ".join(component.lower() for component in signal_components),
            ]
        )

        if not playlist_id and game_variant_category in self.CUSTOM_GAME_VARIANT_CATEGORIES:
            if lifecycle_mode in self.CUSTOM_LIFECYCLE_MODES:
                return "custom", False, "matchinfo_structural"

            if playlist_experience is None and not playlist_map_mode_pair:
                return "custom", False, "matchinfo_structural"

        # Authoritative matchmade-vs-custom signal: LifecycleMode. Matchmade
        # games (ranked AND social alike) are LifecycleMode=3 and always carry
        # a Playlist; custom/local lobbies are LifecycleMode=1 with no Playlist
        # (verified live against the match-stats MatchInfo). A private lobby can
        # run ranked *game modes* (a "Ranked Slayer" variant etc.), so catch
        # customs by lifecycle here - BEFORE the "ranked" name heuristic below -
        # or such a match would be mislabeled ranked and counted in ranked
        # stats. There is no per-match isRanked flag; ranked is a property of
        # the matchmade playlist, which a custom by definition doesn't have.
        if not playlist_id and lifecycle_mode in self.CUSTOM_LIFECYCLE_MODES:
            return "custom", False, "matchinfo_lifecycle"

        # Only a matchmade game (which always has a Playlist) can be ranked, so
        # require playlist_id here - a custom whose variant is merely *named*
        # "Ranked ..." must never be classified ranked by name alone.
        if playlist_id and self.RANKED_NAME_RE.search(signal_text):
            return "ranked", True, "text_heuristic"

        if any(token in signal_text for token in self.CUSTOM_MATCH_TEXT_HINTS):
            return "custom", False, "text_heuristic"

        if playlist_id:
            return "social", False, "default_non_ranked"

        if any(token in signal_text for token in self.SOCIAL_MATCH_TEXT_HINTS):
            return "social", False, "missing_playlist_text"

        return "custom", False, "missing_playlist_fallback"

    async def resolve_playlist_metadata(
        self,
        asset_id: str,
        version_id: Optional[str],
        session: aiohttp.ClientSession,
    ) -> Dict:
        """
        Resolve a playlist's PublicName via discovery-infiniteugc and derive
        ranked status from a case-insensitive "ranked" substring match.

        The inline MatchInfo.Playlist object on a match never carries a name
        field (only AssetKind/AssetId/VersionId), and the response here has
        no HasCsr boolean (confirmed live against the real API) - so
        PublicName text-matching is the only signal available. Response keys
        observed: Admin, AssetHome, AssetId, AssetStats, CloneBehavior,
        Contributors, CustomData, Description, DisplayOwnerOverride, Files,
        InspectionResult, Order, PublicName, PublishedDate, RotationEntries,
        Tags, VersionId, VersionNumber.

        Tries the versioned URL first (if version_id given), then falls back
        to the unversioned form - both resolve correctly per live testing,
        and the backfill script (which only has historical asset ids, no
        stored version id) relies entirely on the unversioned form.

        This is a best-effort, secondary lookup that must never take down the
        primary match-stats fetch it's called from (get_match_stats_for_match)
        - the entire body, including rate-limiter/token setup, is wrapped so
        that any failure here (including exhausting a test's or a real
        rate-limiter backoff) degrades to 'error' instead of propagating.

        Never raises. Always returns {'public_name', 'is_ranked', 'resolution_status'}.
        """
        try:
            account_index = await halo_stats_rate_limiter.wait_if_needed()
            spartan_token = self.get_next_spartan_token(account_index)
            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                spartan_token = spartan_token['token']

            headers = {
                "Authorization": f"Spartan {spartan_token}",
                "x-343-authorization-spartan": spartan_token,
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            }
            # Not required (confirmed 200 without it live) but sent
            # opportunistically for consistency with get_player_stats' headers.
            if self.clearance_token and self.clearance_token != "skip":
                headers["x-343-authorization-clearance"] = self.clearance_token

            urls = []
            if version_id:
                urls.append(f"{self.DISCOVERY_UGC_URL}/hi/playlists/{asset_id}/versions/{version_id}")
            urls.append(f"{self.DISCOVERY_UGC_URL}/hi/playlists/{asset_id}")

            last_status = 'error'
            for url in urls:
                try:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            public_name = str(data.get('PublicName') or '').strip()
                            return {
                                'public_name': public_name or None,
                                'is_ranked': bool(self.RANKED_NAME_RE.search(public_name.lower())),
                                'is_pve': self._public_name_is_pve(public_name),
                                'resolution_status': 'resolved',
                            }
                        elif response.status == 404:
                            last_status = 'not_found'
                            continue
                        elif response.status == 429:
                            retry_after = response.headers.get('Retry-After')
                            wait_time = int(retry_after) if retry_after and retry_after.isdigit() else 3
                            halo_stats_rate_limiter.set_backoff(wait_time, account_index)
                            last_status = 'error'
                            continue
                        else:
                            last_status = 'error'
                            continue
                except Exception as e:
                    print(f"[PLAYLIST] Error resolving {asset_id}: {e}")
                    last_status = 'error'
                    continue

            return {'public_name': None, 'is_ranked': False, 'is_pve': False, 'resolution_status': last_status}
        except Exception as e:
            print(f"[PLAYLIST] Error resolving {asset_id}: {e}")
            return {'public_name': None, 'is_ranked': False, 'is_pve': False, 'resolution_status': 'error'}

    async def _lookup_or_resolve_playlist_ranked(
        self,
        playlist_asset_id: Optional[str],
        playlist_version_id: Optional[str],
        session: aiohttp.ClientSession,
    ) -> Tuple[Optional[bool], Optional[bool]]:
        """
        Consult (and, on a cache miss, populate) playlist_metadata to
        determine whether playlist_asset_id is ranked and/or PvE (Firefight).
        Returns a (is_ranked, is_pve) tuple: each is True/False once resolved,
        None if still unresolvable (no asset id, confirmed 404, or a fresh
        network failure) - callers treat None as "no signal, fall back to
        _classify_match_category's other heuristics".

        Zero-network for any asset id already cached 'resolved' or
        'not_found'. In steady state this only costs a network round trip
        the first time any player's match ever references a given asset id
        anywhere in the bot's history (there are only a few dozen distinct
        playlists active at once, not one per match), which is what makes
        this self-healing at ingest time without waiting for a manual
        backfill run when a playlist rotates.
        """
        if not playlist_asset_id:
            return None, None
        asset_id = playlist_asset_id.strip()
        if asset_id.lower() in self.RANKED_PLAYLIST_IDS:
            return None, None  # already covered by the zero-DB-read fast path

        cached = self.stats_cache.db.get_playlist_metadata(asset_id)
        if cached:
            if cached['resolution_status'] == 'resolved':
                return bool(cached['is_ranked']), self._public_name_is_pve(cached['public_name'])
            if cached['resolution_status'] == 'not_found':
                return None, None  # confirmed unresolvable; don't hammer it every match

        resolved = await self.resolve_playlist_metadata(asset_id, playlist_version_id, session)
        await asyncio.get_running_loop().run_in_executor(
            self._db_write_executor,
            self.stats_cache.db.upsert_playlist_metadata,
            asset_id, resolved['public_name'], resolved['is_ranked'],
            resolved['resolution_status'], playlist_version_id,
        )
        if resolved['resolution_status'] == 'resolved':
            return resolved['is_ranked'], resolved['is_pve']
        return None, None

    async def get_match_stats_for_match(
        self,
        match_id: str,
        player_xuid: str,
        session: aiohttp.ClientSession
    ) -> Optional[Dict]:
        """
        Get detailed stats for a specific match.
        
        Args:
            match_id: Match identifier
            player_xuid: Player's XUID
            session: Shared aiohttp session
        
        Returns:
            Match stats dictionary or None on error
        """
        try:
            # Apply per-account rate limiting and get the account to use
            account_index = await halo_stats_rate_limiter.wait_if_needed()
            
            # Use specific account's Spartan token
            spartan_token = self.get_next_spartan_token(account_index)
            if not spartan_token:
                return None
                
            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                spartan_token = spartan_token['token']
            
            headers = {
                "Authorization": f"Spartan {spartan_token}",
                "x-343-authorization-spartan": spartan_token,
                "User-Agent": self.user_agent,
                "Accept": "application/json"
            }
            
            stats_url = f"https://halostats.svc.halowaypoint.com/hi/matches/{match_id}/stats"
            
            # Retry logic for transient 500 and 429 errors
            max_retries = 2
            max_rate_limit_retries = 3
            stats_data = None
            
            for retry in range(max_retries):
                async with session.get(stats_url, headers=headers) as response:
                    if response.status == 200:
                        try:
                            stats_data = await response.json()
                        except Exception as json_error:
                            print(f"Match {match_id}: JSON parsing error - {json_error}")
                            return None
                        
                        # Check if API returned None or invalid data
                        if stats_data is None or not isinstance(stats_data, dict):
                            print(f"Match {match_id}: API returned invalid data (None or not a dict)")
                            return None
                        
                        # Success - break out of retry loop
                        break
                    elif response.status == 429:
                        # Rate limited - set backoff for this specific account
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                wait_time = int(retry_after)
                            except ValueError:
                                wait_time = 2 ** retry + 3
                        else:
                            wait_time = 2 ** retry + 3  # 4s, 7s
                        
                        # Set backoff for THIS account only
                        halo_stats_rate_limiter.set_backoff(wait_time, account_index)
                        print(f"⚠️ Match {match_id}: Rate limited (429) on account {account_index}, switching account...")
                        
                        # Get a different account and retry
                        account_index = await halo_stats_rate_limiter.wait_if_needed()
                        spartan_token = self.get_next_spartan_token(account_index)
                        if isinstance(spartan_token, dict) and 'token' in spartan_token:
                            spartan_token = spartan_token['token']
                        headers["Authorization"] = f"Spartan {spartan_token}"
                        headers["x-343-authorization-spartan"] = spartan_token
                        continue
                    elif response.status == 500:
                        # Server error - retry with different account
                        if retry < max_retries - 1:
                            await asyncio.sleep(0.3)
                            account_index = await halo_stats_rate_limiter.wait_if_needed()
                            spartan_token = self.get_next_spartan_token(account_index)
                            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                                spartan_token = spartan_token['token']
                            headers["Authorization"] = f"Spartan {spartan_token}"
                            headers["x-343-authorization-spartan"] = spartan_token
                            continue
                        else:
                            # Max retries reached
                            return None
                    else:
                        # Other error - don't retry
                        return None
            
            # Process the successful response (stats_data is set if status was 200)
            if not stats_data:
                return None
                
            players = stats_data.get('Players', [])
            if not players:
                # No player data in response
                return None
            
            # Extract XUIDs of all players in the match
            player_xuids = []
            all_participants = []
            for p in players:
                player_id = p.get('PlayerId', '')
                # Extract XUID from format: 'xuid(2533274924643541)'
                if 'xuid(' in player_id:
                    xuid_str = player_id.replace('xuid(', '').replace(')', '')
                    player_xuids.append(xuid_str)

                    participant_team_stats = p.get('PlayerTeamStats', [])
                    participant_stats_obj = {}
                    participant_core_stats = {}
                    if participant_team_stats and isinstance(participant_team_stats, list):
                        first_team_stats = participant_team_stats[0] if participant_team_stats else {}
                        if isinstance(first_team_stats, dict):
                            participant_stats_obj = first_team_stats.get('Stats') or {}
                    if isinstance(participant_stats_obj, dict):
                        participant_core_stats = participant_stats_obj.get('CoreStats') or {}

                    explicit_team_id = (
                        p.get('TeamId')
                        or p.get('TeamID')
                        or p.get('teamId')
                        or p.get('team_id')
                    )
                    if explicit_team_id is None and participant_team_stats and isinstance(participant_team_stats, list):
                        first_team_stats = participant_team_stats[0] if participant_team_stats else {}
                        if isinstance(first_team_stats, dict):
                            explicit_team_id = (
                                first_team_stats.get('TeamId')
                                or first_team_stats.get('TeamID')
                                or first_team_stats.get('teamId')
                                or first_team_stats.get('team_id')
                            )

                    participant_outcome = int(p.get('Outcome', 0) or 0)
                    inferred_team_id = None
                    if explicit_team_id is None and participant_outcome:
                        inferred_team_id = f"outcome:{participant_outcome}"

                    participant_csr, participant_csr_tier = self._extract_csr_and_tier(p)

                    all_participants.append(
                        {
                            'xuid': xuid_str,
                            'gamertag': p.get('Gamertag') or p.get('PlayerName') or p.get('DisplayName'),
                            'outcome': participant_outcome,
                            'team_id': str(explicit_team_id) if explicit_team_id is not None else None,
                            'inferred_team_id': inferred_team_id,
                            'kills': participant_core_stats.get('Kills', 0),
                            'deaths': participant_core_stats.get('Deaths', 0),
                            'assists': participant_core_stats.get('Assists', 0),
                            'csr': participant_csr,
                            'csr_tier': participant_csr_tier,
                        }
                    )
            
            # Find our player's stats
            for player in players:
                player_id = player.get('PlayerId', '')
                if str(player_xuid) in str(player_id):
                    team_stats = player.get('PlayerTeamStats', [])
                    if team_stats and len(team_stats) > 0:
                        # Safely extract nested stats with None checks
                        stats_obj = team_stats[0].get('Stats')
                        if not stats_obj or not isinstance(stats_obj, dict):
                            return None
                            
                        core_stats = stats_obj.get('CoreStats', {})
                        if not core_stats or not isinstance(core_stats, dict):
                            return None
                        
                        match_info = stats_data.get('MatchInfo', {})
                        if not match_info or not isinstance(match_info, dict):
                            return None
                        
                        # Extract playlist information with None checks
                        playlist_info = match_info.get('Playlist')
                        if playlist_info and isinstance(playlist_info, dict):
                            playlist_asset_id = playlist_info.get('AssetId')
                            playlist_version_id = playlist_info.get('VersionId')
                        else:
                            playlist_asset_id = None
                            playlist_version_id = None
                        
                        # Extract map information with None checks
                        map_info = match_info.get('MapVariant')
                        if map_info and isinstance(map_info, dict):
                            map_asset_id = map_info.get('AssetId')
                            map_version_id = map_info.get('VersionId')
                        else:
                            map_asset_id = None
                            map_version_id = None
                        
                        metadata_is_ranked, metadata_is_pve = await self._lookup_or_resolve_playlist_ranked(
                            playlist_asset_id, playlist_version_id, session
                        )
                        match_category, is_ranked, category_source = self._classify_match_category(
                            playlist_asset_id=playlist_asset_id,
                            playlist_version_id=playlist_version_id,
                            playlist_info=playlist_info,
                            match_info=match_info,
                            metadata_is_ranked=metadata_is_ranked,
                            metadata_is_pve=metadata_is_pve,
                        )

                        # Build match data with playlist information, map, and player XUIDs
                        csr, csr_tier = self._extract_csr_and_tier(player)

                        match_data = {
                            'match_id': match_id,
                            'outcome': player.get('Outcome', 0),  # 2=Win, 3=Loss, 4=DNF
                            'kills': core_stats.get('Kills', 0),
                            'deaths': core_stats.get('Deaths', 0),
                            'assists': core_stats.get('Assists', 0),
                            'start_time': match_info.get('StartTime', ''),
                            'duration': match_info.get('Duration', 'Unknown'),
                            'medals': core_stats.get('Medals', []),
                            'playlist_id': playlist_asset_id,
                            'playlist_version': playlist_version_id,
                            'is_ranked': is_ranked,
                            'match_category': match_category,
                            'category_source': category_source,
                            'csr': csr,
                            'csr_tier': csr_tier,
                            'map_id': map_asset_id,
                            'map_version': map_version_id,
                            'players': player_xuids,
                            'all_participants': all_participants,
                        }
                        return match_data
            
            # If we get here, player wasn't found in match
            # This can happen if the match data is incomplete
            return None
        except Exception as e:
            print(f"Error getting match stats for {match_id}: {e}")
        
        return None
    
    # =========================================================================
    # LIGHTWEIGHT HALO ACTIVITY CHECK
    # =========================================================================
    
    async def check_recent_halo_activity(
        self,
        xuid: str,
        cutoff_date: datetime = None
    ) -> tuple[bool, Optional[datetime]]:
        """
        Lightweight check if a player has recent Halo Infinite activity.
        
        Fetches only the most recent match and checks if it was played
        after the cutoff date. Much faster than full stats calculation.
        
        Args:
            xuid: Player's XUID
            cutoff_date: Minimum date for "recent" activity. 
                        Defaults to September 1, 2025.
        
        Returns:
            Tuple of (is_recently_active: bool, last_match_date: datetime | None)
            - is_recently_active: True if player has a match >= cutoff_date
            - last_match_date: The date of their most recent match, or None if no matches
        """
        if cutoff_date is None:
            cutoff_date = datetime(2025, 9, 1)

        def _normalize_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
            """Convert datetime to UTC naive for safe comparisons."""
            if dt is None:
                return None
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        
        try:
            async with aiohttp.ClientSession() as session:
                # Step 1: Fetch just 1 match from the matches list endpoint
                matches_url = f"https://halostats.svc.halowaypoint.com/hi/players/xuid({xuid})/matches?start=0&count=1"
                
                # Get account and headers
                account_index = await halo_stats_rate_limiter.wait_if_needed()
                spartan_token = self.get_next_spartan_token(account_index)
                if not spartan_token:
                    return (False, None)
                    
                if isinstance(spartan_token, dict) and 'token' in spartan_token:
                    spartan_token = spartan_token['token']
                
                headers = {
                    "Authorization": f"Spartan {spartan_token}",
                    "x-343-authorization-spartan": spartan_token,
                    "User-Agent": self.user_agent,
                    "Accept": "application/json"
                }
                
                # Retry logic for 401/429
                max_account_retries = len(self.spartan_accounts) if self.spartan_accounts else 1
                for attempt in range(max_account_retries):
                    async with session.get(matches_url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get('Results', [])
                            
                            if not results:
                                # No matches = not a Halo player (or brand new)
                                return (False, None)
                            
                            # Get the first (most recent) match ID
                            match_id = results[0].get('MatchId')
                            if not match_id:
                                return (False, None)
                            
                            # Step 2: Fetch match stats to get the date
                            last_match_date = await self._get_match_date(match_id, session)
                            
                            if last_match_date is None:
                                # Couldn't get date, assume not recent
                                return (False, None)
                            
                            # Check if match is after cutoff
                            is_recent = _normalize_utc_naive(last_match_date) >= _normalize_utc_naive(cutoff_date)
                            return (is_recent, last_match_date)
                            
                        elif response.status == 401:
                            # Rotate to next account
                            account_index = (account_index + 1) % max_account_retries
                            spartan_token = self.get_next_spartan_token(account_index)
                            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                                spartan_token = spartan_token['token']
                            headers["Authorization"] = f"Spartan {spartan_token}"
                            headers["x-343-authorization-spartan"] = spartan_token
                            await asyncio.sleep(0.3)
                            continue
                            
                        elif response.status == 429:
                            # Rate limited - wait and retry
                            retry_after = response.headers.get('Retry-After', '5')
                            wait_time = int(retry_after) if retry_after.isdigit() else 5
                            halo_stats_rate_limiter.set_backoff(wait_time, account_index)
                            await asyncio.sleep(1)
                            continue
                        else:
                            # Other error
                            return (False, None)
                
                # Exhausted retries
                return (False, None)
                
        except Exception as e:
            print(f"[API] Error checking Halo activity for {xuid}: {e}")
            return (False, None)
    
    async def _get_match_date(
        self,
        match_id: str,
        session: aiohttp.ClientSession
    ) -> Optional[datetime]:
        """
        Get the start date of a specific match.
        
        Args:
            match_id: The match ID to look up
            session: Shared aiohttp session
            
        Returns:
            datetime of match start, or None on error
        """
        try:
            stats_url = f"https://halostats.svc.halowaypoint.com/hi/matches/{match_id}/stats"
            
            account_index = await halo_stats_rate_limiter.wait_if_needed()
            spartan_token = self.get_next_spartan_token(account_index)
            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                spartan_token = spartan_token['token']
            
            headers = {
                "Authorization": f"Spartan {spartan_token}",
                "x-343-authorization-spartan": spartan_token,
                "User-Agent": self.user_agent,
                "Accept": "application/json"
            }
            
            async with session.get(stats_url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    match_info = data.get('MatchInfo', {})
                    start_time_str = match_info.get('StartTime', '')
                    
                    if start_time_str:
                        # Parse ISO format: "2025-12-15T18:30:00.000Z"
                        # Handle various formats
                        try:
                            # Remove 'Z' and parse
                            if start_time_str.endswith('Z'):
                                start_time_str = start_time_str[:-1]
                            # Handle milliseconds if present
                            if '.' in start_time_str:
                                return datetime.fromisoformat(start_time_str.split('.')[0])
                            return datetime.fromisoformat(start_time_str)
                        except ValueError:
                            return None
                    return None
                else:
                    return None
                    
        except Exception as e:
            print(f"[API] Error getting match date for {match_id}: {e}")
            return None
    
    # =========================================================================
    # COMPREHENSIVE STATS CALCULATION
    # =========================================================================
    
    async def calculate_comprehensive_stats(
        self,
        xuid: str,
        stat_type: str,
        gamertag: str = None,
        matches_to_process: int = 10,
        force_full_fetch: bool = False,
        _retry_count: int = 0
    ) -> Dict:
        """
        Calculate comprehensive stats from match history.
        
        Fetches match history and calculates aggregate statistics for
        overall, ranked, and social game modes.
        
        Args:
            xuid: Player XUID
            stat_type: "overall", "ranked", or "social"
            gamertag: Player gamertag (optional)
            matches_to_process: Number of matches to analyze (None = all)
            force_full_fetch: Ignore cache and fetch all matches
            _retry_count: Internal retry counter
        
        Returns:
            Dictionary containing calculated stats or error info
        """
        if matches_to_process is None:
            matches_to_process = 999999  # Process all matches
        full_history_requested = matches_to_process >= 999999
        try:
            # Check cache first. Runs in the default executor, not on the event
            # loop, since a cold-cache read on the HDD-backed DB can take
            # seconds to minutes and would otherwise stall the Discord gateway
            # heartbeat.
            cached_data = await asyncio.get_running_loop().run_in_executor(
                None, self.load_cached_stats, xuid, stat_type, gamertag
            )
            last_update = None
            existing_matches = {}
            cache_marked_incomplete = False
            
            # Check if cache is sufficient for the request
            cache_is_sufficient = False
            history_is_fresh = False
            if cached_data:
                last_update = cached_data.get('last_update')
                existing_matches = {m['match_id']: m for m in cached_data.get('processed_matches', [])}
                cache_marked_incomplete = bool(cached_data.get('incomplete_data')) or int(cached_data.get('failed_match_count') or 0) > 0
                cached_games = len(existing_matches)
                print(f"Last cache update: {last_update}")
                print(f"Cache contains {cached_games} matches")
                
                # If requesting all matches (999999) but cache has fewer matches than that,
                # we should check if there are more matches available
                # For now, if cache has at least 25 matches and we want all matches,
                # we'll do an incremental fetch to check for new ones
                if full_history_requested:
                    # Requesting all matches - check for new ones, unless a
                    # history check for this xuid completed within the
                    # freshness TTL, in which case serve cache with zero API
                    # calls. force_full_fetch and incomplete caches bypass.
                    checked = self._history_checked_at.get(xuid)
                    age = (time.monotonic() - checked) if checked is not None else None
                    if (not force_full_fetch and not cache_marked_incomplete
                            and age is not None
                            and age < STATS_HISTORY_FRESHNESS_TTL_SECONDS):
                        print(f"History checked {age:.0f}s ago "
                              f"(< {STATS_HISTORY_FRESHNESS_TTL_SECONDS}s TTL), serving cache")
                        cache_is_sufficient = True
                        history_is_fresh = True
                    else:
                        print(f"Full match history requested, will check for updates...")
                        cache_is_sufficient = False
                elif cache_marked_incomplete:
                    print("Cache is marked incomplete, fetching additional match history...")
                    cache_is_sufficient = False
                elif cached_games >= matches_to_process:
                    # Cache has enough matches
                    print(f"Cache has {cached_games} matches, sufficient for request of {matches_to_process}")
                    cache_is_sufficient = True
                else:
                    # Cache doesn't have enough matches
                    print(f"Cache has {cached_games} matches but {matches_to_process} requested, will fetch more...")
                    cache_is_sufficient = False
            
            # If cache is sufficient and we're not requesting ALL matches (or
            # the history check is still fresh), return cached data immediately
            if cache_is_sufficient and (history_is_fresh or not full_history_requested):
                print(f"Using cached data ({len(existing_matches)} matches)")
                cached_matches = cached_data.get('processed_matches', [])
                # Prefer the precomputed per-mode summary over rescanning the
                # cached matches in Python; falls back for players not yet
                # covered by the player_mode_stats backfill.
                stats = self.stats_cache.get_player_mode_summary(xuid, stat_type)
                if stats is None:
                    stats = self._calculate_stats_from_matches(cached_matches, stat_type)
                return {
                    'error': 0,
                    'stats': stats,
                    'matches_processed': len(existing_matches),
                    'new_matches': 0,
                    'processed_matches': cached_matches
                }
            
            # Get match history
            if not self.spartan_token and not self.spartan_accounts:
                return {"error": 4, "message": "No authentication token"}
                
            # Function to get headers for a specific account
            def get_headers_for_account(account_index: int):
                """Get headers with specific account's Spartan token"""
                spartan_token = self.get_next_spartan_token(account_index)
                if isinstance(spartan_token, dict) and 'token' in spartan_token:
                    spartan_token = spartan_token['token']
                return {
                    "Authorization": f"Spartan {spartan_token}",
                    "x-343-authorization-spartan": spartan_token,
                    "User-Agent": self.user_agent,
                    "Accept": "application/json"
                }

            def extract_total_matches_count(payload: Dict) -> Tuple[Optional[int], Optional[str], bool]:
                """Best-effort lifetime total extraction with reliability metadata."""
                reliable_keys = ("TotalCount", "totalCount", "TotalResults", "totalResults")
                ambiguous_keys = ("ResultCount", "Count", "count")

                for key in reliable_keys:
                    value = payload.get(key)
                    if isinstance(value, int) and value >= 0:
                        return value, key, True

                for key in ambiguous_keys:
                    value = payload.get(key)
                    if isinstance(value, int) and value >= 0:
                        return value, key, False

                return None, None, False
            
            async def fetch_match_page(session, start_pos, page_size=25, retry_count=0, account_retry=0, error_retry=0, rate_limit_retry=0, force_account=None):
                """Fetch a single page of matches with retry logic for socket errors, account rotation, 500 and 429 errors"""
                matches_url = f"https://halostats.svc.halowaypoint.com/hi/players/xuid({xuid})/matches?start={start_pos}&count={page_size}"
                max_retries = 3
                max_account_retries = len(self.spartan_accounts) if self.spartan_accounts else 1
                max_error_retries = 2  # Retry 500 errors up to 2 times
                max_rate_limit_retries = 5  # Retry 429 errors with exponential backoff
                
                try:
                    # Apply per-account rate limiting and get the account to use
                    account_index = await halo_stats_rate_limiter.wait_if_needed(force_account)
                    
                    # Get headers for the specific account
                    headers = get_headers_for_account(account_index)
                    async with session.get(matches_url, headers=headers) as response:
                        if start_pos == 0:
                            print(f"Fetching matches for XUID: {xuid} (gamertag: {gamertag or 'unknown'})")
                        
                        if response.status == 200:
                            match_data = await response.json()
                            results = match_data.get('Results', [])
                            if start_pos == 0:
                                print(f"   First page: {len(results)} matches found")
                            return results
                        elif response.status == 429:
                            # Rate limited - use proper exponential backoff to avoid ban
                            if rate_limit_retry < max_rate_limit_retries:
                                # Check for Retry-After header (API may specify wait time)
                                retry_after = response.headers.get('Retry-After')
                                if retry_after:
                                    try:
                                        wait_time = max(int(retry_after), 30)  # Minimum 30s
                                    except ValueError:
                                        wait_time = 30 * (2 ** rate_limit_retry)  # 30s, 60s, 120s, 240s, 480s
                                else:
                                    # Conservative exponential backoff: 30s, 60s, 120s, 240s, 480s
                                    wait_time = 30 * (2 ** rate_limit_retry)
                                
                                # Set backoff for THIS account
                                halo_stats_rate_limiter.set_backoff(wait_time, account_index)
                                
                                # Also set a shorter global backoff to slow down ALL requests
                                global_backoff = 5 * (rate_limit_retry + 1)  # 5s, 10s, 15s, 20s, 25s
                                halo_stats_rate_limiter.set_backoff(global_backoff, account_index=None)
                                
                                print(f"⚠️ Rate limited (429) at page {start_pos} on account {account_index}, waiting {wait_time}s (attempt {rate_limit_retry + 1}/{max_rate_limit_retries})...")
                                
                                # Actually WAIT the backoff time before retrying
                                await asyncio.sleep(wait_time)
                                
                                # Retry with same account (it's now had time to cool down)
                                return await fetch_match_page(session, start_pos, page_size, retry_count, account_retry, error_retry, rate_limit_retry + 1, force_account=account_index)
                            else:
                                print(f"❌ Rate limit exceeded after {max_rate_limit_retries} retries at page {start_pos}")
                                return _PAGE_FETCH_FAILED  # Failure, not end-of-history
                        elif response.status == 401:
                            # Try rotating to next account instead of refreshing tokens
                            if account_retry < max_account_retries:
                                print(f"401 error, rotating to next account (attempt {account_retry + 1}/{max_account_retries})...")
                                # get_headers() will automatically use next account due to round-robin
                                await asyncio.sleep(0.5)  # Small delay before retry
                                return await fetch_match_page(session, start_pos, page_size, retry_count, account_retry + 1, error_retry, rate_limit_retry)
                            else:
                                print(f"401 Unauthorized after trying all accounts - need token refresh")
                                text = await response.text()
                                print(f"   Response: {text[:200]}")
                                # Signal that we got 401 error after trying all accounts
                                return None
                        elif response.status == 500:
                            # Server error - retry with different account
                            if error_retry < max_error_retries:
                                await asyncio.sleep(0.3)
                                # Will automatically get a different account via rate limiter
                                return await fetch_match_page(session, start_pos, page_size, retry_count, account_retry, error_retry + 1, rate_limit_retry, force_account=None)
                            else:
                                # Max retries reached - failure, not end-of-history
                                return _PAGE_FETCH_FAILED
                        else:
                            print(f"Unexpected status: {response.status}")
                            text = await response.text()
                            print(f"   Response: {text[:200]}")
                            return _PAGE_FETCH_FAILED
                except OSError as e:
                    # Handle Windows semaphore timeout errors (WinError 121)
                    if 'semaphore timeout' in str(e).lower() or 'WinError 121' in str(e):
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count  # Exponential backoff: 1s, 2s, 4s
                            print(f"Socket timeout at page {start_pos}, retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})...")
                            await asyncio.sleep(wait_time)
                            return await fetch_match_page(session, start_pos, page_size, retry_count + 1, account_retry, error_retry, rate_limit_retry, force_account=None)
                        else:
                            print(f"Failed after {max_retries} retries due to socket exhaustion at page {start_pos}")
                            return _PAGE_FETCH_FAILED
                    else:
                        print(f"OS Error fetching page at {start_pos}: {e}")
                        return _PAGE_FETCH_FAILED
                except aiohttp.ClientConnectorError as e:
                    # Handle connection errors (including semaphore timeouts)
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count
                        print(f"Connection error at page {start_pos}, retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})...")
                        await asyncio.sleep(wait_time)
                        return await fetch_match_page(session, start_pos, page_size, retry_count + 1, account_retry, error_retry, rate_limit_retry, force_account=None)
                    else:
                        print(f"Failed after {max_retries} retries: {e}")
                        return _PAGE_FETCH_FAILED
                except Exception as e:
                    print(f"Error fetching page at {start_pos}: {e}")
                return _PAGE_FETCH_FAILED
            
            # Fetch multiple pages concurrently to determine total match count
            # Connection limits - conservative to avoid rate limiting
            connector = aiohttp.TCPConnector(
                limit=50,              # Global connection limit - reduced to avoid overwhelming API
                limit_per_host=30,     # Per-host limit - conservative to respect rate limits
                ttl_dns_cache=300,     # DNS cache for 5 minutes
                force_close=False,     # Reuse connections
                enable_cleanup_closed=True
            )
            # INCREASED TIMEOUT: Halo API can be slow, especially for detailed match stats
            timeout = aiohttp.ClientTimeout(total=180, connect=30, sock_connect=30, sock_read=120)
            
            # Page size - API seems to cap at 25 matches per request regardless of count parameter
            PAGE_SIZE = 25  # Matches per request (API maximum)
            
            # Initialize 401 error tracking
            got_401_error = False
            # Set True if a full-crawl page listing stays failed after the repair
            # pass = an "unknown gap" (we don't know which match IDs are missing).
            # Marks the cache incomplete so the next full-history check re-crawls.
            page_fetch_incomplete = False

            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Decide fetching strategy: incremental vs full refetch
                # Force full fetch ignores cache completely.
                # Otherwise use incremental fetch if cache exists
                print(f"Fetch params: force_full_fetch={force_full_fetch}, cached_data={'exists' if cached_data else 'none'}, matches_to_process={matches_to_process}")
                total_matches_hint = None
                total_matches_hint_source = None
                total_matches_hint_reliable = False
                
                if force_full_fetch:
                    print(f"Force full fetch enabled - ignoring cache and fetching all matches...")
                    use_incremental = False
                    existing_matches = {}  # Ignore cache completely
                elif cached_data and existing_matches:
                    use_incremental = True
                    print(f"Using incremental fetch (cache has {len(existing_matches)} matches)")
                else:
                    use_incremental = False
                    print(f"🆕 No cache, doing full fetch")

                # An incomplete cache has two distinct repairs:
                #   - Known failed match IDs (details never saved): refetch exactly
                #     those below, alongside the normal incremental check.
                #   - Unknown gap (a page listing failed, IDs unknown): only a full
                #     re-crawl can rediscover the missing matches.
                repair_ids = []
                if use_incremental and cache_marked_incomplete and not force_full_fetch:
                    known_failed = [
                        mid for mid in (cached_data.get('failed_matches') or [])
                        if mid and mid not in existing_matches
                    ]
                    if known_failed:
                        repair_ids = known_failed
                        print(f"Cache incomplete with {len(repair_ids)} known failed match(es); "
                              f"will attempt targeted repair (no full re-crawl)")
                    else:
                        print("Cache incomplete with unknown gap, forcing full re-crawl")
                        use_incremental = False

                # Smart cache update: fetch pages until we hit a cached match
                if use_incremental:
                    print(f"Checking for new matches (incremental fetch)...")
                    new_matches_found = []
                    page_num = 0
                    max_pages_to_check = 1000  # Increased limit (10000 matches max with PAGE_SIZE=100)
                    found_cached_boundary = False
                    reached_history_end = False
                    
                    while page_num < max_pages_to_check:
                        start_pos = page_num * PAGE_SIZE
                        page = await fetch_match_page(session, start_pos, PAGE_SIZE)
                        
                        if page is None:
                            # Got 401 error - need to refresh token
                            got_401_error = True
                            break

                        if page is _PAGE_FETCH_FAILED:
                            # Page listing failed after retries. This is NOT
                            # end-of-history: leave reached_history_end False so
                            # the incremental result stays unproven and falls back
                            # safely. (Distinguishes a page-0 failure from a genuine
                            # empty first page = "no match history".)
                            print(f"Page {page_num} fetch failed, incremental scan cannot prove completeness")
                            break

                        if not page:
                            # Genuine empty page:
                            # 1. Player has 0 matches (first page, valid scenario)
                            # 2. Reached end of matches (later page)
                            if page_num == 0:
                                # First page empty = player has no match history, not an error
                                print(f"Player has no match history")
                                reached_history_end = True
                                break
                            # A later genuine empty page also means end-of-history.
                            reached_history_end = True
                            break

                        if page_num == 0:
                            # Fetch first page payload once to get API total-count metadata if present.
                            try:
                                first_page_url = f"https://halostats.svc.halowaypoint.com/hi/players/xuid({xuid})/matches?start=0&count={PAGE_SIZE}"
                                account_index = await halo_stats_rate_limiter.wait_if_needed()
                                first_headers = get_headers_for_account(account_index)
                                async with session.get(first_page_url, headers=first_headers) as first_response:
                                    if first_response.status == 200:
                                        first_payload = await first_response.json()
                                        extracted_hint, hint_source, hint_reliable = extract_total_matches_count(first_payload)
                                        total_matches_hint_source = hint_source
                                        total_matches_hint_reliable = hint_reliable
                                        total_matches_hint = extracted_hint if hint_reliable else None
                                        if hint_source:
                                            print(
                                                f"Total-count hint key={hint_source}, value={extracted_hint}, reliable={hint_reliable}"
                                            )
                                        else:
                                            print("Total-count hint unavailable in first-page payload")
                            except Exception:
                                # Total-count hint is optional; ignore extraction failures.
                                total_matches_hint = None
                                total_matches_hint_source = None
                                total_matches_hint_reliable = False
                        
                        # Check each match in this page
                        found_cached_match = False
                        for match in page:
                            match_id = match.get('MatchId')
                            if match_id and match_id not in existing_matches:
                                new_matches_found.append(match)
                            else:
                                # Hit a cached match - all older matches are cached
                                found_cached_match = True
                                break
                        
                        # Stop at first cached boundary, or when API indicates end-of-history.
                        if found_cached_match:
                            found_cached_boundary = True
                            break

                        if len(page) < PAGE_SIZE:
                            reached_history_end = True
                            break
                        
                        page_num += 1

                    probe_plan = build_boundary_probe_plan(
                        full_history_requested=full_history_requested,
                        has_cached_matches=bool(existing_matches),
                        reached_history_end=reached_history_end,
                        total_matches_hint=total_matches_hint,
                        cached_match_count=len(existing_matches),
                        new_match_count=len(new_matches_found),
                        cache_marked_incomplete=cache_marked_incomplete,
                    )

                    probe_checked = False
                    probe_found_uncached = False
                    if probe_plan.required:
                        probe_page = await fetch_match_page(session, probe_plan.start, probe_plan.count)
                        if probe_page is None:
                            got_401_error = True
                        elif probe_page is _PAGE_FETCH_FAILED:
                            # Probe fetch failed - inconclusive. Leave probe_checked
                            # False so completeness stays unproven and falls back safely.
                            print("Boundary probe fetch failed; leaving completeness unproven")
                        else:
                            probe_checked = True
                            probe_match_id = probe_page[0].get('MatchId') if probe_page else None
                            known_match_ids = set(existing_matches.keys())
                            known_match_ids.update(
                                match.get('MatchId')
                                for match in new_matches_found
                                if match.get('MatchId')
                            )
                            if probe_match_id and probe_match_id not in known_match_ids:
                                probe_found_uncached = True

                    reached_search_cap = (
                        page_num >= max_pages_to_check - 1
                        and not found_cached_boundary
                        and not reached_history_end
                    )
                    sync_decision = decide_full_history_sync(
                        full_history_requested=full_history_requested,
                        total_matches_hint=total_matches_hint,
                        cached_match_count=len(existing_matches),
                        new_match_count=len(new_matches_found),
                        found_cached_boundary=found_cached_boundary,
                        reached_history_end=reached_history_end,
                        reached_search_cap=reached_search_cap,
                        probe_required=probe_plan.required,
                        probe_checked=probe_checked,
                        probe_found_uncached=probe_found_uncached,
                        probe_start=probe_plan.start,
                        cache_marked_incomplete=cache_marked_incomplete,
                    )

                    if full_history_requested:
                        print(
                            "History sync check: "
                            f"boundary_found={sync_decision.boundary_found}, "
                            f"probe_checked={sync_decision.probe_checked}, "
                            f"completeness_proven={sync_decision.completeness_proven}, "
                            f"cache_plus_new={sync_decision.cache_plus_new}, "
                            f"cache_marked_incomplete={cache_marked_incomplete}, "
                            f"total_hint_source={total_matches_hint_source or 'none'}, "
                            f"total_hint_reliable={total_matches_hint_reliable}, "
                            f"fallback_reason={sync_decision.fallback_reason or 'none'}"
                        )

                    if not got_401_error and sync_decision.fallback_reason:
                        print(sync_decision.fallback_reason)
                        use_incremental = False
                    
                    # NOTE: Cache completeness check removed - we don't need complete history
                    # for regular stats lookups. If the user wants complete history, they use
                    # force_full_fetch=True, which bypasses cache entirely.
                    # Having 3900 cached matches is still useful even if player has >3900 total.
                    
                    # Check if we got 401 error before returning cached data
                    if not use_incremental:
                        print("Switching to full fetch after incremental completeness check")
                    elif got_401_error and not new_matches_found:
                        print(f"Got 401 error, will attempt token refresh...")
                        # Don't return cached data yet, let it fall through to 401 handling below
                    elif not new_matches_found:
                        if full_history_requested and not sync_decision.completeness_proven:
                            print(
                                "No new matches found but completeness is unproven; "
                                "switching to full fetch."
                            )
                            use_incremental = False
                        elif repair_ids:
                            # No new matches, but there are known-failed matches to
                            # repair. Fall through with an empty new-match list so the
                            # detail-fetch stage below retries exactly those IDs and
                            # re-saves (clearing the incomplete flag) - not a re-crawl.
                            print(f"No new matches; performing targeted repair of "
                                  f"{len(repair_ids)} failed match(es)")
                            all_matches = list(new_matches_found)
                        else:
                            # A real API check just confirmed the cache is
                            # current; stamp it so requests within the
                            # freshness TTL skip the API entirely.
                            self._history_checked_at[xuid] = time.monotonic()
                            print(f"No new matches found, using cache ({len(existing_matches)} matches)")
                            cached_matches = cached_data.get('processed_matches', [])
                            # Prefer the precomputed per-mode summary over
                            # rescanning the cached matches in Python; falls
                            # back for players not yet covered by the
                            # player_mode_stats backfill.
                            stats = self.stats_cache.get_player_mode_summary(xuid, stat_type)
                            if stats is None:
                                stats = self._calculate_stats_from_matches(cached_matches, stat_type)
                            return {
                                'error': 0,
                                'stats': stats,
                                'matches_processed': len(existing_matches),
                                'new_matches': 0,
                                'processed_matches': cached_matches
                            }
                    else:
                        print(f"🆕 Found {len(new_matches_found)} new matches across {page_num + 1} page(s)")
                        all_matches = new_matches_found
                
                # Full fetch (no cache exists or force_full_fetch was requested)
                if not use_incremental:
                    # Fetch all matches from the list endpoint, then merge with cached details.
                    print(f"Running full match-history fetch with rolling queue...")
                    
                    # Rolling queue approach - keep N requests in flight at all times
                    # Conservative concurrency to respect API rate limits and avoid bans
                    all_matches = []
                    num_accounts = len(self.spartan_accounts) if self.spartan_accounts else 1
                    max_in_flight = min(num_accounts * 5, 25)  # 5 per account, max 25 total
                    current_page = 0
                    bounded_fetch = not full_history_requested
                    if bounded_fetch:
                        max_pages = max(0, math.ceil(matches_to_process / PAGE_SIZE))
                        print(f"Bounded fetch enabled: request={matches_to_process} matches, max_pages={max_pages}")
                    else:
                        max_pages = 999999
                    got_empty_page = False
                    # Page-listing fetches that failed after retries. A mid-crawl
                    # failure must NOT stop the crawl (that silently truncates
                    # history); we keep enqueuing and repair these afterwards.
                    failed_page_nums = []

                    # Use asyncio queue for rolling concurrency
                    pending_tasks = set()
                    
                    async def fetch_and_track(page_num):
                        """Fetch a page and return (page_num, results)"""
                        results = await fetch_match_page(session, page_num * PAGE_SIZE, PAGE_SIZE)
                        return page_num, results
                    
                    # Start initial batch of requests
                    for i in range(min(max_in_flight, max_pages)):
                        task = asyncio.create_task(fetch_and_track(i))
                        pending_tasks.add(task)
                    current_page = max_in_flight
                    
                    # Process with rolling queue - as one completes, start another
                    while pending_tasks:
                        done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                        
                        for task in done:
                            page_num, page = task.result()

                            if page is None:
                                got_401_error = True
                                break
                            elif page is _PAGE_FETCH_FAILED:
                                # Page listing failed after retries. Do NOT treat
                                # this as end-of-history: record it for the repair
                                # pass and keep enqueuing subsequent pages so a
                                # single transient failure can't truncate the crawl.
                                failed_page_nums.append(page_num)
                                if current_page < max_pages and not got_empty_page:
                                    new_task = asyncio.create_task(fetch_and_track(current_page))
                                    pending_tasks.add(new_task)
                                    current_page += 1
                            elif page and len(page) > 0:
                                all_matches.extend(page)

                                # For bounded requests, stop enqueuing once enough matches are gathered.
                                if bounded_fetch and len(all_matches) >= matches_to_process:
                                    got_empty_page = True

                                # Start next page request immediately
                                if current_page < max_pages and not got_empty_page:
                                    new_task = asyncio.create_task(fetch_and_track(current_page))
                                    pending_tasks.add(new_task)
                                    current_page += 1
                            else:
                                # Genuine empty page - real end of history, stop starting new requests
                                got_empty_page = True

                        if got_401_error:
                            # Cancel remaining tasks
                            for t in pending_tasks:
                                t.cancel()
                            break

                        # Progress update
                        if len(all_matches) % 500 == 0 and len(all_matches) > 0:
                            print(f"   Fetched {len(all_matches)} matches so far...")

                    # In-crawl repair pass: retry any pages that failed mid-crawl,
                    # serially and at low concurrency (per-request retries already
                    # happened once inside fetch_match_page). Anything still failing
                    # leaves an "unknown gap" flag so the cache is marked incomplete
                    # and the next full-history check re-crawls (see targeted-repair
                    # logic). Skip during 401 handling - that path retries wholesale.
                    if failed_page_nums and not got_401_error:
                        print(f"Repairing {len(failed_page_nums)} failed page(s) from full crawl...")
                        still_failed = []
                        for fp in failed_page_nums:
                            repaired = await fetch_match_page(session, fp * PAGE_SIZE, PAGE_SIZE)
                            if repaired is None:
                                got_401_error = True
                                break
                            if repaired is _PAGE_FETCH_FAILED:
                                still_failed.append(fp)
                            elif repaired:
                                all_matches.extend(repaired)
                            # A genuine empty page here just means that offset is now
                            # past end-of-history; nothing to add, not a failure.
                        if still_failed:
                            page_fetch_incomplete = True
                            print(f"⚠️ {len(still_failed)} page(s) still failed after repair; "
                                  f"cache will be marked incomplete (unknown gap)")

                # Check if we got 401 errors and need to refresh tokens
                if got_401_error:
                    if _retry_count >= 2:
                        print(f"Failed after {_retry_count} token refresh attempts")
                        return {"error": 4, "message": "Authentication failed - token refresh unsuccessful after multiple attempts"}
                    
                    # Try refreshing tokens and retry
                    print(f"Got 401 error, attempting token refresh (attempt {_retry_count + 1}/2)...")
                    if await self.ensure_valid_tokens():
                        print("Retrying after token refresh...")
                        # Recursive call - try once more with new tokens
                        result = await self.calculate_comprehensive_stats(xuid, stat_type, gamertag, matches_to_process, force_full_fetch, _retry_count + 1)
                        # If retry succeeded (no error), we're good
                        if result.get('error', 0) == 0:
                            print("Retry successful after token refresh")
                        return result
                    else:
                        print("Token refresh failed, cannot continue")
                        return {"error": 4, "message": "Authentication failed - unable to refresh tokens"}
                
                if cached_data and existing_matches:
                    # Reuse cached rows and only fetch details for uncached match IDs.
                    uncached_match_ids = [
                        match.get('MatchId') for match in all_matches
                        if match.get('MatchId') and match.get('MatchId') not in existing_matches
                    ]
                    # Targeted repair: also refetch known-failed match IDs. Their
                    # details were never saved, so they aren't in existing_matches;
                    # retrying them here clears the incomplete flag on save.
                    if repair_ids:
                        seen = set(uncached_match_ids)
                        for mid in repair_ids:
                            if mid not in seen:
                                uncached_match_ids.append(mid)
                                seen.add(mid)
                    print(f"Processing {len(uncached_match_ids)} uncached matches from {len(all_matches)} listed matches")
                    matches_to_fetch = [(match_id, xuid) for match_id in uncached_match_ids]
                    all_processed_matches = list(existing_matches.values())
                else:
                    # Full fetch: process all matches
                    print(f"Found {len(all_matches)} total matches in history")
                    
                    # Limit to requested amount
                    matches_to_analyze = all_matches[:matches_to_process] if matches_to_process < 999999 else all_matches
                    
                    # Process matches we haven't seen before
                    all_processed_matches = list(existing_matches.values()) if existing_matches else []
                    
                    # Find matches that need processing
                    matches_to_fetch = []
                    for match in matches_to_analyze:
                        match_id = match.get('MatchId')
                        if match_id not in existing_matches:
                            matches_to_fetch.append((match_id, xuid))
                
                print(f"Fetching {len(matches_to_fetch)} match details with rolling queue...")
                
                # Rolling queue for match details - keep many requests in flight
                # 429 retries handle pacing, maximize concurrency
                max_match_requests = 200  # 40 per account = 200 match details in flight
                new_stats = []
                failed_matches = []
                
                async def fetch_match_detail(match_id, player_xuid):
                    """Fetch single match and return result"""
                    try:
                        result = await self.get_match_stats_for_match(match_id, player_xuid, session)
                    except Exception as e:
                        print(f"Error fetching match detail {match_id}: {e}")
                        result = None
                    return match_id, result

                # Start initial batch
                pending = set()
                # Track task -> match_id so a failure always records the REAL match
                # ID (never "unknown"); those IDs are persisted for targeted repair.
                pending_match_ids = {}
                match_iter = iter(matches_to_fetch)

                def _start_detail_task(mid, pxuid):
                    task = asyncio.create_task(fetch_match_detail(mid, pxuid))
                    pending.add(task)
                    pending_match_ids[task] = mid

                for _ in range(min(max_match_requests, len(matches_to_fetch))):
                    try:
                        match_id, player_xuid = next(match_iter)
                        _start_detail_task(match_id, player_xuid)
                    except StopIteration:
                        break

                # Rolling queue - as each completes, start next
                completed = 0
                total = len(matches_to_fetch)

                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                    for task in done:
                        completed += 1
                        mid = pending_match_ids.pop(task, None)
                        try:
                            match_id, result = task.result()
                            if result is not None:
                                new_stats.append(result)
                            else:
                                failed_matches.append(match_id)
                        except Exception as e:
                            # Defensive net (fetch_match_detail already swallows
                            # its own errors): record the real ID when known.
                            if mid is not None:
                                failed_matches.append(mid)

                        # Start next request immediately
                        try:
                            next_match_id, next_xuid = next(match_iter)
                            _start_detail_task(next_match_id, next_xuid)
                        except StopIteration:
                            pass
                    
                    # Progress update every 100 matches
                    if completed % 100 == 0:
                        print(f"   Progress: {completed}/{total} matches fetched ({len(new_stats)} successful)")
                
                print(f"   Completed: {len(new_stats)} matches fetched, {len(failed_matches)} failed")
                
                all_processed_matches.extend(new_stats)
                new_matches_processed = len(new_stats)
                
                # Log incomplete data if any matches failed
                if failed_matches:
                    print(f"Warning: {len(failed_matches)} matches failed to fetch (timeouts/errors)")
                
                # Sort by timestamp (most recent first) and remove duplicates
                seen_ids = set()
                unique_matches = []
                for match in all_processed_matches:
                    match_id = match.get('match_id')
                    if match_id and match_id not in seen_ids:
                        seen_ids.add(match_id)
                        unique_matches.append(match)
                
                unique_matches.sort(key=lambda x: x.get('start_time', ''), reverse=True)
                all_processed_matches = unique_matches
                
                print(f"Processed {new_matches_processed} new matches, total: {len(all_processed_matches)}")
                
                # Helper function to calculate stats for a set of matches
                def calculate_stats_for_matches(matches):
                    """Calculate aggregate stats from a list of matches"""
                    total_kills = sum(m.get('kills', 0) for m in matches)
                    total_deaths = sum(m.get('deaths', 0) for m in matches)
                    total_assists = sum(m.get('assists', 0) for m in matches)
                    
                    wins = sum(1 for m in matches if m.get('outcome') == 2)
                    losses = sum(1 for m in matches if m.get('outcome') == 3)
                    ties = sum(1 for m in matches if m.get('outcome') == 1)
                    dnf = sum(1 for m in matches if m.get('outcome') == 4)
                    
                    games_played = len(matches)
                    kd_ratio = round(total_kills / total_deaths if total_deaths > 0 else total_kills, 2)
                    kda = round((total_kills + (total_assists / 3)) - total_deaths, 2)
                    avg_kda = round(kda / games_played if games_played > 0 else 0, 2)
                    win_rate = f"{round(wins / games_played * 100 if games_played > 0 else 0, 1)}%"
                    latest_csr = next((m.get('csr') for m in matches if m.get('csr') is not None), None)
                    latest_csr_tier = next((m.get('csr_tier') for m in matches if m.get('csr_tier')), None)
                    
                    return {
                        'total_kills': total_kills,
                        'total_deaths': total_deaths,
                        'total_assists': total_assists,
                        'wins': wins,
                        'losses': losses,
                        'ties': ties,
                        'dnf': dnf,
                        'games_played': games_played,
                        'kd_ratio': kd_ratio,
                        'kda': kda,
                        'avg_kda': avg_kda,
                        'win_rate': win_rate,
                        'estimated_csr': latest_csr,
                        'csr_tier': latest_csr_tier,
                    }
                
                # Split for the CSR fallback lookup below and for the legacy
                # Python-computed fallback if the precomputed summary isn't
                # available yet. Custom/private matches (match_category ==
                # 'custom') are excluded from "social" (and, via
                # overall_matches below, "overall") - they stay in the DB but
                # never count toward aggregates.
                ranked_matches = [m for m in all_processed_matches if m.get('is_ranked', False)]
                social_matches = [
                    m for m in all_processed_matches
                    if not m.get('is_ranked', False) and m.get('match_category') != 'custom'
                ]
                overall_matches = [m for m in all_processed_matches if m.get('match_category') != 'custom']

                print(f"Match breakdown: {len(all_processed_matches)} total, {len(ranked_matches)} ranked, {len(social_matches)} social")

                # Prepare cache data (stats are not stored here - not read by
                # save_player_stats/load_player_stats - only processed_matches is)
                cache_data = {
                    'last_update': datetime.now().isoformat(),
                    'gamertag': gamertag,
                    'xuid': xuid,
                    'stat_type': stat_type,
                    'processed_matches': all_processed_matches,
                    'total_matches_hint': total_matches_hint,
                    'newest_cached_match_id': all_processed_matches[0].get('match_id') if all_processed_matches else None,
                    'newest_cached_match_time': all_processed_matches[0].get('start_time') if all_processed_matches else None,
                    # Incomplete if any match-detail fetch failed (known IDs) OR a
                    # page listing stayed failed after repair (unknown gap, no IDs).
                    'incomplete_data': len(failed_matches) > 0 or page_fetch_incomplete,
                    'failed_match_count': len(failed_matches),
                    # Full list of real failed match IDs (no truncation) so the next
                    # run can retry exactly these instead of re-crawling everything.
                    'failed_matches': failed_matches,
                }

                # Save to cache off the event loop, so the blocking SQLite write
                # batch can't stall the gateway heartbeat or other commands.
                # This is also what applies the player_mode_stats/player_medal_totals
                # deltas (via insert_player_match), so the precomputed summary
                # read below is guaranteed fresh once this completes.
                await asyncio.get_running_loop().run_in_executor(
                    self._db_write_executor,
                    self.save_stats_cache, xuid, stat_type, cache_data, gamertag,
                )

                # Prefer the precomputed per-mode summary (now up to date) over
                # rescanning all_processed_matches in Python; only estimated_csr/
                # csr_tier (not tracked in player_mode_stats) still need a cheap
                # front-of-list lookup, since matches are sorted newest-first.
                def latest_csr_tier(matches):
                    return (
                        next((m.get('csr') for m in matches if m.get('csr') is not None), None),
                        next((m.get('csr_tier') for m in matches if m.get('csr_tier')), None),
                    )

                overall_summary = self.stats_cache.get_player_mode_summary(xuid, "overall")
                ranked_summary = self.stats_cache.get_player_mode_summary(xuid, "ranked")
                social_summary = self.stats_cache.get_player_mode_summary(xuid, "social")

                if overall_summary is not None and ranked_summary is not None and social_summary is not None:
                    overall_csr, overall_csr_tier = latest_csr_tier(all_processed_matches)
                    ranked_csr, ranked_csr_tier = latest_csr_tier(ranked_matches)
                    social_csr, social_csr_tier = latest_csr_tier(social_matches)
                    overall_stats = {**overall_summary, 'estimated_csr': overall_csr, 'csr_tier': overall_csr_tier}
                    ranked_stats = {**ranked_summary, 'estimated_csr': ranked_csr, 'csr_tier': ranked_csr_tier}
                    social_stats = {**social_summary, 'estimated_csr': social_csr, 'csr_tier': social_csr_tier}
                else:
                    overall_stats = calculate_stats_for_matches(overall_matches)
                    ranked_stats = calculate_stats_for_matches(ranked_matches)
                    social_stats = calculate_stats_for_matches(social_matches)

                # Return the appropriate stats based on stat_type
                if stat_type == "ranked":
                    selected_stats = ranked_stats
                elif stat_type in ("core_ranked", "rotational_ranked"):
                    # Not part of the three-summary gate above: players with
                    # zero core (or rotational) games have no summary row at
                    # all, and that's expected - fall back per stat_type only.
                    split_matches = [
                        m for m in ranked_matches
                        if ((m.get('playlist_id') or '').strip().lower() in CORE_RANKED_PLAYLIST_IDS)
                        == (stat_type == "core_ranked")
                    ]
                    summary = self.stats_cache.get_player_mode_summary(xuid, stat_type)
                    if summary is not None:
                        split_csr, split_csr_tier = latest_csr_tier(split_matches)
                        selected_stats = {**summary, 'estimated_csr': split_csr, 'csr_tier': split_csr_tier}
                    else:
                        selected_stats = calculate_stats_for_matches(split_matches)
                elif stat_type == "social":
                    selected_stats = social_stats
                else:
                    selected_stats = overall_stats

                # Stamp only when full history was verified: bounded fetches
                # never proved the history is complete, so they must not let
                # later full-history requests skip their API check.
                if full_history_requested:
                    self._history_checked_at[xuid] = time.monotonic()

                return {
                    'error': 0,
                    'stats': selected_stats,
                    'matches_processed': selected_stats['games_played'],
                    'new_matches': new_matches_processed,
                    'processed_matches': all_processed_matches
                    }
                    
        except Exception as e:
            print(f"Error calculating comprehensive stats: {e}")
            traceback.print_exc()
            return {"error": 4, "message": f"Error calculating stats: {str(e)}"}

    def parse_stats(self, api_data: Dict, stat_type: str, gamertag: str) -> Dict:
        """
        Parse stats into format expected by Discord bot.
        
        Args:
            api_data: Raw stats from calculate_comprehensive_stats
            stat_type: Type of stats requested
            gamertag: Player gamertag
        
        Returns:
            Formatted stats dictionary for Discord bot
        """
        if 'stats' in api_data and api_data.get('error') == 0:
            stats = api_data['stats']
            
            # Format stats for Discord bot (matching original format)
            stats_list = [
                str(stats['kd_ratio']),           # [0] KD Ratio
                stats['win_rate'],                # [1] Win Rate  
                str(stats['avg_kda']),            # [2] Avg KDA per game
                str(stats['total_deaths']),       # [3] Deaths
                str(stats['total_kills']),        # [4] Kills
                str(stats['total_assists']),      # [5] Assists
                str(stats['games_played'])        # [6] Games Played
            ]
            
            return {
                "error": 0,
                "stats_list": stats_list,
                "gamertag": gamertag,
                "stat_type": stat_type,
                "cache_info": f"Processed {api_data.get('matches_processed', 0)} matches ({api_data.get('new_matches', 0)} new)"
            }
        
        return {
            "error": api_data.get('error', 4),
            "message": api_data.get('message', "Failed to calculate stats")
        }


# =============================================================================
# GLOBAL INSTANCES
# =============================================================================

# Create global API client instance
api_client = HaloAPIClient()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


async def _fetch_match_players(
    session: aiohttp.ClientSession,
    match_id: str,
    match_num: int,
    spartan_token: str,
    main_xuid: str
) -> Set[str]:
    """
    Fetch player XUIDs from a single match.
    
    Args:
        session: aiohttp session
        match_id: Match identifier
        match_num: Match number (for logging)
        spartan_token: Authentication token
        main_xuid: Main player's XUID (to exclude)
    
    Returns:
        Set of player XUIDs found in the match
    """
    try:
        stats_url = f"https://halostats.svc.halowaypoint.com/hi/matches/{match_id}/stats"
        headers = {
            "Authorization": f"Spartan {spartan_token}",
            "x-343-authorization-spartan": spartan_token,
            "User-Agent": "HaloWaypoint/2021112313511900",
            "Accept": "application/json"
        }
        
        async with session.get(stats_url, headers=headers) as response:
            if response.status != 200:
                return set()
            
            match_data = await response.json()
            players = match_data.get('Players', [])
            
            found_xuids = set()
            for player in players:
                player_id = player.get('PlayerId', '')
                
                # Skip main player
                if str(main_xuid) in str(player_id):
                    continue
                
                # Extract XUID from format: 'xuid(2535463944911967)'
                if 'xuid(' in player_id:
                    xuid = player_id.replace('xuid(', '').replace(')', '')
                    found_xuids.add(xuid)
            
            return found_xuids
            
    except Exception:
        return set()


async def get_players_from_recent_matches(
    gamertag: str,
    num_matches: int = 50,
    progress_file: str = None
) -> List[str]:
    """
    Get unique player gamertags from a player's recent matches.
    
    Scalable implementation that uses persistent XUID cache to handle
    large numbers of players efficiently.
    
    Args:
        gamertag: Player's Xbox gamertag
        num_matches: Number of matches to analyze
        progress_file: Optional file path for progress tracking
    
    Returns:
        List of unique gamertags encountered
    """
    print(f"Extracting players from last {num_matches} matches for {gamertag}...")
    
    # Load XUID cache
    xuid_cache = load_xuid_cache()
    print(f"Loaded XUID cache with {len(xuid_cache)} mappings")
    
    # Check for saved progress
    progress_data = {}
    if progress_file:
        progress_data = safe_read_json(progress_file, default={})
        if progress_data:
            # Check if already completed
            processed = progress_data.get('processed_matches', 0)
            total = progress_data.get('total_matches', 0)
            if processed > 0 and processed == total:
                print(f"Scan already completed! Found {len(progress_data.get('unique_players', []))} unique players from {total} matches")
                print(f"Skipping match scan, proceeding to gamertag resolution...")
                # Return cached players directly if we have resolved gamertags
                if 'resolved_gamertags' in progress_data and progress_data['resolved_gamertags']:
                    return progress_data['resolved_gamertags']
            else:
                print(f"Resuming from saved progress ({len(progress_data.get('unique_players', []))} players found so far, {processed}/{total} matches processed)")
    
    try:
        # Ensure API client has authentication tokens
        if not api_client.clearance_token:
            print(f"Initializing authentication tokens...")
            if not await api_client.get_clearance_token():
                print(f"Failed to get authentication tokens")
                return []
        
        # Get the main player's XUID and match history
        main_xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
        if not main_xuid:
            print(f"Player {gamertag} not found")
            return []
        
        # Get comprehensive stats for main player (this includes match history)
        # First, try to use cached data
        main_stats = await api_client.calculate_comprehensive_stats(
            main_xuid, 
            "overall", 
            gamertag=gamertag, 
            matches_to_process=num_matches,
            force_full_fetch=False  # Try cache first
        )
        
        # Check if we got matches from cache
        processed_matches = main_stats.get('processed_matches', [])
        
        if processed_matches:
            # We have cached matches! Extract players directly from cache
            print(f"Using cached match data ({len(processed_matches)} matches)")
            unique_players = set()
            
            for match in processed_matches[:num_matches]:
                # Extract players from the cached match data
                players = match.get('players', [])
                for player_xuid in players:
                    # Skip the main player
                    if str(player_xuid) != str(main_xuid):
                        unique_players.add(str(player_xuid))
            
            print(f"Extracted {len(unique_players)} unique players from cache")
            
            # Now resolve XUIDs to gamertags
            xuid_cache = load_xuid_cache()
            resolved_gamertags = []
            xuids_to_resolve = []
            
            for xuid in unique_players:
                # Check if already in cache (format: xuid -> gamertag)
                if xuid in xuid_cache:
                    resolved_gamertags.append(xuid_cache[xuid])
                else:
                    xuids_to_resolve.append(xuid)
            
            print(f"Found {len(resolved_gamertags)} gamertags in cache, need to resolve {len(xuids_to_resolve)} XUIDs")
            
            # Resolve remaining XUIDs
            if xuids_to_resolve:
                print(f"Resolving {len(xuids_to_resolve)} XUIDs to gamertags...")
                for xuid in xuids_to_resolve:
                    try:
                        gamertag_result = await api_client.resolve_xuid_to_gamertag(xuid)
                        if gamertag_result:
                            resolved_gamertags.append(gamertag_result)
                            xuid_cache[xuid] = gamertag_result
                        await asyncio.sleep(0.1)  # Small delay
                    except:
                        continue
                
                # Save updated cache
                save_xuid_cache(xuid_cache)
            
            print(f"Total: {len(resolved_gamertags)} gamertags resolved")
            return resolved_gamertags
        
        # Cache miss - fall back to full match history fetch
        print(f"No cached match data, fetching full match history...")
        main_stats = await api_client.calculate_comprehensive_stats(
            main_xuid, 
            "overall", 
            gamertag=gamertag, 
            matches_to_process=num_matches,
            force_full_fetch=True  # Force fetch from API
        )
        
        # Check if we got matches (even if there was an error fetching new ones)
        processed_matches = main_stats.get('processed_matches', [])
        
        if not processed_matches:
            # No matches at all - check if error
            if main_stats.get('error', 0) != 0:
                print(f"Failed to get match history for {gamertag}: {main_stats.get('message', 'Unknown error')}")
            else:
                print(f"No matches found for {gamertag}")
            return []
        
        # Get match IDs from the processed matches (use what we have, even if fewer than requested)
        match_ids = [m['match_id'] for m in processed_matches[:num_matches]]
        matches_used = len(match_ids)
        if matches_used < num_matches:
            print(f"Only {matches_used} matches available (requested {num_matches})")
        print(f"Found {matches_used} matches to scan")
        
        # Resume from progress or start fresh
        unique_players = set(progress_data.get('unique_players', []))
        processed_match_count = progress_data.get('processed_matches', 0)
        
        # Extract spartan token properly
        spartan_token = api_client.spartan_token
        if isinstance(spartan_token, dict) and 'token' in spartan_token:
            spartan_token = spartan_token['token']
        
        if not spartan_token:
            print(f"No Spartan token available")
            return []
        
        # Fetch details for each match and extract player names
        # Process matches in small batches with minimal delay
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=180, connect=30)
        batch_size = 3  # Conservative concurrency to avoid socket exhaustion
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            remaining_matches = match_ids[processed_match_count:]
            
            for batch_start in range(0, len(remaining_matches), batch_size):
                batch_matches = remaining_matches[batch_start:batch_start + batch_size]
                batch_tasks = []
                
                # Create tasks for this batch
                for offset, match_id in enumerate(batch_matches):
                    i = processed_match_count + batch_start + offset + 1
                    batch_tasks.append(_fetch_match_players(session, match_id, i, spartan_token, main_xuid))
                
                # Execute batch concurrently
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Add new players from successful results
                for result in batch_results:
                    if isinstance(result, set):
                        unique_players.update(result)
                
                # Update progress
                new_processed_count = processed_match_count + batch_start + len(batch_matches)
                if new_processed_count % 10 == 0 or new_processed_count == len(match_ids):
                    print(f"   Processed {new_processed_count}/{len(match_ids)} matches, found {len(unique_players)} unique players so far...")
                    if progress_file:
                        safe_write_json(progress_file, {
                            'unique_players': list(unique_players),
                            'processed_matches': new_processed_count,
                            'total_matches': len(match_ids)
                        })
                
                # Small delay between batches to avoid overwhelming the API
                if batch_start + batch_size < len(remaining_matches):
                    await asyncio.sleep(0.5)
        
        print(f"Found {len(unique_players)} unique XUIDs across {len(match_ids)} matches")
        
        # Convert XUIDs back to gamertags with aggressive rate limiting
        # SCALABLE OPTIMIZATION: Check cache first, only resolve new XUIDs
        xuids_to_resolve = []
        gamertag_list = []
        
        for xuid in unique_players:
            if xuid in xuid_cache:
                # Cache hit - instant lookup!
                gamertag_list.append(xuid_cache[xuid])
                cache_hits += 1
            else:
                # Cache miss - need to resolve
                xuids_to_resolve.append(xuid)
                cache_misses += 1
        
        print(f"Cache stats: {cache_hits} hits, {cache_misses} misses ({cache_hits/(cache_hits+cache_misses)*100:.1f}% hit rate)")
        
        if len(xuids_to_resolve) == 0:
            print(f"All {len(unique_players)} XUIDs found in cache - no API calls needed!")
            # Save the updated list and cleanup
            if progress_file and os.path.exists(progress_file):
                os.remove(progress_file)
            return sorted(gamertag_list)
        
        print(f"Resolving {len(xuids_to_resolve)} new XUIDs to gamertags (one at a time)...")
        
        # Resume from progress if available
        resolved_count = len(progress_data.get('resolved_gamertags', []))
        if resolved_count > 0:
            newly_resolved = progress_data['resolved_gamertags']
            gamertag_list.extend(newly_resolved)
            print(f"Resuming XUID resolution from {resolved_count}/{len(xuids_to_resolve)}")
        else:
            newly_resolved = []
        
        # Load accounts - try to load both account1 and account2
        accounts = []
        
        # Account 1 (primary)
        cache_file = TOKEN_CACHE_FILE
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache = json.load(f)
            xsts_xbox = cache.get('xsts_xbox', {})
            if xsts_xbox and xsts_xbox.get('token') and xsts_xbox.get('uhs'):
                accounts.append({
                    'id': 'account1',
                    'token': xsts_xbox.get('token'),
                    'uhs': xsts_xbox.get('uhs'),
                    'name': 'Account 1'
                })
        
        # Account 2 (secondary - optional)
        cache_file2 = get_token_cache_path(2)
        if os.path.exists(cache_file2):
            with open(cache_file2, 'r') as f:
                cache2 = json.load(f)
            xsts_xbox2 = cache2.get('xsts_xbox', {})
            if xsts_xbox2 and xsts_xbox2.get('token') and xsts_xbox2.get('uhs'):
                accounts.append({
                    'id': 'account2',
                    'token': xsts_xbox2.get('token'),
                    'uhs': xsts_xbox2.get('uhs'),
                    'name': 'Account 2'
                })
                print(f"   Found second account! Speed will be 2x faster")
        
        if not accounts:
            print(f"ERROR: No valid Xbox Live tokens found")
            return sorted(gamertag_list)
        
        print(f"   Using {len(accounts)} account(s) for XUID resolution")
        
        # Set timeout and connector for XUID resolution requests
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=180, connect=30)
        await asyncio.sleep(2)  # Initial delay before starting resolution
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            xuid_index = resolved_count
            account_index = 0  # Round-robin between accounts
            
            while xuid_index < len(xuids_to_resolve):
                xuid = xuids_to_resolve[xuid_index]
                
                # Select account (round-robin)
                account = accounts[account_index % len(accounts)]
                account_index += 1
                
                try:
                    # Acquire rate limiter slot
                    await xbox_profile_rate_limiter.acquire()
                    
                    try:
                        # Prepare headers for this account
                        headers = {
                            'Authorization': f'XBL3.0 x={account["uhs"]};{account["token"]}',
                            'x-xbl-contract-version': '2',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        }
                        
                        single_url = f'https://profile.xboxlive.com/users/xuid({xuid})/profile/settings?settings=Gamertag'
                        async with session.get(single_url, headers=headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                if 'profileUsers' in data and len(data['profileUsers']) > 0:
                                    profile_user = data['profileUsers'][0]
                                    settings = profile_user.get('settings', [])
                                    for setting in settings:
                                        if setting.get('id') == 'Gamertag':
                                            gamertag = setting.get('value')
                                            if gamertag:
                                                gamertag_list.append(gamertag)
                                                newly_resolved.append(gamertag)
                                                xuid_cache[xuid] = gamertag
                                                # Show progress every 10 resolves
                                                if len(newly_resolved) % 10 == 0:
                                                    print(f"      Resolved {len(newly_resolved)}/{len(xuids_to_resolve)} XUIDs...")
                                            break
                                await asyncio.sleep(0.1)  # 0.1 second delay between requests
                                # Move to next XUID on success
                                xuid_index += 1
                            elif response.status == 429:
                                # Rate limit on this account - try next account instead of waiting
                                print(f"   Rate limit on account {account['id'][:8]}... - trying next account")
                                # Don't increment xuid_index - retry same XUID with next account
                                # The account_index is already incremented, so next loop iteration uses next account
                                await asyncio.sleep(0.5)  # Small delay before retry
                                continue  # Retry same XUID with different account
                            else:
                                print(f"   Failed to resolve XUID {xuid}: status {response.status}")
                                # Move to next XUID on error
                                xuid_index += 1
                    finally:
                        xbox_profile_rate_limiter.release()
                    
                    # Save cache every 50 resolves
                    if len(newly_resolved) % 50 == 0 and len(newly_resolved) > 0:
                        save_xuid_cache(xuid_cache)
                        print(f"   Saved cache: {len(xuid_cache)} total mappings")
                    
                    # Save progress
                    if progress_file and len(newly_resolved) % 10 == 0:
                        safe_write_json(progress_file, {
                            'unique_players': list(unique_players),
                            'processed_matches': len(match_ids),
                            'total_matches': len(match_ids),
                            'resolved_gamertags': newly_resolved
                        })
                        
                except Exception as e:
                    print(f"Error resolving XUID {xuid}: {e}")
                    xuid_index += 1  # Skip to next on error
                    continue
        
        # Final cache save
        save_xuid_cache(xuid_cache)
        print(f"Resolved {len(newly_resolved)} new gamertags, {len(gamertag_list)} total (cache now has {len(xuid_cache)} mappings)")
        
        # Clean up progress file if completed successfully
        if progress_file and os.path.exists(progress_file):
            os.remove(progress_file)
            print(f"   Progress file removed (operation completed)")
        
        return sorted(gamertag_list)
        
    except Exception as e:
        print(f"Error in get_players_from_recent_matches: {e}")
        traceback.print_exc()
        
        # Save progress on error
        if progress_file:
            try:
                safe_write_json(progress_file, {
                    'unique_players': list(unique_players) if 'unique_players' in locals() else [],
                    'processed_matches': processed_match_count if 'processed_match_count' in locals() else 0,
                    'total_matches': len(match_ids) if 'match_ids' in locals() else 0,
                    'resolved_gamertags': gamertag_list if 'gamertag_list' in locals() else [],
                    'error': str(e)
                })
                print(f"   Progress saved to {progress_file}")
            except:
                pass
        
        return []


# =============================================================================
# COMPATIBILITY CLASS
# =============================================================================


class StatsFind:
    """
    Compatibility wrapper for legacy bot code.
    
    Maintains the same interface as the original web scraping version
    while using the Halo API internally.
    """
    
    def __init__(
        self,
        gamertag: str = "GT",
        stats_list: str = "NA",
        stat_type: str = "NA",
        error_no: int = 0
    ):
        """Initialize with default values."""
        self.gamertag = gamertag
        self.stats_list = stats_list
        self.stat_type = stat_type
        self.error_no = error_no
    
    async def ensure_valid_tokens(self) -> bool:
        """Wrapper for api_client's token validation."""
        return await api_client.ensure_valid_tokens()
    
    async def page_getter(
        self,
        gamertag: str,
        stat_type: str,
        matches_to_process: int = 10,
        force_full_fetch: bool = False,
        xuid: Optional[str] = None,
    ) -> 'StatsFind':
        """
        Get player stats using Halo API.

        Args:
            gamertag: Player's Xbox gamertag
            stat_type: "stats" (all), "ranked", "core_ranked",
                "rotational_ranked", or "social"
            matches_to_process: Number of matches to process
            force_full_fetch: If True, bypass cache and fetch full history from API
            xuid: Pre-resolved XUID, if the caller already has one

        Returns:
            Self with populated stats
        """
        print(f"Getting {stat_type} stats for {gamertag} "
              f"(matches: {'ALL' if matches_to_process is None else matches_to_process})")

        # Map stat_type to API parameters
        stat_type_map = {
            "stats": "overall",
            "ranked": "ranked",
            "core_ranked": "core_ranked",
            "rotational_ranked": "rotational_ranked",
            "social": "social"
        }
        api_stat_type = stat_type_map.get(stat_type, "overall")

        try:
            result = await api_client.get_player_stats(
                gamertag,
                api_stat_type,
                matches_to_process=matches_to_process,
                force_full_fetch=force_full_fetch,
                xuid=xuid,
            )
            
            if result.get("error", 0) != 0:
                self.error_no = result["error"]
                print(f"API Error {self.error_no}: {result.get('message', 'Unknown')}")
                return self
            
            # Set stats for compatibility with existing bot code
            self.stats_list = result["stats_list"]
            self.gamertag = result["gamertag"]
            self.stat_type = result["stat_type"]
            self.error_no = 0
            
            print(f"Stats retrieved for {gamertag}: {self.stats_list}")
            return self
            
        except Exception as e:
            print(f"Error in page_getter: {e}")
            self.error_no = 4
            return self


# Global instance for backward compatibility
StatsFind1 = StatsFind()