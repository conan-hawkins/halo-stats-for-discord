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
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dotenv import load_dotenv

# Load environment variables before module initialization
load_dotenv()

from src.auth.tokens import run_auth_flow
from src.database.cache import get_cache
from src.config import (
    TOKEN_CACHE_FILE, 
    get_token_cache_path,
    XUID_CACHE_FILE,
    REQUESTS_PER_SECOND_PER_ACCOUNT
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
)
from src.api.xuid_cache import (
    load_xuid_cache,
    save_xuid_cache,
)


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
    USER_AGENT = "HaloWaypoint/2021.01.10.01"
    
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
        # Prevent concurrent refresh attempts
        if self._refresh_in_progress:
            print("Token refresh already in progress, waiting...")
            return False
        
        # Check Account 1 tokens
        cache = safe_read_json(TOKEN_CACHE_FILE, default={})
        if not cache:
            print("No token cache found for Account 1")
            print("Run: python get_auth_tokens.py")
            return False
        
        # Check ALL required tokens for Account 1
        spartan_info = cache.get("spartan")
        xsts_info = cache.get("xsts")  # Main XSTS token for Halo API
        xsts_xbox_info = cache.get("xsts_xbox")  # XSTS token for Xbox Live
        
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
                        # Swap cache files so run_auth_flow uses this account's tokens
                        # 1. Backup Account 1's cache
                        account1_backup = safe_read_json(TOKEN_CACHE_FILE, default={})
                        
                        # 2. Copy this account's cache to token_cache.json
                        safe_write_json(TOKEN_CACHE_FILE, account_cache)
                        
                        # 3. Run auth flow
                        await run_auth_flow(self.client_id, self.client_secret, use_halo=True)
                        
                        # 4. Save the refreshed tokens to this account's file
                        refreshed_cache = safe_read_json(TOKEN_CACHE_FILE, default={})
                        if refreshed_cache:
                            safe_write_json(cache_file, refreshed_cache)
                            
                            # Check if refresh succeeded
                            new_spartan = refreshed_cache.get("spartan")
                            new_xsts = refreshed_cache.get("xsts")
                            new_xbox = refreshed_cache.get("xsts_xbox")
                            if (new_spartan and is_token_valid(new_spartan) and
                                new_xsts and is_token_valid(new_xsts) and
                                new_xbox and is_token_valid(new_xbox)):
                                additional_accounts.append({
                                    'id': f'account{i}',
                                    'token': new_spartan.get("token"),
                                    'name': f'Account {i}',
                                    'cache_file': cache_file
                                })
                                print(f"Account {i} tokens refreshed successfully")
                                refresh_success = True
                        
                        # 5. Restore Account 1's original cache
                        safe_write_json(TOKEN_CACHE_FILE, account1_backup)
                        
                    except Exception as e:
                        print(f"Error refreshing Account {i}: {e}")
                        # Restore Account 1's cache on error
                        try:
                            safe_write_json(TOKEN_CACHE_FILE, account1_backup)
                        except:
                            pass
                
                # If refresh failed, just skip this account (don't spam device code prompts)
                if not refresh_success:
                    print(f"⚠️ Account {i} needs manual re-auth. Run: python -m src.auth.setup_account {i}")
            
            self.spartan_token = spartan_info.get("token")
            
            # Load Spartan accounts
            self.spartan_accounts = []
            self.spartan_accounts.append({
                'id': 'account1',
                'token': spartan_info.get("token"),
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
                oauth_info = cache.get("oauth")
                if not oauth_info or not oauth_info.get("refresh_token"):
                    print("No OAuth refresh token available for Account 1")
                    print("Run: python get_auth_tokens.py")
                    return False
                
                print("Refreshing Account 1 tokens...")
                
                # Force expiry of all tokens for Account 1
                for key in ["spartan", "clearance", "xsts", "xsts_xbox"]:
                    if key in cache:
                        cache[key]["expires_at"] = 0
                safe_write_json(TOKEN_CACHE_FILE, cache)
                
                # Run auth flow for Account 1
                await run_auth_flow(self.client_id, self.client_secret, use_halo=True)
                
                # Reload and validate Account 1
                cache = safe_read_json(TOKEN_CACHE_FILE, default={})
                spartan_info = cache.get("spartan")
                xsts_info = cache.get("xsts")
                xsts_xbox_info = cache.get("xsts_xbox")
                
                spartan_valid = spartan_info and is_token_valid(spartan_info)
                xsts_valid = xsts_info and is_token_valid(xsts_info)
                xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
                account1_valid = spartan_valid and xsts_valid and xbox_valid
                
                if account1_valid:
                    print("Account 1 tokens refreshed successfully")
                else:
                    print("Account 1 token refresh failed - tokens still invalid")
                    return False
            
            # Refresh additional accounts (2-5) if needed
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
                        print(f"Refreshing Account {i} tokens...")
                        
                        # CRITICAL FIX: Swap cache files so run_auth_flow uses this account's tokens
                        # 1. Backup Account 1's cache
                        account1_backup = safe_read_json(TOKEN_CACHE_FILE, default={})
                        
                        # 2. Copy this account's cache to token_cache.json (so run_auth_flow uses it)
                        safe_write_json(TOKEN_CACHE_FILE, account_cache)
                        
                        # 3. Run auth flow (now uses this account's OAuth token)
                        await run_auth_flow(self.client_id, self.client_secret, use_halo=True)
                        
                        # 4. Save the refreshed tokens to this account's file
                        refreshed_cache = safe_read_json(TOKEN_CACHE_FILE, default={})
                        if refreshed_cache:
                            safe_write_json(cache_file, refreshed_cache)
                        
                        # 5. Restore Account 1's original cache
                        safe_write_json(TOKEN_CACHE_FILE, account1_backup)
                        
                        print(f"Account {i} tokens refreshed")
                    else:
                        print(f"No OAuth refresh token for Account {i}")
                        print(f"Run: python setup_account{i}.py")
            
            # Load tokens if Account 1 is valid (other accounts are optional)
            if account1_valid:
                self.spartan_token = spartan_info.get("token")
                
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
                    'token': spartan_info.get("token"),
                    'name': 'Account 1'
                })
                
                # Add all valid additional accounts
                self.spartan_accounts.extend(additional_accounts)
                print(f"All tokens refreshed - Loaded {len(self.spartan_accounts)} Spartan accounts")
                
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
    
    async def _attempt_device_code_auth(self, account_num: int, cache_file: str) -> Optional[Dict]:
        """
        Attempt device code authentication for a specific account.
        
        This is used when refresh tokens have expired (90+ days without use).
        Prints instructions for the user to authenticate via any browser.
        
        Args:
            account_num: Account number (2-5)
            cache_file: Path to the account's token cache file
            
        Returns:
            Account dict if successful, None if failed/skipped
        """
        from src.auth.tokens import OAuthFlow, XboxAuth, HaloAuth
        import webbrowser
        
        try:
            oauth = OAuthFlow(self.client_id, self.client_secret)
            
            # Start device code flow
            device_info = oauth.start_device_code_flow()
            if not device_info:
                print(f"❌ Account {account_num}: Failed to start device code flow")
                return None
            
            user_code = device_info['user_code']
            verification_uri = device_info['verification_uri']
            
            # Try to copy code to clipboard
            clipboard_success = False
            try:
                import subprocess
                # Windows clipboard
                subprocess.run(['clip'], input=user_code.encode(), check=True)
                clipboard_success = True
            except:
                #try:
                    #import pyperclip
                    #pyperclip.copy(user_code)
                    #clipboard_success = True
                #except:
                pass
            
            # Open browser automatically
            browser_opened = False
            try:
                webbrowser.open(verification_uri)
                browser_opened = True
            except:
                pass
            
            # Print clear instructions
            print(f"\n{'='*60}")
            print(f"🔐 ACCOUNT {account_num} REQUIRES RE-AUTHENTICATION")
            print(f"{'='*60}")
            if browser_opened:
                print(f"✅ Browser opened automatically!")
            else:
                print(f"1. Go to: {verification_uri}")
            if clipboard_success:
                print(f"✅ Code copied to clipboard: {user_code}")
                print(f"   Just paste (Ctrl+V) and sign in!")
            else:
                print(f"2. Enter code: {user_code}")
            print(f"3. Sign in with the Microsoft account for Account {account_num}")
            print(f"   (You have {device_info['expires_in'] // 60} minutes)")
            print(f"{'='*60}")
            print(f"Waiting for authentication...")
            
            # Poll for completion (with timeout)
            oauth_tokens = oauth.poll_device_code(
                device_info['device_code'],
                interval=device_info.get('interval', 5),
                timeout=device_info.get('expires_in', 300)
            )
            
            if not oauth_tokens:
                print(f"❌ Account {account_num}: Device code authentication failed or timed out")
                return None
            
            print(f"✅ Account {account_num}: OAuth tokens received, completing auth flow...")
            
            # Complete the full auth flow with the new OAuth tokens
            # Get Xbox user token (sync)
            user_token = XboxAuth.request_user_token(oauth_tokens['access_token'])
            if not user_token:
                print(f"❌ Account {account_num}: Failed to get Xbox user token")
                return None
            
            # Get dual XSTS tokens (Halo + Xbox) - this also gets the XUID
            xsts_dual = XboxAuth.get_dual_xsts_tokens(user_token['token'])
            if not xsts_dual:
                print(f"❌ Account {account_num}: Failed to get XSTS tokens")
                return None
            
            # Get Spartan token (async)
            spartan = await HaloAuth.request_spartan_token(xsts_dual['token'])
            if not spartan:
                print(f"❌ Account {account_num}: Failed to get Spartan token")
                return None
            
            xuid = xsts_dual.get('xuid')
            
            # Get clearance token (async) - optional
            clearance = None
            if xuid:
                clearance = await HaloAuth.request_clearance(spartan['token'], xuid)
            
            # Build separate XSTS entries for cache compatibility
            xsts_halo = {
                'token': xsts_dual['token'],
                'expires_at': xsts_dual['expires_at'],
                'xuid': xuid,
                'uhs': xsts_dual.get('uhs'),
                'xbox_token': xsts_dual.get('xbox_token'),
                'xbox_expires_at': xsts_dual.get('xbox_expires_at')
            }
            xsts_xbox = {
                'token': xsts_dual.get('xbox_token'),
                'expires_at': xsts_dual.get('xbox_expires_at'),
                'uhs': xsts_dual.get('uhs')
            }
            
            # Build cache
            new_cache = {
                'oauth': oauth_tokens,
                'user': user_token,
                'xsts': xsts_halo,
                'xsts_xbox': xsts_xbox,
                'spartan': spartan,
                'clearance': clearance or {}
            }
            
            # Save to account's cache file
            safe_write_json(cache_file, new_cache)
            
            print(f"✅ Account {account_num}: Successfully authenticated and saved!")
            
            return {
                'id': f'account{account_num}',
                'token': spartan.get("token"),
                'name': f'Account {account_num}',
                'cache_file': cache_file
            }
            
        except Exception as e:
            print(f"❌ Account {account_num}: Device code auth error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
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
                print("Run: python get_auth_tokens.py")
                return False
            
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
            # Check cache first (reverse lookup: gamertag => XUID)
            xuid_cache = load_xuid_cache()
            for xuid, cached_gamertag in xuid_cache.items():
                if cached_gamertag.lower() == gamertag.lower():
                    print(f"Cache hit: '{gamertag}' -> XUID: {xuid}")
                    return str(xuid)
            
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
                    print(f"Run get_auth_tokens.py to authenticate with Xbox Live profile access")
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
                                    print(f"Resolved '{gamertag}' to XUID: {xuid}")
                                    # Save to cache for future lookups
                                    xuid_cache[str(xuid)] = gamertag
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
        # Get Xbox account credentials
        if self.xbox_accounts:
            account_idx = await xbox_profile_rate_limiter.acquire(_account_index)
            account = self.xbox_accounts[account_idx] if account_idx < len(self.xbox_accounts) else self.xbox_accounts[0]
            xbox_token = account['token']
            uhs = account['uhs']
        else:
            account_idx = 0
            # Fallback to loading from cache file (single account mode)
            cache_file = TOKEN_CACHE_FILE
            if not os.path.exists(cache_file):
                print(f"Token cache not found")
                return {'friends': [], 'is_private': False, 'error': 'no_cache'}
            
            with open(cache_file, 'r') as f:
                cache = json.load(f)
            
            xsts_xbox = cache.get('xsts_xbox')
            if not xsts_xbox:
                print(f"Xbox Live XSTS token not found in cache")
                return {'friends': [], 'is_private': False, 'error': 'no_token'}
            
            xbox_token = xsts_xbox.get('token')
            uhs = xsts_xbox.get('uhs')
        
        if not xbox_token or not uhs:
            print(f"Xbox Live XSTS token or UHS missing")
            xbox_profile_rate_limiter.release()
            return {'friends': [], 'is_private': False, 'error': 'missing_token'}
        
        # Use People Hub API to get friends list
        friends_url = f'https://peoplehub.xboxlive.com/users/xuid({xuid})/people/social/decoration/preferredcolor,detail'
        
        headers = {
            'Authorization': f'XBL3.0 x={uhs};{xbox_token}',
            'x-xbl-contract-version': '5',
            'Accept': 'application/json',
            'Accept-Language': 'en-US'
        }
        
        # Use shared cache if provided (for batch operations), otherwise load fresh
        use_shared_cache = _xuid_cache is not None
        xuid_cache = _xuid_cache if use_shared_cache else load_xuid_cache()
        
        # Retry loop with exponential backoff
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(friends_url, headers=headers) as response:
                        if response.status == 200:
                            xbox_profile_rate_limiter.release()
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
                                
                                print(f"⚠️ Rate limited (429) for XUID {xuid} - {current_requests}/{max_requests} requests")
                                print(f"   Attempt {attempt + 1}/{max_retries}, waiting {backoff_wait:.0f}s...")
                                
                                # Set backoff on this account
                                xbox_profile_rate_limiter.set_backoff(account_idx, backoff_wait)
                                
                                await asyncio.sleep(backoff_wait)
                                continue  # Retry
                                
                            except Exception as e:
                                # Couldn't parse - use default exponential backoff
                                backoff_wait = 30 * (2 ** attempt)
                                print(f"⚠️ Rate limited (429), waiting {backoff_wait}s (attempt {attempt + 1}/{max_retries})")
                                await asyncio.sleep(backoff_wait)
                                continue
                        
                        elif response.status == 401:
                            xbox_profile_rate_limiter.release()
                            print(f"Unauthorized (401) - Xbox Live XSTS token invalid for friends list")
                            return {'friends': [], 'is_private': False, 'error': 'unauthorized'}
                        
                        elif response.status == 403:
                            xbox_profile_rate_limiter.release()
                            print(f"Forbidden (403) - Friends list is private for XUID {xuid}")
                            return {'friends': [], 'is_private': True, 'error': None}
                        
                        else:
                            xbox_profile_rate_limiter.release()
                            error_text = await response.text()
                            print(f"People Hub API returned status {response.status}: {error_text[:200]}")
                            return {'friends': [], 'is_private': False, 'error': f'status_{response.status}'}
                            
            except Exception as e:
                print(f"Error getting friends list (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    backoff_wait = 5 * (2 ** attempt)
                    await asyncio.sleep(backoff_wait)
                    continue
                traceback.print_exc()
                xbox_profile_rate_limiter.release()
                return {'friends': [], 'is_private': False, 'error': 'exception'}
        
        # Exhausted retries
        xbox_profile_rate_limiter.release()
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
            concurrency: Max concurrent requests (defaults to number of Xbox accounts, capped at 3)
        
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
        
        # Set concurrency - be conservative due to strict API limits (30 req / 5 min per account)
        # With retries, more concurrency just means more waiting on backoff
        if concurrency is None:
            num_accounts = len(self.xbox_accounts) if self.xbox_accounts else 1
            concurrency = min(num_accounts, 3)  # Cap at 3 concurrent to avoid overwhelming
        
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
            
            async def rate_limited_fetch(friend: Dict, idx: int) -> Dict:
                """Wrapper to apply concurrency limit."""
                async with semaphore:
                    account_idx = idx % concurrency  # Distribute across accounts
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
                except asyncio.CancelledError:
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
        matches_to_process: int = 10
    ) -> Dict:
        """
        Get comprehensive player statistics from Halo API.
        
        Args:
            gamertag: Player's Xbox gamertag
            stat_type: Type of stats ("overall", "ranked", "social")
            matches_to_process: Number of matches to analyze
        
        Returns:
            Dictionary containing stats or error information
        """
        if not self.clearance_token:
            if not await self.get_clearance_token():
                return {"error": 4, "message": "Failed to authenticate with Halo API"}
        
        try:
            # First, resolve gamertag to XUID
            print(f"Resolving gamertag '{gamertag}' to XUID...")
            xuid = await self.resolve_gamertag_to_xuid(gamertag)
            
            if not xuid:
                return {"error": 2, "message": f"Could not resolve gamertag '{gamertag}' to XUID"}
            
            print(f"Using XUID: {xuid}")
            
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
                        with open(cache_file, 'r') as f:
                            cache = json.loads(f.read())
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
                matches_to_process=matches_to_process
            )
            
            if stats_result.get('error') == 0:
                print(f"Stats calculated: {stats_result.get('matches_processed', 0)} matches "
                      f"({stats_result.get('new_matches', 0)} new)")
                return self.parse_stats(stats_result, stat_type, gamertag)
            
            return stats_result
                    
        except Exception as e:
            print(f"EXCEPTION in get_player_stats: {e}")
            traceback.print_exc()
            return {"error": 4, "message": f"API request failed: {str(e)}"}
    
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
    
    def _calculate_stats_from_matches(self, matches: List[Dict], stat_type: str) -> Dict:
        """
        Calculate aggregate stats from a list of matches.
        
        Stats are calculated on-demand rather than stored to save database space.
        
        Args:
            matches: List of match dictionaries with kills, deaths, assists, outcome, is_ranked
            stat_type: "overall", "ranked", or "social" to filter matches
        
        Returns:
            Dictionary containing calculated stats (kd_ratio, avg_kda, win_rate, etc.)
        """
        # Filter matches based on stat_type
        if stat_type == "ranked":
            filtered_matches = [m for m in matches if m.get('is_ranked', False)]
        elif stat_type == "social":
            filtered_matches = [m for m in matches if not m.get('is_ranked', False)]
        else:
            filtered_matches = matches
        
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
            for p in players:
                player_id = p.get('PlayerId', '')
                # Extract XUID from format: 'xuid(2533274924643541)'
                if 'xuid(' in player_id:
                    xuid_str = player_id.replace('xuid(', '').replace(')', '')
                    player_xuids.append(xuid_str)
            
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
                        
                        # Determine if ranked or social based on playlist ID
                        # Common ranked playlists in Halo Infinite have specific asset IDs
                        # You can expand this list based on actual playlist IDs
                        is_ranked = False
                        if playlist_asset_id:
                            # These are example IDs - you may need to update based on actual API data
                            ranked_playlist_ids = [
                                '6e4e9372-5d49-4f87-b0a7-4489b5e96a0b',  # Ranked Arena
                                'edfef3ac-9cbe-4fa2-b949-8f29deafd483',  # Ranked Slayer
                                # Add more ranked playlist IDs as discovered
                            ]
                            is_ranked = playlist_asset_id in ranked_playlist_ids
                        
                        # Build match data with playlist information, map, and player XUIDs
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
                            'map_id': map_asset_id,
                            'map_version': map_version_id,
                            'players': player_xuids
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
                            is_recent = last_match_date >= cutoff_date
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
        try:
            # Check cache first
            cached_data = self.load_cached_stats(xuid, stat_type, gamertag)
            last_update = None
            existing_matches = {}
            
            # Check if cache is sufficient for the request
            cache_is_sufficient = False
            if cached_data:
                last_update = cached_data.get('last_update')
                existing_matches = {m['match_id']: m for m in cached_data.get('processed_matches', [])}
                cached_games = len(existing_matches)
                print(f"Last cache update: {last_update}")
                print(f"Cache contains {cached_games} matches")
                
                # If requesting all matches (999999) but cache has fewer matches than that,
                # we should check if there are more matches available
                # For now, if cache has at least 25 matches and we want all matches,
                # we'll do an incremental fetch to check for new ones
                if matches_to_process >= 999999:
                    # Requesting all matches - always check for new matches
                    print(f"Full match history requested, will check for updates...")
                    cache_is_sufficient = False
                elif cached_games >= matches_to_process:
                    # Cache has enough matches
                    print(f"Cache has {cached_games} matches, sufficient for request of {matches_to_process}")
                    cache_is_sufficient = True
                else:
                    # Cache doesn't have enough matches
                    print(f"Cache has {cached_games} matches but {matches_to_process} requested, will fetch more...")
                    cache_is_sufficient = False
            
            # If cache is sufficient and we're not requesting ALL matches, return cached data immediately
            if cache_is_sufficient and matches_to_process < 999999:
                print(f"Using cached data ({len(existing_matches)} matches)")
                # Calculate stats from cached matches (stats are not stored, only raw match data)
                cached_matches = cached_data.get('processed_matches', [])
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
                                return []  # Return empty to continue with other pages
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
                                # Max retries reached - return empty to continue
                                return []
                        else:
                            print(f"Unexpected status: {response.status}")
                            text = await response.text()
                            print(f"   Response: {text[:200]}")
                            return []
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
                            return []
                    else:
                        print(f"OS Error fetching page at {start_pos}: {e}")
                        return []
                except aiohttp.ClientConnectorError as e:
                    # Handle connection errors (including semaphore timeouts)
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count
                        print(f"Connection error at page {start_pos}, retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})...")
                        await asyncio.sleep(wait_time)
                        return await fetch_match_page(session, start_pos, page_size, retry_count + 1, account_retry, error_retry, rate_limit_retry, force_account=None)
                    else:
                        print(f"Failed after {max_retries} retries: {e}")
                        return []
                except Exception as e:
                    print(f"Error fetching page at {start_pos}: {e}")
                return []
            
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
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Decide fetching strategy: incremental vs full refetch
                # Force full fetch ignores cache completely (used by #populate)
                # Otherwise use incremental fetch if cache exists
                print(f"Fetch params: force_full_fetch={force_full_fetch}, cached_data={'exists' if cached_data else 'none'}, matches_to_process={matches_to_process}")
                
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
                
                # Smart cache update: fetch pages until we hit a cached match
                if use_incremental:
                    print(f"Checking for new matches (incremental fetch)...")
                    new_matches_found = []
                    page_num = 0
                    max_pages_to_check = 1000  # Increased limit (10000 matches max with PAGE_SIZE=100)
                    
                    while page_num < max_pages_to_check:
                        page = await fetch_match_page(session, page_num * PAGE_SIZE, PAGE_SIZE)
                        
                        if page is None:
                            # Got 401 error - need to refresh token
                            got_401_error = True
                            break
                        
                        if not page:
                            # Empty page could mean:
                            # 1. Player has 0 matches (first page, valid scenario)
                            # 2. Reached end of matches (later page)
                            # 3. API error (not 401, would be None)
                            if page_num == 0:
                                # First page empty = player has no match history, not an error
                                print(f"Player has no match history")
                                break
                            print(f"Failed to fetch page {page_num}, stopping search")
                            break
                        
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
                        
                        # Stop if we hit a cached match or empty page
                        if found_cached_match or len(page) < PAGE_SIZE:
                            break
                        
                        page_num += 1
                    
                    # NOTE: Cache completeness check removed - we don't need complete history
                    # for regular stats lookups. If the user wants complete history, they use
                    # force_full_fetch=True (e.g., #populate command), which bypasses cache entirely.
                    # Having 3900 cached matches is still useful even if player has >3900 total.
                    
                    # Check if we got 401 error before returning cached data
                    if got_401_error and not new_matches_found:
                        print(f"Got 401 error, will attempt token refresh...")
                        # Don't return cached data yet, let it fall through to 401 handling below
                    elif not new_matches_found:
                        print(f"No new matches found, using cache ({len(existing_matches)} matches)")
                        # Calculate stats from cached matches (stats are not stored, only raw match data)
                        cached_matches = cached_data.get('processed_matches', [])
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
                    # No cache exists, fetch all matches
                    print(f"No cache found, fetching full match history with rolling queue...")
                    
                    # Rolling queue approach - keep N requests in flight at all times
                    # Conservative concurrency to respect API rate limits and avoid bans
                    all_matches = []
                    num_accounts = len(self.spartan_accounts) if self.spartan_accounts else 1
                    max_in_flight = min(num_accounts * 5, 25)  # 5 per account, max 25 total
                    current_page = 0
                    max_pages = 999999
                    got_empty_page = False
                    
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
                            elif page and len(page) > 0:
                                all_matches.extend(page)
                                # Start next page request immediately
                                if current_page < max_pages and not got_empty_page:
                                    new_task = asyncio.create_task(fetch_and_track(current_page))
                                    pending_tasks.add(new_task)
                                    current_page += 1
                            else:
                                # Empty page - stop starting new requests
                                got_empty_page = True
                        
                        if got_401_error:
                            # Cancel remaining tasks
                            for t in pending_tasks:
                                t.cancel()
                            break
                        
                        # Progress update
                        if len(all_matches) % 500 == 0 and len(all_matches) > 0:
                            print(f"   Fetched {len(all_matches)} matches so far...")
                
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
                    # Incremental update: only process new matches
                    print(f"Processing {len(all_matches)} new matches")
                    matches_to_fetch = [(match.get('MatchId'), xuid) for match in all_matches if match.get('MatchId')]
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
                    result = await self.get_match_stats_for_match(match_id, player_xuid, session)
                    return match_id, result
                
                # Start initial batch
                pending = set()
                match_iter = iter(matches_to_fetch)
                
                for _ in range(min(max_match_requests, len(matches_to_fetch))):
                    try:
                        match_id, player_xuid = next(match_iter)
                        task = asyncio.create_task(fetch_match_detail(match_id, player_xuid))
                        pending.add(task)
                    except StopIteration:
                        break
                
                # Rolling queue - as each completes, start next
                completed = 0
                total = len(matches_to_fetch)
                
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    
                    for task in done:
                        completed += 1
                        try:
                            match_id, result = task.result()
                            if result is not None:
                                new_stats.append(result)
                            else:
                                failed_matches.append(match_id)
                        except Exception as e:
                            failed_matches.append("unknown")
                        
                        # Start next request immediately
                        try:
                            next_match_id, next_xuid = next(match_iter)
                            new_task = asyncio.create_task(fetch_match_detail(next_match_id, next_xuid))
                            pending.add(new_task)
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
                
                # Calculate stats for all three types: overall, ranked, and social
                ranked_matches = [m for m in all_processed_matches if m.get('is_ranked', False)]
                social_matches = [m for m in all_processed_matches if not m.get('is_ranked', False)]
                
                print(f"Match breakdown: {len(all_processed_matches)} total, {len(ranked_matches)} ranked, {len(social_matches)} social")
                
                overall_stats = calculate_stats_for_matches(all_processed_matches)
                ranked_stats = calculate_stats_for_matches(ranked_matches)
                social_stats = calculate_stats_for_matches(social_matches)
                
                # Prepare cache data with all three stat types
                cache_data = {
                    'last_update': datetime.now().isoformat(),
                    'gamertag': gamertag,
                    'xuid': xuid,
                    'stat_type': stat_type,
                    'processed_matches': all_processed_matches,
                    'incomplete_data': len(failed_matches) > 0,
                    'failed_match_count': len(failed_matches),
                    'failed_matches': failed_matches[:50] if len(failed_matches) <= 50 else failed_matches[:50] + ['...truncated'],
                    'stats': {
                        'overall': overall_stats,
                        'ranked': ranked_stats,
                        'social': social_stats
                    }
                }
                
                # Save to cache
                self.save_stats_cache(xuid, stat_type, cache_data, gamertag)
                
                # Return the appropriate stats based on stat_type
                if stat_type == "ranked":
                    selected_stats = ranked_stats
                elif stat_type == "social":
                    selected_stats = social_stats
                else:
                    selected_stats = overall_stats
                
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
        matches_to_process: int = 10
    ) -> 'StatsFind':
        """
        Get player stats using Halo API.
        
        Args:
            gamertag: Player's Xbox gamertag
            stat_type: "stats" (all), "ranked", or "social"
            matches_to_process: Number of matches to process
        
        Returns:
            Self with populated stats
        """
        print(f"Getting {stat_type} stats for {gamertag} "
              f"(matches: {'ALL' if matches_to_process is None else matches_to_process})")
        
        # Map stat_type to API parameters
        stat_type_map = {
            "stats": "overall",
            "ranked": "ranked",
            "social": "social"
        }
        api_stat_type = stat_type_map.get(stat_type, "overall")
        
        try:
            result = await api_client.get_player_stats(
                gamertag, api_stat_type, matches_to_process=matches_to_process
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