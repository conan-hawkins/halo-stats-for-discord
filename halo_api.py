import aiohttp
import asyncio
import json
import time
import os
import hashlib
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from get_auth_tokens import run_auth_flow

# Made by Conan Hawkins 
# 14/10/2025

#============================================================================#
#                                                                            #
# Halo Infinite API client for retrieving stats                             #
# Uses official Halo Waypoint API instead of web scraping                   #
#                                                                            #
#============================================================================#

# File locking for concurrent access
try:
    import portalocker
    HAS_FILE_LOCKING = True
except ImportError:
    HAS_FILE_LOCKING = False
    print("Warning: portalocker not installed. Installing for file locking...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'portalocker'])
    import portalocker
    HAS_FILE_LOCKING = True

class XboxProfileRateLimiter:
    """Rate limiter for Xbox Profile API calls - supports multiple accounts
    
    Conservative rate limits to avoid 429 errors:
    - 3 requests per 10 seconds per account (18 requests/minute per account)
    - With 2 accounts: 36 requests/minute total
    """
    def __init__(self):
        # Track calls per account (key = account_id, value = list of timestamps)
        self.calls_per_account = {}
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self, account_id="account1"):
        """Wait if we're at the rate limit for this specific account"""
        async with self.lock:
            # Initialize tracking for this account if needed
            if account_id not in self.calls_per_account:
                self.calls_per_account[account_id] = []
            
            now = time.time()
            calls = self.calls_per_account[account_id]
            
            # Clean old timestamps (older than 10 seconds)
            calls = [t for t in calls if now - t < 10]
            self.calls_per_account[account_id] = calls
            
            # Check limit (3 per 10 seconds = 0.3 requests/second)
            if len(calls) >= 3:
                wait_time = 10 - (now - calls[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time + 0.1)
                    now = time.time()
                    calls = [t for t in calls if now - t < 10]
                    self.calls_per_account[account_id] = calls
            
            # Record this call
            self.calls_per_account[account_id].append(now)

# Global rate limiter instance
xbox_profile_rate_limiter = XboxProfileRateLimiter()

# Thread-safe file operations
def safe_read_json(filepath: str, default=None):
    """Thread-safe JSON file read with file locking"""
    if not os.path.exists(filepath):
        return default
    
    try:
        with open(filepath, 'r') as f:
            portalocker.lock(f, portalocker.LOCK_SH)  # Shared lock for reading
            try:
                data = json.load(f)
            finally:
                portalocker.unlock(f)
            return data
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return default

def is_token_valid(token_info):
    """Check if a token is valid (not expired)"""
    if not token_info:
        return False
    expires_at = token_info.get("expires_at", 0)
    return expires_at > time.time()

def safe_write_json(filepath: str, data, indent=2):
    """Thread-safe JSON file write with file locking and atomic operation"""
    try:
        # Write to temp file first (atomic operation)
        temp_filepath = filepath + '.tmp'
        with open(temp_filepath, 'w') as f:
            portalocker.lock(f, portalocker.LOCK_EX)  # Exclusive lock for writing
            try:
                json.dump(data, f, indent=indent)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            finally:
                portalocker.unlock(f)
        
        # Atomic rename (replaces old file)
        if os.path.exists(filepath):
            os.replace(temp_filepath, filepath)
        else:
            os.rename(temp_filepath, filepath)
    except Exception as e:
        print(f"Error writing {filepath}: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except:
                pass

class HaloAPIClient:
    def __init__(self):
        # Use the correct Halo API endpoints based on the settings service
        self.settings_url = "https://settings.svc.halowaypoint.com"
        self.stats_url = "https://halostats.svc.halowaypoint.com"
        self.profile_url = "https://profile.svc.halowaypoint.com/users/by-gamertag"
        self.clearance_token = None
        self.spartan_token = None
        self.user_agent = "HaloWaypoint/2021.01.10.01"
        
        # OAuth credentials (environment variables)
        self.client_id = os.getenv('client_id')
        self.client_secret = os.getenv('client_secret')
        
        # Create cache directory for player stats
        self.cache_dir = "player_stats_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Track token refresh attempts (prevent infinite loops)
        self._refresh_in_progress = False
        self._last_refresh_time = 0
        
    async def ensure_valid_tokens(self):
        """
        Centralized token validation and refresh
        Checks ALL tokens (Spartan + Xbox Live XSTS) and refreshes if needed
        Returns True if valid tokens loaded, False otherwise
        """
        # Prevent concurrent refresh attempts
        if self._refresh_in_progress:
            print("Token refresh already in progress, waiting...")
            return False
        
        try:
            cache = safe_read_json("token_cache.json", default={})
            if not cache:
                print("No token cache found")
                print("Run: python get_auth_tokens.py")
                return False
            
            # Check both required tokens
            spartan_info = cache.get("spartan")
            xsts_xbox_info = cache.get("xsts_xbox")
            
            spartan_valid = spartan_info and is_token_valid(spartan_info)
            xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
            
            # If both valid, load and return
            if spartan_valid and xbox_valid:
                self.spartan_token = spartan_info.get("token")
                return True
            
            # Need to refresh - check cooldown (1 minute minimum between refreshes)
            time_since_last = time.time() - self._last_refresh_time
            if time_since_last < 60:
                print(f"Refresh cooldown active ({60-time_since_last:.0f}s remaining)")
                return False
            
            # Check if we have OAuth refresh token
            oauth_info = cache.get("oauth")
            if not oauth_info or not oauth_info.get("refresh_token"):
                print("No OAuth refresh token available")
                print("Run: python get_auth_tokens.py")
                return False
            
            # Perform refresh
            self._refresh_in_progress = True
            self._last_refresh_time = time.time()
            
            print("Refreshing authentication tokens...")
            
            # Force expiry of all tokens
            for key in ["spartan", "clearance", "xsts", "xsts_xbox"]:
                if key in cache:
                    cache[key]["expires_at"] = 0
            safe_write_json("token_cache.json", cache)
            
            # Run auth flow
            await run_auth_flow(self.client_id, self.client_secret, use_halo=True)
            
            # Reload and validate
            cache = safe_read_json("token_cache.json", default={})
            spartan_info = cache.get("spartan")
            xsts_xbox_info = cache.get("xsts_xbox")
            
            # Debug: Check what we got
            print(f"Validating refreshed tokens...")
            print(f"   Spartan: {spartan_info is not None}, Valid: {is_token_valid(spartan_info) if spartan_info else False}")
            if spartan_info:
                print(f"   Spartan expires_at: {spartan_info.get('expires_at')}, Current: {time.time()}")
            print(f"   Xbox: {xsts_xbox_info is not None}, Valid: {is_token_valid(xsts_xbox_info) if xsts_xbox_info else False}")
            if xsts_xbox_info:
                print(f"   Xbox expires_at: {xsts_xbox_info.get('expires_at')}, Current: {time.time()}")
            
            if spartan_info and is_token_valid(spartan_info) and xsts_xbox_info and is_token_valid(xsts_xbox_info):
                self.spartan_token = spartan_info.get("token")
                print(f"All tokens refreshed successfully")
                return True
            else:
                print("Token refresh failed - tokens still invalid after refresh")
                return False
                
        except Exception as e:
            print(f"Token validation error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self._refresh_in_progress = False
    
    async def get_clearance_token(self):
        """Get or refresh the clearance token for API access"""
        try:
            cache_file = "token_cache.json"
            
            print(f"Loading token cache from {cache_file}")
            
            if not os.path.exists(cache_file):
                print(f"ERROR: Token cache file '{cache_file}' not found")
                print("ERROR: You need to run authentication first")
                print("Run: python get_auth_tokens.py")
                return False
            
            try:
                cache = safe_read_json(cache_file, default={})
                if not cache:
                    print(f"ERROR: Failed to parse token cache")
                    return False
                print(f"Cache loaded, keys: {list(cache.keys())}")
            except Exception as e:
                print(f"ERROR: Failed to parse token cache: {e}")
                return False
            
            # Check if we have valid spartan token
            spartan_info = cache.get("spartan")
            spartan_valid = spartan_info and is_token_valid(spartan_info)
            
            # Also check if Xbox Live XSTS token is valid (needed for gamertag resolution)
            xsts_xbox_info = cache.get("xsts_xbox")
            xsts_xbox_valid = xsts_xbox_info and is_token_valid(xsts_xbox_info)
            
            if spartan_valid and xsts_xbox_valid:
                self.spartan_token = spartan_info.get("token")
                if self.spartan_token:
                    print(f"Loaded valid Spartan token (expires: {time.ctime(spartan_info.get('expires_at', 0))})")
                    return True
                else:
                    print("Spartan token structure invalid (no 'token' field)")
            elif spartan_info:
                print(f"Tokens expired - need refresh")
            else:
                print("No tokens found in cache")
            
            return False
            
        except Exception as e:
            import traceback
            print(f"EXCEPTION in get_clearance_token: {e}")
            print(f"TRACEBACK: {traceback.format_exc()}")
            return False
    
    async def resolve_gamertag_to_xuid(self, gamertag):
        """
        Convert gamertag to XUID using Xbox Live Profile API
        
        Uses the Xbox Live XSTS token (not Halo XSTS) to access profile.xboxlive.com
        This is the official Microsoft GDK-documented approach for gamertag lookups.
        
        Rate limited to respect Xbox Profile API limits.
        Checks XUID cache first to avoid unnecessary API calls.
        """
        try:
            # Check cache first (reverse lookup: gamertag → XUID)
            xuid_cache = load_xuid_cache()
            for xuid, cached_gamertag in xuid_cache.items():
                if cached_gamertag.lower() == gamertag.lower():
                    print(f"Cache hit: '{gamertag}' -> XUID: {xuid}")
                    return str(xuid)
            
            # Cache miss - need to resolve via API
            print(f"Cache miss for '{gamertag}', resolving via API...")
            
            # Wait if we're at rate limit
            await xbox_profile_rate_limiter.wait_if_needed()
            
            # Load the Xbox Live XSTS token from cache
            cache_file = "token_cache.json"
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
            
        except Exception as e:
            print(f"Error resolving gamertag: {e}")
            import traceback
            print(f"TRACEBACK: {traceback.format_exc()}")
            return None
        
        return None
    
    async def get_player_stats(self, gamertag, stat_type="overall", matches_to_process=10):
        """
        Get player stats from Halo API
        
        Args:
            gamertag (str): Player's Xbox gamertag
            stat_type (str): Type of stats to retrieve ("overall", "ranked", "social")
            matches_to_process (int or None): Number of matches to analyze (None = all matches)
        
        Returns:
            dict: Player statistics or None if error
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
                    cache_file = "token_cache.json"
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
                print("Using Spartan token for API requests")
            
            if self.clearance_token and self.clearance_token != "skip":
                headers["x-343-authorization-clearance"] = self.clearance_token
                print("Also using Clearance token")
            
            if not self.spartan_token:
                print("No Spartan token available")
            
            # Use comprehensive stats calculation instead of simple endpoint calls
            print(f"Calculating comprehensive stats from match history...")
            
            # Calculate stats from match history with caching
            stats_result = await self.calculate_comprehensive_stats(xuid, stat_type, gamertag=gamertag, matches_to_process=matches_to_process)
            
            if stats_result.get('error') == 0:
                print(f"Stats calculated successfully")
                print(f"{stats_result.get('matches_processed', 0)} matches processed ({stats_result.get('new_matches', 0)} new)")
                return self.parse_stats(stats_result, stat_type, gamertag)
            else:
                print(f"ERROR: Failed to calculate comprehensive stats")
                print(f"ERROR: {stats_result.get('message', 'Unknown error')}")
                return stats_result
                    
        except Exception as e:
            import traceback
            print(f"EXCEPTION in get_player_stats: {e}")
            print(f"TRACEBACK: {traceback.format_exc()}")
            return {"error": 4, "message": f"API request failed: {str(e)}"}
    
    def get_cache_filename(self, xuid: str, stat_type: str, gamertag: str = None) -> str:
        """Generate cache filename for player stats"""
        # Use gamertag if provided, otherwise fall back to XUID hash
        if gamertag:
            # Sanitize gamertag for safe filename (replace spaces and special chars)
            safe_gamertag = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in gamertag)
            return os.path.join(self.cache_dir, f"{safe_gamertag}_{stat_type}.json")
        else:
            # Fallback to hash if no gamertag provided
            xuid_hash = hashlib.md5(str(xuid).encode()).hexdigest()[:8]
            return os.path.join(self.cache_dir, f"{xuid_hash}_{stat_type}.json")
    
    def load_cached_stats(self, xuid: str, stat_type: str, gamertag: str = None) -> Optional[Dict]:
        """Load cached player stats if available (thread-safe)"""
        try:
            cache_file = self.get_cache_filename(xuid, stat_type, gamertag)
            cached_data = safe_read_json(cache_file)
            if cached_data:
                print(f"Loaded cached stats from {cache_file}")
                return cached_data
        except Exception as e:
            print(f"Error loading cache: {e}")
        return None
    
    def is_cache_fresh(self, cached_data: Optional[Dict], max_age_minutes: int = 30) -> bool:
        """Check if cached data is fresh enough to use without API call"""
        if not cached_data:
            return False
        
        try:
            last_update = cached_data.get('last_update')
            if not last_update:
                return False
            
            # Parse the timestamp
            from datetime import datetime, timedelta
            cache_time = datetime.fromisoformat(last_update)
            age = datetime.now() - cache_time
            
            is_fresh = age < timedelta(minutes=max_age_minutes)
            if is_fresh:
                minutes_old = age.total_seconds() / 60
                print(f"Cache is fresh ({minutes_old:.1f} minutes old)")
            return is_fresh
        except Exception as e:
            print(f"Error checking cache freshness: {e}")
            return False
    
    def save_stats_cache(self, xuid: str, stat_type: str, stats_data: Dict, gamertag: str = None) -> None:
        """Save player stats to cache (thread-safe)"""
        try:
            cache_file = self.get_cache_filename(xuid, stat_type, gamertag)
            safe_write_json(cache_file, stats_data)
            print(f"Saved stats to cache: {cache_file}")
        except Exception as e:
            print(f"Error saving cache: {e}")
            traceback.print_exc()
    
    async def get_match_stats_for_match(self, match_id: str, player_xuid: str, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Get detailed stats for a specific match using a shared session"""
        try:
            if not self.spartan_token:
                return None
                
            spartan_token = self.spartan_token
            if isinstance(spartan_token, dict) and 'token' in spartan_token:
                spartan_token = spartan_token['token']
            
            headers = {
                "Authorization": f"Spartan {spartan_token}",
                "x-343-authorization-spartan": spartan_token,
                "User-Agent": self.user_agent,
                "Accept": "application/json"
            }
            
            stats_url = f"https://halostats.svc.halowaypoint.com/hi/matches/{match_id}/stats"
            
            async with session.get(stats_url, headers=headers) as response:
                if response.status == 200:
                    stats_data = await response.json()
                    players = stats_data.get('Players', [])
                    
                    # Find our player's stats
                    for player in players:
                        player_id = player.get('PlayerId', '')
                        if str(player_xuid) in str(player_id):
                            team_stats = player.get('PlayerTeamStats', [])
                            if team_stats:
                                core_stats = team_stats[0].get('Stats', {}).get('CoreStats', {})
                                match_info = stats_data.get('MatchInfo', {})
                                
                                # Build match data with essential info only
                                match_data = {
                                    'match_id': match_id,
                                    'outcome': player.get('Outcome', 0),  # 2=Win, 3=Loss, 4=DNF
                                    'kills': core_stats.get('Kills', 0),
                                    'deaths': core_stats.get('Deaths', 0),
                                    'assists': core_stats.get('Assists', 0),
                                    'start_time': match_info.get('StartTime', ''),
                                    'duration': match_info.get('Duration', 'Unknown'),
                                    'medals': core_stats.get('Medals', [])
                                }
                                return match_data
                else:
                    print(f"Failed to get match stats for {match_id}: {response.status}")
        except Exception as e:
            print(f"Error getting match stats for {match_id}: {e}")
        
        return None
    
    async def calculate_comprehensive_stats(self, xuid: str, stat_type: str, gamertag: str = None, matches_to_process: int = 10, force_full_fetch: bool = False, _retry_count: int = 0) -> Dict:
        """Calculate comprehensive stats from match history
        
        Args:
            xuid: Player XUID
            stat_type: "overall", "ranked", or "social"
            gamertag: Player's gamertag (optional, for cache identification)
            matches_to_process: Number of recent matches to analyze (None = all matches, default 10 for speed)
            force_full_fetch: If True, ignore cache and fetch all matches from scratch (for populate command)
            _retry_count: Internal counter to prevent infinite recursion
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
                return {
                    'error': 0,
                    'stats': cached_data['stats'],
                    'matches_processed': len(existing_matches),
                    'new_matches': 0,
                    'processed_matches': cached_data.get('processed_matches', [])
                }
            
            # Get match history
            if not self.spartan_token:
                return {"error": 4, "message": "No authentication token"}
                
            # Function to get current headers (always uses latest token)
            def get_headers():
                """Get headers with current Spartan token"""
                spartan_token = self.spartan_token
                if isinstance(spartan_token, dict) and 'token' in spartan_token:
                    spartan_token = spartan_token['token']
                return {
                    "Authorization": f"Spartan {spartan_token}",
                    "x-343-authorization-spartan": spartan_token,
                    "User-Agent": self.user_agent,
                    "Accept": "application/json"
                }
            
            async def fetch_match_page(session, start_pos, page_size=25):
                """Fetch a single page of matches"""
                matches_url = f"https://halostats.svc.halowaypoint.com/hi/players/xuid({xuid})/matches?start={start_pos}&count={page_size}"
                try:
                    # Always get fresh headers with current token
                    headers = get_headers()
                    async with session.get(matches_url, headers=headers) as response:
                        if start_pos == 0:
                            print(f"Fetching matches for XUID: {xuid} (gamertag: {gamertag or 'unknown'})")
                        
                        if response.status == 200:
                            match_data = await response.json()
                            results = match_data.get('Results', [])
                            if start_pos == 0:
                                print(f"   First page: {len(results)} matches found")
                            return results
                        elif response.status == 401:
                            print(f"401 Unauthorized - Token expired or invalid")
                            text = await response.text()
                            print(f"   Response: {text[:200]}")
                            # Signal that we got 401 error
                            return None  # Return None instead of [] to distinguish from "no matches"
                        else:
                            print(f"Unexpected status: {response.status}")
                            text = await response.text()
                            print(f"   Response: {text[:200]}")
                except Exception as e:
                    print(f"Error fetching page at {start_pos}: {e}")
                    import traceback
                    print(traceback.format_exc())
                return []
            
            # Fetch multiple pages concurrently to determine total match count
            # Use a connector with higher connection limit for better concurrency
            connector = aiohttp.TCPConnector(limit=200, limit_per_host=200)
            # INCREASED TIMEOUT: Halo API can be slow, especially for detailed match stats
            timeout = aiohttp.ClientTimeout(total=180, connect=30)
            
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
                            # Check if we got a 401 error (token expired)
                            # If first page fails, it might be a 401
                            if page_num == 0:
                                got_401_error = True
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
                    
                    # Check if we got 401 error before returning cached data
                    if got_401_error and not new_matches_found:
                        print(f"Got 401 error, will attempt token refresh...")
                        # Don't return cached data yet, let it fall through to 401 handling below
                    elif not new_matches_found:
                        print(f"No new matches found, using cache ({len(existing_matches)} matches)")
                        # Return cached data in the expected format
                        return {
                            'error': 0,
                            'stats': cached_data['stats'],
                            'matches_processed': len(existing_matches),
                            'new_matches': 0,
                            'processed_matches': cached_data.get('processed_matches', [])
                        }
                    
                    print(f"🆕 Found {len(new_matches_found)} new matches across {page_num + 1} page(s)")
                    all_matches = new_matches_found
                else:
                    # No cache exists, fetch all matches
                    print(f"No cache found, fetching full match history in batches...")
                    
                    # Fetch pages in batches to avoid rate limiting
                    # With PAGE_SIZE=25, fetch more pages per batch
                    all_matches = []
                    page_batch_size = 40  # 40 pages per batch (1000 matches)
                    current_page = 0
                    max_pages = 999999  # Effectively unlimited - stop when API returns empty pages
                    
                    while current_page < max_pages:
                        # Create batch of page requests
                        batch_end = min(current_page + page_batch_size, max_pages)
                        page_tasks = [fetch_match_page(session, i * PAGE_SIZE, PAGE_SIZE) for i in range(current_page, batch_end)]
                        batch_results = await asyncio.gather(*page_tasks)
                        
                        # Check for empty pages
                        found_matches = False
                        for page in batch_results:
                            if page and len(page) > 0:
                                all_matches.extend(page)
                                found_matches = True
                            elif not page or len(page) == 0:
                                # Hit empty page, stop fetching
                                break
                        
                        # If first batch completely failed, likely 401 error
                        if current_page == 0 and not found_matches:
                            got_401_error = True
                            break
                        
                        current_page += page_batch_size
                        
                        # Stop if we found an empty page in this batch
                        if not found_matches or (batch_results and len(batch_results[-1]) < 25):
                            break
                
                # Check if we got 401 errors and need to refresh tokens
                if got_401_error or (not all_matches and not cached_data):
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
                
                print(f"Fetching {len(matches_to_fetch)} new matches concurrently...")
                
                # Fetch all match details concurrently in batches
                batch_size = 25  # Small batches for maximum reliability
                new_stats = []
                failed_matches = []  # Track failed match fetches
                
                for i in range(0, len(matches_to_fetch), batch_size):
                    batch = matches_to_fetch[i:i+batch_size]
                    batch_num = i // batch_size + 1
                    total_batches = (len(matches_to_fetch) + batch_size - 1) // batch_size
                    batch_end = i + batch_size  # Calculate batch end position
                    
                    print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} matches)...")
                    
                    # Fetch all matches in this batch concurrently using shared session
                    tasks = [self.get_match_stats_for_match(match_id, player_xuid, session) 
                            for match_id, player_xuid in batch]
                    batch_results = await asyncio.gather(*tasks)
                    
                    # Add successful results and track failures
                    for idx, result in enumerate(batch_results):
                        if result is not None:
                            new_stats.append(result)
                        else:
                            failed_matches.append(batch[idx][0])  # Save failed match_id
                    
                    # Add delay between batches to reduce API pressure
                    if batch_end < len(matches_to_fetch):
                        await asyncio.sleep(2)
                
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
                
                # Calculate aggregate stats
                total_kills = sum(m.get('kills', 0) for m in all_processed_matches)
                total_deaths = sum(m.get('deaths', 0) for m in all_processed_matches)
                total_assists = sum(m.get('assists', 0) for m in all_processed_matches)
                
                wins = sum(1 for m in all_processed_matches if m.get('outcome') == 2)
                losses = sum(1 for m in all_processed_matches if m.get('outcome') == 3)
                ties = sum(1 for m in all_processed_matches if m.get('outcome') == 1)  # Rare
                dnf = sum(1 for m in all_processed_matches if m.get('outcome') == 4)  # Did Not Finish
                
                games_played = len(all_processed_matches)
                kd_ratio = round(total_kills / total_deaths if total_deaths > 0 else total_kills, 2)
                kda = round((total_kills + (total_assists / 3)) - total_deaths, 2)
                avg_kda = round(kda / games_played if games_played > 0 else 0, 2)
                win_rate = f"{round(wins / games_played * 100 if games_played > 0 else 0, 1)}%"
                
                # Prepare cache data
                cache_data = {
                    'last_update': datetime.now().isoformat(),
                    'gamertag': gamertag,
                    'xuid': xuid,
                    'stat_type': stat_type,
                    'processed_matches': all_processed_matches,
                    'incomplete_data': len(failed_matches) > 0,  # Flag if any matches failed
                    'failed_match_count': len(failed_matches),
                    'failed_matches': failed_matches[:50] if len(failed_matches) <= 50 else failed_matches[:50] + ['...truncated'],  # Save up to 50 failed match IDs
                    'stats': {
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
                }
                
                # Save to cache
                self.save_stats_cache(xuid, stat_type, cache_data, gamertag)
                
                return {
                    'error': 0,
                    'stats': cache_data['stats'],
                    'matches_processed': games_played,
                    'new_matches': new_matches_processed,
                    'processed_matches': all_processed_matches
                    }
                    
        except Exception as e:
            print(f"Error calculating comprehensive stats: {e}")
            import traceback
            print(f"TRACEBACK: {traceback.format_exc()}")
            return {"error": 4, "message": f"Error calculating stats: {str(e)}"}

    def parse_stats(self, api_data, stat_type, gamertag):
        """Parse match history and return comprehensive stats"""
        # This method is now just a wrapper - the real work happens in calculate_comprehensive_stats
        # But we need to return the expected format for the Discord bot
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
        else:
            return {"error": api_data.get('error', 4), "message": api_data.get('message', "Failed to calculate stats")}

# Create a global instance for compatibility with existing bot code
api_client = HaloAPIClient()

# Global XUID-to-Gamertag cache for massive scale operations
XUID_CACHE_FILE = "xuid_gamertag_cache.json"

def load_xuid_cache():
    """Load the persistent XUID -> Gamertag cache (thread-safe)"""
    return safe_read_json(XUID_CACHE_FILE, default={})

def save_xuid_cache(cache):
    """Save the XUID -> Gamertag cache (thread-safe)"""
    try:
        safe_write_json(XUID_CACHE_FILE, cache)
    except Exception as e:
        print(f"Failed to save XUID cache: {e}")
        traceback.print_exc()

async def _fetch_match_players(session, match_id, match_num, spartan_token, main_xuid):
    """
    Helper function to fetch players from a single match
    Returns a set of player XUIDs found in the match
    """
    try:
        stats_url = f"https://halostats.svc.halowaypoint.com/hi/matches/{match_id}/stats"
        headers = {
            "Authorization": f"Spartan {spartan_token}",
            "x-343-authorization-spartan": spartan_token,
            "User-Agent": "HaloWaypoint/2021112313511900 CFNetwork/1327.0.4 Darwin/21.2.0",
            "Accept": "application/json"
        }
        
        async with session.get(stats_url, headers=headers) as response:
            if response.status == 200:
                match_data = await response.json()
                players = match_data.get('Players', [])
                
                # Extract XUIDs from players
                found_xuids = set()
                for player in players:
                    player_id = player.get('PlayerId', '')
                    
                    # Skip the main player
                    if str(main_xuid) in str(player_id):
                        continue
                    
                    # Extract XUID from format: 'xuid(2535463944911967)'
                    if player_id and 'xuid(' in player_id:
                        xuid = player_id.replace('xuid(', '').replace(')', '')
                        found_xuids.add(xuid)
                
                return found_xuids
            else:
                # Return empty set on error
                return set()
    except Exception as e:
        # Return empty set on error
        return set()

async def get_players_from_recent_matches(gamertag: str, num_matches: int = 50, progress_file: str = None) -> list:
    """
    Get list of unique player gamertags encountered in a player's recent matches
    SCALABLE VERSION: Uses persistent XUID cache to handle millions of players efficiently
    
    Args:
        gamertag: The player's Xbox gamertag to analyze
        num_matches: Number of recent matches to analyze (default 50)
        progress_file: Optional file path to save/resume progress
    
    Returns:
        List of unique gamertags encountered (excluding the main player)
    """
    print(f"Extracting players from last {num_matches} matches for {gamertag}...")
    
    # Load persistent XUID cache (shared across all operations)
    xuid_cache = load_xuid_cache()
    cache_hits = 0
    cache_misses = 0
    print(f"Loaded XUID cache with {len(xuid_cache)} existing mappings")
    
    # Load progress if resuming
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
                return []
        
        # Get the main player's XUID and match history
        main_xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
        if not main_xuid:
            print(f"Player {gamertag} not found")
            return []
        
        # Get comprehensive stats for main player (this includes match history)
        # Force full fetch to ensure we get ALL matches, not just new ones
        main_stats = await api_client.calculate_comprehensive_stats(
            main_xuid, 
            "overall", 
            gamertag=gamertag, 
            matches_to_process=num_matches,
            force_full_fetch=True  # Always fetch all matches for populate
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
        # OPTIMIZATION: Process matches in small batches with minimal delay
        # Set generous timeout for slow match stat responses
        timeout = aiohttp.ClientTimeout(total=180, connect=30)
        batch_size = 5  # Process 5 matches concurrently
        async with aiohttp.ClientSession(timeout=timeout) as session:
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
        cache_file = "token_cache.json"
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
        cache_file2 = "token_cache_account2.json"
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
        
        # Set timeout for XUID resolution requests
        timeout = aiohttp.ClientTimeout(total=180, connect=30)
        await asyncio.sleep(2)  # Initial delay
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            xuid_index = resolved_count
            account_index = 0  # Round-robin between accounts
            
            while xuid_index < len(xuids_to_resolve):
                xuid = xuids_to_resolve[xuid_index]
                
                # Select account (round-robin)
                account = accounts[account_index % len(accounts)]
                account_index += 1
                
                try:
                    # Use the rate limiter for this specific account
                    await xbox_profile_rate_limiter.wait_if_needed(account['id'])
                    
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
                        elif response.status == 429:
                            print(f"   Rate limit hit - waiting 2 minutes...")
                            save_xuid_cache(xuid_cache)
                            await asyncio.sleep(120)  # Wait 2 minutes on rate limit
                            continue  # Retry same XUID
                        else:
                            print(f"   Failed to resolve XUID {xuid}: status {response.status}")
                    
                    # Move to next XUID
                    xuid_index += 1
                    
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
        import traceback
        print(traceback.format_exc())
        
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

class StatsFind:
    """
    Compatibility class that maintains the same interface as the original web scraping version
    but uses the Halo API internally
    """
    def __init__(self, gamertag="GT", stats_list="NA", stat_type="NA", error_no=0):
        self.gamertag = gamertag
        self.stats_list = stats_list
        self.stat_type = stat_type
        self.error_no = error_no
        
    async def ensure_valid_tokens(self):
        """
        Wrapper to call api_client's ensure_valid_tokens method
        """
        return await api_client.ensure_valid_tokens()
    
    async def page_getter(self, gamertag, stat_type, matches_to_process=10):
        """
        Get player stats using Halo API (replaces web scraping method)
        
        Args:
            gamertag (str): Player's Xbox gamertag  
            stat_type (str): "stats" or "ranked"
            matches_to_process (int or None): Number of matches to process (None = all matches)
        """
        print(f"Getting {stat_type} stats for {gamertag} via Halo API (matches: {'ALL' if matches_to_process is None else matches_to_process})")
        
        # Map stat_type to API parameters
        api_stat_type = "overall" if stat_type == "stats" else "ranked"
        
        try:
            # Get stats from API
            result = await api_client.get_player_stats(gamertag, api_stat_type, matches_to_process=matches_to_process)
            
            if result.get("error", 0) != 0:
                self.error_no = result["error"]
                print(f"API Error {self.error_no}: {result.get('message', 'Unknown error')}")
                return self
            
            # Set stats for compatibility with existing bot code
            self.stats_list = result["stats_list"]
            self.gamertag = result["gamertag"]
            self.stat_type = result["stat_type"]
            self.error_no = 0
            
            print(f"Stats retrieved successfully for {gamertag}: {self.stats_list}")
            return self
            
        except Exception as e:
            print(f"Unexpected error in page_getter: {e}")
            self.error_no = 4
            return self

# For backward compatibility, create the same global instance
StatsFind1 = StatsFind()