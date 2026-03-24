"""
Background Tasks for Halo Stats Discord Bot

Contains Discord.py task loops for automatic token refresh and player caching.
"""

import os
import json
import asyncio
from discord.ext import tasks

from src.config import XUID_CACHE_FILE, CACHE_PROGRESS_FILE
from src.api import StatsFind1, api_client
from src.database import get_cache


@tasks.loop(hours=1)
async def auto_refresh_tokens():
    """Automatically refresh Halo API tokens every hour"""
    print("Checking token validity...")
    
    if await StatsFind1.ensure_valid_tokens():
        print("Tokens are valid")
    else:
        print("Token validation/refresh failed")


@tasks.loop(hours=168)  # Weekly (168 hours = 7 days)
async def proactive_token_refresh():
    """
    Proactively refresh ALL account tokens weekly to prevent 90-day expiration.
    
    Microsoft OAuth refresh tokens expire after 90 days of inactivity.
    This task ensures all accounts stay active by using their refresh tokens regularly.
    """
    print("🔄 Weekly proactive token refresh for all accounts...")
    
    from src.config import TOKEN_CACHE_FILE, get_token_cache_path
    from src.api.utils import safe_read_json, safe_write_json, is_token_valid
    from src.auth.tokens import run_auth_flow
    
    try:
        # Get client credentials from api_client
        client_id = api_client.client_id
        client_secret = api_client.client_secret
        
        # Refresh accounts 2-5 even if their tokens are still valid
        # This keeps the refresh tokens active
        for i in range(2, 6):
            cache_file = get_token_cache_path(i)
            account_cache = safe_read_json(cache_file, default={})
            
            if not account_cache:
                continue
            
            oauth_info = account_cache.get("oauth")
            if oauth_info and oauth_info.get("refresh_token"):
                print(f"🔄 Proactively refreshing Account {i}...")
                account1_backup = safe_read_json(TOKEN_CACHE_FILE, default={})
                
                try:
                    # Swap to this account's cache
                    candidate_cache = dict(account_cache)

                    # Force-refresh path: expire derived tokens so auth flow must re-mint them.
                    for key in ["spartan", "clearance", "xsts", "xsts_xbox"]:
                        token_info = candidate_cache.get(key)
                        if token_info:
                            token_copy = dict(token_info)
                            token_copy["expires_at"] = 0
                            candidate_cache[key] = token_copy

                    safe_write_json(TOKEN_CACHE_FILE, candidate_cache)
                    
                    # Run auth flow to refresh
                    await run_auth_flow(client_id, client_secret, use_halo=True)
                    
                    # Save refreshed tokens back only if they are valid.
                    refreshed = safe_read_json(TOKEN_CACHE_FILE, default={})
                    refreshed_spartan = refreshed.get("spartan")
                    refreshed_xsts = refreshed.get("xsts")
                    refreshed_xbox = refreshed.get("xsts_xbox")
                    refreshed_valid = (
                        refreshed_spartan and is_token_valid(refreshed_spartan) and
                        refreshed_xsts and is_token_valid(refreshed_xsts) and
                        refreshed_xbox and is_token_valid(refreshed_xbox)
                    )

                    if refreshed and refreshed_valid:
                        safe_write_json(cache_file, refreshed)
                        print(f"✅ Account {i} tokens refreshed proactively")
                    else:
                        print(f"⚠️ Account {i} proactive refresh produced invalid tokens")
                    
                except Exception as e:
                    print(f"⚠️ Account {i} proactive refresh failed: {e}")
                finally:
                    # Always restore Account 1's cache
                    safe_write_json(TOKEN_CACHE_FILE, account1_backup)
        
        print("🔄 Weekly proactive refresh complete")
    except Exception as e:
        print(f"⚠️ Proactive refresh error: {e}")


@tasks.loop(hours=24)
async def auto_cache_all_players():
    """
    Background process: Cache full stats for all players in XUID cache if not already cached
    
    Performance optimizations:
    - Parallel processing: Process 10 players concurrently (with 5 accounts)
    - Progress tracking: Resume from last position on restart
    - Smart skipping: Pre-filter cached players before processing
    """
    print("Starting background stats caching...")
    
    try:
        with open(XUID_CACHE_FILE, 'r') as f:
            xuid_cache = json.load(f)
    except Exception as e:
        print(f"Error loading XUID cache: {e}")
        return
    
    # Load progress tracker
    progress_file = str(CACHE_PROGRESS_FILE)
    progress = {"last_processed_index": 0, "completed_xuids": []}
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            print(f"Resuming from player index {progress['last_processed_index']}")
        except:
            pass
    
    # Convert to list for indexing
    xuid_items = list(xuid_cache.items())
    total = len(xuid_items)
    
    # Pre-filter: Skip players who already have cache
    players_to_process = []
    already_processed = 0
    
    start_idx = progress['last_processed_index']
    completed_set = set(progress['completed_xuids'])
    
    # Count players already processed
    already_processed = start_idx
    
    # Get SQLite v2 cache instance for checking
    stats_cache = get_cache()
    
    for idx in range(start_idx, total):
        xuid, gamertag = xuid_items[idx]
        
        # Skip if already completed in this session
        if xuid in completed_set:
            already_processed += 1
            continue
        
        # Skip if player has cached data in SQLite v2 database
        if stats_cache.check_player_cached(xuid, "overall", gamertag):
            already_processed += 1
            completed_set.add(xuid)
            continue
        
        players_to_process.append((idx, xuid, gamertag))
    
    print(f"Total: {total}, Already processed: {already_processed}, To process: {len(players_to_process)}")
    
    if not players_to_process:
        print("All players already cached!")
        return
    
    # Process with rolling concurrency - aggressive throughput
    cached = errors = 0
    max_concurrent = 25  # 5 per account with rolling queue
    
    async def process_player(idx, xuid, gamertag):
        """Process a single player and return result"""
        try:
            print(f"[{idx+1}/{total}] Caching: {gamertag} (XUID: {xuid})")
            result = await api_client.calculate_comprehensive_stats(xuid, "overall", gamertag, None)
            
            if result and result.get('error') == 0:
                return ('success', idx, xuid)
            else:
                return ('error', idx, xuid)
        except Exception as e:
            print(f"Error caching {gamertag}: {e}")
            return ('error', idx, xuid)
    
    # Rolling concurrency - keep all slots busy
    pending = set()
    player_iter = iter(players_to_process)
    
    # Start initial batch
    for _ in range(max_concurrent):
        try:
            player_data = next(player_iter)
            task = asyncio.create_task(process_player(*player_data))
            task.player_data = player_data  # Store for progress tracking
            pending.add(task)
        except StopIteration:
            break
    
    # Process with rolling concurrency
    while pending:
        # Wait for any task to complete
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        
        # Process completed tasks
        for task in done:
            result = task.result() if not task.exception() else ('error', task.player_data[0], task.player_data[1])
            
            if isinstance(result, tuple):
                if result[0] == 'success':
                    cached += 1
                    completed_set.add(result[2])
                else:
                    errors += 1
            else:
                errors += 1
            
            # Save progress
            progress['last_processed_index'] = task.player_data[0] + 1
            progress['completed_xuids'] = list(completed_set)
            
            try:
                with open(progress_file, 'w') as f:
                    json.dump(progress, f)
            except:
                pass
            
            # Start next player to keep slot filled
            try:
                player_data = next(player_iter)
                new_task = asyncio.create_task(process_player(*player_data))
                new_task.player_data = player_data
                pending.add(new_task)
            except StopIteration:
                pass  # No more players
        
        # Progress update periodically
        if cached % 50 == 0:
            print(f"Progress: {cached} cached, {errors} errors, {already_processed + len(completed_set)} total completed")
    
    print(f"Caching complete: {cached} new, {already_processed} already processed, {errors} errors")
    
    # Reset progress file when complete
    try:
        if os.path.exists(progress_file):
            os.remove(progress_file)
    except:
        pass
