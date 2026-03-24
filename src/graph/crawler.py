"""
Social Graph Crawler for Halo Infinite Player Network Analysis
==============================================================

Implements a depth-limited graph crawler that:
- Expands only Halo-active players to avoid exponential explosion
- Collects friend relationships and Halo stats
- Supports resumable crawling with queue persistence
- Integrates with existing HaloAPIClient

Key Design Decisions:
- Halo-only filter: Only expand nodes for players active in Halo since Sept 2025
- Depth limit: Default max depth of 3 hops
- Rate limiting: Uses existing Xbox account pool for parallel requests
- Incremental: Saves progress continuously for resumability

Author: Graph Analysis Extension
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

from src.database.graph_schema import get_graph_db, HaloSocialGraphDB
from src.api.client import HaloAPIClient


class CrawlStatus(Enum):
    """Status of a crawl operation"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CrawlConfig:
    """Configuration for graph crawl"""
    max_depth: int = 3
    halo_active_since: datetime = field(default_factory=lambda: datetime(2025, 9, 1))
    concurrency: int = 3
    batch_size: int = 10
    collect_stats: bool = True
    stats_matches_to_process: int = 50
    collect_full_history: bool = False
    min_crawl_age_hours: int = 24
    max_friends_per_node: int = 1000
    sample_high_degree: bool = True
    progress_callback: Optional[Callable] = None
    save_interval: int = 10  # Save progress every N nodes


@dataclass
class CrawlProgress:
    """Track crawl progress"""
    nodes_discovered: int = 0
    edges_discovered: int = 0
    halo_players_found: int = 0
    nodes_crawled: int = 0
    nodes_with_stats: int = 0
    private_profiles: int = 0
    errors: int = 0
    current_depth: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    

class GraphCrawler:
    """
    Async graph crawler for building the Halo social network.
    
    Uses BFS traversal with Halo-active filtering to avoid explosion.
    Integrates with existing HaloAPIClient for data fetching.
    """
    
    def __init__(
        self,
        api_client: HaloAPIClient,
        config: CrawlConfig = None,
        graph_db: HaloSocialGraphDB = None
    ):
        self.api = api_client
        self.config = config or CrawlConfig()
        self.db = graph_db or get_graph_db()
        self.progress = CrawlProgress()
        self._running = False
        self._paused = False
    
    async def crawl_from_seed(
        self,
        seed_gamertag: str = None,
        seed_xuid: str = None,
        crawl_name: str = None
    ) -> CrawlProgress:
        """
        Start a crawl from a seed player.
        
        Args:
            seed_gamertag: Starting player's gamertag (will resolve to XUID)
            seed_xuid: Starting player's XUID (alternative to gamertag)
            crawl_name: Name for this crawl session (for tracking)
        
        Returns:
            CrawlProgress with final statistics
        """
        # Resolve seed player
        if seed_gamertag and not seed_xuid:
            print(f"[CRAWLER] Resolving seed gamertag: {seed_gamertag}")
            seed_xuid = await self.api.resolve_gamertag_to_xuid(seed_gamertag)
            if not seed_xuid:
                print(f"[CRAWLER] Failed to resolve gamertag: {seed_gamertag}")
                return self.progress
        
        if not seed_xuid:
            print("[CRAWLER] No seed provided")
            return self.progress
        
        print(f"[CRAWLER] Starting crawl from seed XUID: {seed_xuid}")

        # Recover stale queue state from interrupted runs.
        recovered_in_progress = self.db.requeue_in_progress_items()
        if recovered_in_progress:
            print(f"[CRAWLER] Re-queued {recovered_in_progress} stale in-progress items")

        # Retry previously failed items caused by known datetime mismatch issues.
        retried_datetime_failures = self.db.retry_failed_items(
            error_contains="offset-naive and offset-aware"
        )
        if retried_datetime_failures:
            print(f"[CRAWLER] Re-queued {retried_datetime_failures} prior datetime-failed items")
        
        # Check if seed is Halo-active using the API
        from datetime import datetime
        CUTOFF_DATE = self.config.halo_active_since if hasattr(self.config, 'halo_active_since') else datetime(2025, 9, 1)
        is_active = False
        try:
            is_active, _ = await self.api.check_recent_halo_activity(seed_xuid, CUTOFF_DATE)
        except Exception as e:
            print(f"[CRAWLER] Error checking seed activity: {e}")

        self.db.insert_or_update_player(
            xuid=seed_xuid,
            gamertag=seed_gamertag,
            halo_active=is_active,
            crawl_depth=0,
            is_seed=True
        )
        
        # Reset the seed's last_crawled so it gets processed again
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE graph_players SET last_crawled = NULL WHERE xuid = ?", (seed_xuid,))
        conn.commit()
        
        # Add to crawl queue with force_pending=True to reset any previous status
        self.db.add_to_crawl_queue(seed_xuid, priority=100, depth=0, force_pending=True)
        
        print(f"[CRAWLER] Seed player queued for processing")
        
        # Start BFS crawl
        self._running = True
        self.progress = CrawlProgress()
        
        try:
            await self._bfs_crawl()
        except Exception as e:
            print(f"[CRAWLER] Crawl error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._running = False
        
        return self.progress
    
    async def resume_crawl(self) -> CrawlProgress:
        """Resume a previously paused or interrupted crawl"""
        recovered_in_progress = self.db.requeue_in_progress_items()
        if recovered_in_progress:
            print(f"[CRAWLER] Re-queued {recovered_in_progress} stale in-progress items")

        queue_stats = self.db.get_queue_stats()
        pending = queue_stats.get('pending', 0) + queue_stats.get('in_progress', 0)
        
        if pending == 0:
            print("[CRAWLER] No pending items in queue")
            return self.progress
        
        print(f"[CRAWLER] Resuming crawl with {pending} pending items")
        
        self._running = True
        self.progress = CrawlProgress()
        
        try:
            await self._bfs_crawl()
        except Exception as e:
            print(f"[CRAWLER] Resume error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._running = False
        
        return self.progress
    
    async def _bfs_crawl(self):
        """
        Perform BFS traversal of the social graph.
        
        Key optimizations:
        - Only expands Halo-active players
        - Uses queue persistence for resumability
        - Batches API requests for efficiency
        """
        nodes_since_save = 0
        
        while self._running and not self._paused:
            # Get next batch from queue
            batch = self.db.get_next_from_queue(batch_size=self.config.batch_size)
            
            if not batch:
                print("[CRAWLER] Queue empty, crawl complete")
                break
            
            # Process batch concurrently
            tasks = []
            for item in batch:
                if item['depth'] > self.config.max_depth:
                    self.db.mark_queue_item_complete(item['xuid'])
                    continue
                
                tasks.append(self._process_node(item['xuid'], item['depth']))
            
            if tasks:
                await asyncio.gather(*tasks)
            
            nodes_since_save += len(batch)
            
            # Progress callback
            if self.config.progress_callback:
                await self._call_progress_callback()
            
            # Periodic logging
            if nodes_since_save >= self.config.save_interval:
                self._log_progress()
                nodes_since_save = 0
        
        self._log_progress()
    
    async def _process_node(self, xuid: str, depth: int):
        """
        Process a single node in the graph.
        
        1. Fetch friends list
        2. Check if friends are Halo-active
        3. Add edges to graph
        4. Queue Halo-active friends for expansion
        5. Optionally collect stats
        """
        try:
            self.progress.current_depth = depth
            
            # Check if we should expand this node
            if depth >= self.config.max_depth:
                self.db.mark_queue_item_complete(xuid)
                return
            
            # Get player info
            player = self.db.get_player(xuid)
            gamertag = player.get('gamertag') if player else None
            
            print(f"[CRAWLER] Processing: {gamertag or xuid} (depth={depth})")
            
            # Early check: skip API call if profile is already known to be private
            profile_visibility = player.get('profile_visibility') if player else 'unknown'
            if profile_visibility == 'private':
                print(f"[CRAWLER] Skipping {gamertag or xuid} - profile already marked as private")
                self.progress.private_profiles += 1
                self.db.mark_queue_item_complete(xuid)
                return
            
            # Fetch friends list
            friends_result = await self.api.get_friends_list(xuid)
            
            if friends_result.get('error'):
                if friends_result.get('is_private'):
                    self.progress.private_profiles += 1
                    self.db.insert_or_update_player(
                        xuid=xuid,
                        profile_visibility='private'
                    )
                else:
                    self.progress.errors += 1
                
                self.db.mark_queue_item_complete(xuid, error=friends_result.get('error'))
                return
            
            friends = friends_result.get('friends', [])
            self.progress.nodes_crawled += 1
            
            # Update player with friends count
            self.db.insert_or_update_player(
                xuid=xuid,
                friends_count=len(friends),
                profile_visibility='public'
            )
            self.db.mark_player_crawled(xuid)
            
            # Sample if high degree
            if len(friends) > self.config.max_friends_per_node and self.config.sample_high_degree:
                print(f"[CRAWLER] High degree node ({len(friends)} friends), sampling {self.config.max_friends_per_node}")
                # Keep mutual friends and sample the rest
                mutual = [f for f in friends if f.get('is_mutual')]
                others = [f for f in friends if not f.get('is_mutual')]
                import random
                random.shuffle(others)
                friends = mutual + others[:self.config.max_friends_per_node - len(mutual)]
            
            # Process friends - collect info but DON'T insert yet
            # We'll insert with correct halo_active status after checking
            edges_to_insert = []
            players_to_check = []
            players_to_insert_stub = []  # Stub players needed for FK constraint
            
            for friend in friends:
                friend_xuid = friend.get('xuid')
                friend_gamertag = friend.get('gamertag')
                is_mutual = friend.get('is_mutual', False)
                
                if not friend_xuid:
                    continue
                
                # Collect stub player data for FK constraint
                # Will be updated with correct halo_active status after checking
                players_to_insert_stub.append((friend_xuid, friend_gamertag))
                
                self.progress.nodes_discovered += 1
                
                # Add edge
                edges_to_insert.append((
                    xuid, friend_xuid, is_mutual,
                    xuid, depth + 1
                ))
                
                # Also add reverse edge if mutual
                if is_mutual:
                    edges_to_insert.append((
                        friend_xuid, xuid, True,
                        xuid, depth + 1
                    ))
                
                players_to_check.append((friend_xuid, friend_gamertag, is_mutual))
            
            # Insert stub players FIRST to satisfy FK constraint
            # These will be updated with correct halo_active status in _check_and_queue_halo_players
            for stub_xuid, stub_gamertag in players_to_insert_stub:
                self.db.insert_or_update_player(
                    xuid=stub_xuid,
                    gamertag=stub_gamertag,
                    crawl_depth=depth + 1
                )
            
            # Batch insert edges (now FK constraint is satisfied)
            if edges_to_insert:
                self.db.insert_friend_edges_batch(edges_to_insert)
                self.progress.edges_discovered += len(edges_to_insert)
            
            # Check which friends are Halo-active and queue them
            await self._check_and_queue_halo_players(players_to_check, depth + 1, discovered_from=xuid)
            
            # Collect stats for this node if configured
            if self.config.collect_stats and depth <= 1:  # Only collect stats for first 2 levels
                await self._collect_player_stats(xuid, gamertag)
            
            self.db.mark_queue_item_complete(xuid)
            
        except Exception as e:
            print(f"[CRAWLER] Error processing {xuid}: {e}")
            self.progress.errors += 1
            self.db.mark_queue_item_complete(xuid, error=str(e))
    
    async def _check_and_queue_halo_players(
        self,
        players: List[Tuple[str, str, bool]],
        depth: int,
        discovered_from: str = None
    ):
        """
        Check if players are Halo-active and queue them for crawling.
        
        Uses XUID cache for instant recognition, then concurrent API calls for all others.
        Re-validates all players to catch those who became active again.
        Only queues Halo-active players to avoid exponential explosion.
        
        Args:
            players: List of (xuid, gamertag, is_mutual) tuples
            depth: Crawl depth for these players
            discovered_from: XUID of the player who led us to these friends
        """
        if depth > self.config.max_depth:
            return
        
        queue_items = []

        def _parse_iso_datetime(value):
            """Best-effort ISO datetime parsing for DB timestamps."""
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    return None
            return None

        def _normalize_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
            """Convert datetime to UTC naive for safe comparisons."""
            if dt is None:
                return None
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        
        print(f"[CRAWLER] Checking {len(players)} friends for Halo activity...")
        
        # Categorize players for efficient checking.
        # Strict fast-path only for players with recent Halo feature evidence.
        confirmed_cache_hits = []
        needs_check = []
        stale_revalidation_candidates = 0
        
        for xuid, gamertag, is_mutual in players:
            player = self.db.get_player(xuid)

            # Early short-circuit: do not spend activity-check budget on known private profiles.
            if player and player.get('profile_visibility') == 'private':
                self.progress.private_profiles += 1
                self.db.insert_or_update_player(
                    xuid=xuid,
                    gamertag=gamertag,
                    halo_active=False,
                    crawl_depth=depth,
                    profile_visibility='private',
                )
                continue

            if player and player.get('halo_active') == 1:
                # Strict fast-path: require recent match evidence and non-zero match volume.
                features = self.db.get_halo_features(xuid)
                matches_played = (features or {}).get('matches_played') or 0
                last_match_value = (features or {}).get('last_match') or (features or {}).get('last_match_date')
                last_match_dt = _parse_iso_datetime(last_match_value)
                normalized_last_match = _normalize_utc_naive(last_match_dt)
                normalized_cutoff = _normalize_utc_naive(self.config.halo_active_since)

                if normalized_last_match and normalized_last_match >= normalized_cutoff and matches_played > 0:
                    confirmed_cache_hits.append((xuid, gamertag, is_mutual))
                else:
                    stale_revalidation_candidates += 1
                    needs_check.append((xuid, gamertag, is_mutual, False))
            else:
                # Unconfirmed players are re-validated.
                needs_check.append((xuid, gamertag, is_mutual, False))

        print(f"[CRAWLER]   Confirmed active (strict fast path): {len(confirmed_cache_hits)}")
        print(f"[CRAWLER]   Stale/ambiguous active players to revalidate: {stale_revalidation_candidates}")
        print(f"[CRAWLER]   Need verification: {len(needs_check)}/{len(players)}")
        
        active_count = 0
        
        # Process confirmed cache hits (instant, no API calls needed)
        for xuid, gamertag, is_mutual in confirmed_cache_hits:
            player = self.db.get_player(xuid)
            
            self.db.insert_or_update_player(
                xuid=xuid,
                gamertag=gamertag,
                halo_active=True,
                crawl_depth=depth
            )
            
            # Depth-1 should always be traversed for a new seed crawl, even if previously crawled.
            if depth == 1 or not player or player.get('last_crawled') is None:
                queue_items.append((xuid, 50, depth))
                self.progress.halo_players_found += 1
                active_count += 1
        
        # Process players that need verification - run concurrently with 5-player semaphore
        async def check_and_insert(xuid: str, gamertag: str, is_mutual: bool, is_cache_hit: bool) -> bool:
            """Check player activity and insert into DB, return True if Halo-active."""
            try:
                # Activity checks are DB/API-based; XUID cache is not used as an activity signal.
                is_active = await self._is_halo_active(
                    xuid,
                    gamertag,
                    xuid_cache=None,
                    trust_xuid_cache=False,
                )
                
                self.db.insert_or_update_player(
                    xuid=xuid,
                    gamertag=gamertag,
                    halo_active=is_active,
                    crawl_depth=depth
                )
                
                return is_active
            except Exception as e:
                print(f"[CRAWLER] Error checking {xuid}: {e}")
                return False
        
        # Run checks with limited concurrency (default 5 concurrent API calls)
        if needs_check:
            sem = asyncio.Semaphore(5)
            verified_players = len(needs_check)
            print(f"[CRAWLER]   Verified players via DB/API checks: {verified_players}")
            
            async def sem_check(xuid: str, gamertag: str, is_mutual: bool, is_cache_hit: bool):
                async with sem:
                    return await check_and_insert(xuid, gamertag, is_mutual, is_cache_hit)
            
            tasks = [
                sem_check(xuid, gamertag, is_mutual, is_cache_hit)
                for xuid, gamertag, is_mutual, is_cache_hit in needs_check
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for (xuid, gamertag, is_mutual, is_cache_hit), result in zip(needs_check, results):
                if isinstance(result, bool) and result:
                    # Queue if active, regardless of prior last_crawled status
                    # (players can become active again after being inactive)
                    queue_items.append((xuid, 50, depth))
                    self.progress.halo_players_found += 1
                    active_count += 1
        
        print(f"[CRAWLER] Halo check complete: {active_count}/{len(players)} are Halo-active")
        
        # Batch add to queue
        if queue_items:
            # Depth-1 items should be re-queued for each new seed crawl even if previously completed.
            self.db.add_to_crawl_queue_batch(queue_items, force_pending=(depth == 1))
            print(f"[CRAWLER] Queued {len(queue_items)} players for crawling at depth {depth}")
    
    async def _is_halo_active(
        self,
        xuid: str,
        gamertag: str = None,
        xuid_cache: dict = None,
        trust_xuid_cache: bool = True,
    ) -> bool:
        """
        Check if a player is recently active in Halo Infinite.
        
        Uses multiple checks in order of speed (fastest first):
        1. Check graph DB halo_features table - very fast
        2. Lightweight API check - fetches only last match date (slow, use sparingly)
        
        Returns True if player has played Halo since September 2025.
        
        Args:
            xuid: Player's XUID
            gamertag: Player's gamertag (optional)
            xuid_cache: Reserved for compatibility (not used for activity decisions)
        """
        cutoff_date = self.config.halo_active_since if hasattr(self.config, 'halo_active_since') else datetime(2025, 9, 1)

        def _parse_iso_datetime(value):
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    return None
            return None

        def _normalize_utc_naive(dt: Optional[datetime]) -> Optional[datetime]:
            """Convert datetime to UTC naive for safe comparisons."""
            if dt is None:
                return None
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        
        try:
            # 1. Check if we already have halo_features for this player (cached DB lookup)
            features = self.db.get_halo_features(xuid)
            if features and (features.get('last_match') or features.get('last_match_date')):
                last_match = _parse_iso_datetime(features.get('last_match') or features.get('last_match_date'))
                if last_match is None:
                    return False
                normalized_last_match = _normalize_utc_naive(last_match)
                normalized_cutoff = _normalize_utc_naive(cutoff_date)
                return normalized_last_match >= normalized_cutoff
            
            # 2. Slow check: API call only if not in DB
            # This is expensive, so we only do it for players not yet evaluated
            is_recent, last_match_date = await self.api.check_recent_halo_activity(xuid, cutoff_date)
            return is_recent
        except Exception as e:
            print(f"[CRAWLER] Error checking Halo activity for {xuid}: {e}")
            return False
    
    async def _collect_player_stats(self, xuid: str, gamertag: str = None):
        """
        Collect comprehensive Halo stats for a player.
        
        Stores results in the halo_features table.
        """
        try:
            matches_to_process = None if self.config.collect_full_history else self.config.stats_matches_to_process

            # Get stats via API
            stats = await self.api.calculate_comprehensive_stats(
                xuid=xuid,
                stat_type="overall",
                gamertag=gamertag,
                matches_to_process=matches_to_process,
                force_full_fetch=self.config.collect_full_history
            )
            
            if stats.get('error') != 0:
                return
            
            # Extract features
            processed_matches = stats.get('processed_matches', [])
            computed_stats = stats.get('stats', {})
            
            if not processed_matches:
                return
            
            # Calculate features
            total_matches = len(processed_matches)
            ranked_matches = sum(1 for m in processed_matches if m.get('is_ranked'))
            social_matches = total_matches - ranked_matches
            
            total_kills = sum(m.get('kills', 0) for m in processed_matches)
            total_deaths = sum(m.get('deaths', 0) for m in processed_matches)
            total_assists = sum(m.get('assists', 0) for m in processed_matches)
            
            wins = sum(1 for m in processed_matches if m.get('outcome') == 2)
            
            kd_ratio = total_kills / max(total_deaths, 1)
            win_rate = (wins / max(total_matches, 1)) * 100
            avg_kills = total_kills / max(total_matches, 1)
            avg_deaths = total_deaths / max(total_matches, 1)
            
            ranked_ratio = ranked_matches / max(total_matches, 1)
            
            # Get match timestamps
            match_times = []
            for m in processed_matches:
                start_time = m.get('start_time')
                if start_time:
                    try:
                        parsed = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        # Normalize all timestamps to naive UTC so mixed timezone inputs compare safely.
                        if parsed.tzinfo is not None:
                            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
                        match_times.append(parsed)
                    except:
                        pass
            
            last_match = max(match_times).isoformat() if match_times else None
            first_match = min(match_times).isoformat() if match_times else None
            
            # Calculate matches per week
            matches_week = 0
            if match_times and len(match_times) >= 2:
                time_span = (max(match_times) - min(match_times)).days
                if time_span > 0:
                    matches_week = (total_matches / time_span) * 7
            
            # CSR is optional in the current stats payload. Keep it as NULL when absent.
            csr = computed_stats.get('estimated_csr')
            if csr is None:
                csr = computed_stats.get('csr')
            csr_tier = computed_stats.get('csr_tier')
            
            # Save to database
            self.db.insert_or_update_halo_features(
                xuid=xuid,
                gamertag=gamertag,
                csr=csr,
                csr_tier=csr_tier,
                kd_ratio=round(kd_ratio, 2),
                win_rate=round(win_rate, 1),
                matches_played=total_matches,
                matches_week=round(matches_week, 1),
                ranked_matches=ranked_matches,
                social_matches=social_matches,
                ranked_ratio=round(ranked_ratio, 2),
                total_kills=total_kills,
                total_deaths=total_deaths,
                total_assists=total_assists,
                avg_kills=round(avg_kills, 1),
                avg_deaths=round(avg_deaths, 1),
                last_match=last_match,
                first_match=first_match
            )
            
            cutoff_date = self.config.halo_active_since if hasattr(self.config, 'halo_active_since') else datetime(2025, 9, 1)
            if cutoff_date.tzinfo is not None:
                cutoff_date = cutoff_date.astimezone(timezone.utc).replace(tzinfo=None)
            latest_match_dt = max(match_times) if match_times else None
            is_recently_active = bool(latest_match_dt and latest_match_dt >= cutoff_date)

            # Keep halo_active aligned with the configured recency cutoff.
            self.db.insert_or_update_player(
                xuid=xuid,
                gamertag=gamertag,
                halo_active=is_recently_active
            )
            
            self.progress.nodes_with_stats += 1
            
        except Exception as e:
            print(f"[CRAWLER] Error collecting stats for {xuid}: {e}")
    
    async def _call_progress_callback(self):
        """Call the progress callback if configured"""
        if self.config.progress_callback:
            try:
                if asyncio.iscoroutinefunction(self.config.progress_callback):
                    await self.config.progress_callback(self.progress)
                else:
                    self.config.progress_callback(self.progress)
            except Exception as e:
                print(f"[CRAWLER] Progress callback error: {e}")
    
    def _log_progress(self):
        """Log current crawl progress"""
        elapsed = (datetime.now() - self.progress.start_time).total_seconds()
        rate = self.progress.nodes_crawled / max(elapsed, 1) * 60  # nodes per minute
        
        print(f"\n[CRAWLER] === Progress Report ===")
        print(f"  Nodes discovered: {self.progress.nodes_discovered}")
        print(f"  Edges discovered: {self.progress.edges_discovered}")
        print(f"  Halo players found: {self.progress.halo_players_found}")
        print(f"  Nodes crawled: {self.progress.nodes_crawled}")
        print(f"  Nodes with stats: {self.progress.nodes_with_stats}")
        print(f"  Private profiles: {self.progress.private_profiles}")
        print(f"  Errors: {self.progress.errors}")
        print(f"  Current depth: {self.progress.current_depth}")
        print(f"  Rate: {rate:.1f} nodes/min")
        print(f"  Elapsed: {elapsed/60:.1f} min")
        
        # Get queue stats
        queue_stats = self.db.get_queue_stats()
        print(f"  Queue: {queue_stats}")
        print("=" * 40 + "\n")
    
    def pause(self):
        """Pause the crawl (can be resumed)"""
        self._paused = True
        print("[CRAWLER] Crawl paused")
    
    def stop(self):
        """Stop the crawl"""
        self._running = False
        self._paused = False
        print("[CRAWLER] Crawl stopped")


async def collect_coplay_data(
    api_client: HaloAPIClient,
    graph_db: HaloSocialGraphDB = None,
    xuids: List[str] = None,
    matches_to_analyze: int = 50
):
    """
    Collect co-play data from match history.
    
    For each player, analyzes their recent matches to find
    who they've played with (teammates and opponents).
    
    Args:
        api_client: HaloAPIClient instance
        graph_db: Graph database instance
        xuids: List of XUIDs to analyze (defaults to all Halo-active players)
        matches_to_analyze: Number of matches per player to analyze
    """
    db = graph_db or get_graph_db()
    
    if not xuids:
        # Get all Halo-active players
        players = db.get_halo_active_players(limit=10000)
        xuids = [p['xuid'] for p in players]
    
    print(f"[COPLAY] Analyzing co-play data for {len(xuids)} players")
    
    for i, xuid in enumerate(xuids):
        if i % 10 == 0:
            print(f"[COPLAY] Progress: {i}/{len(xuids)}")
        
        try:
            # Get player's matches
            stats = await api_client.calculate_comprehensive_stats(
                xuid=xuid,
                stat_type="overall",
                matches_to_process=matches_to_analyze
            )
            
            if stats.get('error') != 0:
                continue
            
            processed_matches = stats.get('processed_matches', [])
            
            # Track co-play relationships
            coplay_map = {}  # partner_xuid -> {matches, wins, minutes}
            
            for match in processed_matches:
                match_id = match.get('match_id')
                team = match.get('team')
                outcome = match.get('outcome')
                duration = match.get('duration', '00:00:00')
                
                # Parse duration to minutes
                try:
                    parts = duration.split(':')
                    minutes = int(parts[0]) * 60 + int(parts[1]) + int(parts[2]) / 60
                except:
                    minutes = 10  # Default
                
                # Get other players in this match from API or cache
                # Note: This would require additional API calls to get full match details
                # For now, we track basic co-play from friend overlaps
                
            # Note: Full co-play analysis requires match participant data
            # which would need additional API integration
            
        except Exception as e:
            print(f"[COPLAY] Error analyzing {xuid}: {e}")
            continue
    
    print("[COPLAY] Co-play data collection complete")


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def quick_crawl(
    api_client: HaloAPIClient,
    seed_gamertag: str,
    max_depth: int = 2,
    collect_stats: bool = True
) -> Dict:
    """
    Quick crawl from a seed player with sensible defaults.
    
    Returns:
        Dict with crawl statistics
    """
    config = CrawlConfig(
        max_depth=max_depth,
        collect_stats=collect_stats,
        concurrency=3,
        batch_size=5
    )
    
    crawler = GraphCrawler(api_client, config)
    progress = await crawler.crawl_from_seed(seed_gamertag=seed_gamertag)
    
    # Get final stats
    db = get_graph_db()
    graph_stats = db.get_graph_stats()
    
    return {
        'seed': seed_gamertag,
        'progress': {
            'nodes_discovered': progress.nodes_discovered,
            'edges_discovered': progress.edges_discovered,
            'halo_players_found': progress.halo_players_found,
            'nodes_crawled': progress.nodes_crawled,
            'nodes_with_stats': progress.nodes_with_stats
        },
        'graph_stats': graph_stats
    }


def get_graph_summary() -> Dict:
    """Get a summary of the current graph state"""
    db = get_graph_db()
    return db.get_graph_stats()
