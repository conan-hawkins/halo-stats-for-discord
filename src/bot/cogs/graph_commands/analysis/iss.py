"""Implementation for ISS workflow commands."""

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional

import discord
from discord.ext import commands

from src.config import PROJECT_ROOT


BLACKLIST_FILE = PROJECT_ROOT / "data" / "xuid_gamertag_blacklist.json"


class ISSCommandMixin:
    def _load_blacklist(self) -> Dict[str, str]:
        """Load XUID->gamertag blacklist from disk."""
        if not BLACKLIST_FILE.exists():
            return {}

        try:
            raw = BLACKLIST_FILE.read_text(encoding="utf-8-sig").strip()
            if not raw:
                return {}
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return {}
            return {
                str(xuid).strip(): str(name).strip() or str(xuid).strip()
                for xuid, name in payload.items()
                if str(xuid).strip()
            }
        except Exception as exc:
            print(f"Failed to load blacklist from {BLACKLIST_FILE}: {exc}")
            return {}

    @staticmethod
    def _parse_gamertag_input(inputs: tuple) -> str:
        return " ".join(str(part).strip() for part in inputs if str(part).strip()).strip()

    @staticmethod
    def _parse_match_start(raw_value: str) -> Optional[datetime]:
        """Parse potentially-zoned ISO timestamp to UTC-naive datetime."""
        value = str(raw_value or "").strip()
        if not value:
            return None

        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None

        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def _is_match_in_window(self, match_row: Dict, cutoff: datetime) -> bool:
        started_at = self._parse_match_start(match_row.get("start_time") if isinstance(match_row, dict) else None)
        if not started_at:
            return False
        return started_at >= cutoff

    def _persist_social_discovery(
        self,
        target_xuid: str,
        target_gamertag: str,
        friends: List[Dict],
        friends_of_friends: Optional[List[Dict]] = None,
        private_friends: Optional[List[Dict]] = None,
    ) -> Dict[str, int]:
        """Persist discovered friend graph relationships for ISS workflows."""
        friends_of_friends = friends_of_friends or []
        private_friends = private_friends or []

        self.db.insert_or_update_player(
            xuid=target_xuid,
            gamertag=target_gamertag,
            profile_visibility="public",
            friends_count=len(friends),
            is_seed=True,
            crawl_depth=0,
        )

        direct_edges = []
        fof_edges = []
        friend_gt_to_xuid: Dict[str, str] = {}

        for friend in friends:
            friend_xuid = str(friend.get("xuid") or "").strip()
            if not friend_xuid:
                continue

            friend_gt = friend.get("gamertag")
            self.db.insert_or_update_player(
                xuid=friend_xuid,
                gamertag=friend_gt,
                profile_visibility="public",
                crawl_depth=1,
            )
            direct_edges.append(
                (
                    target_xuid,
                    friend_xuid,
                    bool(friend.get("is_mutual", False)),
                    target_xuid,
                    1,
                )
            )
            if friend_gt:
                friend_gt_to_xuid[friend_gt.lower()] = friend_xuid

        if direct_edges:
            self.db.insert_friend_edges_batch(direct_edges)

        for fof in friends_of_friends:
            fof_xuid = str(fof.get("xuid") or "").strip()
            if not fof_xuid:
                continue

            fof_gt = fof.get("gamertag")
            self.db.insert_or_update_player(
                xuid=fof_xuid,
                gamertag=fof_gt,
                profile_visibility="public",
                crawl_depth=2,
            )

            via_gamertag = str(fof.get("via") or "").strip().lower()
            via_xuid = friend_gt_to_xuid.get(via_gamertag)
            if via_xuid:
                fof_edges.append(
                    (
                        via_xuid,
                        fof_xuid,
                        bool(fof.get("is_mutual", False)),
                        target_xuid,
                        2,
                    )
                )

        if fof_edges:
            self.db.insert_friend_edges_batch(fof_edges)

        for private_friend in private_friends:
            private_xuid = str(private_friend.get("xuid") or "").strip()
            if not private_xuid:
                continue
            self.db.insert_or_update_player(
                xuid=private_xuid,
                gamertag=private_friend.get("gamertag"),
                profile_visibility="private",
                crawl_depth=1,
            )

        return {
            "direct_edges": len(direct_edges),
            "fof_edges": len(fof_edges),
            "private_nodes": len(private_friends),
        }

    def _collect_blacklist_hits(self, entries: List[Dict], blacklist: Dict[str, str], relation: str) -> List[Dict]:
        hits = []
        for entry in entries:
            entry_xuid = str(entry.get("xuid") or "").strip()
            if not entry_xuid or entry_xuid not in blacklist:
                continue

            hits.append(
                {
                    "xuid": entry_xuid,
                    "gamertag": entry.get("gamertag") or entry_xuid,
                    "blacklist_name": blacklist.get(entry_xuid, entry_xuid),
                    "relation": relation,
                    "via": entry.get("via"),
                }
            )
        return hits

    @staticmethod
    def _format_blacklist_hits(hits: List[Dict], include_via: bool = False, max_lines: int = 12) -> str:
        if not hits:
            return "None"

        lines = []
        for hit in hits[:max_lines]:
            name = hit.get("blacklist_name") or hit.get("gamertag") or hit.get("xuid")
            if include_via and hit.get("via"):
                lines.append(f"- {name} (via {hit['via']})")
            else:
                lines.append(f"- {name}")

        if len(hits) > max_lines:
            lines.append(f"... and {len(hits) - max_lines} more")
        return "\n".join(lines)

    async def _run_iss_social_scan(
        self,
        gamertag: str,
        include_fof: bool,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> Dict[str, object]:
        """Run ISS social scan and persist graph edges."""
        blacklist = self._load_blacklist()
        api = self._get_api_client()

        if progress_callback:
            await progress_callback(
                {
                    "stage": "Resolving player",
                    "percent": 5.0,
                    "detail": f"Resolving {gamertag}",
                }
            )

        if include_fof:

            async def _fof_progress(current, total, stage, fof_count):
                if not progress_callback:
                    return
                if stage == "friends_found":
                    await progress_callback(
                        {
                            "stage": "Scanning direct friends",
                            "percent": 18.0,
                            "detail": f"Found {total} direct friends",
                        }
                    )
                    return

                ratio = float(current) / float(total) if total else 0.0
                await progress_callback(
                    {
                        "stage": "Scanning friends-of-friends",
                        "percent": 18.0 + (ratio * 34.0),
                        "detail": f"Checked {current}/{total}; discovered {fof_count} second-degree players",
                    }
                )

            result = await api.get_friends_of_friends(
                gamertag,
                max_depth=2,
                progress_callback=_fof_progress if progress_callback else None,
            )
            if result.get("error"):
                return {"error": f"{result.get('error')}"}

            target = result.get("target") or {}
            target_xuid = str(target.get("xuid") or "").strip()
            if not target_xuid:
                return {"error": "Could not resolve target player"}

            friends = result.get("friends", []) or []
            friends_of_friends = result.get("friends_of_friends", []) or []
            private_friends = result.get("private_friends", []) or []
        else:
            target_xuid = await api.resolve_gamertag_to_xuid(gamertag)
            if not target_xuid:
                return {"error": "Could not resolve target player"}

            friend_result = await api.get_friends_list(target_xuid)
            if friend_result.get("error") and friend_result.get("error") != "unauthorized":
                return {"error": f"Could not fetch direct friends ({friend_result.get('error')})"}

            if friend_result.get("is_private"):
                self.db.insert_or_update_player(
                    xuid=target_xuid,
                    gamertag=gamertag,
                    profile_visibility="private",
                    crawl_depth=0,
                    is_seed=True,
                )
                return {
                    "target_xuid": target_xuid,
                    "target_gamertag": gamertag,
                    "friends": [],
                    "friends_of_friends": [],
                    "private_friends": [],
                    "direct_hits": [],
                    "fof_hits": [],
                    "blacklist_size": len(blacklist),
                    "is_private": True,
                    "persisted": {"direct_edges": 0, "fof_edges": 0, "private_nodes": 0},
                }

            friends = friend_result.get("friends", []) or []
            friends_of_friends = []
            private_friends = []

        if progress_callback:
            await progress_callback(
                {
                    "stage": "Persisting relationships",
                    "percent": 58.0,
                    "detail": "Writing discovered friend graph rows",
                }
            )

        persisted = self._persist_social_discovery(
            target_xuid=target_xuid,
            target_gamertag=gamertag,
            friends=friends,
            friends_of_friends=friends_of_friends,
            private_friends=private_friends,
        )

        direct_hits = self._collect_blacklist_hits(friends, blacklist, relation="direct")
        fof_hits = self._collect_blacklist_hits(friends_of_friends, blacklist, relation="fof")

        if progress_callback:
            await progress_callback(
                {
                    "stage": "Applying blacklist checks",
                    "percent": 70.0,
                    "detail": f"Direct hits: {len(direct_hits)} | FoF hits: {len(fof_hits)}",
                }
            )

        return {
            "target_xuid": target_xuid,
            "target_gamertag": gamertag,
            "friends": friends,
            "friends_of_friends": friends_of_friends,
            "private_friends": private_friends,
            "direct_hits": direct_hits,
            "fof_hits": fof_hits,
            "blacklist_size": len(blacklist),
            "persisted": persisted,
            "is_private": False,
        }

    def _persist_halo_features_from_stats(
        self,
        xuid: str,
        gamertag: str,
        stats_payload: Dict,
    ) -> None:
        """Mirror fetched stat payload into graph feature store for later graph analysis."""
        stats = stats_payload.get("stats") or {}
        processed_matches = stats_payload.get("processed_matches") or []

        def _as_float(value, default=0.0):
            try:
                if value is None:
                    return float(default)
                if isinstance(value, str):
                    value = value.replace("%", "").strip()
                    if not value:
                        return float(default)
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        def _as_int(value, default=0):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return int(default)

        timestamps = [str(row.get("start_time")) for row in processed_matches if row.get("start_time")]
        first_match = min(timestamps) if timestamps else None
        last_match = max(timestamps) if timestamps else None

        total_matches = _as_int(stats.get("games_played"))
        total_kills = _as_int(stats.get("total_kills"))
        total_deaths = _as_int(stats.get("total_deaths"))
        total_assists = _as_int(stats.get("total_assists"))

        avg_kills = (float(total_kills) / float(total_matches)) if total_matches else 0.0
        avg_deaths = (float(total_deaths) / float(total_matches)) if total_matches else 0.0

        self.db.insert_or_update_player(
            xuid=xuid,
            gamertag=gamertag,
            halo_active=total_matches > 0,
            crawl_depth=0,
        )
        self.db.insert_or_update_halo_features(
            xuid=xuid,
            gamertag=gamertag,
            csr=_as_float(stats.get("estimated_csr")),
            csr_tier=stats.get("csr_tier"),
            kd_ratio=_as_float(stats.get("kd_ratio")),
            win_rate=_as_float(stats.get("win_rate")),
            matches_played=total_matches,
            total_kills=total_kills,
            total_deaths=total_deaths,
            total_assists=total_assists,
            avg_kills=avg_kills,
            avg_deaths=avg_deaths,
            last_match=last_match,
            first_match=first_match,
        )

    def _persist_coplay_for_subject(
        self,
        subject_xuid: str,
        processed_matches: List[Dict],
        source_type: str,
        cutoff: Optional[datetime] = None,
    ) -> Dict[str, int]:
        """Upsert co-play edges for a single subject player from match participant payloads."""
        pair_counts: Dict[tuple[str, str], int] = defaultdict(int)
        same_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
        opposing_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
        first_played: Dict[tuple[str, str], str] = {}
        last_played: Dict[tuple[str, str], str] = {}
        matches_considered = 0

        normalized_subject = str(subject_xuid).strip()

        for match in processed_matches:
            if cutoff and not self._is_match_in_window(match, cutoff):
                continue

            participants = match.get("all_participants") or []
            if not participants:
                continue

            by_xuid: Dict[str, Dict] = {}
            for participant in participants:
                participant_xuid = str(participant.get("xuid") or "").strip()
                if participant_xuid and participant_xuid not in by_xuid:
                    by_xuid[participant_xuid] = participant

            subject_row = by_xuid.get(normalized_subject)
            if not subject_row:
                continue

            matches_considered += 1
            subject_team = subject_row.get("team_id") or subject_row.get("inferred_team_id")
            start_time = str(match.get("start_time") or "")

            for partner_xuid, partner in by_xuid.items():
                if partner_xuid == normalized_subject:
                    continue

                pair_key = tuple(sorted((normalized_subject, partner_xuid)))
                pair_counts[pair_key] += 1

                partner_team = partner.get("team_id") or partner.get("inferred_team_id")
                if subject_team and partner_team:
                    if str(subject_team) == str(partner_team):
                        same_team_counts[pair_key] += 1
                    else:
                        opposing_team_counts[pair_key] += 1

                if start_time:
                    existing_first = first_played.get(pair_key)
                    existing_last = last_played.get(pair_key)
                    if not existing_first or start_time < existing_first:
                        first_played[pair_key] = start_time
                    if not existing_last or start_time > existing_last:
                        last_played[pair_key] = start_time

                self.db.insert_or_update_player(
                    xuid=partner_xuid,
                    gamertag=partner.get("gamertag"),
                )

        rows_written = 0
        for src_xuid, dst_xuid in pair_counts:
            first_ts = first_played.get((src_xuid, dst_xuid))
            last_ts = last_played.get((src_xuid, dst_xuid))
            is_halo_active_pair = False

            src_node = self.db.get_player(src_xuid)
            dst_node = self.db.get_player(dst_xuid)
            if src_node and dst_node:
                is_halo_active_pair = bool(src_node.get("halo_active")) and bool(dst_node.get("halo_active"))

            if self.db.upsert_coplay_edge(
                src_xuid=src_xuid,
                dst_xuid=dst_xuid,
                matches_together=pair_counts[(src_xuid, dst_xuid)],
                wins_together=0,
                first_played=first_ts,
                last_played=last_ts,
                total_minutes=0,
                same_team_count=same_team_counts.get((src_xuid, dst_xuid), 0),
                opposing_team_count=opposing_team_counts.get((src_xuid, dst_xuid), 0),
                source_type=source_type,
                is_halo_active_pair=is_halo_active_pair,
            ):
                rows_written += 1

            if self.db.upsert_coplay_edge(
                src_xuid=dst_xuid,
                dst_xuid=src_xuid,
                matches_together=pair_counts[(src_xuid, dst_xuid)],
                wins_together=0,
                first_played=first_ts,
                last_played=last_ts,
                total_minutes=0,
                same_team_count=same_team_counts.get((src_xuid, dst_xuid), 0),
                opposing_team_count=opposing_team_counts.get((src_xuid, dst_xuid), 0),
                source_type=source_type,
                is_halo_active_pair=is_halo_active_pair,
            ):
                rows_written += 1

        return {
            "pairs": len(pair_counts),
            "rows_written": rows_written,
            "matches_considered": matches_considered,
        }

    async def iss_level0(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ) -> str:
        """ISS level 0: direct-friends blacklist check with graph persistence."""
        gamertag = self._parse_gamertag_input(inputs)
        if not gamertag:
            await ctx.send("Usage: `ISS LEVEL 0` requires a gamertag input.")
            return "ISS level 0 requires gamertag input"

        scan = await self._run_iss_social_scan(gamertag, include_fof=False, progress_callback=progress_callback)
        if scan.get("error"):
            await ctx.send(f"ISS level 0 failed for **{gamertag}**: {scan['error']}")
            return f"ISS level 0 failed for {gamertag}: {scan['error']}"

        if scan.get("is_private"):
            await ctx.send(f"ISS level 0: **{gamertag}** has a private friends list. Target node persisted.")
            return f"ISS level 0 completed for {gamertag}: private friends list"

        direct_hits = scan.get("direct_hits", [])
        friends = scan.get("friends", [])
        persisted = scan.get("persisted", {})

        embed = discord.Embed(
            title=f"ISS Level 0: {gamertag}",
            colour=0x2ECC71,
            timestamp=datetime.now(),
        )
        embed.add_field(
            name="Direct Friends",
            value=(
                f"Discovered: **{len(friends)}**\n"
                f"Blacklisted hits: **{len(direct_hits)}**\n"
                f"Blacklist size: **{scan.get('blacklist_size', 0)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Persisted",
            value=(
                f"Direct edges: **{persisted.get('direct_edges', 0)}**\n"
                f"Depth-2 edges: **{persisted.get('fof_edges', 0)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Blacklisted Direct Hits",
            value=self._format_blacklist_hits(direct_hits, include_via=False),
            inline=False,
        )
        await ctx.send(embed=embed)

        return (
            f"ISS level 0 complete for {gamertag}: {len(direct_hits)} blacklisted direct hits, "
            f"{persisted.get('direct_edges', 0)} direct edges persisted."
        )

    async def iss_level1(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ) -> str:
        """ISS level 1: direct and friends-of-friends blacklist check with graph persistence."""
        gamertag = self._parse_gamertag_input(inputs)
        if not gamertag:
            await ctx.send("Usage: `ISS LEVEL 1` requires a gamertag input.")
            return "ISS level 1 requires gamertag input"

        scan = await self._run_iss_social_scan(gamertag, include_fof=True, progress_callback=progress_callback)
        if scan.get("error"):
            await ctx.send(f"ISS level 1 failed for **{gamertag}**: {scan['error']}")
            return f"ISS level 1 failed for {gamertag}: {scan['error']}"

        direct_hits = scan.get("direct_hits", [])
        fof_hits = scan.get("fof_hits", [])
        friends = scan.get("friends", [])
        friends_of_friends = scan.get("friends_of_friends", [])
        private_friends = scan.get("private_friends", [])
        persisted = scan.get("persisted", {})

        embed = discord.Embed(
            title=f"ISS Level 1: {gamertag}",
            colour=0x3498DB,
            timestamp=datetime.now(),
        )
        embed.add_field(
            name="Network Coverage",
            value=(
                f"Direct friends: **{len(friends)}**\n"
                f"Friends-of-friends: **{len(friends_of_friends)}**\n"
                f"Private friend-lists: **{len(private_friends)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Blacklist Hits",
            value=(
                f"Direct: **{len(direct_hits)}**\n"
                f"Second-degree: **{len(fof_hits)}**\n"
                f"Total: **{len(direct_hits) + len(fof_hits)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Persisted",
            value=(
                f"Direct edges: **{persisted.get('direct_edges', 0)}**\n"
                f"Depth-2 edges: **{persisted.get('fof_edges', 0)}**\n"
                f"Private nodes: **{persisted.get('private_nodes', 0)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Direct Blacklist Hits",
            value=self._format_blacklist_hits(direct_hits, include_via=False),
            inline=False,
        )
        embed.add_field(
            name="FoF Blacklist Hits",
            value=self._format_blacklist_hits(fof_hits, include_via=True),
            inline=False,
        )
        await ctx.send(embed=embed)

        return (
            f"ISS level 1 complete for {gamertag}: {len(direct_hits)} direct and {len(fof_hits)} FoF blacklist hits; "
            f"persisted {persisted.get('direct_edges', 0)} direct + {persisted.get('fof_edges', 0)} depth-2 edges."
        )

    async def _run_iss_history_level(
        self,
        ctx: commands.Context,
        gamertag: str,
        full_history: bool,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> str:
        """Shared implementation for ISS level 2 and level 3 history checks."""
        scan = await self._run_iss_social_scan(gamertag, include_fof=True, progress_callback=progress_callback)
        if scan.get("error"):
            await ctx.send(f"ISS history scan failed for **{gamertag}**: {scan['error']}")
            return f"ISS history scan failed for {gamertag}: {scan['error']}"

        direct_hits = scan.get("direct_hits", [])
        fof_hits = scan.get("fof_hits", [])
        persisted = scan.get("persisted", {})

        candidates: Dict[str, str] = {}
        for hit in direct_hits + fof_hits:
            candidate_xuid = str(hit.get("xuid") or "").strip()
            if not candidate_xuid:
                continue
            candidates[candidate_xuid] = str(hit.get("blacklist_name") or hit.get("gamertag") or candidate_xuid)

        if not candidates:
            embed = discord.Embed(
                title=f"ISS Level {'3' if full_history else '2'}: {gamertag}",
                description="No blacklisted players found in direct/FoF checks. Social data persisted for future analysis.",
                colour=0x95A5A6,
                timestamp=datetime.now(),
            )
            embed.add_field(
                name="Persisted",
                value=(
                    f"Direct edges: **{persisted.get('direct_edges', 0)}**\n"
                    f"Depth-2 edges: **{persisted.get('fof_edges', 0)}**"
                ),
                inline=True,
            )
            await ctx.send(embed=embed)
            return (
                f"ISS level {'3' if full_history else '2'} complete for {gamertag}: no blacklisted candidates; "
                "social graph persisted."
            )

        recent_cutoff = datetime.now(timezone.utc).astimezone(timezone.utc).replace(tzinfo=None) - timedelta(days=183)

        matches_per_candidate = None if full_history else 120
        force_full_fetch = bool(full_history)

        recent_active = []
        history_rows_written = 0
        coplay_rows_written = 0
        failed_candidates = []

        total_candidates = max(1, len(candidates))
        api = self._get_api_client()
        for idx, (candidate_xuid, candidate_name) in enumerate(candidates.items(), start=1):
            if progress_callback:
                base = 72.0
                span = 24.0
                progress_pct = base + ((float(idx - 1) / float(total_candidates)) * span)
                await progress_callback(
                    {
                        "stage": "Checking blacklist history",
                        "percent": progress_pct,
                        "detail": f"{idx}/{total_candidates}: {candidate_name}",
                    }
                )

            try:
                stats_payload = await api.calculate_comprehensive_stats(
                    candidate_xuid,
                    "overall",
                    gamertag=candidate_name,
                    matches_to_process=matches_per_candidate,
                    force_full_fetch=force_full_fetch,
                )
            except Exception as exc:
                failed_candidates.append(f"{candidate_name} ({exc})")
                continue

            if stats_payload.get("error"):
                failed_candidates.append(f"{candidate_name} ({stats_payload.get('message', 'stats error')})")
                continue

            history_rows_written += 1
            self._persist_halo_features_from_stats(candidate_xuid, candidate_name, stats_payload)

            processed_matches = stats_payload.get("processed_matches") or []
            recent_matches = [m for m in processed_matches if self._is_match_in_window(m, recent_cutoff)]
            if recent_matches:
                recent_active.append(
                    {
                        "name": candidate_name,
                        "recent_matches": len(recent_matches),
                        "all_matches": len(processed_matches),
                    }
                )

            coplay_result = self._persist_coplay_for_subject(
                subject_xuid=candidate_xuid,
                processed_matches=processed_matches,
                source_type="iss-level3" if full_history else "iss-level2",
                cutoff=None if full_history else recent_cutoff,
            )
            coplay_rows_written += int(coplay_result.get("rows_written", 0))

        if progress_callback:
            await progress_callback(
                {
                    "stage": "Finalizing",
                    "percent": 100.0,
                    "detail": "ISS history checks complete",
                }
            )

        recent_active_sorted = sorted(recent_active, key=lambda row: row.get("recent_matches", 0), reverse=True)
        activity_lines = [
            f"- {row['name']}: {row['recent_matches']} matches in last 6 months"
            for row in recent_active_sorted[:12]
        ]
        if len(recent_active_sorted) > 12:
            activity_lines.append(f"... and {len(recent_active_sorted) - 12} more")

        embed = discord.Embed(
            title=f"ISS Level {'3' if full_history else '2'}: {gamertag}",
            colour=0xE67E22 if not full_history else 0xC0392B,
            timestamp=datetime.now(),
        )
        embed.add_field(
            name="Blacklist Candidates",
            value=(
                f"Candidates checked: **{len(candidates)}**\n"
                f"Direct hits: **{len(direct_hits)}**\n"
                f"FoF hits: **{len(fof_hits)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Persistence",
            value=(
                f"Social direct edges: **{persisted.get('direct_edges', 0)}**\n"
                f"Social depth-2 edges: **{persisted.get('fof_edges', 0)}**\n"
                f"History rows updated: **{history_rows_written}**\n"
                f"Co-play rows written: **{coplay_rows_written}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Recent Activity (6 Months)",
            value="\n".join(activity_lines) if activity_lines else "None",
            inline=False,
        )
        if failed_candidates:
            failed_preview = "\n".join(f"- {item}" for item in failed_candidates[:8])
            if len(failed_candidates) > 8:
                failed_preview += f"\n... and {len(failed_candidates) - 8} more"
            embed.add_field(name="Candidates With Errors", value=failed_preview, inline=False)

        embed.set_footer(
            text=(
                "Level 3 uses full-history fetches"
                if full_history
                else "Level 2 uses bounded history + 6-month activity filtering"
            )
        )
        await ctx.send(embed=embed)

        return (
            f"ISS level {'3' if full_history else '2'} complete for {gamertag}: "
            f"checked {len(candidates)} blacklisted candidates, updated {history_rows_written} history rows, "
            f"wrote {coplay_rows_written} co-play rows."
        )

    async def iss_level2(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ) -> str:
        """ISS level 2: level 1 checks plus 6-month history checks for blacklisted players."""
        gamertag = self._parse_gamertag_input(inputs)
        if not gamertag:
            await ctx.send("Usage: `ISS LEVEL 2` requires a gamertag input.")
            return "ISS level 2 requires gamertag input"

        return await self._run_iss_history_level(
            ctx,
            gamertag,
            full_history=False,
            progress_callback=progress_callback,
        )

    async def iss_level3(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ) -> str:
        """ISS level 3: level 2 checks plus full-history checks for blacklisted players."""
        gamertag = self._parse_gamertag_input(inputs)
        if not gamertag:
            await ctx.send("Usage: `ISS LEVEL 3` requires a gamertag input.")
            return "ISS level 3 requires gamertag input"

        return await self._run_iss_history_level(
            ctx,
            gamertag,
            full_history=True,
            progress_callback=progress_callback,
        )
