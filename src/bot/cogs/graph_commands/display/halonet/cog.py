"""HaloNet command ownership and co-play graph rendering."""

import asyncio
import io
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord.ext import commands

from src.api import api_client
from src.bot.cogs.graph_commands.display.halonet.ui import HaloNetFilterView
from src.database.graph_schema import get_graph_db


class HaloNetCog(commands.Cog, name="HaloNet"):
    """Owns co-play graph command handling and rendering for #halonet."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()
        self._halonet_repair_cooldowns: Dict[str, datetime] = {}
        self._halonet_repair_tasks: Dict[str, asyncio.Task] = {}
        self._halonet_repair_cooldown_window = timedelta(hours=24)
        self._halonet_repair_failure_cooldown_window = timedelta(minutes=15)
        self._halonet_repair_timeout_seconds = 300
        self._halonet_repair_matches_to_process = 300
        self._halonet_repair_seed_match_limit = 750

    @staticmethod
    def _format_duration_brief(delta: timedelta) -> str:
        total_seconds = max(0, int(delta.total_seconds()))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        if minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def _is_halonet_repair_allowed(self, seed_xuid: str) -> Tuple[bool, Optional[str]]:
        last_refresh = self._halonet_repair_cooldowns.get(seed_xuid)
        if not last_refresh:
            return True, None

        if last_refresh.tzinfo is None:
            last_refresh = last_refresh.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        elapsed = now - last_refresh
        if elapsed >= self._halonet_repair_cooldown_window:
            return True, None

        remaining = self._halonet_repair_cooldown_window - elapsed
        return False, (
            "recently refreshed "
            f"({self._format_duration_brief(elapsed)} ago); retry in {self._format_duration_brief(remaining)}"
        )

    def _set_halonet_repair_cooldown(self, seed_xuid: str, success: bool) -> None:
        """Record cooldowns with shorter windows for failed/empty attempts."""
        now = datetime.now(timezone.utc)
        if success:
            self._halonet_repair_cooldowns[seed_xuid] = now
            return

        shortened = now - (self._halonet_repair_cooldown_window - self._halonet_repair_failure_cooldown_window)
        self._halonet_repair_cooldowns[seed_xuid] = shortened

    def _get_halonet_neighbors_with_fallback(
        self,
        xuid: str,
        min_matches: int,
        max_nodes: int,
    ) -> Tuple[List[Dict], int, bool]:
        active_min_matches = min_matches
        threshold_relaxed = False

        neighbors = self.db.get_coplay_neighbors(xuid, min_matches=active_min_matches, limit=max_nodes - 1)
        if not neighbors:
            relaxed_neighbors = self.db.get_coplay_neighbors(xuid, min_matches=1, limit=max_nodes - 1)
            if relaxed_neighbors:
                neighbors = relaxed_neighbors
                active_min_matches = 1
                threshold_relaxed = True

        return neighbors, active_min_matches, threshold_relaxed

    def _load_existing_seed_coplay_edges(self, seed_xuid: str) -> Dict[Tuple[str, str], Dict]:
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                src_xuid,
                dst_xuid,
                matches_together,
                wins_together,
                total_minutes,
                same_team_count,
                opposing_team_count,
                first_played,
                last_played
            FROM graph_coplay
            WHERE src_xuid = ? OR dst_xuid = ?
            """,
            (seed_xuid, seed_xuid),
        )
        return {
            (str(row["src_xuid"]), str(row["dst_xuid"])): dict(row)
            for row in cursor.fetchall()
            if row["src_xuid"] and row["dst_xuid"]
        }

    async def _hydrate_seed_match_history(self, seed_xuid: str, seed_gamertag: str) -> Dict[str, object]:
        if not hasattr(api_client, "calculate_comprehensive_stats"):
            return {"ok": False, "message": "Stats API hydration is unavailable."}

        try:
            stats_result = await api_client.calculate_comprehensive_stats(
                xuid=seed_xuid,
                stat_type="overall",
                gamertag=seed_gamertag,
                matches_to_process=self._halonet_repair_matches_to_process,
                force_full_fetch=True,
            )
        except Exception as exc:
            return {"ok": False, "message": f"Stats refresh failed: {exc}"}

        if int(stats_result.get("error") or 0) != 0:
            return {
                "ok": False,
                "message": f"Stats refresh failed: {stats_result.get('message', 'unknown API error')}",
            }

        processed_matches = stats_result.get("processed_matches") or []
        matches_with_participants = sum(1 for row in processed_matches if row.get("all_participants"))
        return {
            "ok": True,
            "matches_processed": len(processed_matches),
            "matches_with_participants": matches_with_participants,
        }

    async def _rebuild_seed_coplay_edges_from_stats_cache(self, seed_xuid: str) -> Dict[str, object]:
        stats_db = getattr(getattr(api_client, "stats_cache", None), "db", None)
        if not stats_db:
            return {
                "ok": False,
                "message": "Stats participant database unavailable.",
                "seed_qualifying_matches": 0,
                "seed_pairs": 0,
                "rows_written": 0,
                "write_failures": 0,
                "stub_rows": 0,
            }

        if hasattr(stats_db, "get_seed_match_participants"):
            match_participants = stats_db.get_seed_match_participants(
                seed_xuid,
                limit_matches=self._halonet_repair_seed_match_limit,
            ) or {}
        elif hasattr(stats_db, "get_all_match_participants"):
            all_rows = stats_db.get_all_match_participants(limit_matches=self._halonet_repair_seed_match_limit) or {}
            match_participants = {
                match_id: rows
                for match_id, rows in all_rows.items()
                if any(str(r.get("xuid") or "").strip() == seed_xuid for r in rows)
            }
        else:
            return {
                "ok": False,
                "message": "Stats DB has no participant retrieval API.",
                "seed_qualifying_matches": 0,
                "seed_pairs": 0,
                "rows_written": 0,
                "write_failures": 0,
                "stub_rows": 0,
            }

        partner_match_counts: Dict[str, int] = defaultdict(int)
        same_team_counts: Dict[str, int] = defaultdict(int)
        opposing_team_counts: Dict[str, int] = defaultdict(int)
        first_played: Dict[str, str] = {}
        last_played: Dict[str, str] = {}
        counted_partner_matches: Set[Tuple[str, str]] = set()

        analyzed_players: Set[str] = {seed_xuid}
        seed_qualifying_matches = 0

        for match_id, participants in match_participants.items():
            normalized_participants: Dict[str, Dict] = {}
            start_time = ""
            for participant in participants:
                participant_xuid = str(participant.get("xuid") or "").strip()
                if not participant_xuid or participant_xuid in normalized_participants:
                    continue
                normalized_participants[participant_xuid] = participant
                if not start_time:
                    start_time = str(participant.get("start_time") or "")

            if seed_xuid not in normalized_participants or len(normalized_participants) < 2:
                continue

            seed_qualifying_matches += 1
            seed_row = normalized_participants[seed_xuid]
            seed_team = seed_row.get("team_id") or seed_row.get("inferred_team_id")

            for partner_xuid, partner in normalized_participants.items():
                if partner_xuid == seed_xuid:
                    continue

                partner_match_key = (str(match_id), partner_xuid)
                if match_id and partner_match_key in counted_partner_matches:
                    continue
                if match_id:
                    counted_partner_matches.add(partner_match_key)

                analyzed_players.add(partner_xuid)
                partner_match_counts[partner_xuid] += 1

                partner_team = partner.get("team_id") or partner.get("inferred_team_id")
                if seed_team and partner_team:
                    if str(seed_team) == str(partner_team):
                        same_team_counts[partner_xuid] += 1
                    else:
                        opposing_team_counts[partner_xuid] += 1

                if start_time:
                    existing_first = first_played.get(partner_xuid)
                    existing_last = last_played.get(partner_xuid)
                    if not existing_first or start_time < existing_first:
                        first_played[partner_xuid] = start_time
                    if not existing_last or start_time > existing_last:
                        last_played[partner_xuid] = start_time

        if analyzed_players:
            if hasattr(self.db, "insert_or_update_players_stub_batch"):
                stub_rows = int(self.db.insert_or_update_players_stub_batch(list(analyzed_players)) or 0)
            else:
                stub_rows = 0
                for participant_xuid in analyzed_players:
                    if self.db.insert_or_update_player(xuid=participant_xuid):
                        stub_rows += 1
        else:
            stub_rows = 0

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT xuid FROM graph_players WHERE halo_active = 1")
        halo_active_xuids = {str(row["xuid"]) for row in cursor.fetchall() if row["xuid"]}

        existing_edges = self._load_existing_seed_coplay_edges(seed_xuid)

        def _min_non_empty(left: Optional[str], right: Optional[str]) -> Optional[str]:
            if left and right:
                return left if left < right else right
            return left or right

        def _max_non_empty(left: Optional[str], right: Optional[str]) -> Optional[str]:
            if left and right:
                return left if left > right else right
            return left or right

        rows_written = 0
        write_failures = 0
        for partner_xuid, matches_together in partner_match_counts.items():
            same_team_count = same_team_counts.get(partner_xuid, 0)
            opposing_team_count = opposing_team_counts.get(partner_xuid, 0)
            first_ts = first_played.get(partner_xuid)
            last_ts = last_played.get(partner_xuid)
            is_halo_active_pair = seed_xuid in halo_active_xuids and partner_xuid in halo_active_xuids

            for src_xuid, dst_xuid in ((seed_xuid, partner_xuid), (partner_xuid, seed_xuid)):
                existing = existing_edges.get((src_xuid, dst_xuid), {})
                merged_matches = max(int(existing.get("matches_together") or 0), int(matches_together or 0))
                merged_wins = max(int(existing.get("wins_together") or 0), 0)
                merged_minutes = max(int(existing.get("total_minutes") or 0), 0)
                merged_same_team = max(int(existing.get("same_team_count") or 0), int(same_team_count or 0))
                merged_opposing = max(int(existing.get("opposing_team_count") or 0), int(opposing_team_count or 0))
                merged_first = _min_non_empty(existing.get("first_played"), first_ts)
                merged_last = _max_non_empty(existing.get("last_played"), last_ts)

                wrote = self.db.upsert_coplay_edge(
                    src_xuid=src_xuid,
                    dst_xuid=dst_xuid,
                    matches_together=merged_matches,
                    wins_together=merged_wins,
                    first_played=merged_first,
                    last_played=merged_last,
                    total_minutes=merged_minutes,
                    same_team_count=merged_same_team,
                    opposing_team_count=merged_opposing,
                    source_type="participants-runtime",
                    is_halo_active_pair=is_halo_active_pair,
                    suppress_errors=True,
                )
                if wrote:
                    rows_written += 1
                else:
                    write_failures += 1

        return {
            "ok": True,
            "message": "Seed co-play rebuild complete.",
            "participant_matches": len(match_participants),
            "seed_qualifying_matches": seed_qualifying_matches,
            "seed_pairs": len(partner_match_counts),
            "rows_written": rows_written,
            "write_failures": write_failures,
            "stub_rows": stub_rows,
        }

    async def _run_halonet_auto_heal(self, seed_xuid: str, seed_gamertag: str) -> Dict[str, object]:
        hydrate_result = await self._hydrate_seed_match_history(seed_xuid, seed_gamertag)
        rebuild_result = await self._rebuild_seed_coplay_edges_from_stats_cache(seed_xuid)

        seed_pairs = int(rebuild_result.get("seed_pairs") or 0)
        write_failures = int(rebuild_result.get("write_failures") or 0)
        rows_written = int(rebuild_result.get("rows_written") or 0)

        if seed_pairs <= 0:
            message = (
                "Auto-refresh completed, but no seed co-play pairs were found in persisted match participants."
            )
        elif write_failures > 0:
            message = (
                f"Auto-refresh found {seed_pairs} seed pairs but encountered {write_failures} edge write failures."
            )
        else:
            message = (
                f"Auto-refresh wrote {rows_written} co-play rows across {seed_pairs} seed pairs."
            )

        ok = seed_pairs > 0 and write_failures == 0
        return {
            "ok": ok,
            "message": message,
            "hydrate": hydrate_result,
            "rebuild": rebuild_result,
        }

    async def _attempt_halonet_auto_heal(self, seed_xuid: str, seed_gamertag: str) -> Dict[str, object]:
        allowed, cooldown_reason = self._is_halonet_repair_allowed(seed_xuid)
        if not allowed:
            return {
                "attempted": False,
                "ok": False,
                "message": f"Auto-refresh skipped: {cooldown_reason}.",
            }

        existing_task = self._halonet_repair_tasks.get(seed_xuid)
        started_new = existing_task is None or existing_task.done()
        repair_task = existing_task
        if started_new:
            repair_task = asyncio.create_task(self._run_halonet_auto_heal(seed_xuid, seed_gamertag))
            self._halonet_repair_tasks[seed_xuid] = repair_task

        try:
            repair_result = await asyncio.wait_for(
                asyncio.shield(repair_task),
                timeout=self._halonet_repair_timeout_seconds,
            )
        except asyncio.TimeoutError:
            if started_new:
                self._set_halonet_repair_cooldown(seed_xuid, success=False)
            return {
                "attempted": started_new,
                "ok": False,
                "message": "Auto-refresh timed out before completion.",
            }
        except Exception as exc:
            if started_new:
                self._set_halonet_repair_cooldown(seed_xuid, success=False)
            return {
                "attempted": started_new,
                "ok": False,
                "message": f"Auto-refresh failed: {exc}",
            }
        finally:
            if repair_task and repair_task.done():
                self._halonet_repair_tasks.pop(seed_xuid, None)

        if started_new:
            self._set_halonet_repair_cooldown(seed_xuid, success=bool(repair_result.get("ok")))

        return {
            "attempted": True,
            "ok": bool(repair_result.get("ok")),
            "message": repair_result.get("message") or "Auto-refresh completed.",
            "result": repair_result,
        }

    @commands.command(
        name="halonet",
        help="Show a co-play network visualization from graph DB data with seed auto-refresh. Usage: #halonet <gamertag>",
    )
    async def show_halonet(self, ctx: commands.Context, *inputs):
        """Show a player's local co-play network from stored graph_coplay data."""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#halonet GAMERTAG`")
            return

        gamertag = " ".join(inputs)
        min_matches = 2
        max_nodes = 60

        loading_embed = discord.Embed(
            title="Building Co-play Network...",
            description=f"Fetching co-play edges for **{gamertag}**",
            colour=0xFFA500,
            timestamp=datetime.now(),
        )
        loading_msg = await ctx.send(embed=loading_embed)

        try:
            xuid = await api_client.resolve_gamertag_to_xuid(gamertag)
            if not xuid:
                await loading_msg.delete()
                await ctx.send(f"Could not find player **{gamertag}**")
                return

            player = self.db.get_player(xuid)
            if not player:
                await loading_msg.delete()
                await ctx.send(
                    f"**{gamertag}** is not in the graph database. Run `#crawlfriends` and `#crawlgames` first."
                )
                return

            center_features = self.db.get_halo_features(xuid)
            auto_heal_result: Optional[Dict[str, object]] = None
            neighbors, active_min_matches, threshold_relaxed = self._get_halonet_neighbors_with_fallback(
                xuid,
                min_matches=min_matches,
                max_nodes=max_nodes,
            )

            if not neighbors:
                loading_embed.description = (
                    f"No co-play edges found yet for **{gamertag}**. "
                    "Running auto-refresh from recent match history..."
                )
                loading_embed.colour = 0xE67E22
                await loading_msg.edit(embed=loading_embed)

                auto_heal_result = await self._attempt_halonet_auto_heal(xuid, gamertag)

                neighbors, active_min_matches, threshold_relaxed = self._get_halonet_neighbors_with_fallback(
                    xuid,
                    min_matches=min_matches,
                    max_nodes=max_nodes,
                )

                if not neighbors:
                    await loading_msg.delete()
                    auto_heal_message = str(
                        auto_heal_result.get("message") or "Auto-refresh did not create any co-play edges."
                    )
                    await ctx.send(
                        f"No co-play edges found for **{gamertag}** after auto-refresh.\n"
                        f"{auto_heal_message}\n"
                        f"Try `#crawlgames {gamertag}` for focused backfill (default scoped mode) or "
                        f"`#crawlgames {gamertag} --global` for full-scope rebuild."
                    )
                    return

            node_map: Dict[str, Dict] = {
                xuid: {
                    "xuid": xuid,
                    "gamertag": gamertag,
                    "is_center": True,
                    "kd_ratio": center_features.get("kd_ratio") if center_features else None,
                    "win_rate": center_features.get("win_rate") if center_features else None,
                    "matches_played": center_features.get("matches_played") if center_features else None,
                }
            }

            for row in neighbors:
                partner_xuid = row.get("partner_xuid")
                if not partner_xuid:
                    continue
                node_map[partner_xuid] = {
                    "xuid": partner_xuid,
                    "gamertag": row.get("gamertag") or partner_xuid,
                    "is_center": False,
                    "kd_ratio": row.get("kd_ratio"),
                    "win_rate": row.get("win_rate"),
                    "matches_played": row.get("matches_played"),
                }

            all_xuids = list(node_map.keys())
            raw_edges = self.db.get_coplay_edges_within_set(all_xuids, min_matches=active_min_matches)

            aggregated_edges: Dict[tuple, Dict[str, object]] = {}
            for edge in raw_edges:
                src = edge.get("src_xuid")
                dst = edge.get("dst_xuid")
                if not src or not dst or src == dst:
                    continue
                key = tuple(sorted((src, dst)))
                bucket = aggregated_edges.setdefault(
                    key,
                    {
                        "src_xuid": key[0],
                        "dst_xuid": key[1],
                        "matches_together": 0,
                        "wins_together": 0,
                        "total_minutes": 0,
                    },
                )
                bucket["matches_together"] += int(edge.get("matches_together") or 0)
                bucket["wins_together"] += int(edge.get("wins_together") or 0)
                bucket["total_minutes"] += int(edge.get("total_minutes") or 0)

            if not aggregated_edges:
                for row in neighbors:
                    partner_xuid = row.get("partner_xuid")
                    if not partner_xuid:
                        continue
                    key = tuple(sorted((xuid, partner_xuid)))
                    aggregated_edges[key] = {
                        "src_xuid": key[0],
                        "dst_xuid": key[1],
                        "matches_together": int(row.get("matches_together") or 0),
                        "wins_together": int(row.get("wins_together") or 0),
                        "total_minutes": int(row.get("total_minutes") or 0),
                    }

            edges = [
                edge
                for edge in aggregated_edges.values()
                if int(edge.get("matches_together") or 0) >= active_min_matches
            ]

            if not edges:
                await loading_msg.delete()
                await ctx.send(
                    f"Co-play data exists for **{gamertag}**, but no edges met the current minimum shared-match threshold ({active_min_matches})."
                )
                return

            total_shared_matches = sum(int(edge.get("matches_together") or 0) for edge in edges)
            top_partners = sorted(
                neighbors,
                key=lambda row: int(row.get("matches_together") or 0),
                reverse=True,
            )[:5]

            embed = discord.Embed(
                title=f"HaloNet: {gamertag}",
                description=(
                    "Co-play links weighted by shared matches from graph DB data."
                    + (" Showing fallback edges with at least 1 shared match." if threshold_relaxed else "")
                ),
                colour=0x1ABC9C,
                timestamp=datetime.now(),
            )
            embed.add_field(
                name="Summary",
                value=(
                    f"Nodes: **{len(node_map)}**\n"
                    f"Edges: **{len(edges)}**\n"
                    f"Total shared matches: **{total_shared_matches:,}**\n"
                    f"Min edge weight: **{active_min_matches}**"
                ),
                inline=True,
            )

            if auto_heal_result:
                auto_heal_message = str(auto_heal_result.get("message") or "Auto-refresh completed.")
                embed.add_field(
                    name="Auto-refresh",
                    value=auto_heal_message[:1024],
                    inline=False,
                )

            if center_features and (center_features.get("matches_played") or 0) > 0:
                kd_val = center_features.get("kd_ratio")
                wr_val = center_features.get("win_rate")
                kd_str = f"{kd_val:.2f}" if kd_val is not None else "N/A"
                wr_str = f"{wr_val:.1f}%" if wr_val is not None else "N/A"
                embed.add_field(
                    name="Player Stats",
                    value=(
                        f"K/D: {kd_str}\n"
                        f"Win Rate: {wr_str}\n"
                        f"Matches: {center_features.get('matches_played') or 0}"
                    ),
                    inline=True,
                )

            if top_partners:
                lines = []
                for row in top_partners:
                    partner_name = row.get("gamertag") or row.get("partner_xuid", "Unknown")
                    matches_together = int(row.get("matches_together") or 0)
                    same_team = int(row.get("same_team_count") or 0)
                    lines.append(f"**{partner_name}**: {matches_together} shared matches ({same_team} same-team)")
                embed.add_field(name="Top Co-play Partners", value="\n".join(lines), inline=False)

            await loading_msg.delete()

            loop = asyncio.get_event_loop()
            buf = await loop.run_in_executor(
                None,
                lambda: self._render_coplay_graph(
                    xuid,
                    gamertag,
                    node_map,
                    edges,
                    min_edge_weight=active_min_matches,
                ),
            )

            file = discord.File(fp=buf, filename="halonet.png")
            requester_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
            filter_view = HaloNetFilterView(
                cog=self,
                requester_id=requester_id,
                center_xuid=xuid,
                center_gamertag=gamertag,
                node_map=node_map,
                edges=edges,
                base_embed=embed,
            )
            filter_view.min_edge_weight = int(active_min_matches)
            filter_view._sync_select_defaults()
            embed.description = filter_view._build_description(controls_active=True)
            embed.set_image(url="attachment://halonet.png")
            embed.set_footer(text=f"XUID: {xuid} | Gold=center | Edge width/color=shared matches | Node size=weighted degree")
            graph_message = await ctx.send(embed=embed, file=file, view=filter_view)
            filter_view.message = graph_message

        except Exception as e:
            try:
                await loading_msg.delete()
            except Exception:
                pass
            await ctx.send(f"Error showing halonet: {str(e)}")
            raise

    def _render_coplay_graph(
        self,
        center_xuid: str,
        center_gamertag: str,
        node_map: Dict[str, Dict],
        edges: List[Dict],
        clustered: bool = False,
        min_node_strength: int = 0,
        min_edge_weight: int = 1,
    ) -> io.BytesIO:
        """Render a weighted co-play graph as a PNG and return a BytesIO buffer."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.cm as cm
        import matplotlib.colors as mcolors
        import matplotlib.pyplot as plt
        import matplotlib.patheffects as path_effects
        from matplotlib.lines import Line2D
        import networkx as nx

        G = nx.Graph()

        center_row = node_map.get(center_xuid) or {
            "xuid": center_xuid,
            "gamertag": center_gamertag,
            "is_center": True,
        }

        for xuid, data in node_map.items():
            G.add_node(
                xuid,
                label=data.get("gamertag") or xuid,
                is_center=bool(data.get("is_center")),
            )

        active_min_edge_weight = max(1, int(min_edge_weight or 1))
        for edge in edges:
            src = edge.get("src_xuid")
            dst = edge.get("dst_xuid")
            matches = int(edge.get("matches_together") or 0)
            if not src or not dst or src == dst or matches < active_min_edge_weight:
                continue
            if not G.has_node(src) or not G.has_node(dst):
                continue
            if G.has_edge(src, dst):
                G[src][dst]["weight"] += matches
            else:
                G.add_edge(src, dst, weight=matches)

        if min_node_strength > 0:
            min_strength_threshold = float(min_node_strength)
            changed = True
            while changed:
                changed = False
                for node in list(G.nodes):
                    if node == center_xuid:
                        continue
                    if float(G.degree(node, weight="weight")) < min_strength_threshold:
                        G.remove_node(node)
                        changed = True

        if not G.has_node(center_xuid):
            G.add_node(
                center_xuid,
                label=center_row.get("gamertag") or center_gamertag,
                is_center=True,
            )

        if G.has_node(center_xuid):
            connected = nx.node_connected_component(G, center_xuid)
            disconnected = [n for n in G.nodes if n not in connected]
            if disconnected:
                G.remove_nodes_from(disconnected)

        if not G.edges:
            G = nx.Graph()
            G.add_node(
                center_xuid,
                label=center_row.get("gamertag") or center_gamertag,
                is_center=True,
            )

        weighted_degree = {node: float(G.degree(node, weight="weight")) for node in G.nodes}
        edge_weights = [float(data.get("weight") or 0.0) for _, _, data in G.edges(data=True)]

        def _safe_norm(values: List[float]) -> mcolors.Normalize:
            if not values:
                return mcolors.Normalize(vmin=0, vmax=1)
            vmin = min(values)
            vmax = max(values)
            if vmin == vmax:
                vmax = vmin + 1
            return mcolors.Normalize(vmin=vmin, vmax=vmax)

        node_norm = _safe_norm(list(weighted_degree.values()))
        edge_norm = _safe_norm(edge_weights)
        node_cmap = cm.Blues
        edge_cmap = cm.Greens

        pos = nx.spring_layout(G, seed=42, k=5.8 / max(1, len(G.nodes) ** 0.5), iterations=120, weight="weight")

        if clustered and len(G.nodes) >= 5 and len(G.edges) >= 4:
            communities = list(nx.algorithms.community.greedy_modularity_communities(G, weight="weight"))
            if len(communities) > 1:
                cluster_of = {}
                for cid, members in enumerate(communities):
                    for n in members:
                        cluster_of[n] = cid

                cluster_graph = nx.Graph()
                for cid in range(len(communities)):
                    cluster_graph.add_node(cid)
                for u, v, data in G.edges(data=True):
                    cu = cluster_of.get(u)
                    cv = cluster_of.get(v)
                    if cu is None or cv is None or cu == cv:
                        continue
                    w = data.get("weight", 1.0)
                    if cluster_graph.has_edge(cu, cv):
                        cluster_graph[cu][cv]["weight"] += w
                    else:
                        cluster_graph.add_edge(cu, cv, weight=w)

                cluster_k = 1.9 / max(1, len(cluster_graph.nodes()) ** 0.5)
                cluster_pos = nx.spring_layout(cluster_graph, seed=42, k=cluster_k, iterations=100, weight="weight")

                clustered_pos = {}
                for cid, members in enumerate(communities):
                    sub = G.subgraph(members)
                    local_k = 1.3 / max(1, len(sub.nodes()) ** 0.5)
                    local_pos = nx.spring_layout(sub, seed=42, k=local_k, iterations=70, weight="weight")
                    center = cluster_pos.get(cid, (0.0, 0.0))
                    radius = 0.18 + 0.025 * min(10, len(sub.nodes()))
                    for n, coords in local_pos.items():
                        clustered_pos[n] = (
                            center[0] + coords[0] * radius,
                            center[1] + coords[1] * radius,
                        )

                if len(clustered_pos) == len(G.nodes):
                    pos = clustered_pos

        x_values = [p[0] for p in pos.values()]
        y_values = [p[1] for p in pos.values()]
        x_min, x_max = min(x_values), max(x_values)
        y_min, y_max = min(y_values), max(y_values)
        x_span = (x_max - x_min) or 1.0
        y_span = (y_max - y_min) or 1.0

        x_left, x_right = 0.02, 0.84
        y_bottom, y_top = 0.03, 0.87
        pos = {
            node: (
                x_left + ((coords[0] - x_min) / x_span) * (x_right - x_left),
                y_bottom + ((coords[1] - y_min) / y_span) * (y_top - y_bottom),
            )
            for node, coords in pos.items()
        }

        node_colors = []
        node_sizes = []
        for node in G.nodes:
            if node == center_xuid:
                node_colors.append("#FFD700")
                node_sizes.append(760)
            else:
                strength = weighted_degree.get(node, 0.0)
                node_colors.append(node_cmap(node_norm(strength)))
                node_sizes.append(180 + 22 * strength)

        edge_colors = []
        edge_widths = []
        for _, _, data in G.edges(data=True):
            weight = float(data.get("weight") or 0.0)
            edge_colors.append(edge_cmap(edge_norm(weight)))
            edge_widths.append(0.8 + 2.8 * edge_norm(weight))

        bg = "#1a1a2e"
        fig, ax = plt.subplots(figsize=(14.5, 11), facecolor=bg)
        fig.subplots_adjust(left=0.02, right=0.98, top=0.90, bottom=0.06)
        ax.set_facecolor(bg)

        nx.draw_networkx_edges(
            G,
            pos,
            edge_color=edge_colors,
            width=edge_widths,
            alpha=0.65,
            ax=ax,
        )
        nx.draw_networkx_nodes(
            G,
            pos,
            node_color=node_colors,
            node_size=node_sizes,
            linewidths=1.2,
            edgecolors="white",
            ax=ax,
        )

        labels = {n: G.nodes[n].get("label", n) for n in G.nodes}
        label_artists = nx.draw_networkx_labels(G, pos, labels=labels, font_size=7, font_color="white", ax=ax)
        for text_artist in label_artists.values():
            text_artist.set_path_effects([
                path_effects.Stroke(linewidth=2.2, foreground="black"),
                path_effects.Normal(),
            ])

        node_cax = fig.add_axes([0.36, 0.915, 0.25, 0.018])
        edge_cax = fig.add_axes([0.66, 0.915, 0.25, 0.018])
        node_cax.set_facecolor(bg)
        edge_cax.set_facecolor(bg)

        node_sm = cm.ScalarMappable(cmap=node_cmap, norm=node_norm)
        node_sm.set_array([])
        node_cbar = fig.colorbar(node_sm, cax=node_cax, orientation="horizontal")
        node_cbar.set_label("Node Weighted Degree (Blues: low -> high)", color="white", fontsize=8)
        node_cbar.ax.xaxis.set_tick_params(color="white", labelsize=7)
        plt.setp(node_cbar.ax.xaxis.get_ticklabels(), color="white")
        node_cbar.outline.set_edgecolor("white")

        edge_sm = cm.ScalarMappable(cmap=edge_cmap, norm=edge_norm)
        edge_sm.set_array([])
        edge_cbar = fig.colorbar(edge_sm, cax=edge_cax, orientation="horizontal")
        edge_cbar.set_label("Shared Matches per Edge (Greens: low -> high)", color="white", fontsize=9)
        edge_cbar.ax.xaxis.set_tick_params(color="white", labelsize=7)
        plt.setp(edge_cbar.ax.xaxis.get_ticklabels(), color="white")
        edge_cbar.outline.set_edgecolor("white")

        legend_handles = [
            Line2D([0], [0], marker="o", color="none", label="Center Player", markerfacecolor="#FFD700",
                   markeredgecolor="white", markeredgewidth=1.0, markersize=9),
            Line2D([0], [0], color=edge_cmap(0.25), linewidth=1.2, label="Weaker Link Strength"),
            Line2D([0], [0], color=edge_cmap(0.85), linewidth=2.6, label="Stronger Link Strength"),
            Line2D([0], [0], marker="o", color="none", label="Smaller Node (Lower Weighted Degree)", markerfacecolor="#cccccc",
                   markeredgecolor="white", markeredgewidth=0.6, markersize=5),
            Line2D([0], [0], marker="o", color="none", label="Larger Node (Higher Weighted Degree)", markerfacecolor="#cccccc",
                   markeredgecolor="white", markeredgewidth=0.6, markersize=10),
        ]
        legend = ax.legend(
            handles=legend_handles,
            loc="upper left",
            frameon=True,
            facecolor=bg,
            edgecolor="white",
            fontsize=8,
        )
        for text in legend.get_texts():
            text.set_color("white")

        title = (
            f"HaloNet Co-play Graph: {center_gamertag}  |  Nodes: {len(G.nodes)}"
            f"  |  Edges: {len(G.edges)}"
        )
        if min_node_strength > 0 or active_min_edge_weight > 1:
            title += f"  |  Filter N>={int(min_node_strength)}, E>={int(active_min_edge_weight)}"
        if clustered:
            title += "  |  Layout: Clustered"
        fig.text(0.5, 0.015, title, color="white", fontsize=12, ha="center", va="bottom")

        ax.axis("off")
        ax.margins(x=0.0, y=0.0)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=120, bbox_inches="tight", facecolor=bg)
        buf.seek(0)
        plt.close(fig)
        return buf


async def setup(bot: commands.Bot):
    await bot.add_cog(HaloNetCog(bot))
