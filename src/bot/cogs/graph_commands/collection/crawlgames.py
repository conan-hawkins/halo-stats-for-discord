"""Implementation for the #crawlgames command."""

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from typing import Awaitable, Callable, Dict, List, Optional

import discord
from discord.ext import commands


NETWORK_CONTROLS_TIMEOUT_SECONDS = 900


class CrawlProgressView(discord.ui.View):
    """Live crawl controls for long-running #crawlgames execution."""

    def __init__(self, cog, requester_id: int):
        super().__init__(timeout=NETWORK_CONTROLS_TIMEOUT_SECONDS)
        self.cog = cog
        self.requester_id = requester_id
        self.message: Optional[discord.Message] = None
        self.cancel_requested = False

    def _can_control(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.requester_id:
            return True
        perms = getattr(interaction.user, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False))

    def _disable_buttons(self) -> None:
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True

    async def deactivate(self) -> None:
        self._disable_buttons()
        if not self.message:
            return
        try:
            await self.message.edit(view=self)
        except Exception:
            return

    @discord.ui.button(label="Cancel Crawl", style=discord.ButtonStyle.danger)
    async def cancel_crawl(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self._can_control(interaction):
            await interaction.response.send_message(
                "Only the requester or an admin can cancel this crawl.",
                ephemeral=True,
            )
            return

        active_task = getattr(self.cog, "_crawl_task", None)
        if not active_task or active_task.done():
            self._disable_buttons()
            if self.message:
                try:
                    await self.message.edit(view=self)
                except Exception:
                    pass
            await interaction.response.send_message("No running crawl found to cancel.", ephemeral=True)
            return

        self.cancel_requested = True
        active_task.cancel()
        self._disable_buttons()
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception:
                pass

        await interaction.response.send_message(
            "Cancellation requested. The crawl will stop shortly.",
            ephemeral=True,
        )

    async def on_timeout(self):
        self._disable_buttons()
        if not self.message:
            return
        try:
            await self.message.edit(view=self)
        except Exception:
            return


class CrawlGamesCommandMixin:
    async def _hydrate_seed_match_history(self, seed_xuid: str, seed_gamertag: str) -> Dict[str, object]:
        api = self._get_api_client()

        if not hasattr(api, "calculate_comprehensive_stats"):
            return {"ok": False, "message": "Stats API hydration is unavailable."}

        try:
            stats_result = await api.calculate_comprehensive_stats(
                xuid=seed_xuid,
                stat_type="overall",
                gamertag=seed_gamertag,
                matches_to_process=300,
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

    def _collect_halo_active_scope(self, seed_xuid: str, max_depth: int) -> List[str]:
        """Collect halo-active players reachable from seed within depth in current graph DB."""
        visited = {seed_xuid}
        frontier = {seed_xuid}

        for _ in range(max(0, max_depth)):
            next_frontier = set()
            for current_xuid in frontier:
                for edge in self.db.get_friends(current_xuid):
                    dst_xuid = edge.get("dst_xuid")
                    if not dst_xuid or dst_xuid in visited:
                        continue
                    if not bool(edge.get("halo_active")):
                        continue
                    visited.add(dst_xuid)
                    next_frontier.add(dst_xuid)
            if not next_frontier:
                break
            frontier = next_frontier

        return sorted(visited)

    @commands.command(
        name="crawlgames",
        help="Build co-play edges from shared match history (default scoped; use --global for full sweep). Admin only.",
    )
    @commands.has_permissions(administrator=True)
    async def start_crawl_games(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ):
        """Start background game-history crawl and refresh co-play edge weights."""
        if not inputs:
            await ctx.send(
                "Usage: `#crawlgames GAMERTAG [depth] [--scoped|--global]`\n"
                "Examples:\n"
                "- `#crawlgames YourGamertag 2` (default scoped/focused mode)\n"
                "- `#crawlgames YourGamertag 2 --global` (full global participants)"
            )
            return

        raw_inputs = [str(part).strip() for part in inputs if str(part).strip()]
        requested_scope_mode = "scoped"
        filtered_inputs: List[str] = []
        for token in raw_inputs:
            lowered = token.lower()
            if lowered in {"--scoped", "scoped"}:
                requested_scope_mode = "scoped"
                continue
            if lowered in {"--global", "global"}:
                requested_scope_mode = "global"
                continue
            filtered_inputs.append(token)

        if not filtered_inputs:
            await ctx.send(
                "Usage: `#crawlgames GAMERTAG [depth] [--scoped|--global]`\n"
                "Provide a gamertag and optionally depth or `--global`."
            )
            return

        if len(filtered_inputs) > 1 and filtered_inputs[-1].isdigit():
            gamertag = " ".join(filtered_inputs[:-1]).strip()
            depth = int(filtered_inputs[-1])
        else:
            gamertag = " ".join(filtered_inputs).strip()
            depth = 2

        if not gamertag:
            await ctx.send(
                "Usage: `#crawlgames GAMERTAG [depth] [--scoped|--global]`\n"
                "Provide a non-empty gamertag."
            )
            return

        if self._crawl_task and not self._crawl_task.done():
            await ctx.send("A crawl is already running. Wait for it to complete or restart the bot.")
            return

        api = self._get_api_client()

        crawl_started_at = datetime.now(timezone.utc)
        progress_message: Optional[discord.Message] = None
        progress_view: Optional[CrawlProgressView] = None
        last_embed_update_at = 0.0

        progress_state: Dict[str, object] = {
            "stage": "Preparing scope",
            "percent": 0.0,
            "detail": "Initializing crawl",
            "scope_mode": "Global participants",
            "players_analyzed": 0,
            "unique_pairs": 0,
            "rows_written": 0,
            "write_failures": 0,
            "seed_pairs": 0,
            "seed_rows": 0,
            "seed_matches": 0,
            "stub_rows": 0,
            "failure_examples": [],
        }

        def _progress_bar(percent: float, width: int = 20) -> str:
            bounded = max(0.0, min(100.0, float(percent)))
            filled = int(round((bounded / 100.0) * width))
            filled = max(0, min(width, filled))
            return f"[{'#' * filled}{'-' * (width - filled)}] {bounded:5.1f}%"

        def _build_progress_embed(status: str) -> discord.Embed:
            elapsed = max(0, int((datetime.now(timezone.utc) - crawl_started_at).total_seconds()))
            percent = float(progress_state.get("percent") or 0.0)

            title_map = {
                "RUNNING": "Co-play Crawl In Progress",
                "COMPLETED": "Co-play Crawl Complete",
                "FAILED": "Co-play Crawl Failed",
                "CANCELLED": "Co-play Crawl Cancelled",
            }
            color_map = {
                "RUNNING": 0x3498DB,
                "COMPLETED": 0x00FF88,
                "FAILED": 0xE74C3C,
                "CANCELLED": 0xE67E22,
            }

            embed = discord.Embed(
                title=title_map.get(status, "Co-play Crawl"),
                colour=color_map.get(status, 0x3498DB),
                timestamp=datetime.now(),
            )
            embed.add_field(name="Seed", value=gamertag, inline=True)
            embed.add_field(name="Depth", value=str(depth), inline=True)
            embed.add_field(name="Scope Mode", value=str(progress_state.get("scope_mode") or "Global participants"), inline=True)
            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(name="Progress", value=_progress_bar(percent), inline=False)
            embed.add_field(name="Stage", value=str(progress_state.get("stage") or "Preparing"), inline=True)
            embed.add_field(name="Elapsed", value=f"{elapsed}s", inline=True)

            detail = str(progress_state.get("detail") or "")
            if detail:
                embed.add_field(name="Detail", value=detail[:1024], inline=False)

            embed.add_field(name="Players Analyzed", value=str(progress_state.get("players_analyzed") or 0), inline=True)
            embed.add_field(name="Unique Co-play Pairs", value=str(progress_state.get("unique_pairs") or 0), inline=True)
            embed.add_field(name="Co-play Rows Written", value=str(progress_state.get("rows_written") or 0), inline=True)
            embed.add_field(name="Write Failures", value=str(progress_state.get("write_failures") or 0), inline=True)
            embed.add_field(name="Seed Pairs Found", value=str(progress_state.get("seed_pairs") or 0), inline=True)
            embed.add_field(name="Seed Rows Written", value=str(progress_state.get("seed_rows") or 0), inline=True)
            embed.add_field(name="Seed Qualifying Matches", value=str(progress_state.get("seed_matches") or 0), inline=True)
            embed.add_field(name="Stub Players Ensured", value=str(progress_state.get("stub_rows") or 0), inline=True)

            failure_examples = progress_state.get("failure_examples") or []
            if failure_examples:
                preview = "\n".join(f"- {item}" for item in failure_examples[:5])
                embed.add_field(name="Failure Samples", value=preview, inline=False)

            return embed

        async def _refresh_progress_embed(status: str = "RUNNING", force: bool = False) -> None:
            nonlocal last_embed_update_at
            if run_inline or not progress_message:
                return

            now = asyncio.get_running_loop().time()
            if status == "RUNNING" and not force and (now - last_embed_update_at) < 1.5:
                return

            last_embed_update_at = now
            try:
                await progress_message.edit(embed=_build_progress_embed(status), view=progress_view)
            except Exception:
                return

        async def _emit_progress(stage: str, percent: float, detail: str, force: bool = False) -> None:
            progress_state["stage"] = stage
            progress_state["percent"] = max(0.0, min(100.0, float(percent)))
            progress_state["detail"] = detail

            if progress_callback:
                try:
                    await progress_callback(
                        {
                            "stage": stage,
                            "percent": progress_state["percent"],
                            "detail": detail,
                        }
                    )
                except Exception:
                    pass

            await _refresh_progress_embed(status="RUNNING", force=force)

        if not run_inline:
            requester_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)
            progress_view = CrawlProgressView(self, requester_id=requester_id)
            progress_message = await ctx.send(embed=_build_progress_embed("RUNNING"), view=progress_view)
            progress_view.message = progress_message

        async def run_coplay_crawl():
            try:
                await _emit_progress("Preparing scope", 5.0, f"Resolving seed player {gamertag}", force=True)

                seed_xuid = await api.resolve_gamertag_to_xuid(gamertag)
                if not seed_xuid:
                    progress_state["detail"] = (
                        f"Could not resolve {gamertag}. Run #crawlfriends first if this player is not yet discovered."
                    )
                    await _refresh_progress_embed(status="FAILED", force=True)
                    if progress_view:
                        await progress_view.deactivate()
                    return f"Could not resolve {gamertag}; co-play build skipped."

                await _emit_progress("Hydrating seed history", 12.0, f"Fetching recent match history for {gamertag}", force=True)
                seed_hydrate_result = await self._hydrate_seed_match_history(seed_xuid, gamertag)
                if seed_hydrate_result.get("ok"):
                    hydrate_detail = (
                        f"Seed history hydrated: {int(seed_hydrate_result.get('matches_processed') or 0)} matches, "
                        f"{int(seed_hydrate_result.get('matches_with_participants') or 0)} with participants"
                    )
                else:
                    hydrate_detail = str(seed_hydrate_result.get("message") or "Seed history hydration failed")
                await _emit_progress("Reading participants", 20.0, hydrate_detail, force=True)

                match_edge_counts: Dict[tuple[str, str], int] = defaultdict(int)
                same_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
                opposing_team_counts: Dict[tuple[str, str], int] = defaultdict(int)
                first_played: Dict[tuple[str, str], str] = {}
                last_played: Dict[tuple[str, str], str] = {}
                counted_pair_matches = set()

                players_analyzed = 0
                stats_db = getattr(getattr(api, "stats_cache", None), "db", None)
                if not stats_db:
                    progress_state["detail"] = "Participant database is unavailable."
                    await _refresh_progress_embed(status="FAILED", force=True)
                    if progress_view:
                        await progress_view.deactivate()
                    return "Participant-first co-play build requires stats DB participant access, but it is unavailable."

                if requested_scope_mode == "scoped":
                    if hasattr(stats_db, "get_scope_match_participants"):
                        scope_xuids = self._collect_halo_active_scope(seed_xuid, depth)
                        if seed_xuid not in scope_xuids:
                            scope_xuids = [seed_xuid] + scope_xuids
                        progress_state["scope_mode"] = f"Scoped participants ({len(scope_xuids)} players)"
                        progress_state["detail"] = f"Loading scoped participants from {len(scope_xuids)} scoped players"
                        await _refresh_progress_embed(status="RUNNING", force=True)
                        all_match_participants = stats_db.get_scope_match_participants(scope_xuids) or {}

                        seed_overlay_matches = 0
                        if hasattr(stats_db, "get_seed_match_participants"):
                            seed_match_participants = stats_db.get_seed_match_participants(seed_xuid) or {}
                            if seed_match_participants:
                                all_match_participants.update(seed_match_participants)
                                seed_overlay_matches = len(seed_match_participants)

                        if seed_overlay_matches > 0:
                            progress_state["scope_mode"] = (
                                f"Scoped participants + seed rosters ({len(scope_xuids)} players, "
                                f"{seed_overlay_matches} seed matches)"
                            )
                            progress_state["detail"] = (
                                f"Loaded scoped participants and full rosters for {seed_overlay_matches} seed matches"
                            )
                        else:
                            progress_state["detail"] = (
                                f"Loaded scoped participants from {len(scope_xuids)} scoped players"
                            )
                    elif hasattr(stats_db, "get_all_match_participants"):
                        progress_state["scope_mode"] = "Global participants (scoped fallback)"
                        progress_state["detail"] = "Scoped mode unavailable; falling back to global participants"
                        await _refresh_progress_embed(status="RUNNING", force=True)
                        all_match_participants = stats_db.get_all_match_participants() or {}
                    else:
                        progress_state["detail"] = "Stats DB does not support participant retrieval methods."
                        await _refresh_progress_embed(status="FAILED", force=True)
                        if progress_view:
                            await progress_view.deactivate()
                        return "Participant retrieval methods are unavailable on stats DB."
                else:
                    progress_state["scope_mode"] = "Global participants"
                    if not hasattr(stats_db, "get_all_match_participants"):
                        progress_state["detail"] = "Stats DB does not support global participant retrieval."
                        await _refresh_progress_embed(status="FAILED", force=True)
                        if progress_view:
                            await progress_view.deactivate()
                        return "Global participant retrieval is unavailable on stats DB."
                    all_match_participants = stats_db.get_all_match_participants() or {}

                analyzed_players = set()
                seed_qualifying_matches = 0

                total_matches = len(all_match_participants)
                for idx, (match_id, participants) in enumerate(all_match_participants.items(), start=1):
                    if len(participants) < 2:
                        continue

                    normalized_participants = []
                    seen_in_match = set()
                    seed_in_match = False
                    start_time = ""
                    for participant in participants:
                        participant_xuid = str(participant.get("xuid") or "").strip()
                        if not participant_xuid or participant_xuid in seen_in_match:
                            continue
                        seen_in_match.add(participant_xuid)
                        normalized_participants.append(participant)
                        analyzed_players.add(participant_xuid)
                        if participant_xuid == seed_xuid:
                            seed_in_match = True
                        if not start_time:
                            start_time = str(participant.get("start_time") or "")

                    if len(normalized_participants) < 2:
                        continue
                    if seed_in_match:
                        seed_qualifying_matches += 1

                    for left, right in combinations(normalized_participants, 2):
                        left_xuid = str(left.get("xuid") or "").strip()
                        right_xuid = str(right.get("xuid") or "").strip()
                        if not left_xuid or not right_xuid or left_xuid == right_xuid:
                            continue

                        src_xuid, dst_xuid = sorted((left_xuid, right_xuid))
                        pair_key = (src_xuid, dst_xuid)
                        pair_match_key = (match_id, src_xuid, dst_xuid)
                        if match_id and pair_match_key in counted_pair_matches:
                            continue
                        if match_id:
                            counted_pair_matches.add(pair_match_key)

                        match_edge_counts[pair_key] += 1

                        left_team = left.get("team_id") or left.get("inferred_team_id")
                        right_team = right.get("team_id") or right.get("inferred_team_id")
                        if left_team and right_team:
                            if str(left_team) == str(right_team):
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

                    if total_matches > 0:
                        analysis_pct = 25.0 + (float(idx) / float(total_matches)) * 60.0
                        await _emit_progress(
                            "Analyzing co-play",
                            min(85.0, analysis_pct),
                            f"Analyzed {idx}/{total_matches} matches",
                        )

                players_analyzed = len(analyzed_players)
                seed_pairs_found = sum(1 for src_xuid, dst_xuid in match_edge_counts if seed_xuid in (src_xuid, dst_xuid))
                progress_state["players_analyzed"] = players_analyzed
                progress_state["unique_pairs"] = len(match_edge_counts)
                progress_state["seed_pairs"] = seed_pairs_found
                progress_state["seed_matches"] = seed_qualifying_matches

                await _emit_progress(
                    "Ensuring player nodes",
                    86.0,
                    f"Ensuring graph players for {len(analyzed_players)} participant xuids",
                    force=True,
                )

                ensured_stubs = 0
                if analyzed_players:
                    if hasattr(self.db, "insert_or_update_players_stub_batch"):
                        ensured_stubs = int(self.db.insert_or_update_players_stub_batch(list(analyzed_players)) or 0)
                    else:
                        for participant_xuid in analyzed_players:
                            if self.db.insert_or_update_player(xuid=participant_xuid):
                                ensured_stubs += 1
                progress_state["stub_rows"] = ensured_stubs

                halo_active_xuids: set[str] = set()
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT xuid FROM graph_players WHERE halo_active = 1")
                halo_active_xuids = {str(row["xuid"]) for row in cursor.fetchall() if row["xuid"]}

                await _emit_progress(
                    "Writing edges",
                    88.0,
                    f"Writing {len(match_edge_counts)} co-play pairs",
                    force=True,
                )

                rows_written = 0
                seed_rows_written = 0
                write_failures = 0
                failure_examples: List[str] = []
                total_pairs = max(1, len(match_edge_counts))
                for pair_idx, ((src_xuid, dst_xuid), matches_together) in enumerate(match_edge_counts.items(), start=1):
                    first_ts = first_played.get((src_xuid, dst_xuid))
                    last_ts = last_played.get((src_xuid, dst_xuid))
                    is_halo_active_pair = src_xuid in halo_active_xuids and dst_xuid in halo_active_xuids
                    is_seed_pair = seed_xuid in (src_xuid, dst_xuid)

                    base_kwargs = {
                        "matches_together": matches_together,
                        "wins_together": 0,
                        "first_played": first_ts,
                        "last_played": last_ts,
                        "total_minutes": 0,
                        "same_team_count": same_team_counts.get((src_xuid, dst_xuid), 0),
                        "opposing_team_count": opposing_team_counts.get((src_xuid, dst_xuid), 0),
                        "source_type": "participants-runtime",
                        "is_halo_active_pair": is_halo_active_pair,
                    }

                    try:
                        wrote_forward = self.db.upsert_coplay_edge(
                            src_xuid=src_xuid,
                            dst_xuid=dst_xuid,
                            suppress_errors=True,
                            **base_kwargs,
                        )
                    except TypeError:
                        wrote_forward = self.db.upsert_coplay_edge(
                            src_xuid=src_xuid,
                            dst_xuid=dst_xuid,
                            **base_kwargs,
                        )

                    if wrote_forward:
                        rows_written += 1
                        if is_seed_pair:
                            seed_rows_written += 1
                    else:
                        write_failures += 1
                        if len(failure_examples) < 5:
                            failure_examples.append(f"{src_xuid}->{dst_xuid}")

                    try:
                        wrote_reverse = self.db.upsert_coplay_edge(
                            src_xuid=dst_xuid,
                            dst_xuid=src_xuid,
                            suppress_errors=True,
                            **base_kwargs,
                        )
                    except TypeError:
                        wrote_reverse = self.db.upsert_coplay_edge(
                            src_xuid=dst_xuid,
                            dst_xuid=src_xuid,
                            **base_kwargs,
                        )

                    if wrote_reverse:
                        rows_written += 1
                        if is_seed_pair:
                            seed_rows_written += 1
                    else:
                        write_failures += 1
                        if len(failure_examples) < 5:
                            failure_examples.append(f"{dst_xuid}->{src_xuid}")

                    progress_state["rows_written"] = rows_written
                    progress_state["write_failures"] = write_failures
                    progress_state["seed_rows"] = seed_rows_written
                    progress_state["failure_examples"] = failure_examples

                    if pair_idx == 1 or pair_idx == total_pairs or pair_idx % 250 == 0:
                        write_pct = 88.0 + (float(pair_idx) / float(total_pairs)) * 10.0
                        await _emit_progress(
                            "Writing edges",
                            min(98.0, write_pct),
                            f"Processed {pair_idx}/{total_pairs} pairs",
                        )

                progress_state["rows_written"] = rows_written
                progress_state["write_failures"] = write_failures
                progress_state["seed_rows"] = seed_rows_written
                progress_state["failure_examples"] = failure_examples

                await _emit_progress(
                    "Finalizing",
                    100.0,
                    f"Co-play crawl complete. Rows written: {rows_written}, failures: {write_failures}",
                    force=True,
                )
                await _refresh_progress_embed(status="COMPLETED", force=True)
                if progress_view:
                    await progress_view.deactivate()

                return (
                    f"Co-play crawl completed for {gamertag}. "
                    f"{progress_state.get('scope_mode')} mode; analyzed {players_analyzed}, pairs {len(match_edge_counts)}, "
                    f"rows written {rows_written}, write failures {write_failures}, "
                    f"seed pairs {seed_pairs_found}, seed rows {seed_rows_written}."
                )
            except asyncio.CancelledError:
                progress_state["detail"] = "Cancellation requested"
                await _refresh_progress_embed(status="CANCELLED", force=True)
                if progress_view:
                    await progress_view.deactivate()
                raise
            except Exception as e:
                progress_state["detail"] = str(e)
                await _refresh_progress_embed(status="FAILED", force=True)
                if progress_view:
                    await progress_view.deactivate()
                if run_inline:
                    raise
                await ctx.channel.send(f"Co-play crawl error: {str(e)}")
                return f"Co-play crawl failed for {gamertag}: {str(e)}"

        self._crawl_task = asyncio.create_task(run_coplay_crawl())
        if run_inline:
            return await self._crawl_task
