import json
import os
from typing import Optional, Tuple

from src.config import CACHE_PROGRESS_FILE, PROJECT_ROOT, XUID_CACHE_FILE
from src.database.graph_schema import get_graph_db


def parse_crawl_input(raw: str) -> Tuple[str, Optional[int]]:
    value = (raw or "").strip()
    if not value:
        return "", None

    if "|" in value:
        gamertag, maybe_depth = [part.strip() for part in value.split("|", 1)]
        if maybe_depth.isdigit():
            return gamertag, int(maybe_depth)
        return gamertag, None

    parts = value.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0].strip(), int(parts[1])

    return value, None


async def execute_terminal_action(bot, command_ctx, action: str, user_input: str = "") -> str:
    stats_cog = bot.get_cog("Stats")
    graph_cog = bot.get_cog("Graph")

    if action == "status_graph":
        db = get_graph_db()
        stats = db.get_graph_stats()
        total_edges = int(stats.get("total_friend_edges", 0)) + int(stats.get("total_coplay_edges", 0))
        return (
            f"Graph players: {stats.get('total_players', 0):,}\n"
            f"Halo active: {stats.get('halo_active_players', 0):,}\n"
            f"Friend edges: {stats.get('total_friend_edges', 0):,}\n"
            f"Co-play edges: {stats.get('total_coplay_edges', 0):,}\n"
            f"Total edges: {total_edges:,}"
        )

    if action == "status_cache":
        xuid_mappings = 0
        try:
            with open(XUID_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
            xuid_mappings = len(cache)
        except Exception:
            return "CACHE STATUS UNAVAILABLE"

        if xuid_mappings == 0:
            return "CACHE EMPTY"

        progress_candidates = [str(CACHE_PROGRESS_FILE), os.path.join(PROJECT_ROOT, "cache_progress.json")]
        progress_path = next((path for path in progress_candidates if os.path.exists(path)), None)
        if not progress_path:
            return (
                f"XUID cache mappings: {xuid_mappings:,}\n"
                "Progress: No active progress file"
            )

        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                progress = json.load(f)
        except Exception:
            return (
                f"XUID cache mappings: {xuid_mappings:,}\n"
                "Progress: File unreadable"
            )

        processed_matches = int(progress.get("processed_matches", progress.get("last_processed_index", 0)) or 0)
        total_matches = int(progress.get("total_matches", 0) or 0)

        # Support both old and new progress schemas.
        unique_players_raw = progress.get("unique_players")
        if isinstance(unique_players_raw, list):
            unique_players = len(unique_players_raw)
        elif isinstance(unique_players_raw, int):
            unique_players = unique_players_raw
        else:
            completed_xuids = progress.get("completed_xuids", [])
            unique_players = len(completed_xuids) if isinstance(completed_xuids, list) else 0

        resolved_raw = progress.get("resolved_gamertags")
        if isinstance(resolved_raw, list):
            resolved_gamertags = len(resolved_raw)
        elif isinstance(resolved_raw, int):
            resolved_gamertags = resolved_raw
        else:
            # Fallback: count non-empty gamertag mappings from XUID cache.
            resolved_gamertags = sum(1 for value in cache.values() if str(value).strip())

        if total_matches > 0:
            pct = (processed_matches / total_matches) * 100
            return (
                f"XUID cache mappings: {xuid_mappings:,}\n"
                f"Match scan: {processed_matches:,}/{total_matches:,} ({pct:.1f}%)\n"
                f"Unique players: {unique_players:,}\n"
                f"Resolved GTs: {resolved_gamertags:,}"
            )

        return (
            f"XUID cache mappings: {xuid_mappings:,}\n"
            f"Unique players tracked: {unique_players:,}\n"
            f"Resolved GTs: {resolved_gamertags:,}"
        )

    if not stats_cog and action.startswith("cmd_"):
        return "Stats cog unavailable"

    if action == "cmd_full":
        await stats_cog.full(command_ctx, user_input)
        return f"Executed #full for {user_input}"

    if action == "cmd_ranked":
        await stats_cog.ranked(command_ctx, user_input)
        return f"Executed #ranked for {user_input}"

    if action == "cmd_casual":
        await stats_cog.casual(command_ctx, user_input)
        return f"Executed #casual for {user_input}"

    if action == "cmd_server":
        await stats_cog.server_stats(command_ctx)
        return "Executed #server"

    if action == "cmd_populate":
        await stats_cog.populate_cache(command_ctx, user_input)
        return f"Executed #populate for {user_input}"

    if action == "cmd_xboxfriends":
        await stats_cog.friends_list(command_ctx, user_input)
        return f"Executed #xboxfriends for {user_input}"

    if not graph_cog and action.startswith("cmd_"):
        return "Graph cog unavailable"

    if action == "cmd_network":
        await graph_cog.show_network(command_ctx, user_input)
        return f"Executed #network for {user_input}"

    if action == "cmd_similar":
        await graph_cog.find_similar(command_ctx, user_input)
        return f"Executed #similar for {user_input}"

    if action == "cmd_hubs":
        value = (user_input or "").strip()
        if value.isdigit():
            await graph_cog.find_hubs(command_ctx, int(value))
            return f"Executed #hubs {value}"
        await graph_cog.find_hubs(command_ctx)
        return "Executed #hubs"

    if action == "cmd_crawl":
        if not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for crawl actions"

        gamertag, depth = parse_crawl_input(user_input)
        if not gamertag:
            return "Crawl input required: Gamertag|Depth"

        if depth is not None:
            await graph_cog.start_crawl(command_ctx, gamertag, str(depth))
            return f"Executed #crawl {gamertag} {depth}"

        await graph_cog.start_crawl(command_ctx, gamertag)
        return f"Executed #crawl {gamertag}"

    if action == "cmd_crawlstop":
        if not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for crawl actions"
        await graph_cog.stop_crawl(command_ctx)
        return "Executed #crawlstop"

    return "No action executed"
