import os
from typing import Callable, Optional, Tuple

from src.bot.cache_status import load_cache_status_metrics
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


async def execute_terminal_action(
    bot,
    command_ctx,
    action: str,
    user_input: str = "",
    access_level: str = "admin",
    progress_callback: Optional[Callable[[dict], object]] = None,
) -> str:
    if access_level not in {"admin", "user"}:
        return "Login required before running terminal commands."

    user_allowed_actions = {
        "cmd_full",
        "cmd_ranked",
        "cmd_casual",
        "cmd_iss_level0",
        "cmd_iss_level1",
    }

    if access_level == "user" and action not in user_allowed_actions:
        return "Action not available in user terminal mode."

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
        try:
            metrics = load_cache_status_metrics(
                XUID_CACHE_FILE,
                [str(CACHE_PROGRESS_FILE), os.path.join(PROJECT_ROOT, "cache_progress.json")],
            )
        except Exception:
            return "CACHE STATUS UNAVAILABLE"

        if metrics.xuid_mappings == 0:
            return "CACHE EMPTY"

        if metrics.progress_state == "missing":
            return (
                f"XUID cache mappings: {metrics.xuid_mappings:,}\n"
                f"Resolved GTs: {metrics.resolved_gamertags:,}\n"
                "Progress: No active progress file"
            )

        if metrics.progress_state == "unreadable":
            return (
                f"XUID cache mappings: {metrics.xuid_mappings:,}\n"
                f"Resolved GTs: {metrics.resolved_gamertags:,}\n"
                "Progress: File unreadable"
            )

        if metrics.total_matches > 0:
            pct = (metrics.processed_matches / metrics.total_matches) * 100
            return (
                f"XUID cache mappings: {metrics.xuid_mappings:,}\n"
                f"Match scan: {metrics.processed_matches:,}/{metrics.total_matches:,} ({pct:.1f}%)\n"
                f"Resolved GTs: {metrics.resolved_gamertags:,}"
            )

        return (
            f"XUID cache mappings: {metrics.xuid_mappings:,}\n"
            f"Resolved GTs: {metrics.resolved_gamertags:,}"
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

    if action == "cmd_xboxfriends":
        await stats_cog.friends_list(command_ctx, user_input)
        return f"Executed #xboxfriends for {user_input}"

    if not graph_cog and action.startswith("cmd_"):
        return "Graph cog unavailable"

    if action == "cmd_iss_level0":
        result = await graph_cog.iss_level0(
            command_ctx,
            user_input,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return result or f"Executed ISS level 0 for {user_input}"

    if action == "cmd_iss_level1":
        result = await graph_cog.iss_level1(
            command_ctx,
            user_input,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return result or f"Executed ISS level 1 for {user_input}"

    if action == "cmd_iss_level2":
        if not command_ctx or not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for ISS level 2"
        result = await graph_cog.iss_level2(
            command_ctx,
            user_input,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return result or f"Executed ISS level 2 for {user_input}"

    if action == "cmd_iss_level3":
        if not command_ctx or not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for ISS level 3"
        result = await graph_cog.iss_level3(
            command_ctx,
            user_input,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return result or f"Executed ISS level 3 for {user_input}"

    if action == "cmd_network":
        await graph_cog.show_network(command_ctx, user_input)
        return f"Executed #network for {user_input}"

    if action == "cmd_halonet":
        await graph_cog.show_halonet(command_ctx, user_input)
        return f"Executed #halonet for {user_input}"

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

    if action == "cmd_crawlfriends":
        if not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for crawl actions"

        gamertag, depth = parse_crawl_input(user_input)
        if not gamertag:
            return "Crawl input required: Gamertag|Depth"

        if depth is not None:
            await graph_cog.start_crawl(
                command_ctx,
                gamertag,
                str(depth),
                progress_callback=progress_callback,
                run_inline=progress_callback is not None,
            )
            return f"Executed #crawlfriends {gamertag} {depth}"

        await graph_cog.start_crawl(
            command_ctx,
            gamertag,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return f"Executed #crawlfriends {gamertag}"

    if action == "cmd_crawlgames":
        if not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for crawl actions"

        gamertag, depth = parse_crawl_input(user_input)
        if not gamertag:
            return "Crawl input required: Gamertag|Depth"

        if depth is not None:
            await graph_cog.start_crawl_games(
                command_ctx,
                gamertag,
                str(depth),
                progress_callback=progress_callback,
                run_inline=progress_callback is not None,
            )
            return f"Executed #crawlgames {gamertag} {depth}"

        await graph_cog.start_crawl_games(
            command_ctx,
            gamertag,
            progress_callback=progress_callback,
            run_inline=progress_callback is not None,
        )
        return f"Executed #crawlgames {gamertag}"

    if action == "cmd_crawlstop":
        if not command_ctx.author.guild_permissions.administrator:
            return "Admin permission required for crawl actions"
        await graph_cog.stop_crawl(command_ctx)
        return "Executed #crawlstop"

    return "No action executed"
