#!/usr/bin/env python3
"""
Backfill graph_coplay from existing normalized match participant data.

This utility builds co-play edges from stats DB tables (matches + match_participants)
and writes deterministic edges into graph DB's graph_coplay table.

Default mode is dry-run. Use --run to write rows.

Examples:
    python one_time_backfill_graph_coplay.py --dry-run
    python one_time_backfill_graph_coplay.py --run --limit-matches 5000 --batch-size 200
    python one_time_backfill_graph_coplay.py --run --seed-gamertag "Player Name" --depth 2
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import Dict, List, Optional, Set, Tuple

from src.database.cache import get_cache
from src.database.graph_schema import get_graph_db


@dataclass
class PairStats:
    matches_together: int = 0
    wins_together: int = 0
    same_team_count: int = 0
    opposing_team_count: int = 0
    first_played: Optional[str] = None
    last_played: Optional[str] = None
    inferred_matches: int = 0
    partial_matches: int = 0
    complete_matches: int = 0


@dataclass
class BackfillResult:
    scope_players: int = 0
    matches_considered: int = 0
    pairs_built: int = 0
    rows_written: int = 0
    inferred_pairs: int = 0
    partial_pairs: int = 0


def _collect_halo_active_scope(graph_db, seed_xuid: str, max_depth: int) -> List[str]:
    """Collect halo-active players reachable from seed within depth in current graph DB."""
    visited = {seed_xuid}
    frontier = {seed_xuid}

    for _ in range(max(0, max_depth)):
        next_frontier: Set[str] = set()
        for current_xuid in frontier:
            for edge in graph_db.get_friends(current_xuid):
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


def _get_scope_xuids(graph_db, stats_cache, seed_xuid: Optional[str], seed_gamertag: Optional[str], depth: int) -> List[str]:
    """Resolve requested scope or fall back to all known graph players."""
    resolved_seed = (seed_xuid or "").strip() if seed_xuid else None

    if not resolved_seed and seed_gamertag:
        player = graph_db.get_player_by_gamertag(seed_gamertag)
        if player and player.get("xuid"):
            resolved_seed = str(player["xuid"])
        else:
            resolved_seed = stats_cache.resolve_xuid_by_gamertag(seed_gamertag)

    if resolved_seed:
        return _collect_halo_active_scope(graph_db, resolved_seed, depth)

    conn = graph_db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT xuid FROM graph_players")
    return sorted({str(row["xuid"]) for row in cursor.fetchall() if row["xuid"]})


def _team_value(participant: Dict) -> Tuple[Optional[str], bool]:
    """Return team value and whether it was inferred from fallback fields."""
    team_id = participant.get("team_id")
    if team_id:
        return str(team_id), False

    inferred = participant.get("inferred_team_id")
    if inferred:
        return str(inferred), True

    return None, False


def _update_time_bounds(pair_stats: PairStats, start_time: Optional[str]) -> None:
    if not start_time:
        return

    if not pair_stats.first_played or start_time < pair_stats.first_played:
        pair_stats.first_played = start_time
    if not pair_stats.last_played or start_time > pair_stats.last_played:
        pair_stats.last_played = start_time


def _aggregate_pairs(scope_match_participants: Dict[str, List[Dict]], limit_matches: Optional[int]) -> Tuple[Dict[Tuple[str, str], PairStats], int]:
    """Aggregate unordered pair metrics from scoped match participants."""
    matches_iter = list(scope_match_participants.items())

    # Prefer deterministic ordering by newest known start_time first.
    matches_iter.sort(
        key=lambda item: max((str(p.get("start_time") or "") for p in item[1]), default=""),
        reverse=True,
    )

    if limit_matches is not None and limit_matches > 0:
        matches_iter = matches_iter[:limit_matches]

    pair_map: Dict[Tuple[str, str], PairStats] = defaultdict(PairStats)

    for _, participants in matches_iter:
        if len(participants) < 2:
            continue

        for left, right in combinations(participants, 2):
            left_xuid = str(left.get("xuid") or "").strip()
            right_xuid = str(right.get("xuid") or "").strip()
            if not left_xuid or not right_xuid or left_xuid == right_xuid:
                continue

            src_xuid, dst_xuid = sorted((left_xuid, right_xuid))
            pair_key = (src_xuid, dst_xuid)
            stats = pair_map[pair_key]

            stats.matches_together += 1

            start_time = left.get("start_time") or right.get("start_time")
            _update_time_bounds(stats, str(start_time) if start_time else None)

            left_team, left_inferred = _team_value(left)
            right_team, right_inferred = _team_value(right)
            if left_inferred or right_inferred:
                stats.inferred_matches += 1

            if left_team and right_team:
                stats.complete_matches += 1
                if left_team == right_team:
                    stats.same_team_count += 1
                else:
                    stats.opposing_team_count += 1
            else:
                stats.partial_matches += 1

            left_outcome = left.get("outcome")
            right_outcome = right.get("outcome")
            if left_outcome is None or right_outcome is None:
                stats.partial_matches += 1
            elif left_outcome == 2 and right_outcome == 2:
                stats.wins_together += 1

    return pair_map, len(matches_iter)


def _load_scope_match_participants(stats_db, scope_xuids: List[str], limit_matches: Optional[int]) -> Dict[str, List[Dict]]:
    """Load scoped participants using a temp scope table to avoid SQLite variable limits."""
    normalized_scope = sorted({str(x).strip() for x in scope_xuids if str(x).strip()})
    if not normalized_scope:
        return {}

    conn = stats_db._get_connection()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS temp_scope_xuids")
    cursor.execute("CREATE TEMP TABLE temp_scope_xuids (xuid TEXT PRIMARY KEY)")
    cursor.executemany(
        "INSERT OR IGNORE INTO temp_scope_xuids (xuid) VALUES (?)",
        [(xuid,) for xuid in normalized_scope],
    )

    limit_clause = ""
    params: List[int] = []
    if limit_matches is not None and limit_matches > 0:
        limit_clause = "LIMIT ?"
        params.append(int(limit_matches))

    cursor.execute(
        f"""
        WITH candidate_matches AS (
            SELECT DISTINCT mp.match_id
            FROM match_participants mp
            JOIN temp_scope_xuids ts ON ts.xuid = mp.xuid
        ),
        scope_matches AS (
            SELECT cm.match_id
            FROM candidate_matches cm
            LEFT JOIN matches m ON m.match_id = cm.match_id
            ORDER BY COALESCE(m.start_time, '') DESC, cm.match_id ASC
            {limit_clause}
        )
        SELECT
            mp.match_id,
            mp.xuid,
            mp.outcome,
            mp.team_id,
            mp.inferred_team_id,
            m.start_time
        FROM match_participants mp
        JOIN scope_matches sm ON sm.match_id = mp.match_id
        JOIN temp_scope_xuids ts ON ts.xuid = mp.xuid
        LEFT JOIN matches m ON m.match_id = mp.match_id
        ORDER BY COALESCE(m.start_time, '') DESC, mp.match_id ASC
        """,
        params,
    )

    grouped: Dict[str, List[Dict]] = {}
    for row in cursor.fetchall():
        row_dict = dict(row)
        grouped.setdefault(row_dict["match_id"], []).append(row_dict)

    cursor.execute("DROP TABLE IF EXISTS temp_scope_xuids")
    conn.commit()
    return grouped


def run_backfill(
    dry_run: bool,
    batch_size: int,
    limit_matches: Optional[int],
    seed_xuid: Optional[str],
    seed_gamertag: Optional[str],
    depth: int,
    reset_target: bool,
) -> BackfillResult:
    graph_db = get_graph_db()
    stats_cache = get_cache()
    stats_db = stats_cache.db

    scope_xuids = _get_scope_xuids(graph_db, stats_cache, seed_xuid, seed_gamertag, depth)
    if not scope_xuids:
        raise RuntimeError("No scope players were found for this backfill run.")

    scope_match_participants = _load_scope_match_participants(stats_db, scope_xuids, limit_matches)
    pair_map, matches_considered = _aggregate_pairs(scope_match_participants, limit_matches=None)

    result = BackfillResult(
        scope_players=len(scope_xuids),
        matches_considered=matches_considered,
        pairs_built=len(pair_map),
    )

    if dry_run:
        for pair_stats in pair_map.values():
            is_inferred = pair_stats.inferred_matches > 0
            is_partial = pair_stats.partial_matches > 0
            if is_inferred:
                result.inferred_pairs += 1
            if is_partial:
                result.partial_pairs += 1
        return result

    conn = graph_db._get_connection()
    cursor = conn.cursor()
    if reset_target:
        cursor.execute("DELETE FROM graph_coplay WHERE source_type = ?", ("participants",))
        conn.commit()

    total_pairs = len(pair_map)
    for idx, ((src_xuid, dst_xuid), pair_stats) in enumerate(pair_map.items(), start=1):
        graph_db.insert_or_update_player(src_xuid)
        graph_db.insert_or_update_player(dst_xuid)

        is_inferred = pair_stats.inferred_matches > 0
        is_partial = pair_stats.partial_matches > 0
        coverage_ratio = (
            pair_stats.complete_matches / pair_stats.matches_together
            if pair_stats.matches_together > 0
            else 0.0
        )

        if is_inferred:
            result.inferred_pairs += 1
        if is_partial:
            result.partial_pairs += 1

        if graph_db.upsert_coplay_edge(
            src_xuid=src_xuid,
            dst_xuid=dst_xuid,
            matches_together=pair_stats.matches_together,
            wins_together=pair_stats.wins_together,
            first_played=pair_stats.first_played,
            last_played=pair_stats.last_played,
            total_minutes=0,
            same_team_count=pair_stats.same_team_count,
            opposing_team_count=pair_stats.opposing_team_count,
            source_type="participants",
            is_inferred=is_inferred,
            is_partial=is_partial,
            coverage_ratio=coverage_ratio,
        ):
            result.rows_written += 1

        if graph_db.upsert_coplay_edge(
            src_xuid=dst_xuid,
            dst_xuid=src_xuid,
            matches_together=pair_stats.matches_together,
            wins_together=pair_stats.wins_together,
            first_played=pair_stats.first_played,
            last_played=pair_stats.last_played,
            total_minutes=0,
            same_team_count=pair_stats.same_team_count,
            opposing_team_count=pair_stats.opposing_team_count,
            source_type="participants",
            is_inferred=is_inferred,
            is_partial=is_partial,
            coverage_ratio=coverage_ratio,
        ):
            result.rows_written += 1

        if batch_size > 0 and (idx % batch_size == 0 or idx == total_pairs):
            print(f"[BACKFILL] Processed {idx}/{total_pairs} pairs")

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill graph_coplay from match participants.")
    parser.add_argument("--dry-run", action="store_true", help="Preview work without writing (default if --run is not set)")
    parser.add_argument("--run", action="store_true", help="Execute writes to graph_coplay")
    parser.add_argument("--limit-matches", type=int, default=None, help="Limit number of scoped matches analyzed")
    parser.add_argument("--batch-size", type=int, default=200, help="Progress log interval in pair count")
    parser.add_argument("--seed-xuid", type=str, default=None, help="Optional seed XUID for scoped traversal")
    parser.add_argument("--seed-gamertag", type=str, default=None, help="Optional seed gamertag for scoped traversal")
    parser.add_argument("--depth", type=int, default=2, help="Traversal depth when seed scope is used")
    parser.add_argument("--reset-target", action="store_true", help="Delete existing participants-source rows before writing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = not args.run or args.dry_run

    print("=" * 70)
    print("Graph Co-play Backfill")
    print("Source: matches + match_participants")
    print(f"Mode: {'DRY-RUN' if dry_run else 'WRITE'}")
    print("=" * 70)

    try:
        result = run_backfill(
            dry_run=dry_run,
            batch_size=max(1, int(args.batch_size or 1)),
            limit_matches=args.limit_matches,
            seed_xuid=args.seed_xuid,
            seed_gamertag=args.seed_gamertag,
            depth=max(0, int(args.depth or 0)),
            reset_target=bool(args.reset_target),
        )
    except Exception as exc:
        print(f"[BACKFILL] Failed: {exc}")
        return 1

    print("\nSummary")
    print(f"- Scope players: {result.scope_players}")
    print(f"- Matches considered: {result.matches_considered}")
    print(f"- Unique pairs built: {result.pairs_built}")
    print(f"- Inferred pairs: {result.inferred_pairs}")
    print(f"- Partial pairs: {result.partial_pairs}")
    print(f"- Rows written: {result.rows_written}")

    if dry_run:
        print("\nDry-run complete. Re-run with --run to apply changes.")
    else:
        print("\nWrite complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
