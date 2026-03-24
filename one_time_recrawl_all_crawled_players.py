#!/usr/bin/env python3
"""
One-time recrawl utility for previously crawled graph players.

This script finds every player with last_crawled set, re-queues them with
force_pending=True, and optionally runs the crawler immediately.

Usage examples:
  python one_time_recrawl_all_crawled_players.py --dry-run
  python one_time_recrawl_all_crawled_players.py
  python one_time_recrawl_all_crawled_players.py --run
  python one_time_recrawl_all_crawled_players.py --run --max-depth 3 --batch-size 500
    python one_time_recrawl_all_crawled_players.py --limit 2000
    python one_time_recrawl_all_crawled_players.py --include-non-halo-active --dry-run
"""

import argparse
import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.api import api_client
from src.database.graph_schema import get_graph_db
from src.graph.crawler import CrawlConfig, GraphCrawler


@dataclass
class RecrawlPlan:
    total_found: int
    queued_candidates: int
    skipped_private: int
    skipped_null_xuid: int
    max_depth_seen: int
    queue_items: List[Tuple[str, int, int]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Re-queue all previously crawled graph players for a one-time completeness recrawl."
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=60,
        help="Queue priority for re-queued players (default: 60).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size used when writing queue rows (default: 500).",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help="Optional depth clamp for queued players and runtime config.",
    )
    parser.add_argument(
        "--include-private",
        action="store_true",
        help="Include players marked profile_visibility=private (default: skip).",
    )
    parser.add_argument(
        "--include-non-halo-active",
        action="store_true",
        help="Include non-Halo-active previously crawled players (default: only halo_active=1).",
    )
    parser.add_argument(
        "--only-halo-active",
        action="store_true",
        help="Deprecated alias kept for compatibility; halo_active-only is now the default.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of crawled players to consider (0 = all).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview counts only. Do not write queue rows.",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="After queueing, immediately run crawler.resume_crawl().",
    )
    return parser.parse_args()


def build_recrawl_plan(args: argparse.Namespace) -> RecrawlPlan:
    db = get_graph_db()
    conn = db._get_connection()
    cursor = conn.cursor()

    sql = [
        "SELECT xuid, gamertag, crawl_depth, profile_visibility, halo_active, last_crawled",
        "FROM graph_players",
        "WHERE last_crawled IS NOT NULL",
    ]
    params: List[object] = []

    if not args.include_non_halo_active:
        sql.append("AND halo_active = 1")

    sql.append("ORDER BY last_crawled DESC")

    if args.limit and args.limit > 0:
        sql.append("LIMIT ?")
        params.append(args.limit)

    cursor.execute("\n".join(sql), tuple(params))
    rows = [dict(r) for r in cursor.fetchall()]

    queue_items: List[Tuple[str, int, int]] = []
    skipped_private = 0
    skipped_null_xuid = 0
    max_depth_seen = 0

    for row in rows:
        xuid = row.get("xuid")
        if not xuid:
            skipped_null_xuid += 1
            continue

        visibility = (row.get("profile_visibility") or "").lower()
        if visibility == "private" and not args.include_private:
            skipped_private += 1
            continue

        depth_raw = row.get("crawl_depth")
        depth = int(depth_raw) if isinstance(depth_raw, int) else 0
        if depth < 0:
            depth = 0
        if args.max_depth is not None:
            depth = min(depth, args.max_depth)

        max_depth_seen = max(max_depth_seen, depth)
        queue_items.append((str(xuid), args.priority, depth))

    return RecrawlPlan(
        total_found=len(rows),
        queued_candidates=len(queue_items),
        skipped_private=skipped_private,
        skipped_null_xuid=skipped_null_xuid,
        max_depth_seen=max_depth_seen,
        queue_items=queue_items,
    )


def enqueue_plan(plan: RecrawlPlan, batch_size: int) -> int:
    db = get_graph_db()
    if not plan.queue_items:
        print("[RECRAWL] Queue write skipped: no candidates to enqueue.")
        return 0

    total_items = len(plan.queue_items)
    total_batches = (total_items + batch_size - 1) // batch_size
    start = time.monotonic()
    last_status = start

    print(f"[RECRAWL] Queue write started: {total_items} items across {total_batches} batches (batch_size={batch_size})")

    total = 0
    for batch_index, i in enumerate(range(0, total_items, batch_size), start=1):
        batch = plan.queue_items[i:i + batch_size]
        print(f"[RECRAWL] Queueing batch {batch_index}/{total_batches} (size={len(batch)})...")
        added = db.add_to_crawl_queue_batch(batch, force_pending=True)
        total += added

        processed = min(i + len(batch), total_items)
        now = time.monotonic()
        elapsed = max(now - start, 0.001)
        should_print_progress = (
            batch_index == 1
            or batch_index == total_batches
            or batch_index % 10 == 0
            or (now - last_status) >= 15
        )
        if should_print_progress:
            pct = (processed / total_items) * 100
            rate = processed / elapsed
            print(
                "[RECRAWL] Queue progress: "
                f"{processed}/{total_items} ({pct:.1f}%), "
                f"added={total}, rate={rate:.1f} items/sec, elapsed={elapsed:.1f}s"
            )
            last_status = now

    elapsed_total = time.monotonic() - start
    print(f"[RECRAWL] Queue write complete in {elapsed_total:.1f}s. Added rows: {total}")
    return total


async def maybe_run_crawler(plan: RecrawlPlan, args: argparse.Namespace) -> int:
    if not args.run:
        return 0

    print("[RECRAWL] Validating tokens before running crawler...")
    tokens_ok = await api_client.ensure_valid_tokens()
    if not tokens_ok:
        print("[RECRAWL] ERROR: token validation failed; queue prepared but crawl not started.")
        return 2

    runtime_max_depth = args.max_depth if args.max_depth is not None else max(1, plan.max_depth_seen)
    config = CrawlConfig(
        max_depth=runtime_max_depth,
        collect_stats=True,
        stats_matches_to_process=50,
    )

    crawler = GraphCrawler(api_client=api_client, config=config, graph_db=get_graph_db())
    print(f"[RECRAWL] Starting resume crawl (max_depth={runtime_max_depth})...")
    progress = await crawler.resume_crawl()

    print("[RECRAWL] Crawl finished")
    print(f"[RECRAWL] Nodes crawled: {progress.nodes_crawled}")
    print(f"[RECRAWL] Halo players found: {progress.halo_players_found}")
    print(f"[RECRAWL] Nodes with stats: {progress.nodes_with_stats}")
    print(f"[RECRAWL] Errors: {progress.errors}")
    return 0


async def main() -> int:
    args = parse_args()
    plan = build_recrawl_plan(args)
    halo_filter_mode = "halo_active=1 only" if not args.include_non_halo_active else "all previously crawled players"

    print("[RECRAWL] Plan summary")
    print(f"[RECRAWL] Filter mode: {halo_filter_mode}")
    print(f"[RECRAWL] Crawled players found: {plan.total_found}")
    print(f"[RECRAWL] Queue candidates: {plan.queued_candidates}")
    print(f"[RECRAWL] Skipped private: {plan.skipped_private}")
    print(f"[RECRAWL] Skipped null xuid: {plan.skipped_null_xuid}")
    print(f"[RECRAWL] Max depth in queue set: {plan.max_depth_seen}")

    if args.dry_run:
        print("[RECRAWL] Dry run complete. No queue rows were modified.")
        return 0

    queued = enqueue_plan(plan, args.batch_size)
    print(f"[RECRAWL] Re-queued players (force_pending=True): {queued}")

    return await maybe_run_crawler(plan, args)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))