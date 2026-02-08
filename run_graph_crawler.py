#!/usr/bin/env python3
"""
Graph Crawler Runner Script
============================

Command-line interface for running the Halo social graph crawler.

Usage:
    python run_graph_crawler.py --seed "GAMERTAG" --depth 3
    python run_graph_crawler.py --resume
    python run_graph_crawler.py --stats
    
Examples:
    # Start a new crawl from a seed player
    python run_graph_crawler.py --seed "YourGamertag" --depth 2
    
    # Resume an interrupted crawl
    python run_graph_crawler.py --resume
    
    # Show current graph statistics
    python run_graph_crawler.py --stats
    
    # Crawl with stats collection disabled (faster)
    python run_graph_crawler.py --seed "YourGamertag" --no-stats

Author: Graph Analysis Extension
"""

import argparse
import asyncio
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, '.')

from src.api.client import HaloAPIClient
from src.graph.crawler import GraphCrawler, CrawlConfig, quick_crawl
from src.database.graph_schema import get_graph_db


async def run_crawl(args):
    """Run the graph crawler"""
    
    # Initialize API client
    print("[RUNNER] Initializing API client...")
    api = HaloAPIClient()
    
    # Validate tokens
    if not await api.ensure_valid_tokens():
        print("[RUNNER] ERROR: Failed to authenticate. Run get_auth_tokens.py first.")
        return 1
    
    # Get clearance token
    if not await api.get_clearance_token():
        print("[RUNNER] WARNING: Could not get clearance token, some features may be limited")
    
    # Show current graph stats first
    db = get_graph_db()
    stats = db.get_graph_stats()
    print(f"\n[RUNNER] Current Graph Stats:")
    print(f"  Total players: {stats['total_players']}")
    print(f"  Halo active: {stats['halo_active_players']}")
    print(f"  Friend edges: {stats['total_friend_edges']}")
    print(f"  Players with stats: {stats['players_with_stats']}")
    print(f"  DB size: {stats.get('db_size_mb', 0):.2f} MB\n")
    
    if args.stats:
        # Just show stats and exit
        print("\n[RUNNER] Depth distribution:")
        for depth, count in sorted(stats.get('depth_distribution', {}).items()):
            print(f"  Depth {depth}: {count} players")
        
        # Show hubs
        hubs = db.find_hubs(min_degree=20)
        if hubs:
            print(f"\n[RUNNER] Top hubs (20+ friends):")
            for hub in hubs[:10]:
                print(f"  {hub['gamertag'] or hub['xuid']}: {hub['friend_count']} friends")
        
        return 0
    
    # Configure crawl
    config = CrawlConfig(
        max_depth=args.depth,
        collect_stats=not args.no_stats,
        stats_matches_to_process=args.matches,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        halo_active_since=datetime(2025, 11, 1),
    )
    
    # Create crawler
    crawler = GraphCrawler(api, config, db)
    
    if args.resume:
        # Resume existing crawl
        print("[RUNNER] Resuming crawl...")
        progress = await crawler.resume_crawl()
    elif args.seed:
        # Start new crawl from seed
        print(f"[RUNNER] Starting crawl from seed: {args.seed}")
        print(f"[RUNNER] Max depth: {args.depth}")
        print(f"[RUNNER] Collect stats: {not args.no_stats}")
        progress = await crawler.crawl_from_seed(seed_gamertag=args.seed)
    else:
        print("[RUNNER] ERROR: Must specify --seed GAMERTAG or --resume")
        return 1
    
    # Final stats
    final_stats = db.get_graph_stats()
    print(f"\n[RUNNER] === CRAWL COMPLETE ===")
    print(f"  Nodes discovered: {progress.nodes_discovered}")
    print(f"  Edges discovered: {progress.edges_discovered}")
    print(f"  Halo players found: {progress.halo_players_found}")
    print(f"  Nodes crawled: {progress.nodes_crawled}")
    print(f"  Nodes with stats: {progress.nodes_with_stats}")
    print(f"  Private profiles: {progress.private_profiles}")
    print(f"  Errors: {progress.errors}")
    print(f"\n[RUNNER] Final Graph Stats:")
    print(f"  Total players: {final_stats['total_players']}")
    print(f"  Halo active: {final_stats['halo_active_players']}")
    print(f"  Friend edges: {final_stats['total_friend_edges']}")
    print(f"  Avg Halo friend degree: {final_stats['avg_halo_friend_degree']}")
    print(f"  DB size: {final_stats.get('db_size_mb', 0):.2f} MB")
    
    return 0


async def export_graph(args):
    """Export graph data to CSV for external analysis"""
    db = get_graph_db()
    conn = db._get_connection()
    cursor = conn.cursor()
    
    import csv
    from pathlib import Path
    
    output_dir = Path(args.output) if args.output else Path("data/graph_export")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[EXPORT] Exporting to {output_dir}")
    
    # Export players
    cursor.execute("""
        SELECT xuid, gamertag, halo_active, first_seen, last_seen, crawl_depth, friends_count
        FROM graph_players
    """)
    
    with open(output_dir / "players.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['xuid', 'gamertag', 'halo_active', 'first_seen', 'last_seen', 'crawl_depth', 'friends_count'])
        for row in cursor.fetchall():
            writer.writerow(row)
    print(f"[EXPORT] Exported players.csv")
    
    # Export friends (edges)
    cursor.execute("""
        SELECT src_xuid, dst_xuid, is_mutual, depth, created_at
        FROM graph_friends
    """)
    
    with open(output_dir / "friends.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['src_xuid', 'dst_xuid', 'is_mutual', 'depth', 'created_at'])
        for row in cursor.fetchall():
            writer.writerow(row)
    print(f"[EXPORT] Exported friends.csv")
    
    # Export halo features
    cursor.execute("""
        SELECT xuid, gamertag, csr, kd_ratio, win_rate, matches_played, 
               matches_week, ranked_ratio, last_match
        FROM halo_features
        WHERE matches_played > 0
    """)
    
    with open(output_dir / "halo_features.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['xuid', 'gamertag', 'csr', 'kd_ratio', 'win_rate', 'matches_played',
                        'matches_week', 'ranked_ratio', 'last_match'])
        for row in cursor.fetchall():
            writer.writerow(row)
    print(f"[EXPORT] Exported halo_features.csv")
    
    # Export coplay if exists
    cursor.execute("SELECT COUNT(*) FROM graph_coplay")
    if cursor.fetchone()[0] > 0:
        cursor.execute("""
            SELECT src_xuid, dst_xuid, matches_together, wins_together, 
                   last_played, total_minutes
            FROM graph_coplay
        """)
        
        with open(output_dir / "coplay.csv", 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['src_xuid', 'dst_xuid', 'matches_together', 'wins_together',
                            'last_played', 'total_minutes'])
            for row in cursor.fetchall():
                writer.writerow(row)
        print(f"[EXPORT] Exported coplay.csv")
    
    print(f"[EXPORT] Done! Files saved to {output_dir}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Halo Infinite Social Graph Crawler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_graph_crawler.py --seed "YourGamertag" --depth 2
  python run_graph_crawler.py --resume
  python run_graph_crawler.py --stats
  python run_graph_crawler.py --export --output data/my_export
        """
    )
    
    # Actions
    parser.add_argument('--seed', type=str, help='Starting gamertag for crawl')
    parser.add_argument('--resume', action='store_true', help='Resume previous crawl')
    parser.add_argument('--stats', action='store_true', help='Show graph statistics only')
    parser.add_argument('--export', action='store_true', help='Export graph to CSV')
    
    # Crawl options
    parser.add_argument('--depth', type=int, default=3, 
                        help='Maximum crawl depth (default: 3)')
    parser.add_argument('--no-stats', action='store_true',
                        help='Skip collecting Halo stats (faster crawl)')
    parser.add_argument('--matches', type=int, default=50,
                        help='Matches to analyze per player (default: 50)')
    parser.add_argument('--concurrency', type=int, default=3,
                        help='Concurrent requests (default: 3)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Queue batch size (default: 10)')
    
    # Export options
    parser.add_argument('--output', type=str, help='Export output directory')
    
    args = parser.parse_args()
    
    # Run appropriate action
    if args.export:
        return asyncio.run(export_graph(args))
    else:
        return asyncio.run(run_crawl(args))


if __name__ == '__main__':
    sys.exit(main())
