"""
Graph Analysis Module for Halo Infinite Social Network
=======================================================

This module provides tools for building and analyzing the Halo Infinite
social graph, including:

- Graph database (SQLite-based)
- BFS crawler with Halo-active filtering
- Feature extraction for ML/KNN
- Graph analytics (hubs, communities, connected components)

Usage:
    from src.graph import GraphCrawler, get_graph_db, quick_crawl
    
    # Quick crawl from a seed player
    result = await quick_crawl(api_client, "SomeGamertag", max_depth=2)
    
    # Or with more control
    crawler = GraphCrawler(api_client, CrawlConfig(max_depth=3))
    progress = await crawler.crawl_from_seed(seed_gamertag="SomeGamertag")
    
    # Get graph statistics
    stats = get_graph_db().get_graph_stats()
"""

from src.graph.crawler import (
    GraphCrawler,
    CrawlConfig,
    CrawlProgress,
    CrawlStatus,
    quick_crawl,
    get_graph_summary,
    collect_coplay_data,
)

from src.database.graph_schema import (
    HaloSocialGraphDB,
    get_graph_db,
    GRAPH_DATABASE_FILE,
)

__all__ = [
    # Crawler
    'GraphCrawler',
    'CrawlConfig',
    'CrawlProgress',
    'CrawlStatus',
    'quick_crawl',
    'get_graph_summary',
    'collect_coplay_data',
    
    # Database
    'HaloSocialGraphDB',
    'get_graph_db',
    'GRAPH_DATABASE_FILE',
]
