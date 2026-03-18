from unittest.mock import AsyncMock

import pytest

from src.graph.crawler import CrawlConfig, GraphCrawler


class DummyCursor:
    def __init__(self):
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))


class DummyConnection:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class DummyDB:
    def __init__(self):
        self.conn = DummyConnection()
        self.queue_stats = {"pending": 0, "in_progress": 0}
        self.next_queue_batches = []
        self.completed = []
        self.inserted_players = []
        self.inserted_edges_batches = []
        self.queue_batch_added = []
        self.halo_features = []
        self.crawled = []

    def _get_connection(self):
        return self.conn

    def get_queue_stats(self):
        return self.queue_stats

    def get_next_from_queue(self, batch_size=10):
        return self.next_queue_batches.pop(0) if self.next_queue_batches else []

    def mark_queue_item_complete(self, xuid, error=None):
        self.completed.append((xuid, error))
        return True

    def insert_or_update_player(self, **kwargs):
        self.inserted_players.append(kwargs)
        return True

    def add_to_crawl_queue(self, *args, **kwargs):
        self.queue_batch_added.append((args, kwargs))
        return True

    def add_to_crawl_queue_batch(self, items):
        self.queue_batch_added.append(items)
        return len(items)

    def insert_friend_edges_batch(self, edges):
        self.inserted_edges_batches.append(edges)
        return len(edges)

    def mark_player_crawled(self, xuid):
        self.crawled.append(xuid)
        return True

    def get_player(self, xuid):
        return {"xuid": xuid, "gamertag": f"GT-{xuid}", "halo_active": 0, "last_crawled": None}

    def insert_or_update_halo_features(self, **kwargs):
        self.halo_features.append(kwargs)
        return True


@pytest.mark.asyncio
async def test_crawl_from_seed_without_input_returns_immediately():
    api = AsyncMock()
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db)

    progress = await crawler.crawl_from_seed()

    assert progress.nodes_crawled == 0
    assert db.queue_batch_added == []


@pytest.mark.asyncio
async def test_crawl_from_seed_resolve_failure_returns_without_queueing():
    api = AsyncMock()
    api.resolve_gamertag_to_xuid.return_value = None
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db)

    progress = await crawler.crawl_from_seed(seed_gamertag="Missing")

    assert progress.nodes_crawled == 0
    assert db.queue_batch_added == []


@pytest.mark.asyncio
async def test_crawl_from_seed_success_sets_seed_and_runs_bfs():
    api = AsyncMock()
    api.resolve_gamertag_to_xuid.return_value = "xuid-1"
    api.check_recent_halo_activity.return_value = (True, "2026-01-01")
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db)
    crawler._bfs_crawl = AsyncMock()

    await crawler.crawl_from_seed(seed_gamertag="SeedGT")

    assert any(p.get("xuid") == "xuid-1" and p.get("is_seed") for p in db.inserted_players)
    assert db.queue_batch_added
    assert crawler._bfs_crawl.await_count == 1
    assert db.conn.commits >= 1


@pytest.mark.asyncio
async def test_resume_crawl_skips_when_nothing_pending():
    api = AsyncMock()
    db = DummyDB()
    db.queue_stats = {"pending": 0, "in_progress": 0}
    crawler = GraphCrawler(api_client=api, graph_db=db)
    crawler._bfs_crawl = AsyncMock()

    await crawler.resume_crawl()

    assert crawler._bfs_crawl.await_count == 0


@pytest.mark.asyncio
async def test_bfs_crawl_marks_over_depth_and_processes_valid_nodes():
    api = AsyncMock()
    db = DummyDB()
    db.next_queue_batches = [
        [{"xuid": "a", "depth": 1}, {"xuid": "b", "depth": 3}],
        [],
    ]
    config = CrawlConfig(max_depth=2, batch_size=10, save_interval=1)
    crawler = GraphCrawler(api_client=api, graph_db=db, config=config)
    crawler._running = True
    crawler._paused = False
    crawler._process_node = AsyncMock()
    crawler._log_progress = lambda: None

    await crawler._bfs_crawl()

    crawler._process_node.assert_awaited_once_with("a", 1)
    assert ("b", None) in db.completed


@pytest.mark.asyncio
async def test_process_node_handles_private_profile_error():
    api = AsyncMock()
    api.get_friends_list.return_value = {"error": "private", "is_private": True}
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=3))

    await crawler._process_node("x-private", 1)

    assert crawler.progress.private_profiles == 1
    assert ("x-private", "private") in db.completed


@pytest.mark.asyncio
async def test_process_node_happy_path_inserts_edges_and_queues_checks():
    api = AsyncMock()
    api.get_friends_list.return_value = {
        "error": 0,
        "friends": [
            {"xuid": "f1", "gamertag": "Friend1", "is_mutual": True},
            {"xuid": "f2", "gamertag": "Friend2", "is_mutual": False},
        ],
    }
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=3, collect_stats=False))
    crawler._check_and_queue_halo_players = AsyncMock()

    await crawler._process_node("seed", 0)

    assert crawler.progress.nodes_crawled == 1
    assert db.inserted_edges_batches
    assert any(item[0] == "seed" and item[1] == "f1" for item in db.inserted_edges_batches[0])
    assert any(item[0] == "f1" and item[1] == "seed" for item in db.inserted_edges_batches[0])
    crawler._check_and_queue_halo_players.assert_awaited_once()
    assert ("seed", None) in db.completed


@pytest.mark.asyncio
async def test_check_and_queue_halo_players_uses_known_status_and_api_check():
    api = AsyncMock()
    db = DummyDB()

    def get_player_side_effect(xuid):
        if xuid == "known-active":
            return {"halo_active": 1, "last_crawled": None}
        if xuid == "known-inactive":
            return {"halo_active": 0, "last_crawled": "2026-01-01"}
        return None

    db.get_player = get_player_side_effect
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=3))
    crawler._is_halo_active = AsyncMock(return_value=True)

    players = [
        ("known-active", "A", True),
        ("known-inactive", "B", False),
        ("unknown", "C", False),
    ]

    await crawler._check_and_queue_halo_players(players, depth=1, discovered_from="seed")

    # known-active and unknown should be queued
    flattened = []
    for entry in db.queue_batch_added:
        if isinstance(entry, list):
            flattened.extend(entry)
    assert ("known-active", 50, 1) in flattened
    assert ("unknown", 50, 1) in flattened
    assert crawler._is_halo_active.await_count == 1


@pytest.mark.asyncio
async def test_collect_player_stats_persists_halo_features():
    api = AsyncMock()
    api.calculate_comprehensive_stats.return_value = {
        "error": 0,
        "stats": {"estimated_csr": 1200, "csr_tier": "Gold"},
        "processed_matches": [
            {"kills": 10, "deaths": 5, "assists": 4, "outcome": 2, "is_ranked": True, "start_time": "2026-01-02T00:00:00"},
            {"kills": 6, "deaths": 7, "assists": 3, "outcome": 3, "is_ranked": False, "start_time": "2026-01-01T00:00:00"},
        ],
    }
    db = DummyDB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(stats_matches_to_process=25))

    await crawler._collect_player_stats("xuid-1", "PlayerOne")

    assert db.halo_features
    assert db.halo_features[0]["xuid"] == "xuid-1"
    assert db.halo_features[0]["matches_played"] == 2
    assert crawler.progress.nodes_with_stats == 1
