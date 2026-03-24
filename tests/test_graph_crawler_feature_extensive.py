from unittest.mock import AsyncMock
from datetime import datetime

import pytest

from src.graph.crawler import CrawlConfig, GraphCrawler


class _DB:
    def __init__(self):
        self.completed = []
        self.inserted_players = []
        self.inserted_edges = []
        self.queue_batch = []
        self.crawled = []
        self.features = []
        self.halo_features_by_xuid = {}

    def mark_queue_item_complete(self, xuid, error=None):
        self.completed.append((xuid, error))
        return True

    def insert_or_update_player(self, **kwargs):
        self.inserted_players.append(kwargs)
        return True

    def insert_friend_edges_batch(self, edges):
        self.inserted_edges.append(edges)
        return len(edges)

    def add_to_crawl_queue_batch(self, items, force_pending=False):
        self.queue_batch.append(items)
        return len(items)

    def mark_player_crawled(self, xuid):
        self.crawled.append(xuid)
        return True

    def get_player(self, xuid):
        return {"xuid": xuid, "gamertag": f"GT-{xuid}", "halo_active": 0, "last_crawled": None}

    def insert_or_update_halo_features(self, **kwargs):
        self.features.append(kwargs)
        return True

    def get_halo_features(self, xuid):
        return self.halo_features_by_xuid.get(xuid)


@pytest.mark.asyncio
async def test_process_node_depth_cutoff_exits_without_api_call():
    api = AsyncMock()
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=2))

    await crawler._process_node("x1", 2)

    assert db.completed == [("x1", None)]
    api.get_friends_list.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_node_non_private_error_increments_error_counter():
    api = AsyncMock()
    api.get_friends_list.return_value = {"error": "status_500", "is_private": False}
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=3))

    await crawler._process_node("x2", 0)

    assert crawler.progress.errors == 1
    assert db.completed == [("x2", "status_500")]


@pytest.mark.asyncio
async def test_process_node_samples_high_degree_and_keeps_mutual_first(monkeypatch):
    api = AsyncMock()
    friends = [
        {"xuid": "m1", "gamertag": "M1", "is_mutual": True},
        {"xuid": "m2", "gamertag": "M2", "is_mutual": True},
        {"xuid": "o1", "gamertag": "O1", "is_mutual": False},
        {"xuid": "o2", "gamertag": "O2", "is_mutual": False},
        {"xuid": "o3", "gamertag": "O3", "is_mutual": False},
    ]
    api.get_friends_list.return_value = {"error": None, "friends": friends}

    db = _DB()
    crawler = GraphCrawler(
        api_client=api,
        graph_db=db,
        config=CrawlConfig(max_depth=3, max_friends_per_node=3, sample_high_degree=True, collect_stats=False),
    )
    crawler._check_and_queue_halo_players = AsyncMock()

    # Keep shuffle deterministic.
    monkeypatch.setattr("random.shuffle", lambda values: None)

    await crawler._process_node("seed", 0)

    # 2 mutual + 1 other should be processed after sampling to max=3
    queued_args = crawler._check_and_queue_halo_players.await_args.args[0]
    queued_xuids = [x[0] for x in queued_args]
    assert queued_xuids == ["m1", "m2", "o1"]


@pytest.mark.asyncio
async def test_process_node_collect_stats_only_first_two_levels():
    api = AsyncMock()
    api.get_friends_list.return_value = {"error": None, "friends": []}

    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=5, collect_stats=True))
    crawler._check_and_queue_halo_players = AsyncMock()
    crawler._collect_player_stats = AsyncMock()

    await crawler._process_node("x3", 2)
    crawler._collect_player_stats.assert_not_awaited()

    await crawler._process_node("x4", 1)
    crawler._collect_player_stats.assert_awaited_once()


@pytest.mark.asyncio
async def test_check_and_queue_returns_early_above_max_depth():
    api = AsyncMock()
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=1))
    crawler._is_halo_active = AsyncMock(return_value=True)

    await crawler._check_and_queue_halo_players([("x1", "GT1", False)], depth=2)

    assert crawler._is_halo_active.await_count == 0
    assert db.queue_batch == []


@pytest.mark.asyncio
async def test_check_and_queue_tracks_known_and_new_players(monkeypatch):
    api = AsyncMock()
    db = _DB()

    def get_player_side_effect(xuid):
        if xuid == "known_active":
            return {"halo_active": 1, "last_crawled": None}
        if xuid == "known_inactive":
            return {"halo_active": 0, "last_crawled": "2026-01-01"}
        return None

    db.get_player = get_player_side_effect
    db.halo_features_by_xuid = {
        "known_active": {"last_match": "2026-01-05T00:00:00", "matches_played": 25}
    }
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=4))
    crawler._is_halo_active = AsyncMock(side_effect=[True, True, False])

    from src.api import xuid_cache as xuid_cache_module

    monkeypatch.setattr(xuid_cache_module, "load_xuid_cache", lambda: {})

    players = [
        ("known_active", "A", False),
        ("known_inactive", "B", False),
        ("unknown1", "C", False),
        ("unknown2", "D", False),
    ]

    await crawler._check_and_queue_halo_players(players, depth=1, discovered_from="seed")

    # known_active and unknown1 are active and queued
    assert db.queue_batch
    queued = db.queue_batch[0]
    assert ("known_active", 50, 1) in queued
    assert ("unknown1", 50, 1) in queued
    assert ("unknown2", 50, 1) not in queued


@pytest.mark.asyncio
async def test_is_halo_active_handles_api_exception():
    api = AsyncMock()
    api.check_recent_halo_activity.side_effect = RuntimeError("boom")
    crawler = GraphCrawler(api_client=api, graph_db=_DB())

    result = await crawler._is_halo_active("xuid-1", "GT")

    assert result is False


@pytest.mark.asyncio
async def test_is_halo_active_uses_configured_cutoff():
    api = AsyncMock()
    api.check_recent_halo_activity = AsyncMock(return_value=(True, datetime(2026, 1, 1)))
    cutoff = datetime(2026, 1, 1)
    crawler = GraphCrawler(api_client=api, graph_db=_DB(), config=CrawlConfig(halo_active_since=cutoff))

    result = await crawler._is_halo_active("xuid-1", "GT")

    assert result is True
    api.check_recent_halo_activity.assert_awaited_once_with("xuid-1", cutoff)


@pytest.mark.asyncio
async def test_collect_player_stats_ignores_error_and_empty_results():
    api = AsyncMock()
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db)

    api.calculate_comprehensive_stats.return_value = {"error": 4, "processed_matches": []}
    await crawler._collect_player_stats("xuid-1", "GT")
    assert db.features == []

    api.calculate_comprehensive_stats.return_value = {"error": 0, "processed_matches": [], "stats": {}}
    await crawler._collect_player_stats("xuid-1", "GT")
    assert db.features == []


@pytest.mark.asyncio
async def test_collect_player_stats_parses_dates_and_matches_week():
    api = AsyncMock()
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(stats_matches_to_process=50))

    api.calculate_comprehensive_stats.return_value = {
        "error": 0,
        "stats": {"estimated_csr": 1400, "csr_tier": "Platinum"},
        "processed_matches": [
            {
                "kills": 8,
                "deaths": 4,
                "assists": 2,
                "outcome": 2,
                "is_ranked": True,
                "start_time": "2026-01-10T10:00:00Z",
            },
            {
                "kills": 6,
                "deaths": 6,
                "assists": 3,
                "outcome": 3,
                "is_ranked": False,
                "start_time": "2026-01-01T10:00:00",
            },
            {
                "kills": 1,
                "deaths": 1,
                "assists": 0,
                "outcome": 2,
                "is_ranked": False,
                "start_time": "bad-date",
            },
        ],
    }

    await crawler._collect_player_stats("xuid-7", "GT7")

    assert db.features
    feature_row = db.features[0]
    assert feature_row["xuid"] == "xuid-7"
    assert feature_row["matches_played"] == 3
    assert feature_row["ranked_matches"] == 1
    assert feature_row["social_matches"] == 2
    assert feature_row["matches_week"] > 0
    assert feature_row["last_match"] is not None
    assert feature_row["first_match"] is not None
    assert crawler.progress.nodes_with_stats == 1


@pytest.mark.asyncio
async def test_process_node_unexpected_exception_marks_failed():
    api = AsyncMock()
    api.get_friends_list.side_effect = RuntimeError("network down")
    db = _DB()
    crawler = GraphCrawler(api_client=api, graph_db=db, config=CrawlConfig(max_depth=3))

    await crawler._process_node("xerr", 0)

    assert crawler.progress.errors == 1
    assert db.completed
    assert db.completed[0][0] == "xerr"
    assert "network down" in db.completed[0][1]
