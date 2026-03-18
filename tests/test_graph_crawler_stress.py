import pytest

from src.graph.crawler import CrawlConfig, GraphCrawler


class _StressDB:
    def __init__(self, batches):
        self._batches = list(batches)
        self.completed = []
        self.inserted_players = []
        self.crawled = []

    def get_next_from_queue(self, batch_size=10):
        if self._batches:
            return self._batches.pop(0)
        return []

    def mark_queue_item_complete(self, xuid, error=None):
        self.completed.append((xuid, error))
        return True

    def insert_or_update_player(self, **kwargs):
        self.inserted_players.append(kwargs)
        return True

    def mark_player_crawled(self, xuid):
        self.crawled.append(xuid)
        return True

    def get_player(self, xuid):
        return {"xuid": xuid, "gamertag": f"GT-{xuid}", "halo_active": 0, "last_crawled": None}

    def insert_friend_edges_batch(self, edges):
        return len(edges)

    def add_to_crawl_queue_batch(self, items):
        return len(items)

    def get_queue_stats(self):
        return {"pending": 0, "in_progress": 0, "total": len(self.completed)}


@pytest.mark.asyncio
async def test_bfs_crawl_stress_handles_intermittent_failures(monkeypatch):
    # 120 nodes processed in 6 batches of 20.
    nodes = [{"xuid": f"n{i}", "depth": 1} for i in range(120)]
    batches = [nodes[i : i + 20] for i in range(0, 120, 20)] + [[]]
    db = _StressDB(batches)

    class _Api:
        async def get_friends_list(self, xuid):
            idx = int(xuid[1:])
            # Intermittent failure every 10th node.
            if idx % 10 == 0:
                return {"error": "status_500", "is_private": False}
            # Some nodes return friends; most return empty list.
            if idx % 7 == 0:
                return {
                    "error": None,
                    "friends": [
                        {"xuid": f"f{idx}a", "gamertag": f"F{idx}A", "is_mutual": True},
                        {"xuid": f"f{idx}b", "gamertag": f"F{idx}B", "is_mutual": False},
                    ],
                }
            return {"error": None, "friends": []}

    crawler = GraphCrawler(
        api_client=_Api(),
        graph_db=db,
        config=CrawlConfig(max_depth=3, batch_size=20, collect_stats=False, save_interval=1000),
    )

    # Keep test focused on BFS + node processing throughput.
    async def _noop_check(*args, **kwargs):
        return None

    crawler._check_and_queue_halo_players = _noop_check
    crawler._log_progress = lambda: None
    crawler._running = True
    crawler._paused = False

    await crawler._bfs_crawl()

    assert len(db.completed) == 120

    # 12 nodes fail (0,10,...,110), the rest should be crawled.
    expected_failures = 12
    actual_failures = sum(1 for _, err in db.completed if err == "status_500")
    assert actual_failures == expected_failures

    assert crawler.progress.errors == expected_failures
    assert crawler.progress.nodes_crawled == 120 - expected_failures
    assert len(db.crawled) == 120 - expected_failures
