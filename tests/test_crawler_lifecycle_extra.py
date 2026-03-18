import pytest

from src.graph.crawler import CrawlConfig, GraphCrawler


class _DummyDB:
    def get_queue_stats(self):
        return {"pending": 0, "in_progress": 0}


@pytest.mark.asyncio
async def test_progress_callback_errors_are_swallowed():
    async def bad_callback(progress):
        raise RuntimeError("callback failed")

    crawler = GraphCrawler(api_client=object(), config=CrawlConfig(progress_callback=bad_callback), graph_db=_DummyDB())
    await crawler._call_progress_callback()


def test_pause_and_stop_update_state_flags():
    crawler = GraphCrawler(api_client=object(), graph_db=_DummyDB())
    crawler._running = True
    crawler._paused = False

    crawler.pause()
    assert crawler._paused is True

    crawler.stop()
    assert crawler._running is False
    assert crawler._paused is False
