from concurrent.futures import ThreadPoolExecutor
import threading

from src.api.utils import safe_read_json, safe_write_json
from src.database.schema import HaloStatsDBv2


def test_safe_read_json_returns_default_for_corrupted_json(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{invalid json", encoding="utf-8")

    result = safe_read_json(str(bad), default={"fallback": True})

    assert result == {"fallback": True}


def test_safe_write_json_concurrent_independent_writes(tmp_path):
    targets = [tmp_path / f"shared-{i}.json" for i in range(20)]

    def writer(i):
        safe_write_json(str(targets[i]), {"value": i})

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(writer, range(20)))

    for i, target in enumerate(targets):
        data = safe_read_json(str(target), default={})
        assert data == {"value": i}
        assert not (tmp_path / f"shared-{i}.json.tmp").exists()


def test_sqlite_schema_handles_basic_concurrent_player_upserts(tmp_path):
    db_path = str(tmp_path / "concurrent.db")
    db = HaloStatsDBv2(db_path)

    def worker(i):
        local_db = HaloStatsDBv2(db_path)
        xuid = f"xuid-{i % 10}"
        local_db.insert_or_update_player(xuid, f"GT{i}")
        local_db.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = db.get_stats_summary()
    assert stats["total_players"] <= 10
    assert stats["total_players"] > 0

    db.close()
