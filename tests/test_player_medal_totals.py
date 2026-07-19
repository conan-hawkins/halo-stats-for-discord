from src.database.cache import PlayerStatsCacheV2
from src.database.player_medal_totals_backfill import backfill_player_medal_totals

DOUBLE_KILL = 622331684  # named in MEDAL_NAME_MAPPING
TRIPLE_KILL = 2063152177  # named in MEDAL_NAME_MAPPING
UNKNOWN_MEDAL = 999999999  # not in MEDAL_NAME_MAPPING


def _match(match_id, ranked, start_time, medals=None):
    return {
        "match_id": match_id,
        "kills": 10,
        "deaths": 5,
        "assists": 2,
        "outcome": 2,
        "duration": "PT10M",
        "start_time": start_time,
        "is_ranked": ranked,
        "match_category": "ranked" if ranked else "social",
        "category_source": "test",
        "playlist_id": "playlist",
        "map_id": "map",
        "map_version": "v1",
        "medals": medals or [],
    }


def _summary_by_name(cache, xuid, stat_type):
    summary = cache.get_player_medal_summary(xuid, stat_type)
    if summary is None:
        return {}
    return {entry["medal_name"]: entry["count"] for entry in summary}


def _ground_truth_by_name(db, xuid, stat_type):
    # get_player_medal_totals uses "Unknown (<id>)" for medals outside
    # MEDAL_NAME_MAPPING; get_player_medal_summary uses "Unknown Medal <id>" -
    # only compare medals that are in MEDAL_NAME_MAPPING to avoid a label
    # mismatch that isn't a real bug.
    return {
        name: count for name, count in db.get_player_medal_totals(xuid, stat_type).items()
        if not name.startswith("Unknown")
    }


def test_incremental_delta_matches_sql_aggregation(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-1"

    db.insert_or_update_player(xuid, "TestPlayer", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00",
                medals=[{"NameId": DOUBLE_KILL, "Count": 2}])
    m2 = _match("m2", ranked=False, start_time="2026-01-02T00:00:00",
                medals=[{"NameId": DOUBLE_KILL, "Count": 1}, {"NameId": TRIPLE_KILL, "Count": 3}])

    db.insert_match(m1)
    db.insert_match(m2)
    db.insert_player_match(xuid, m1)
    db.insert_player_match(xuid, m2)

    for stat_type in ("overall", "ranked", "social"):
        summary = _summary_by_name(cache, xuid, stat_type)
        ground_truth = _ground_truth_by_name(db, xuid, stat_type)
        assert summary == ground_truth


def test_reprocessing_same_match_does_not_double_count(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-2"

    db.insert_or_update_player(xuid, "TestPlayer2", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00",
                medals=[{"NameId": DOUBLE_KILL, "Count": 2}])
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)
    db.insert_player_match(xuid, m1)  # reprocess unchanged

    summary = _summary_by_name(cache, xuid, "ranked")
    assert summary == {"Double Kill": 2}


def test_reprocessing_with_changed_medal_set_diffs_correctly(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-3"

    db.insert_or_update_player(xuid, "TestPlayer3", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00",
                medals=[{"NameId": DOUBLE_KILL, "Count": 2}])
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    # Corrected re-fetch: same match now shows a Triple Kill instead.
    m1_revised = _match("m1", ranked=True, start_time="2026-01-01T00:00:00",
                        medals=[{"NameId": TRIPLE_KILL, "Count": 1}])
    db.insert_match(m1_revised)
    db.insert_player_match(xuid, m1_revised)

    summary = _summary_by_name(cache, xuid, "ranked")
    assert summary == {"Triple Kill": 1}
    ground_truth = _ground_truth_by_name(db, xuid, "ranked")
    assert summary == ground_truth


def test_unknown_medal_id_tracked_by_id_not_dropped(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-4"

    db.insert_or_update_player(xuid, "TestPlayer4", "2026-01-01T00:00:00")
    m1 = _match("m1", ranked=False, start_time="2026-01-01T00:00:00",
                medals=[{"NameId": UNKNOWN_MEDAL, "Count": 5}])
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    summary = cache.get_player_medal_summary(xuid, "social")
    assert summary == [{
        "medal_name_id": UNKNOWN_MEDAL,
        "medal_name": f"Unknown Medal {UNKNOWN_MEDAL}",
        "count": 5,
    }]


def test_no_medals_returns_none(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-5"

    db.insert_or_update_player(xuid, "TestPlayer5", "2026-01-01T00:00:00")
    m1 = _match("m1", ranked=False, start_time="2026-01-01T00:00:00", medals=[])
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    assert cache.get_player_medal_summary(xuid, "overall") is None


def test_custom_matches_excluded_from_medal_totals(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-custom"

    db.insert_or_update_player(xuid, "TestPlayerCustom", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=False, start_time="2026-01-01T00:00:00",
                medals=[{"NameId": DOUBLE_KILL, "Count": 2}])
    custom = _match("m2", ranked=False, start_time="2026-01-02T00:00:00",
                     medals=[{"NameId": TRIPLE_KILL, "Count": 5}])
    custom["match_category"] = "custom"
    custom["playlist_id"] = None

    db.insert_match(m1)
    db.insert_match(custom)
    db.insert_player_match(xuid, m1)
    db.insert_player_match(xuid, custom)

    for stat_type in ("overall", "social"):
        summary = _summary_by_name(cache, xuid, stat_type)
        assert summary == {"Double Kill": 2}, f"custom match leaked into {stat_type}"
        ground_truth = _ground_truth_by_name(db, xuid, stat_type)
        assert summary == ground_truth


def test_backfill_recomputes_matching_incremental_state(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-6"

    db.insert_or_update_player(xuid, "TestPlayer6", "2026-01-01T00:00:00")
    matches = [
        _match("m1", ranked=True, start_time="2026-01-01T00:00:00",
               medals=[{"NameId": DOUBLE_KILL, "Count": 2}]),
        _match("m2", ranked=False, start_time="2026-01-02T00:00:00",
               medals=[{"NameId": TRIPLE_KILL, "Count": 1}]),
        _match("m3", ranked=False, start_time="2026-01-03T00:00:00",
               medals=[{"NameId": DOUBLE_KILL, "Count": 3}]),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    incremental = {
        stat_type: _summary_by_name(cache, xuid, stat_type)
        for stat_type in ("overall", "ranked", "social")
    }

    conn = db._get_connection()
    conn.execute("DELETE FROM player_medal_totals")
    conn.commit()

    result = backfill_player_medal_totals(db_path)
    assert result.rows_written > 0

    for stat_type in ("overall", "ranked", "social"):
        backfilled = _summary_by_name(cache, xuid, stat_type)
        assert backfilled == incremental[stat_type]
