from src.config import CORE_RANKED_PLAYLIST_IDS
from src.database.cache import PlayerStatsCacheV2
from src.database.player_mode_stats_backfill import backfill_player_mode_stats

CORE_PLAYLIST_ID = sorted(CORE_RANKED_PLAYLIST_IDS)[0]

# Every game_mode bucket player_mode_stats can hold.
ALL_STAT_TYPES = ("overall", "ranked", "core_ranked", "rotational_ranked", "social")


def _match(match_id, ranked, start_time, kills=10, deaths=5, assists=2, outcome=2,
           playlist_id="playlist"):
    return {
        "match_id": match_id,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "outcome": outcome,
        "duration": "PT10M",
        "start_time": start_time,
        "is_ranked": ranked,
        "match_category": "ranked" if ranked else "social",
        "category_source": "test",
        "playlist_id": playlist_id,
        "map_id": "map",
        "map_version": "v1",
        "medals": [],
    }


def test_incremental_delta_matches_sql_aggregation(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-1"

    db.insert_or_update_player(xuid, "TestPlayer", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=2, outcome=2)
    m2 = _match("m2", ranked=False, start_time="2026-01-02T00:00:00", kills=4, deaths=8, assists=3, outcome=3)

    db.insert_match(m1)
    db.insert_match(m2)
    db.insert_player_match(xuid, m1)
    db.insert_player_match(xuid, m2)

    for stat_type in ("overall", "ranked", "social"):
        ground_truth = db.get_player_stats(xuid, stat_type)
        summary = cache.get_player_mode_summary(xuid, stat_type)
        assert summary is not None
        assert summary["games_played"] == ground_truth["games_played"]
        assert summary["total_kills"] == ground_truth["total_kills"]
        assert summary["total_deaths"] == ground_truth["total_deaths"]
        assert summary["total_assists"] == ground_truth["total_assists"]
        assert summary["wins"] == ground_truth["wins"]
        assert summary["losses"] == ground_truth["losses"]


def test_reprocessing_same_match_does_not_double_count(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-2"

    db.insert_or_update_player(xuid, "TestPlayer2", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=2, outcome=2)
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    # Reprocess the same match with revised stats (simulates a backfill/reprocess
    # run where the API returns corrected numbers for an already-seen match_id).
    m1_revised = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=15, deaths=3, assists=6, outcome=2)
    db.insert_match(m1_revised)
    db.insert_player_match(xuid, m1_revised)

    summary = cache.get_player_mode_summary(xuid, "ranked")
    ground_truth = db.get_player_stats(xuid, "ranked")

    assert summary["games_played"] == 1
    assert summary["total_kills"] == 15
    assert summary["total_deaths"] == 3
    assert summary["total_assists"] == 6
    assert summary["games_played"] == ground_truth["games_played"]
    assert summary["total_kills"] == ground_truth["total_kills"]


def test_reprocessing_with_changed_outcome_moves_bucket(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-3"

    db.insert_or_update_player(xuid, "TestPlayer3", "2026-01-01T00:00:00")

    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", outcome=3)  # loss
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    m1_revised = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", outcome=2)  # win
    db.insert_match(m1_revised)
    db.insert_player_match(xuid, m1_revised)

    summary = cache.get_player_mode_summary(xuid, "ranked")
    assert summary["wins"] == 1
    assert summary["losses"] == 0
    assert summary["games_played"] == 1


def test_backfill_recomputes_matching_incremental_state(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-4"

    db.insert_or_update_player(xuid, "TestPlayer4", "2026-01-01T00:00:00")
    matches = [
        _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=2, outcome=2),
        _match("m2", ranked=False, start_time="2026-01-02T00:00:00", kills=4, deaths=8, assists=3, outcome=3),
        _match("m3", ranked=False, start_time="2026-01-03T00:00:00", kills=7, deaths=7, assists=1, outcome=1),
        _match("m4", ranked=True, start_time="2026-01-04T00:00:00", kills=12, deaths=9, assists=4, outcome=3,
               playlist_id=CORE_PLAYLIST_ID),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    incremental = {
        stat_type: cache.get_player_mode_summary(xuid, stat_type)
        for stat_type in ALL_STAT_TYPES
    }

    # Wipe the summary table to simulate a fresh backfill against pre-existing
    # match history, then confirm the recompute matches the incremental state.
    conn = db._get_connection()
    conn.execute("DELETE FROM player_mode_stats")
    conn.commit()

    result = backfill_player_mode_stats(db_path)
    assert result.rows_written > 0

    for stat_type in ALL_STAT_TYPES:
        backfilled = cache.get_player_mode_summary(xuid, stat_type)
        assert backfilled == incremental[stat_type]


def test_core_and_rotational_ranked_buckets_split_by_playlist(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-split"

    db.insert_or_update_player(xuid, "SplitPlayer", "2026-01-01T00:00:00")
    matches = [
        # Mixed-case core playlist id: bucketing must normalize before matching.
        _match("core1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=3, outcome=2,
               playlist_id=CORE_PLAYLIST_ID.upper()),
        _match("rot1", ranked=True, start_time="2026-01-02T00:00:00", kills=7, deaths=6, assists=1, outcome=3,
               playlist_id="some-rotational-playlist"),
        _match("soc1", ranked=False, start_time="2026-01-03T00:00:00", kills=4, deaths=4, assists=2, outcome=2),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    core = cache.get_player_mode_summary(xuid, "core_ranked")
    rotational = cache.get_player_mode_summary(xuid, "rotational_ranked")
    ranked = cache.get_player_mode_summary(xuid, "ranked")

    assert core["games_played"] == 1
    assert core["total_kills"] == 10
    assert core["wins"] == 1
    assert rotational["games_played"] == 1
    assert rotational["total_kills"] == 7
    assert rotational["losses"] == 1
    # The split must partition ranked exactly.
    assert ranked["games_played"] == core["games_played"] + rotational["games_played"]
    assert ranked["total_kills"] == core["total_kills"] + rotational["total_kills"]
    # Social matches must not leak into either ranked sub-bucket.
    assert cache.get_player_mode_summary(xuid, "social")["games_played"] == 1


def test_reprocessing_core_match_does_not_double_count_new_buckets(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db
    xuid = "xuid-split-2"

    db.insert_or_update_player(xuid, "SplitPlayer2", "2026-01-01T00:00:00")
    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=2, outcome=2,
                playlist_id=CORE_PLAYLIST_ID)
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    m1_revised = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=15, deaths=3, assists=6, outcome=2,
                        playlist_id=CORE_PLAYLIST_ID)
    db.insert_match(m1_revised)
    db.insert_player_match(xuid, m1_revised)

    core = cache.get_player_mode_summary(xuid, "core_ranked")
    assert core["games_played"] == 1
    assert core["total_kills"] == 15
    assert cache.get_player_mode_summary(xuid, "rotational_ranked") is None


def test_null_playlist_ranked_match_lands_in_rotational(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-null-playlist"

    db.insert_or_update_player(xuid, "NullPlaylist", "2026-01-01T00:00:00")
    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00", playlist_id=None)
    db.insert_match(m1)
    db.insert_player_match(xuid, m1)

    assert cache.get_player_mode_summary(xuid, "rotational_ranked")["games_played"] == 1
    assert cache.get_player_mode_summary(xuid, "core_ranked") is None

    # The SQL backfill must agree (COALESCE routes NULL playlists to rotational).
    conn = db._get_connection()
    conn.execute("DELETE FROM player_mode_stats")
    conn.commit()
    backfill_player_mode_stats(db_path)
    assert cache.get_player_mode_summary(xuid, "rotational_ranked")["games_played"] == 1
    assert cache.get_player_mode_summary(xuid, "core_ranked") is None


def _assert_summary_consistent_with_recompute(cache, db_path, xuid):
    """The incrementally-maintained player_mode_stats must equal a full recompute
    from player_match. A mismatch means a partial/orphaned delta leaked past a
    failed match."""
    incremental = {
        st: cache.get_player_mode_summary(xuid, st) for st in ALL_STAT_TYPES
    }
    backfill_player_mode_stats(db_path)  # overwrites player_mode_stats from player_match
    recomputed = {
        st: cache.get_player_mode_summary(xuid, st) for st in ALL_STAT_TYPES
    }
    assert incremental == recomputed, f"summary desync: {incremental} != {recomputed}"


def _three_match_batch():
    return [
        _match("m1", ranked=True, start_time="2026-01-01T00:00:00", kills=10, deaths=5, assists=2, outcome=2),
        _match("bad", ranked=True, start_time="2026-01-02T00:00:00", kills=7, deaths=3, assists=1, outcome=2),
        _match("m3", ranked=True, start_time="2026-01-03T00:00:00", kills=4, deaths=8, assists=0, outcome=3),
    ]


def test_mid_batch_propagating_exception_isolated_to_one_match(tmp_path):
    """A match whose write raises must roll back ONLY itself; the rest of the
    batch is still saved (114-of-115, not 0-of-115), and the summary stays
    consistent with the persisted rows."""
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-mid-a"
    db.insert_or_update_player(xuid, "MidBatchA", "2026-01-01T00:00:00")

    orig = db.insert_player_match

    def raising(x, md, commit=True):
        if md.get("match_id") == "bad":
            raise RuntimeError("boom mid-insert")
        return orig(x, md, commit=commit)

    db.insert_player_match = raising
    try:
        ok = cache.save_player_stats(xuid, "overall", {"processed_matches": _three_match_batch()})
    finally:
        db.insert_player_match = orig

    assert ok is True
    conn = db._get_connection()
    saved = {r["match_id"] for r in conn.execute(
        "SELECT match_id FROM player_match WHERE xuid=?", (xuid,)).fetchall()}
    assert saved == {"m1", "m3"}  # only "bad" dropped
    # "bad"'s match row was rolled back with it (nothing partial left behind)
    assert conn.execute("SELECT COUNT(*) FROM matches WHERE match_id='bad'").fetchone()[0] == 0
    assert cache.get_player_mode_summary(xuid, "overall")["games_played"] == 2
    _assert_summary_consistent_with_recompute(cache, db_path, xuid)


def test_mid_batch_swallowed_failure_does_not_leak_partial_delta(tmp_path):
    """The dangerous case: a sub-method applies the summary delta then swallows a
    later failure (returns False). The per-match savepoint must undo that orphan
    delta so player_mode_stats does not silently over-count."""
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-mid-b"
    db.insert_or_update_player(xuid, "MidBatchB", "2026-01-01T00:00:00")

    orig = db.insert_player_match

    def swallow_after_delta(x, md, commit=True):
        if md.get("match_id") == "bad":
            # mirror the real method's ordering: delta applied, then the row
            # insert fails and is swallowed (returns False) -> pending orphan delta
            cur = db._get_connection().cursor()
            db._apply_player_mode_stats_delta(cur, x, md["match_id"], md)
            return False
        return orig(x, md, commit=commit)

    db.insert_player_match = swallow_after_delta
    try:
        ok = cache.save_player_stats(xuid, "overall", {"processed_matches": _three_match_batch()})
    finally:
        db.insert_player_match = orig

    assert ok is True
    conn = db._get_connection()
    saved = {r["match_id"] for r in conn.execute(
        "SELECT match_id FROM player_match WHERE xuid=?", (xuid,)).fetchall()}
    assert saved == {"m1", "m3"}
    # Orphan delta from "bad" must NOT have survived: 2 games, not 3.
    assert cache.get_player_mode_summary(xuid, "overall")["games_played"] == 2
    _assert_summary_consistent_with_recompute(cache, db_path, xuid)


def test_rolled_back_new_medal_column_is_not_left_stale_in_cache(tmp_path):
    """If a failed match added a new medal column via ALTER TABLE, the savepoint
    rollback removes the column - and the in-memory column cache must be dropped
    too, or a later legitimate use of that medal would INSERT into a missing
    column."""
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-mid-c"
    db.insert_or_update_player(xuid, "MidBatchC", "2026-01-01T00:00:00")

    new_medal = 787878787
    good = _match("g1", ranked=False, start_time="2026-01-01T00:00:00")

    orig = db.insert_player_match

    def alter_then_fail(x, md, commit=True):
        if md.get("match_id") == "bad":
            db.get_or_create_medal_set([{"NameId": new_medal, "Count": 1}], commit=False)  # ALTER
            raise RuntimeError("fail after ALTER")
        return orig(x, md, commit=commit)

    db.insert_player_match = alter_then_fail
    try:
        cache.save_player_stats(xuid, "overall", {"processed_matches": [
            good, _match("bad", ranked=False, start_time="2026-01-02T00:00:00")]})
    finally:
        db.insert_player_match = orig

    conn = db._get_connection()
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(medal_sets)").fetchall()}
    assert f"medal_{new_medal}" not in cols  # ALTER was rolled back

    # A later match legitimately using that medal must still save correctly
    # (fails if the cache wrongly still lists the rolled-back column).
    later = _match("g2", ranked=False, start_time="2026-01-03T00:00:00")
    later["medals"] = [{"NameId": new_medal, "Count": 2}]
    assert cache.save_player_stats(xuid, "overall", {"processed_matches": [later]}) is True

    cols2 = {r["name"] for r in conn.execute("PRAGMA table_info(medal_sets)").fetchall()}
    assert f"medal_{new_medal}" in cols2
    row = conn.execute(
        "SELECT medal_set_id FROM player_match WHERE xuid=? AND match_id='g2'", (xuid,)).fetchone()
    assert row is not None and row["medal_set_id"] is not None
