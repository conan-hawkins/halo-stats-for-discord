from src.database.cache import PlayerStatsCacheV2


def _stats_payload():
    return {
        "last_update": "2026-01-10T00:00:00",
        "processed_matches": [
            {
                "match_id": "m-ranked",
                "kills": 11,
                "deaths": 4,
                "assists": 6,
                "outcome": 2,
                "duration": "PT11M",
                "start_time": "2026-01-10T12:00:00",
                "is_ranked": True,
                "playlist_id": "ranked",
                "map_id": "map1",
                "map_version": "v1",
                "medals": [{"NameId": 622331684, "Count": 2}],
            },
            {
                "match_id": "m-social",
                "kills": 5,
                "deaths": 7,
                "assists": 8,
                "outcome": 3,
                "duration": "PT09M",
                "start_time": "2026-01-09T12:00:00",
                "is_ranked": False,
                "playlist_id": "social",
                "map_id": "map2",
                "map_version": "v1",
                "medals": [],
            },
            {
                "kills": 1,
                "deaths": 1,
                "assists": 1,
                "outcome": 1,
            },
        ],
    }


def test_save_and_load_player_stats_roundtrip(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))

    saved = cache.save_player_stats("xuid-1", "overall", _stats_payload(), "PlayerOne")
    loaded = cache.load_player_stats("xuid-1", "overall")

    assert saved is True
    assert loaded is not None
    assert loaded["xuid"] == "xuid-1"
    assert loaded["gamertag"] == "PlayerOne"
    assert len(loaded["processed_matches"]) == 2

    cache.close()


def test_custom_matches_excluded_from_player_mode_stats(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    payload = {
        "last_update": "2026-01-10T00:00:00",
        "processed_matches": [
            {
                "match_id": "m-ranked",
                "kills": 11, "deaths": 4, "assists": 6, "outcome": 2,
                "duration": "PT11M", "start_time": "2026-01-10T12:00:00",
                "is_ranked": True, "match_category": "ranked",
                "playlist_id": "ranked", "map_id": "map1", "map_version": "v1",
            },
            {
                "match_id": "m-social",
                "kills": 5, "deaths": 7, "assists": 8, "outcome": 3,
                "duration": "PT09M", "start_time": "2026-01-09T12:00:00",
                "is_ranked": False, "match_category": "social",
                "playlist_id": "social", "map_id": "map2", "map_version": "v1",
            },
            {
                "match_id": "m-custom",
                "kills": 99, "deaths": 0, "assists": 0, "outcome": 2,
                "duration": "PT05M", "start_time": "2026-01-08T12:00:00",
                "is_ranked": False, "match_category": "custom",
                "playlist_id": None, "map_id": "map3", "map_version": "v1",
            },
        ],
    }

    saved = cache.save_player_stats("xuid-custom", "overall", payload, "PlayerCustom")
    assert saved is True

    overall = cache.get_player_mode_summary("xuid-custom", "overall")
    social = cache.get_player_mode_summary("xuid-custom", "social")
    ranked = cache.get_player_mode_summary("xuid-custom", "ranked")

    # Custom match's 99 kills / 0 deaths must not appear anywhere.
    assert overall["games_played"] == 2
    assert overall["total_kills"] == 16
    assert social["games_played"] == 1
    assert social["total_kills"] == 5
    assert ranked["games_played"] == 1

    # matches/player_match still have all 3 rows - customs aren't deleted.
    conn = cache.db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM player_match WHERE xuid = ?", ("xuid-custom",))
    assert cursor.fetchone()["cnt"] == 3

    cache.close()


def test_incomplete_data_flag_persists_and_round_trips(tmp_path):
    """incomplete_data/failed_match_count must survive a save/load cycle
    (previously hardcoded to False/0 on load, discarding whatever the fetch
    actually found) so the history-sync completeness checks can act on it."""
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    payload = _stats_payload()
    payload["incomplete_data"] = True
    payload["failed_match_count"] = 3

    cache.save_player_stats("xuid-1", "overall", payload, "PlayerOne")
    loaded = cache.load_player_stats("xuid-1", "overall")

    assert loaded["incomplete_data"] is True
    assert loaded["failed_match_count"] == 3

    # A subsequent clean save must clear the flag, not leave it stuck True.
    clean_payload = _stats_payload()
    cache.save_player_stats("xuid-1", "overall", clean_payload, "PlayerOne")
    reloaded = cache.load_player_stats("xuid-1", "overall")

    assert reloaded["incomplete_data"] is False
    assert reloaded["failed_match_count"] == 0

    cache.close()


def test_cached_match_id_filters(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    cache.save_player_stats("xuid-1", "overall", _stats_payload(), "PlayerOne")

    all_ids = cache.get_cached_match_ids("xuid-1", "overall")
    ranked_ids = cache.get_cached_match_ids("xuid-1", "ranked")
    social_ids = cache.get_cached_match_ids("xuid-1", "social")

    assert all_ids == {"m-ranked", "m-social"}
    assert ranked_ids == {"m-ranked"}
    assert social_ids == {"m-social"}

    cache.close()


def test_check_player_cached_supports_gamertag_lookup(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    cache.save_player_stats("xuid-1", "overall", _stats_payload(), "PlayerOne")

    assert cache.check_player_cached("xuid-1") is True
    assert cache.check_player_cached("missing-xuid", gamertag="PlayerOne") is True
    assert cache.check_player_cached("missing-xuid", gamertag="MissingPlayer") is False

    cache.close()


def test_get_player_processed_matches_includes_medals_when_available(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    cache.save_player_stats("xuid-1", "overall", _stats_payload(), "PlayerOne")

    matches = cache.get_player_processed_matches("xuid-1")
    ranked = [m for m in matches if m["match_id"] == "m-ranked"][0]

    assert ranked["medals"]
    assert ranked["medals"][0]["NameId"] == 622331684

    cache.close()


def test_get_player_processed_matches_batches_medal_lookups(tmp_path):
    """Medals must be fetched with a handful of batched queries, not one
    per-match round trip, regardless of how many distinct medal sets exist."""
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))

    num_matches = 1200  # forces multiple chunks at the 500-id batch size
    payload = {
        "last_update": "2026-01-10T00:00:00",
        "processed_matches": [
            {
                "match_id": f"m-{i}",
                "kills": i,
                "deaths": 1,
                "assists": 1,
                "outcome": 2,
                "duration": "PT10M",
                "start_time": f"2026-01-10T00:{i % 60:02d}:00",
                "is_ranked": True,
                "playlist_id": "ranked",
                "map_id": "map1",
                "map_version": "v1",
                # Unique medal per match forces a distinct medal_set_id per match.
                "medals": [{"NameId": 900000 + i, "Count": 1}],
            }
            for i in range(num_matches)
        ],
    }

    cache.save_player_stats("xuid-1", "overall", payload, "PlayerOne")

    conn = cache.db._get_connection()
    medal_set_queries = 0

    def trace(sql):
        nonlocal medal_set_queries
        if "medal_sets" in sql and "SELECT" in sql.upper():
            medal_set_queries += 1

    conn.set_trace_callback(trace)
    try:
        matches = cache.get_player_processed_matches("xuid-1")
    finally:
        conn.set_trace_callback(None)

    assert len(matches) == num_matches
    by_id = {m["match_id"]: m for m in matches}
    for i in range(num_matches):
        medals = by_id[f"m-{i}"]["medals"]
        assert medals == [{"NameId": 900000 + i, "Count": 1, "TotalPersonalScoreAwarded": 0}]

    # 1200 distinct medal sets at a 500-id batch size should be ~3 SELECTs,
    # not 1200 (the old N+1 behavior this test guards against).
    assert medal_set_queries <= 5

    cache.close()


def test_save_player_stats_persists_match_participants_with_team_fields(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "cache.db"))
    payload = _stats_payload()
    payload["processed_matches"][0]["all_participants"] = [
        {
            "xuid": "xuid-1",
            "gamertag": "PlayerOne",
            "outcome": 2,
            "team_id": "1",
            "inferred_team_id": None,
            "kills": 11,
            "deaths": 4,
            "assists": 6,
            "csr": 1500,
            "csr_tier": "Platinum 2",
        },
        {
            "xuid": "xuid-2",
            "gamertag": "Teammate",
            "outcome": 2,
            "team_id": "1",
            "inferred_team_id": None,
            "kills": 8,
            "deaths": 7,
            "assists": 3,
            "csr": None,
            "csr_tier": None,
        },
        {
            "xuid": "xuid-3",
            "gamertag": "Opponent",
            "outcome": 3,
            "team_id": "2",
            "inferred_team_id": None,
            "kills": 9,
            "deaths": 8,
            "assists": 2,
            "csr": None,
            "csr_tier": None,
        },
    ]

    assert cache.save_player_stats("xuid-1", "overall", payload, "PlayerOne") is True

    participants = cache.db.get_match_participants("m-ranked")
    assert len(participants) == 3

    by_xuid = {row["xuid"]: row for row in participants}
    assert by_xuid["xuid-1"]["team_id"] == "1"
    assert by_xuid["xuid-2"]["team_id"] == "1"
    assert by_xuid["xuid-3"]["team_id"] == "2"

    cache.close()


def test_load_player_stats_index_only_skips_the_expensive_join(tmp_path):
    """include_matches=False must answer "which matches?" without reading them.

    The full load joins `matches` for ordering and match columns, which costs
    roughly one random disk seek per match on a cold page cache - measured at
    ~17ms/match, so 16.4s for a 696-match player. A freshness check only needs
    membership to find the boundary between new and known, and the id set
    answers that from one index: 0.03s against 12s for the same player.
    """
    from src.database.cache import PlayerStatsCacheV2

    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    cache.save_player_stats("xuid-1", "overall", {
        "last_update": "2026-01-01T00:00:00",
        "processed_matches": [
            {"match_id": "m1", "kills": 3, "deaths": 2, "assists": 1,
             "outcome": 2, "duration": "PT10M",
             "start_time": "2026-01-10T12:00:00", "is_ranked": False,
             "playlist_id": "p", "map_id": "map1", "map_version": "v1",
             "medals": []},
            {"match_id": "m2", "kills": 3, "deaths": 2, "assists": 1,
             "outcome": 2, "duration": "PT10M",
             "start_time": "2026-01-09T12:00:00", "is_ranked": False,
             "playlist_id": "p", "map_id": "map1", "map_version": "v1",
             "medals": []},
        ],
    }, gamertag="G")

    light = cache.load_player_stats("xuid-1", "overall", "G", include_matches=False)
    assert light is not None
    assert light["match_ids"] == {"m1", "m2"}
    assert light["matches_included"] is False
    # The expensive half is genuinely skipped, not just hidden.
    assert light["processed_matches"] == []
    # Metadata the caller still needs is present.
    assert light["last_update"] == "2026-01-01T00:00:00"
    assert light["incomplete_data"] is False

    full = cache.load_player_stats("xuid-1", "overall", "G")
    assert full["matches_included"] is True
    assert {m["match_id"] for m in full["processed_matches"]} == {"m1", "m2"}
    assert full["match_ids"] == {"m1", "m2"}


def test_index_only_load_reports_empty_cache_the_same_way(tmp_path):
    """An unknown player must be None on both paths, not an empty-but-present
    payload the caller would read as "cached with zero matches"."""
    from src.database.cache import PlayerStatsCacheV2

    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    assert cache.load_player_stats("nobody", "overall", "X", include_matches=False) is None
    assert cache.load_player_stats("nobody", "overall", "X") is None


def test_get_cached_match_id_set_does_not_need_the_matches_table(tmp_path):
    from src.database.cache import PlayerStatsCacheV2

    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    cache.save_player_stats("xuid-1", "overall", {
        "last_update": "2026-01-01T00:00:00",
        "processed_matches": [
            {"match_id": f"m{i}", "kills": 0, "deaths": 0, "assists": 0,
             "outcome": 2, "duration": "PT10M",
             "start_time": f"2026-01-{i + 1:02d}T12:00:00", "is_ranked": False,
             "playlist_id": "p", "map_id": "map1", "map_version": "v1",
             "medals": []}
            for i in range(5)
        ],
    }, gamertag="G")

    assert cache.get_cached_match_id_set("xuid-1") == {f"m{i}" for i in range(5)}
    assert cache.get_cached_match_id_set("nobody") == set()

    plan = [r[-1] for r in cache.db._get_connection().execute(
        "EXPLAIN QUERY PLAN SELECT match_id FROM player_match WHERE xuid = ?", ("xuid-1",)
    )]
    # One index lookup, no join - the join is what makes the full load seek.
    assert not any("matches" in step and "JOIN" in step.upper() for step in plan), plan
