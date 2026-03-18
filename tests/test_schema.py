from src.database.schema import HaloStatsDBv2, get_medal_id, get_medal_name


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
        "playlist_id": "playlist",
        "map_id": "map",
        "map_version": "v1",
        "medals": medals or [],
    }


def test_insert_and_aggregate_player_stats(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    xuid = "xuid-1"

    db.insert_or_update_player(xuid, "TestPlayer", "2026-01-01T00:00:00")
    m1 = _match("m1", ranked=True, start_time="2026-01-01T00:00:00")
    m2 = _match("m2", ranked=False, start_time="2026-01-02T00:00:00")
    m2["kills"] = 4
    m2["deaths"] = 8
    m2["assists"] = 3
    m2["outcome"] = 3

    db.insert_match(m1)
    db.insert_match(m2)
    db.insert_player_match(xuid, m1)
    db.insert_player_match(xuid, m2)

    overall = db.get_player_stats(xuid, "overall")
    ranked = db.get_player_stats(xuid, "ranked")
    social = db.get_player_stats(xuid, "social")

    assert overall["games_played"] == 2
    assert overall["total_kills"] == 14
    assert overall["wins"] == 1
    assert overall["losses"] == 1
    assert ranked["games_played"] == 1
    assert social["games_played"] == 1

    db.close()


def test_medal_set_deduplicates_by_hash(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    medals = [{"NameId": 622331684, "Count": 2}, {"NameId": 2758320809, "Count": 1}]

    first = db.get_or_create_medal_set(medals)
    second = db.get_or_create_medal_set(list(reversed(medals)))

    assert first == second
    db.close()


def test_unknown_medal_creates_dynamic_column_and_total(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    xuid = "xuid-2"
    db.insert_or_update_player(xuid, "PlayerTwo")

    match = _match(
        "m-unknown",
        ranked=True,
        start_time="2026-01-03T00:00:00",
        medals=[{"NameId": 999999999, "Count": 3}],
    )

    db.insert_match(match)
    assert db.insert_player_match(xuid, match) is True

    totals = db.get_player_medal_totals(xuid)
    assert totals["Unknown (999999999)"] == 3

    db.close()


def test_get_player_matches_filters_by_stat_type(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    xuid = "xuid-3"
    db.insert_or_update_player(xuid, "PlayerThree")

    ranked = _match("ranked-1", ranked=True, start_time="2026-01-01T00:00:00")
    social = _match("social-1", ranked=False, start_time="2026-01-02T00:00:00")

    db.insert_match(ranked)
    db.insert_match(social)
    db.insert_player_match(xuid, ranked)
    db.insert_player_match(xuid, social)

    ranked_rows = db.get_player_matches(xuid, stat_type="ranked")
    social_rows = db.get_player_matches(xuid, stat_type="social")

    assert len(ranked_rows) == 1
    assert ranked_rows[0]["match_id"] == "ranked-1"
    assert len(social_rows) == 1
    assert social_rows[0]["match_id"] == "social-1"

    db.close()


def test_medal_lookup_helpers_cover_known_and_unknown_values():
    assert get_medal_name(622331684) == "Double Kill"
    assert get_medal_id("Double Kill") == 622331684
    assert get_medal_name(123456789) == "Unknown Medal (123456789)"
    assert get_medal_id("not-a-medal") is None
