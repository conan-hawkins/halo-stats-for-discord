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


def test_match_participants_persist_and_scope_query(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    match = _match("mp-1", ranked=False, start_time="2026-01-04T00:00:00")
    db.insert_match(match)
    db.insert_or_update_player("xuid-a", "Alpha")
    db.insert_or_update_player("xuid-b", "Bravo")
    db.insert_or_update_player("xuid-c", "Charlie")

    inserted = db.insert_match_participants(
        "mp-1",
        [
            {
                "xuid": "xuid-a",
                "outcome": 2,
                "team_id": "1",
                "inferred_team_id": None,
                "kills": 10,
                "deaths": 4,
                "assists": 3,
            },
            {
                "xuid": "xuid-b",
                "outcome": 2,
                "team_id": "1",
                "inferred_team_id": None,
                "kills": 7,
                "deaths": 6,
                "assists": 2,
            },
            {
                "xuid": "xuid-c",
                "outcome": 3,
                "team_id": "2",
                "inferred_team_id": None,
                "kills": 4,
                "deaths": 9,
                "assists": 1,
            },
        ],
    )

    assert inserted is True

    participants = db.get_match_participants("mp-1")
    assert len(participants) == 3

    scope_rows = db.get_scope_match_participants(["xuid-a", "xuid-b"])
    assert "mp-1" in scope_rows
    assert len(scope_rows["mp-1"]) == 2
    assert {row["xuid"] for row in scope_rows["mp-1"]} == {"xuid-a", "xuid-b"}

    db.close()


def test_get_seed_match_participants_returns_full_rosters_with_limit(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    db.insert_or_update_player("seed", "SeedPlayer")
    db.insert_or_update_player("xuid-a", "Alpha")
    db.insert_or_update_player("xuid-b", "Bravo")
    db.insert_or_update_player("xuid-c", "Charlie")
    db.insert_or_update_player("xuid-d", "Delta")

    db.insert_match(_match("m-seed-old", ranked=False, start_time="2026-01-01T00:00:00"))
    db.insert_match(_match("m-seed-new", ranked=False, start_time="2026-01-02T00:00:00"))
    db.insert_match(_match("m-other", ranked=False, start_time="2026-01-03T00:00:00"))

    db.insert_match_participants(
        "m-seed-old",
        [
            {"xuid": "seed", "team_id": "1", "outcome": 2},
            {"xuid": "xuid-a", "team_id": "1", "outcome": 2},
            {"xuid": "xuid-b", "team_id": "2", "outcome": 3},
        ],
    )
    db.insert_match_participants(
        "m-seed-new",
        [
            {"xuid": "seed", "team_id": "2", "outcome": 3},
            {"xuid": "xuid-c", "team_id": "1", "outcome": 2},
        ],
    )
    db.insert_match_participants(
        "m-other",
        [
            {"xuid": "xuid-d", "team_id": "1", "outcome": 2},
            {"xuid": "xuid-a", "team_id": "2", "outcome": 3},
        ],
    )

    limited_rows = db.get_seed_match_participants("seed", limit_matches=1)
    assert list(limited_rows.keys()) == ["m-seed-new"]
    assert {row["xuid"] for row in limited_rows["m-seed-new"]} == {"seed", "xuid-c"}

    all_rows = db.get_seed_match_participants("seed")
    assert set(all_rows.keys()) == {"m-seed-old", "m-seed-new"}
    assert {row["xuid"] for row in all_rows["m-seed-old"]} == {"seed", "xuid-a", "xuid-b"}
    assert "m-other" not in all_rows

    db.close()


def test_insert_match_persists_category_fields(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    db.insert_match(
        {
            "match_id": "cat-1",
            "duration": "PT10M",
            "start_time": "2026-01-05T00:00:00",
            "is_ranked": False,
            "playlist_id": "playlist-social",
            "match_category": "social",
            "category_source": "default_non_ranked",
            "map_id": "map",
            "map_version": "v1",
        }
    )

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT match_category, category_source FROM matches WHERE match_id = ?",
        ("cat-1",),
    )
    row = cursor.fetchone()

    assert row is not None
    assert row["match_category"] == "social"
    assert row["category_source"] == "default_non_ranked"

    db.close()
