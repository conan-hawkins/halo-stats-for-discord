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


def test_seed_verified_match_ids_and_participant_coverage(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    db.insert_or_update_player("seed", "SeedPlayer")
    db.insert_or_update_player("xuid-a", "Alpha")

    match_old = _match("cov-old", ranked=False, start_time="2026-01-01T00:00:00")
    match_mid = _match("cov-mid", ranked=False, start_time="2026-01-02T00:00:00")
    match_new = _match("cov-new", ranked=False, start_time="2026-01-03T00:00:00")

    for match in [match_old, match_mid, match_new]:
        db.insert_match(match)
        db.insert_player_match("seed", match)

    db.insert_match_participants(
        "cov-old",
        [
            {"xuid": "seed", "team_id": "1", "outcome": 2},
            {"xuid": "xuid-a", "team_id": "1", "outcome": 2},
        ],
    )

    # Participant row exists but seed row is missing.
    db.insert_match_participants(
        "cov-mid",
        [
            {"xuid": "xuid-a", "team_id": "1", "outcome": 2},
        ],
    )

    verified_ids = db.get_seed_verified_match_ids("seed")
    assert verified_ids == ["cov-new", "cov-mid", "cov-old"]

    limited_ids = db.get_seed_verified_match_ids("seed", limit_matches=2)
    assert limited_ids == ["cov-new", "cov-mid"]

    coverage = db.get_participant_coverage_for_matches(verified_ids, "seed")
    assert coverage["cov-old"]["participant_count"] == 2
    assert coverage["cov-old"]["seed_present"] is True
    assert coverage["cov-mid"]["participant_count"] == 1
    assert coverage["cov-mid"]["seed_present"] is False
    assert coverage["cov-new"]["participant_count"] == 0
    assert coverage["cov-new"]["seed_present"] is False

    db.close()


def test_get_pair_match_category_counts_by_scope(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    for xuid, gamertag in [("seed", "Seed"), ("xuid-a", "Alpha"), ("xuid-b", "Bravo"), ("xuid-c", "Charlie")]:
        db.insert_or_update_player(xuid, gamertag)

    match_rows = [
        ("pair-ranked-1", "2026-01-01T00:00:00", "ranked", ["seed", "xuid-a", "xuid-b"]),
        ("pair-ranked-2", "2026-01-02T00:00:00", "ranked", ["seed", "xuid-a"]),
        ("pair-social-1", "2026-01-03T00:00:00", "social", ["seed", "xuid-a", "xuid-b"]),
        ("pair-custom-1", "2026-01-04T00:00:00", "custom", ["seed", "xuid-b"]),
        ("pair-unknown-1", "2026-01-05T00:00:00", "unknown", ["seed", "xuid-a"]),
    ]

    for match_id, start_time, category, participants in match_rows:
        match = _match(match_id, ranked=(category == "ranked"), start_time=start_time)
        match["match_category"] = category
        match["category_source"] = "test"
        db.insert_match(match)
        db.insert_match_participants(
            match_id,
            [{"xuid": xuid, "team_id": "1", "outcome": 2} for xuid in participants],
        )

    counts = db.get_pair_match_category_counts(["seed", "xuid-a", "xuid-b", "xuid-c"])

    assert counts[("seed", "xuid-a")] == {
        "ranked": 2,
        "social": 2,
        "custom": 0,
        "unknown": 0,
    }
    assert counts[("seed", "xuid-b")] == {
        "ranked": 1,
        "social": 1,
        "custom": 1,
        "unknown": 0,
    }
    assert counts[("xuid-a", "xuid-b")] == {
        "ranked": 1,
        "social": 1,
        "custom": 0,
        "unknown": 0,
    }
    assert all("xuid-c" not in pair for pair in counts)

    db.close()


def test_get_pair_match_category_counts_infers_legacy_unknown_categories(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    for xuid, gamertag in [("seed", "Seed"), ("xuid-a", "Alpha"), ("xuid-b", "Bravo")]:
        db.insert_or_update_player(xuid, gamertag)

    legacy_rows = [
        ("legacy-ranked", "2026-01-01T00:00:00", "unknown", "6e4e9372-5d49-4f87-b0a7-4489b5e96a0b", ["seed", "xuid-a"]),
        ("legacy-social", "2026-01-02T00:00:00", "unknown", "playlist-social-legacy", ["seed", "xuid-b"]),
        ("legacy-custom-missing-playlist", "2026-01-03T00:00:00", "unknown", None, ["seed", "xuid-a"]),
        ("legacy-custom-token", "2026-01-04T00:00:00", "unknown", "custom-playlist-token", ["seed", "xuid-b"]),
    ]

    for match_id, start_time, category, playlist_id, participants in legacy_rows:
        match = _match(match_id, ranked=False, start_time=start_time)
        match["match_category"] = category
        match["category_source"] = None
        match["playlist_id"] = playlist_id
        db.insert_match(match)
        db.insert_match_participants(
            match_id,
            [{"xuid": xuid, "team_id": "1", "outcome": 2} for xuid in participants],
        )

    counts = db.get_pair_match_category_counts(["seed", "xuid-a", "xuid-b"])

    assert counts[("seed", "xuid-a")] == {
        "ranked": 1,
        "social": 0,
        "custom": 1,
        "unknown": 0,
    }
    assert counts[("seed", "xuid-b")] == {
        "ranked": 0,
        "social": 1,
        "custom": 1,
        "unknown": 0,
    }

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
