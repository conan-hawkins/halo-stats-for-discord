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
