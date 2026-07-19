from src.database.cache import PlayerStatsCacheV2
from src.database.reclassify_pve_matches import reclassify_pve_matches
from src.database.player_mode_stats_backfill import backfill_player_mode_stats


FIREFIGHT_ID = "96aedf55-1c7e-46d5-bdaf-19a1329fb95d"
QUICKPLAY_ID = "bdceefb3-1c52-4848-a6b7-d49acd13109d"
RANKED_ID = "f7f30787-f607-436b-bdec-44c65bc2ecef"


def _match(match_id, playlist_id, is_ranked, match_category, kills=10, deaths=5, assists=4, outcome=2):
    return {
        "match_id": match_id,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "outcome": outcome,
        "duration": "PT10M",
        "start_time": "2026-01-01T00:00:00",
        "is_ranked": is_ranked,
        "match_category": match_category,
        "category_source": "test",
        "playlist_id": playlist_id,
        "map_id": "map",
        "map_version": "v1",
    }


def _seed(db, xuid="xuid-1"):
    db.insert_or_update_player(xuid, "TestPlayer", "2026-01-01T00:00:00")
    # Resolved playlist metadata: one Firefight (PvE), one social, one ranked.
    db.upsert_playlist_metadata(FIREFIGHT_ID, "Firefight: King of the Hill", False, "resolved")
    db.upsert_playlist_metadata(QUICKPLAY_ID, "Quick Play", False, "resolved")
    db.upsert_playlist_metadata(RANKED_ID, "Ranked Arena", True, "resolved")
    return xuid


def _category(db, match_id):
    cur = db._get_connection().cursor()
    cur.execute(
        "SELECT match_category, category_source, is_ranked FROM matches WHERE match_id = ?",
        (match_id,),
    )
    row = cur.fetchone()
    return (row["match_category"], row["category_source"], row["is_ranked"])


def test_reclassify_moves_firefight_to_custom_and_leaves_others(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = _seed(db)

    matches = [
        _match("ff1", FIREFIGHT_ID, False, "social", kills=150, deaths=1, assists=20),
        _match("qp1", QUICKPLAY_ID, False, "social", kills=10, deaths=8, assists=4),
        _match("rk1", RANKED_ID, True, "ranked", kills=12, deaths=10, assists=5),
        _match("cs1", None, False, "custom", kills=99, deaths=1, assists=0),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    result = reclassify_pve_matches(db_path=db_path)

    assert result.pve_playlists == 1
    assert result.matches_reclassified == 1
    assert result.affected_players == 1

    # Firefight -> custom/pve_firefight; everything else untouched.
    assert _category(db, "ff1") == ("custom", "pve_firefight", 0)
    assert _category(db, "qp1") == ("social", "test", 0)
    assert _category(db, "rk1") == ("ranked", "test", 1)
    assert _category(db, "cs1") == ("custom", "test", 0)


def test_reclassify_is_idempotent(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = _seed(db)
    m = _match("ff1", FIREFIGHT_ID, False, "social", kills=150, deaths=1, assists=20)
    db.insert_match(m)
    db.insert_player_match(xuid, m)

    first = reclassify_pve_matches(db_path=db_path)
    assert first.matches_reclassified == 1

    # Second run finds the row already 'custom' and changes nothing.
    second = reclassify_pve_matches(db_path=db_path)
    assert second.matches_reclassified == 0
    assert second.affected_players == 1  # counted by PvE playlist, not the update
    assert _category(db, "ff1") == ("custom", "pve_firefight", 0)


def test_dry_run_reports_without_writing(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = _seed(db)
    m = _match("ff1", FIREFIGHT_ID, False, "social", kills=150, deaths=1, assists=20)
    db.insert_match(m)
    db.insert_player_match(xuid, m)

    result = reclassify_pve_matches(db_path=db_path, dry_run=True)
    assert result.matches_reclassified == 1  # would reclassify
    # ...but nothing was written.
    assert _category(db, "ff1") == ("social", "test", 0)


def test_reclassify_then_backfill_drops_firefight_from_overall_and_social(tmp_path):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = _seed(db)

    matches = [
        _match("ff1", FIREFIGHT_ID, False, "social", kills=150, deaths=1, assists=20),
        _match("qp1", QUICKPLAY_ID, False, "social", kills=10, deaths=8, assists=4),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    # Before: incremental deltas counted the Firefight game in social + overall.
    before = cache.get_player_mode_summary(xuid, "overall")
    assert before["games_played"] == 2
    assert before["total_kills"] == 160

    reclassify_pve_matches(db_path=db_path)
    backfill_player_mode_stats(db_path=db_path)

    overall = cache.get_player_mode_summary(xuid, "overall")
    social = cache.get_player_mode_summary(xuid, "social")
    assert overall["games_played"] == 1
    assert overall["total_kills"] == 10
    assert social["games_played"] == 1
    assert social["total_kills"] == 10
