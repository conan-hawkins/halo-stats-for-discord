import pytest

from src.api.client import HaloAPIClient
from src.database.cache import PlayerStatsCacheV2
from src.database import reclassify_playlists_backfill as backfill_module
from src.database.reclassify_playlists_backfill import backfill_playlist_reclassification


def _fake_spartan_accounts():
    return [{"id": "account-test", "token": "test-token", "name": "Account Test"}]


def _match(match_id, playlist_id, is_ranked, match_category, start_time):
    return {
        "match_id": match_id,
        "kills": 5,
        "deaths": 5,
        "assists": 5,
        "outcome": 2,
        "duration": "PT10M",
        "start_time": start_time,
        "is_ranked": is_ranked,
        "match_category": match_category,
        "category_source": "test",
        "playlist_id": playlist_id,
        "map_id": "map",
        "map_version": "v1",
    }


def test_playlist_metadata_get_upsert_roundtrip(tmp_path):
    cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = cache.db

    assert db.get_playlist_metadata("asset-1") is None

    db.upsert_playlist_metadata("asset-1", "Ranked Doubles", True, "resolved", version_id="v1")
    row = db.get_playlist_metadata("asset-1")
    assert row["public_name"] == "Ranked Doubles"
    assert row["is_ranked"] == 1
    assert row["resolution_status"] == "resolved"

    # INSERT OR REPLACE overwrites cleanly on re-resolution.
    db.upsert_playlist_metadata("asset-1", None, False, "not_found", version_id="v2")
    row = db.get_playlist_metadata("asset-1")
    assert row["resolution_status"] == "not_found"
    assert row["is_ranked"] == 0


@pytest.mark.asyncio
async def test_backfill_reclassifies_ranked_playlists_only(tmp_path, monkeypatch):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-1"

    db.insert_or_update_player(xuid, "TestPlayer", "2026-01-01T00:00:00")

    matches = [
        _match("m1", "known-ranked", True, "ranked", "2026-01-01T00:00:00"),
        _match("m2", "rotated-ranked", False, "social", "2026-01-02T00:00:00"),
        _match("m3", "quick-play", False, "social", "2026-01-03T00:00:00"),
        _match("m4", None, False, "custom", "2026-01-04T00:00:00"),
    ]
    for m in matches:
        db.insert_match(m)
        db.insert_player_match(xuid, m)

    # The real backfill resolves a playlist by fetching one live sample match
    # for it (get_match_stats_for_match) - the discovery endpoint requires a
    # version id that only a fresh match fetch can supply. Simulate that
    # side effect (populating playlist_metadata) keyed by which sample
    # match_id gets fetched, mirroring what
    # HaloAPIClient._lookup_or_resolve_playlist_ranked would do for real.
    names = {"m2": "Ranked Arena", "m3": "Quick Play"}

    async def fake_get_clearance_token(self):
        self.clearance_token = "skip"
        return True

    async def fake_get_match_stats_for_match(self, match_id, xuid, session):
        name = names.get(match_id)
        if name:
            cursor = db._get_connection().cursor()
            cursor.execute("SELECT playlist_id FROM matches WHERE match_id = ?", (match_id,))
            asset_id = cursor.fetchone()["playlist_id"]
            db.upsert_playlist_metadata(asset_id, name, "ranked" in name.lower(), "resolved")
        return None

    monkeypatch.setattr(backfill_module, "_load_cached_spartan_accounts", _fake_spartan_accounts)
    monkeypatch.setattr(HaloAPIClient, "get_clearance_token", fake_get_clearance_token)
    monkeypatch.setattr(HaloAPIClient, "get_match_stats_for_match", fake_get_match_stats_for_match)

    result = await backfill_playlist_reclassification(db_path)

    assert result.matches_reclassified_to_ranked == 1  # m2 only

    conn = db._get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT match_category, is_ranked FROM matches WHERE match_id = 'm2'")
    row = cursor.fetchone()
    assert (row["match_category"], row["is_ranked"]) == ("ranked", 1)

    cursor.execute("SELECT match_category FROM matches WHERE match_id = 'm3'")
    assert cursor.fetchone()["match_category"] == "social"

    # Custom matches must never be reclassified into ranked.
    cursor.execute("SELECT match_category FROM matches WHERE match_id = 'm4'")
    assert cursor.fetchone()["match_category"] == "custom"

    # Already-ranked match untouched.
    cursor.execute("SELECT match_category FROM matches WHERE match_id = 'm1'")
    assert cursor.fetchone()["match_category"] == "ranked"


@pytest.mark.asyncio
async def test_backfill_is_idempotent_and_skips_cached_playlists(tmp_path, monkeypatch):
    db_path = str(tmp_path / "stats.db")
    cache = PlayerStatsCacheV2(db_path)
    db = cache.db
    xuid = "xuid-2"

    db.insert_or_update_player(xuid, "TestPlayer2", "2026-01-01T00:00:00")
    m = _match("m1", "rotated-ranked", False, "social", "2026-01-01T00:00:00")
    db.insert_match(m)
    db.insert_player_match(xuid, m)

    call_count = {"n": 0}

    async def fake_get_clearance_token(self):
        self.clearance_token = "skip"
        return True

    async def fake_get_match_stats_for_match(self, match_id, xuid, session):
        call_count["n"] += 1
        cursor = db._get_connection().cursor()
        cursor.execute("SELECT playlist_id FROM matches WHERE match_id = ?", (match_id,))
        asset_id = cursor.fetchone()["playlist_id"]
        db.upsert_playlist_metadata(asset_id, "Ranked Arena", True, "resolved")
        return None

    monkeypatch.setattr(backfill_module, "_load_cached_spartan_accounts", _fake_spartan_accounts)
    monkeypatch.setattr(HaloAPIClient, "get_clearance_token", fake_get_clearance_token)
    monkeypatch.setattr(HaloAPIClient, "get_match_stats_for_match", fake_get_match_stats_for_match)

    first = await backfill_playlist_reclassification(db_path)
    assert first.matches_reclassified_to_ranked == 1
    assert call_count["n"] == 1

    second = await backfill_playlist_reclassification(db_path)
    assert second.playlists_checked == 0  # already cached - zero network calls
    assert call_count["n"] == 1
