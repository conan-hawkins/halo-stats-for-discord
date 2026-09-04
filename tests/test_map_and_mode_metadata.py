"""Map and game-variant resolution: the two things a match display was missing.

A match has always stored a MapVariant GUID and nothing else, and stored nothing
at all about the mode. These cover the resolvers that turn those into names, and
the storage that lets a re-ingest FILL a column without ever blanking one.
"""
import pytest

from src.api.client import HaloAPIClient
from src.database.schema import HaloStatsDBv2


class _FakeResponse:
    def __init__(self, status, json_data=None, headers=None):
        self.status = status
        self.headers = headers or {}
        self._json_data = json_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._json_data


class _FakeSession:
    """Serves queued responses and records the URLs it was asked for."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, *args, **kwargs):
        self.urls.append(url)
        return self.responses.pop(0)


@pytest.fixture
def client(monkeypatch):
    c = HaloAPIClient()
    c.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)
    monkeypatch.setattr(c, "get_next_spartan_token", lambda idx=None: "tok")
    return c


# ---------------------------------------------------------------------------
# Picking the artwork out of a UGC asset
# ---------------------------------------------------------------------------


def test_thumbnail_is_preferred_over_a_screenshot():
    files = {
        "FileRelativePaths": [
            "images/screenshot1.jpg",
            "images/thumbnail.jpg",
            "map.mvar",
        ]
    }
    assert HaloAPIClient._pick_map_thumbnail(files) == "images/thumbnail.jpg"


def test_screenshot_is_the_fallback_when_there_is_no_thumbnail():
    files = {"FileRelativePaths": ["map.mvar", "images/screenshot1.jpg"]}
    assert HaloAPIClient._pick_map_thumbnail(files) == "images/screenshot1.jpg"


def test_any_image_beats_nothing():
    files = {"FileRelativePaths": ["map.mvar", "images/hero.png"]}
    assert HaloAPIClient._pick_map_thumbnail(files) == "images/hero.png"


def test_an_asset_with_no_usable_image_yields_none():
    assert HaloAPIClient._pick_map_thumbnail({"FileRelativePaths": ["map.mvar"]}) is None
    assert HaloAPIClient._pick_map_thumbnail({}) is None
    assert HaloAPIClient._pick_map_thumbnail(None) is None


# ---------------------------------------------------------------------------
# Resolving a map
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_map_builds_the_artwork_url_from_prefix_and_path(client):
    session = _FakeSession([
        _FakeResponse(200, {
            "PublicName": "Aquarius",
            "Files": {
                "Prefix": "https://blobs.example/ugcstorage/map/abc/v1/",
                "FileRelativePaths": ["images/thumbnail.jpg"],
            },
        })
    ])

    result = await client.resolve_map_metadata("abc", "v1", session)

    assert result["public_name"] == "Aquarius"
    assert result["resolution_status"] == "resolved"
    # The prefix is the only place the version appears, so the two must be
    # joined rather than the relative path used on its own.
    assert result["thumbnail_url"] == "https://blobs.example/ugcstorage/map/abc/v1/images/thumbnail.jpg"


@pytest.mark.asyncio
async def test_resolve_map_falls_back_to_the_unversioned_url(client):
    """A retired version 404s while the asset itself still resolves. That
    fallback is what keeps historical matches nameable at all."""
    session = _FakeSession([
        _FakeResponse(404),
        _FakeResponse(200, {"PublicName": "Streets", "Files": {}}),
    ])

    result = await client.resolve_map_metadata("abc", "old-version", session)

    assert result["public_name"] == "Streets"
    assert result["thumbnail_url"] is None
    assert "/versions/old-version" in session.urls[0]
    assert session.urls[1].endswith("/hi/maps/abc")


@pytest.mark.asyncio
async def test_resolve_map_404_on_both_urls_is_not_found(client):
    session = _FakeSession([_FakeResponse(404), _FakeResponse(404)])

    result = await client.resolve_map_metadata("gone", "v1", session)

    assert result["resolution_status"] == "not_found"
    assert result["public_name"] is None


# ---------------------------------------------------------------------------
# Resolving the mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_game_variant_returns_the_public_name(client):
    session = _FakeSession([_FakeResponse(200, {"PublicName": "Capture the Flag"})])

    result = await client.resolve_game_variant_metadata("variant-1", "v1", session)

    assert result == {"public_name": "Capture the Flag", "resolution_status": "resolved"}
    assert "ugcGameVariants" in session.urls[0]


@pytest.mark.asyncio
async def test_resolve_game_variant_error_yields_no_name(client):
    session = _FakeSession([_FakeResponse(500), _FakeResponse(500)])

    result = await client.resolve_game_variant_metadata("variant-1", "v1", session)

    assert result["public_name"] is None
    assert result["resolution_status"] == "error"


# ---------------------------------------------------------------------------
# The cached lookups - the reason ingest can afford to call these per match
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cached_map_costs_no_network(client, tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    db.upsert_map_metadata("abc", "Aquarius", "images/thumbnail.jpg", "resolved", "v1")
    client.stats_cache = type("_Cache", (), {"db": db})()

    # An empty session: any request at all would raise IndexError.
    name = await client._lookup_or_resolve_map("abc", "v1", _FakeSession([]))

    assert name == "Aquarius"


@pytest.mark.asyncio
async def test_a_map_cached_not_found_is_not_retried(client, tmp_path):
    """Confirmed-unresolvable assets must not be re-requested on every match
    that references them - that is the whole point of caching the negative."""
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    db.upsert_map_metadata("gone", None, None, "not_found", None)
    client.stats_cache = type("_Cache", (), {"db": db})()

    assert await client._lookup_or_resolve_map("gone", None, _FakeSession([])) is None


@pytest.mark.asyncio
async def test_cached_game_variant_costs_no_network(client, tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    db.upsert_game_variant_metadata("variant-1", "Oddball", "resolved", "v1")
    client.stats_cache = type("_Cache", (), {"db": db})()

    assert await client._lookup_or_resolve_game_variant("variant-1", "v1", _FakeSession([])) == "Oddball"


@pytest.mark.asyncio
async def test_no_asset_id_resolves_to_nothing_without_touching_the_db(client):
    assert await client._lookup_or_resolve_map(None, None, _FakeSession([])) is None
    assert await client._lookup_or_resolve_map("  ", None, _FakeSession([])) is None
    assert await client._lookup_or_resolve_game_variant(None, None, _FakeSession([])) is None


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def _match(match_id="m1", **overrides):
    data = {
        "match_id": match_id,
        "kills": 10,
        "deaths": 5,
        "assists": 2,
        "outcome": 2,
        "duration": "PT10M",
        "start_time": "2026-01-01T00:00:00",
        "is_ranked": True,
        "playlist_id": "playlist",
        "map_id": "map",
        "map_version": "v1",
        "medals": [],
    }
    data.update(overrides)
    return data


def test_insert_match_stores_the_variant_columns(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))
    db.insert_match(_match(
        game_variant_id="variant-1",
        game_variant_version="v1",
        game_variant_category=18,
        game_variant_name="Inline Name",
    ))

    row = db._get_connection().execute(
        "SELECT * FROM matches WHERE match_id = 'm1'"
    ).fetchone()

    assert row["game_variant_id"] == "variant-1"
    assert row["game_variant_version"] == "v1"
    assert row["game_variant_category"] == 18
    assert row["game_variant_name"] == "Inline Name"


def test_reingesting_fills_a_missing_variant_without_blanking_one(tmp_path):
    """This is what lets backfill_match_modes enrich the 64M rows written before
    the columns existed: replay a match through the normal path and the NULL
    fills in. The second half matters just as much - a later ingest that happens
    to carry nothing must not wipe what an earlier one established."""
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    # As written before any of this existed.
    db.insert_match(_match())
    assert db._get_connection().execute(
        "SELECT game_variant_id FROM matches WHERE match_id = 'm1'"
    ).fetchone()["game_variant_id"] is None

    # Replayed with the mode in hand: it fills.
    db.insert_match(_match(game_variant_id="variant-1", game_variant_category=18))
    row = db._get_connection().execute(
        "SELECT * FROM matches WHERE match_id = 'm1'"
    ).fetchone()
    assert row["game_variant_id"] == "variant-1"
    assert row["game_variant_category"] == 18

    # Replayed again with nothing: it must survive.
    db.insert_match(_match())
    row = db._get_connection().execute(
        "SELECT * FROM matches WHERE match_id = 'm1'"
    ).fetchone()
    assert row["game_variant_id"] == "variant-1"
    assert row["game_variant_category"] == 18


def test_map_and_variant_metadata_round_trip(tmp_path):
    db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    db.upsert_map_metadata("abc", "Aquarius", "images/thumbnail.jpg", "resolved", "v1")
    db.upsert_game_variant_metadata("variant-1", "Oddball", "resolved", "v1")

    assert db.get_map_metadata("abc")["public_name"] == "Aquarius"
    assert db.get_game_variant_metadata("variant-1")["public_name"] == "Oddball"
    assert db.get_map_metadata("never-seen") is None
    assert db.get_game_variant_metadata("never-seen") is None
