"""Persistence for CSR pulled from skill.svc, and the batch safety around it.

Two measured API behaviours drive most of this file:

  - the playlist-csrs endpoint tops out at **32 players**. A 33rd is not an
    error; it is silently dropped from an HTTP 200, so an unchunked call would
    make 68 of 100 players look like they hold no rank.
  - season ids only work in the bare form (``CsrSeason13-3``). The
    ``Csr/Seasons/CsrSeason13-3.json`` form the service record reports is
    accepted and then ignored, which returns the CURRENT season's numbers
    labelled as a historic season - wrong data rather than no data.
"""

import asyncio
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlparse

import pytest

from src.api.client import HaloAPIClient
from src.database.schema import HaloStatsDBv2


# --------------------------------------------------------------------------
# schema / persistence
# --------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    database = HaloStatsDBv2(str(tmp_path / "stats.db"))
    database.insert_or_update_player("111", "Tester")
    database.insert_or_update_player("222", "Other")
    return database


def test_both_csr_tables_are_created(db):
    cur = db._get_connection().cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    assert {"player_playlist_csr", "player_csr_season"} <= tables


def test_playlist_csr_roundtrips(db):
    db.upsert_player_playlist_csr("111", "pl-1", 1714, "Onyx", 0, 1897)

    row = dict(db.get_player_csr("111")[0])
    assert row["current_csr"] == 1714
    assert row["current_tier"] == "Onyx"
    assert row["all_time_max"] == 1897
    assert row["last_updated"]


def test_playlist_csr_upsert_replaces_rather_than_duplicating(db):
    db.upsert_player_playlist_csr("111", "pl-1", 1400, "Diamond", 4, 1500)
    db.upsert_player_playlist_csr("111", "pl-1", 1714, "Onyx", 0, 1897)

    rows = db.get_player_csr("111")
    assert len(rows) == 1, "composite PK did not collapse the second write"
    assert dict(rows[0])["current_csr"] == 1714


def test_season_rows_roundtrip_and_are_ordered(db):
    for season, csr in (("CsrSeason13-3", 1714), ("CsrSeason12-1", 1707),
                        ("CsrSeason10-1", 1625)):
        db.upsert_player_csr_season("111", "pl-1", season, csr, "Onyx", 0, csr + 60)

    rows = [dict(r) for r in db.get_player_csr_seasons("111")]
    assert [r["season_id"] for r in rows] == sorted(r["season_id"] for r in rows)
    assert {r["csr"] for r in rows} == {1714, 1707, 1625}


def test_season_rows_can_be_filtered_to_one_playlist(db):
    db.upsert_player_csr_season("111", "pl-1", "CsrSeason13-3", 1714, "Onyx", 0, 1777)
    db.upsert_player_csr_season("111", "pl-2", "CsrSeason13-3", 1200, "Diamond", 1, 1250)

    rows = db.get_player_csr_seasons("111", playlist_asset_id="pl-2")

    assert len(rows) == 1
    assert dict(rows[0])["csr"] == 1200


def test_get_player_csr_returns_the_playlist_name_when_known(db):
    db.upsert_playlist_metadata("pl-1", "Ranked Slayer", True, "resolved")
    db.upsert_player_playlist_csr("111", "pl-1", 1714, "Onyx", 0, 1897)

    assert dict(db.get_player_csr("111")[0])["public_name"] == "Ranked Slayer"


def test_an_unresolved_playlist_still_returns_its_rank(db):
    # LEFT JOIN, not JOIN: a playlist whose metadata has not been resolved yet
    # must not make the player's rank vanish from the page.
    db.upsert_player_playlist_csr("111", "pl-unknown", 1500, "Onyx", 0, 1600)

    rows = db.get_player_csr("111")

    assert len(rows) == 1
    assert dict(rows[0])["public_name"] is None
    assert dict(rows[0])["current_csr"] == 1500


def test_csr_is_scoped_per_player(db):
    db.upsert_player_playlist_csr("111", "pl-1", 1714, "Onyx", 0, 1897)
    db.upsert_player_csr_season("222", "pl-1", "CsrSeason13-3", 900, "Gold", 2, 950)

    assert len(db.get_player_csr("222")) == 0
    assert len(db.get_player_csr_seasons("111")) == 0


# --------------------------------------------------------------------------
# season id normalisation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("Csr/Seasons/CsrSeason13-3.json", "CsrSeason13-3"),
    ("CsrSeason13-3", "CsrSeason13-3"),
    ("Csr/Seasons/CsrSeason5-1.JSON", "CsrSeason5-1"),
    (None, None),
    ("", None),
])
def test_season_ids_are_normalised_to_the_bare_form(raw, expected):
    assert HaloAPIClient._normalise_season_id(raw) == expected


# --------------------------------------------------------------------------
# batching against the 32-player ceiling
# --------------------------------------------------------------------------

class _FakeLimiter:
    async def wait_if_needed(self, force_account=None, bucket=None):
        return 0

    @asynccontextmanager
    async def slot(self, force_account=None, bucket=None):
        yield await self.wait_if_needed(force_account, bucket)

    def set_backoff(self, seconds, account_index=None):
        return None

    def note_result(self, bucket=None, rate_limited=False):
        return None


class _FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload
        self.headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return ""


def _install(monkeypatch, handler):
    from src.api import client as client_module

    requests = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            q = parse_qs(urlparse(url).query)
            requests.append(q)
            return handler(q)

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *a, **k: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *a, **k: object())
    return requests


def _client():
    c = HaloAPIClient()
    c.spartan_accounts = [{"id": "account1", "token": "tok", "name": "Account 1"}]
    return c


def _entry(xuid, csr, season_max=None, all_time=1897):
    return {"Id": f"xuid({xuid})", "Result": {
        "Current": {"Value": csr, "Tier": "Onyx" if csr > 0 else "", "SubTier": 0},
        "SeasonMax": {"Value": season_max if season_max is not None else csr},
        "AllTimeMax": {"Value": all_time},
    }}


def _truncating_handler(q):
    """Mimics the real endpoint: answers at most 32 players, silently."""
    players = q.get("players", [])[:HaloAPIClient.PLAYLIST_CSR_BATCH_MAX]
    return _FakeResponse(200, {"Value": [
        _entry(p[5:-1], 1500) for p in players
    ]})


def test_a_hundred_players_are_chunked_and_all_come_back(monkeypatch):
    # Unchunked this would return 32 of 100, and the other 68 would be
    # indistinguishable from players with no rank.
    requests = _install(monkeypatch, _truncating_handler)

    xuids = [str(i) for i in range(100)]
    result = asyncio.run(_client().get_playlist_csr("pl-1", xuids))

    assert [len(r["players"]) for r in requests] == [32, 32, 32, 4]
    assert len(result) == 100


def test_a_single_chunk_is_not_split(monkeypatch):
    requests = _install(monkeypatch, _truncating_handler)

    asyncio.run(_client().get_playlist_csr("pl-1", [str(i) for i in range(32)]))

    assert len(requests) == 1


def test_the_season_param_is_sent_in_bare_form(monkeypatch):
    requests = _install(monkeypatch, _truncating_handler)

    asyncio.run(_client().get_playlist_csr(
        "pl-1", ["1"], season_id="Csr/Seasons/CsrSeason13-3.json"))

    assert requests[0]["season"] == ["CsrSeason13-3"]


def test_unranked_players_are_omitted_by_default(monkeypatch):
    _install(monkeypatch, lambda q: _FakeResponse(200, {"Value": [
        _entry("1", 1500), _entry("2", -1, season_max=-1),
    ]}))

    result = asyncio.run(_client().get_playlist_csr("pl-1", ["1", "2"]))

    assert set(result) == {"1"}


def test_include_unranked_exposes_all_time_max_for_the_scoping_probe(monkeypatch):
    # This is what makes the backfill cheap: a player with no CSR this season
    # still reports the all-time peak, which says "has history here".
    _install(monkeypatch, lambda q: _FakeResponse(200, {"Value": [
        _entry("2", -1, season_max=-1, all_time=1897),
    ]}))

    result = asyncio.run(_client().get_playlist_csr(
        "pl-1", ["2"], include_unranked=True))

    assert result["2"]["csr"] is None
    assert result["2"]["season_max"] is None
    assert result["2"]["all_time_max"] == 1897


def test_a_never_ranked_player_reports_no_all_time_max(monkeypatch):
    # -1 must not survive as a number anywhere; it would render as a rank.
    _install(monkeypatch, lambda q: _FakeResponse(200, {"Value": [
        _entry("3", -1, season_max=-1, all_time=-1),
    ]}))

    result = asyncio.run(_client().get_playlist_csr(
        "pl-1", ["3"], include_unranked=True))

    assert result["3"]["all_time_max"] is None
    assert result["3"]["csr"] is None


def test_a_short_response_is_reported_not_silently_accepted(monkeypatch, capsys):
    # If the ceiling ever moves, the symptom must be a log line, not players
    # quietly losing their rank.
    _install(monkeypatch, lambda q: _FakeResponse(200, {"Value": [_entry("1", 1500)]}))

    result = asyncio.run(_client().get_playlist_csr("pl-1", ["1", "2", "3"]))

    assert "Truncated response" in capsys.readouterr().out
    assert set(result) == {"1"}
