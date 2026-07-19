import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from src.api.client import HaloAPIClient
from src.config import CORE_RANKED_PLAYLIST_IDS


def test_get_next_spartan_token_round_robin_and_index_selection():
    client = HaloAPIClient()
    client.spartan_accounts = [
        {"token": "tok-1"},
        {"token": "tok-2"},
        {"token": "tok-3"},
    ]

    assert client.get_next_spartan_token(1) == "tok-2"
    assert client.get_next_spartan_token() == "tok-1"
    assert client.get_next_spartan_token() == "tok-2"
    assert client.get_next_spartan_token() == "tok-3"


def test_get_next_spartan_token_falls_back_to_single_token():
    client = HaloAPIClient()
    client.spartan_accounts = []
    client.spartan_token = "fallback"

    assert client.get_next_spartan_token() == "fallback"


def test_is_cache_fresh_true_false_and_invalid():
    client = HaloAPIClient()
    fresh = {"last_update": (datetime.now() - timedelta(minutes=3)).isoformat()}
    stale = {"last_update": (datetime.now() - timedelta(minutes=90)).isoformat()}
    invalid = {"last_update": "not-a-date"}

    assert client.is_cache_fresh(fresh, max_age_minutes=30) is True
    assert client.is_cache_fresh(stale, max_age_minutes=30) is False
    assert client.is_cache_fresh(invalid, max_age_minutes=30) is False
    assert client.is_cache_fresh(None, max_age_minutes=30) is False


def test_calculate_stats_from_matches_filters_stat_types():
    client = HaloAPIClient()
    matches = [
        {"kills": 10, "deaths": 5, "assists": 6, "outcome": 2, "is_ranked": True},
        {"kills": 4, "deaths": 8, "assists": 2, "outcome": 3, "is_ranked": False},
        {"kills": 2, "deaths": 2, "assists": 1, "outcome": 1, "is_ranked": False},
    ]

    overall = client._calculate_stats_from_matches(matches, "overall")
    ranked = client._calculate_stats_from_matches(matches, "ranked")
    social = client._calculate_stats_from_matches(matches, "social")

    assert overall["games_played"] == 3
    assert overall["wins"] == 1
    assert overall["losses"] == 1
    assert overall["ties"] == 1
    assert ranked["games_played"] == 1
    assert social["games_played"] == 2


def test_calculate_stats_from_matches_splits_core_and_rotational_ranked():
    client = HaloAPIClient()
    core_id = sorted(CORE_RANKED_PLAYLIST_IDS)[0]
    matches = [
        # Mixed-case playlist id: the filter must normalize before matching.
        {"kills": 10, "deaths": 5, "assists": 6, "outcome": 2, "is_ranked": True,
         "playlist_id": core_id.upper()},
        {"kills": 7, "deaths": 6, "assists": 1, "outcome": 3, "is_ranked": True,
         "playlist_id": "some-rotational-playlist"},
        {"kills": 3, "deaths": 9, "assists": 0, "outcome": 4, "is_ranked": True,
         "playlist_id": None},
        {"kills": 4, "deaths": 8, "assists": 2, "outcome": 3, "is_ranked": False,
         "playlist_id": core_id},
    ]

    ranked = client._calculate_stats_from_matches(matches, "ranked")
    core = client._calculate_stats_from_matches(matches, "core_ranked")
    rotational = client._calculate_stats_from_matches(matches, "rotational_ranked")

    assert core["games_played"] == 1
    assert core["total_kills"] == 10
    # Rotational = ranked outside core, including a missing playlist_id.
    assert rotational["games_played"] == 2
    assert rotational["total_kills"] == 10
    # Core/rotational must partition ranked; unranked matches on a core
    # playlist id never count.
    assert ranked["games_played"] == core["games_played"] + rotational["games_played"]


def test_calculate_stats_from_matches_excludes_custom_from_social_and_overall():
    client = HaloAPIClient()
    matches = [
        {"kills": 10, "deaths": 5, "assists": 6, "outcome": 2, "is_ranked": True, "match_category": "ranked"},
        {"kills": 4, "deaths": 8, "assists": 2, "outcome": 3, "is_ranked": False, "match_category": "social"},
        {"kills": 99, "deaths": 1, "assists": 0, "outcome": 2, "is_ranked": False, "match_category": "custom"},
    ]

    overall = client._calculate_stats_from_matches(matches, "overall")
    ranked = client._calculate_stats_from_matches(matches, "ranked")
    social = client._calculate_stats_from_matches(matches, "social")

    assert overall["games_played"] == 2
    assert overall["total_kills"] == 14
    assert ranked["games_played"] == 1
    assert social["games_played"] == 1
    assert social["total_kills"] == 4


def test_classify_match_category_metadata_is_ranked_overrides_default_social():
    client = HaloAPIClient()

    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="f7f30787-f607-436b-bdec-44c65bc2ecef",
        playlist_version_id="v1",
        playlist_info={},
        match_info={},
        metadata_is_ranked=True,
    )
    assert (category, is_ranked, source) == ("ranked", True, "playlist_metadata")


def test_classify_match_category_metadata_not_ranked_falls_back_to_default_social():
    client = HaloAPIClient()

    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="bdceefb3-1c52-4848-a6b7-d49acd13109d",
        playlist_version_id="v1",
        playlist_info={},
        match_info={},
        metadata_is_ranked=False,
    )
    assert (category, is_ranked, source) == ("social", False, "default_non_ranked")


def test_classify_match_category_metadata_none_falls_back_unchanged():
    client = HaloAPIClient()

    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="bdceefb3-1c52-4848-a6b7-d49acd13109d",
        playlist_version_id="v1",
        playlist_info={},
        match_info={},
        metadata_is_ranked=None,
    )
    assert (category, is_ranked, source) == ("social", False, "default_non_ranked")


def test_classify_match_category_metadata_is_pve_returns_custom():
    client = HaloAPIClient()

    # A Firefight playlist has a real playlist_id and isn't ranked, so it would
    # otherwise default to 'social'; metadata_is_pve must bucket it as custom.
    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="96aedf55-1c7e-46d5-bdaf-19a1329fb95d",
        playlist_version_id="v1",
        playlist_info={},
        match_info={},
        metadata_is_ranked=False,
        metadata_is_pve=True,
    )
    assert (category, is_ranked, source) == ("custom", False, "pve_firefight")


def test_classify_match_category_metadata_is_pve_false_stays_social():
    client = HaloAPIClient()

    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="bdceefb3-1c52-4848-a6b7-d49acd13109d",
        playlist_version_id="v1",
        playlist_info={},
        match_info={},
        metadata_is_ranked=False,
        metadata_is_pve=False,
    )
    assert (category, is_ranked, source) == ("social", False, "default_non_ranked")


def test_public_name_is_pve_detects_firefight_case_insensitively():
    assert HaloAPIClient._public_name_is_pve("Firefight: King of the Hill") is True
    assert HaloAPIClient._public_name_is_pve("FIREFIGHT CLASSIC") is True
    assert HaloAPIClient._public_name_is_pve("Firefight:Gruntpocalypse") is True
    assert HaloAPIClient._public_name_is_pve("Quick Play") is False
    assert HaloAPIClient._public_name_is_pve("Ranked Arena") is False
    assert HaloAPIClient._public_name_is_pve(None) is False
    assert HaloAPIClient._public_name_is_pve("") is False


def test_classify_custom_with_ranked_variant_name_is_not_ranked():
    client = HaloAPIClient()

    # A private lobby (no Playlist -> no playlist_id, LifecycleMode=1) running a
    # game variant *named* "Ranked Slayer". Must be classified custom via the
    # authoritative lifecycle signal, NOT ranked-by-name.
    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id=None,
        playlist_version_id=None,
        playlist_info={},
        match_info={
            "LifecycleMode": 1,
            "GameVariantCategory": 18,
            "GameVariant": {"Name": "Ranked Slayer"},
        },
    )
    assert (category, is_ranked, source) == ("custom", False, "matchinfo_lifecycle")


def test_classify_matchmade_ranked_by_name_still_detected():
    client = HaloAPIClient()

    # A real matchmade ranked playlist (has a Playlist -> playlist_id) whose ID
    # isn't in the static set and hasn't resolved via metadata, but whose name
    # contains "ranked". The playlist_id gate must still let the name heuristic
    # classify it ranked.
    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="11111111-2222-3333-4444-555555555555",
        playlist_version_id="v1",
        playlist_info={"Name": "Ranked Doubles"},
        match_info={"LifecycleMode": 3},
        metadata_is_ranked=None,
    )
    assert (category, is_ranked, source) == ("ranked", True, "text_heuristic")


def test_classify_unranked_playlist_name_is_not_ranked():
    client = HaloAPIClient()

    # Word-boundary guard: "Unranked ..." must not substring-match "ranked".
    category, is_ranked, source = client._classify_match_category(
        playlist_asset_id="11111111-2222-3333-4444-555555555555",
        playlist_version_id="v1",
        playlist_info={"Name": "Unranked Arena"},
        match_info={"LifecycleMode": 3},
        metadata_is_ranked=None,
    )
    assert (category, is_ranked, source) == ("social", False, "default_non_ranked")


def test_classify_ranked_name_word_boundary_still_matches_real_names():
    client = HaloAPIClient()

    # Event/rotational naming styles must still classify ranked.
    for name in ("RANKED 1V1 SHOWDOWN", "Squad Battle: Ranked"):
        category, is_ranked, source = client._classify_match_category(
            playlist_asset_id="11111111-2222-3333-4444-555555555555",
            playlist_version_id="v1",
            playlist_info={"Name": name},
            match_info={"LifecycleMode": 3},
            metadata_is_ranked=None,
        )
        assert (category, is_ranked, source) == ("ranked", True, "text_heuristic"), name


@pytest.mark.asyncio
async def test_resolve_playlist_metadata_ranked_name_detected(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"PublicName": "Ranked Arena", "AssetId": "f7f30787-f607-436b-bdec-44c65bc2ecef"}

    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse()

    result = await client.resolve_playlist_metadata(
        "f7f30787-f607-436b-bdec-44c65bc2ecef", "v1", _FakeSession()
    )

    assert result == {
        "public_name": "Ranked Arena",
        "is_ranked": True,
        "is_pve": False,
        "resolution_status": "resolved",
    }


@pytest.mark.asyncio
async def test_resolve_playlist_metadata_social_name_not_ranked(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"PublicName": "Quick Play"}

    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse()

    result = await client.resolve_playlist_metadata("bdceefb3-...", "v1", _FakeSession())

    assert result["is_ranked"] is False
    assert result["is_pve"] is False
    assert result["resolution_status"] == "resolved"


@pytest.mark.asyncio
async def test_resolve_playlist_metadata_firefight_name_detected_as_pve(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {"PublicName": "Firefight: King of the Hill"}

    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse()

    result = await client.resolve_playlist_metadata("96aedf55-...", "v1", _FakeSession())

    assert result["is_pve"] is True
    assert result["is_ranked"] is False
    assert result["resolution_status"] == "resolved"


@pytest.mark.asyncio
async def test_resolve_playlist_metadata_404_on_both_urls_returns_not_found(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)

    class _FakeResponse:
        status = 404

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    call_count = {"n": 0}

    class _FakeSession:
        def get(self, *args, **kwargs):
            call_count["n"] += 1
            return _FakeResponse()

    result = await client.resolve_playlist_metadata("aa41f6a9-...", "v1", _FakeSession())

    assert result == {"public_name": None, "is_ranked": False, "is_pve": False, "resolution_status": "not_found"}
    assert call_count["n"] == 2  # versioned URL, then unversioned fallback


@pytest.mark.asyncio
async def test_resolve_playlist_metadata_429_sets_backoff_and_reports_error(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "tok"
    from src.api import client as client_module

    async def _wait_if_needed(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.halo_stats_rate_limiter, "wait_if_needed", _wait_if_needed)

    backoff_calls = []
    monkeypatch.setattr(
        client_module.halo_stats_rate_limiter,
        "set_backoff",
        lambda seconds, account_index=None: backoff_calls.append((seconds, account_index)),
    )

    class _FakeResponse:
        status = 429
        headers = {"Retry-After": "5"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse()

    result = await client.resolve_playlist_metadata("some-id", None, _FakeSession())

    assert result["resolution_status"] == "error"
    assert backoff_calls == [(5, 0)]


def test_parse_stats_success_and_error_payloads():
    client = HaloAPIClient()
    payload = {
        "error": 0,
        "stats": {
            "kd_ratio": 1.5,
            "win_rate": "55.0%",
            "avg_kda": 3.2,
            "total_deaths": 10,
            "total_kills": 15,
            "total_assists": 7,
            "games_played": 12,
        },
        "matches_processed": 12,
        "new_matches": 4,
    }

    parsed = client.parse_stats(payload, "overall", "PlayerOne")
    assert parsed["error"] == 0
    assert parsed["gamertag"] == "PlayerOne"
    assert parsed["stats_list"][0] == "1.5"
    assert parsed["stats_list"][6] == "12"

    failure = client.parse_stats({"error": 3, "message": "private"}, "overall", "PlayerOne")
    assert failure == {"error": 3, "message": "private"}


@pytest.mark.asyncio
async def test_get_player_stats_forwards_force_full_fetch(monkeypatch):
    client = HaloAPIClient()
    client.clearance_token = "cached-clearance"

    captured = {}

    async def _fake_resolve_gamertag_to_xuid(gamertag):
        assert gamertag == "PlayerOne"
        return "xuid-1"

    async def _fake_calculate_comprehensive_stats(
        xuid,
        stat_type,
        gamertag=None,
        matches_to_process=None,
        force_full_fetch=False,
        _retry_count=0,
    ):
        captured.update(
            {
                "xuid": xuid,
                "stat_type": stat_type,
                "gamertag": gamertag,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
            }
        )
        return {"error": 1, "message": "stop after forwarding check"}

    monkeypatch.setattr(client, "resolve_gamertag_to_xuid", _fake_resolve_gamertag_to_xuid)
    monkeypatch.setattr(client, "calculate_comprehensive_stats", _fake_calculate_comprehensive_stats)

    result = await client.get_player_stats(
        "PlayerOne",
        "overall",
        matches_to_process=None,
        force_full_fetch=True,
    )

    assert result["error"] == 1
    assert captured["xuid"] == "xuid-1"
    assert captured["stat_type"] == "overall"
    assert captured["gamertag"] == "PlayerOne"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is True


@pytest.mark.asyncio
async def test_statsfind_page_getter_forwards_force_full_fetch(monkeypatch):
    from src.api import client as client_module

    captured = {}

    async def _fake_get_player_stats(gamertag, stat_type, matches_to_process=10, force_full_fetch=False, xuid=None):
        captured.update(
            {
                "gamertag": gamertag,
                "stat_type": stat_type,
                "matches_to_process": matches_to_process,
                "force_full_fetch": force_full_fetch,
                "xuid": xuid,
            }
        )
        return {
            "error": 0,
            "stats_list": ["1.0", "50.0%", "2.0", "10", "10", "10", "5"],
            "gamertag": gamertag,
            "stat_type": stat_type,
        }

    monkeypatch.setattr(client_module, "api_client", SimpleNamespace(get_player_stats=_fake_get_player_stats))

    finder = client_module.StatsFind()
    result = await finder.page_getter(
        "PlayerOne",
        "stats",
        matches_to_process=None,
        force_full_fetch=True,
    )

    assert result.error_no == 0
    assert captured["gamertag"] == "PlayerOne"
    assert captured["stat_type"] == "overall"
    assert captured["matches_to_process"] is None
    assert captured["force_full_fetch"] is True


def test_get_cached_match_ids_handles_cache_exceptions(monkeypatch):
    client = HaloAPIClient()

    class BadCache:
        def get_cached_match_ids(self, *args, **kwargs):
            raise RuntimeError("boom")

    client.stats_cache = BadCache()
    assert client.get_cached_match_ids("xuid-1") == set()


@pytest.mark.asyncio
async def test_backfill_seed_match_participants_fetches_only_incomplete_verified_matches(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    class _FakeStatsDB:
        def __init__(self):
            self.coverage = {
                "m-1": {"participant_count": 2, "seed_present": True},
                "m-2": {"participant_count": 1, "seed_present": True},
                "m-3": {"participant_count": 0, "seed_present": False},
            }

        def get_seed_verified_match_ids(self, seed_xuid, limit_matches=None):
            all_ids = ["m-1", "m-2", "m-3"]
            if limit_matches:
                return all_ids[: int(limit_matches)]
            return all_ids

        def get_participant_coverage_for_matches(self, match_ids, seed_xuid):
            return {
                match_id: dict(self.coverage.get(match_id, {"participant_count": 0, "seed_present": False}))
                for match_id in match_ids
            }

        def insert_match(self, match_data):
            return True

        def insert_match_participants(self, match_id, participants):
            self.coverage[match_id] = {
                "participant_count": len(participants),
                "seed_present": any(str(p.get("xuid") or "").strip() == "seed-xuid" for p in participants),
            }
            return True

    fake_db = _FakeStatsDB()
    client.stats_cache = SimpleNamespace(
        db=fake_db,
        get_seed_verified_match_ids=fake_db.get_seed_verified_match_ids,
        get_participant_coverage_for_matches=fake_db.get_participant_coverage_for_matches,
    )

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "all_participants": [
                {"xuid": "seed-xuid", "team_id": "1", "outcome": 2},
                {"xuid": f"ally-{match_id}", "team_id": "1", "outcome": 2},
            ],
        }

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    result = await client.backfill_seed_match_participants("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["verified_matches"] == 3
    assert result["complete_matches_before"] == 1
    assert result["incomplete_matches_before"] == 2
    assert result["attempted_backfills"] == 2
    assert result["successful_backfills"] == 2
    assert result["failed_backfills"] == 0
    assert result["complete_matches_after"] == 3
    assert result["incomplete_matches_after"] == 0
    assert set(detail_calls) == {"m-2", "m-3"}


@pytest.mark.asyncio
async def test_backfill_seed_match_participants_skips_when_coverage_already_complete(monkeypatch):
    client = HaloAPIClient()

    class _FakeStatsDB:
        def get_seed_verified_match_ids(self, seed_xuid, limit_matches=None):
            return ["m-1", "m-2"]

        def get_participant_coverage_for_matches(self, match_ids, seed_xuid):
            return {
                "m-1": {"participant_count": 4, "seed_present": True},
                "m-2": {"participant_count": 8, "seed_present": True},
            }

    fake_db = _FakeStatsDB()
    client.stats_cache = SimpleNamespace(
        db=fake_db,
        get_seed_verified_match_ids=fake_db.get_seed_verified_match_ids,
        get_participant_coverage_for_matches=fake_db.get_participant_coverage_for_matches,
    )

    async def _unexpected_match_detail(match_id, player_xuid, session):
        raise AssertionError("match detail fetch should not run when coverage is complete")

    monkeypatch.setattr(client, "get_match_stats_for_match", _unexpected_match_detail)

    result = await client.backfill_seed_match_participants("seed-xuid", "SeedTag")

    assert result["ok"] is True
    assert result["verified_matches"] == 2
    assert result["complete_matches_before"] == 2
    assert result["incomplete_matches_before"] == 0
    assert result["attempted_backfills"] == 0
    assert result["successful_backfills"] == 0
    assert result["failed_backfills"] == 0
    assert result["complete_matches_after"] == 2
    assert result["incomplete_matches_after"] == 0


@pytest.mark.asyncio
async def test_get_clearance_token_missing_cache_file(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    monkeypatch.setattr(client_module.os.path, "exists", lambda path: False)
    assert await client.get_clearance_token() is False


@pytest.mark.asyncio
async def test_get_clearance_token_loads_valid_spartan(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    token_data = {
        "spartan": {"token": "spartan-token", "expires_at": 9999999999},
        "xsts_xbox": {"token": "xbox-token", "expires_at": 9999999999},
    }

    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)
    monkeypatch.setattr(client_module, "safe_read_json", lambda *args, **kwargs: token_data)
    monkeypatch.setattr(client_module, "is_token_valid", lambda info: True)

    ok = await client.get_clearance_token()
    assert ok is True
    assert client.spartan_token == "spartan-token"


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_bounded_fetch_stops_at_required_pages(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    requested_starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            requested_starts.append(start)

            if start >= 50:
                return _FakeResponse(200, {"Results": []})

            matches = [{"MatchId": f"m{start + i}"} for i in range(25)]
            return _FakeResponse(200, {"Results": matches})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(client, "load_cached_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-01T00:00:00",
            "csr": None,
            "csr_tier": None,
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=50,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 50
    assert sorted(requested_starts) == [0, 25]


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_cache_hit_with_spacing_normalization(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "Some   Player"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on normalized cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_gamertag_to_xuid(" some player ")
    assert result == "123"


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_cache_alias_hit_without_spaces(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "Some Player"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on alias cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_gamertag_to_xuid("SomePlayer")
    assert result == "123"


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_alias_ambiguity_falls_back_to_api(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(
        client_module,
        "load_xuid_cache",
        lambda: {
            "111": "Some Player",
            "222": "Some  Player",
        },
    )
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: False)

    acquire_calls = {"count": 0}
    release_calls = {"count": 0}

    async def _acquire(*args, **kwargs):
        acquire_calls["count"] += 1
        return 0

    def _release(*args, **kwargs):
        release_calls["count"] += 1

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", _release)

    result = await client.resolve_gamertag_to_xuid("SomePlayer")

    assert result is None
    assert acquire_calls["count"] == 1
    assert release_calls["count"] == 1


@pytest.mark.asyncio
async def test_resolve_gamertag_to_xuid_api_success_persists_canonical_gamertag(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    saved = {}
    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {})
    monkeypatch.setattr(client_module, "save_xuid_cache", lambda payload: saved.update(payload))
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)

    class _FakeTokenFile:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(client_module, "open", lambda *args, **kwargs: _FakeTokenFile(), raising=False)
    monkeypatch.setattr(
        client_module.json,
        "load",
        lambda _file: {"xsts_xbox": {"token": "token", "uhs": "uhs"}},
    )

    async def _acquire(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "profileUsers": [
                    {
                        "id": "123",
                        "settings": [
                            {"id": "Gamertag", "value": "Some Player"},
                        ],
                    }
                ]
            }

        async def text(self):
            return ""

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            return _FakeResponse()

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())

    result = await client.resolve_gamertag_to_xuid("SomePlayer")
    assert result == "123"
    assert saved == {"123": "Some Player"}


@pytest.mark.asyncio
async def test_resolve_xuid_to_gamertag_cache_hit(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {"123": "CachedTag"})

    async def _fail_acquire(*args, **kwargs):
        raise AssertionError("rate limiter should not be called on cache hit")

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _fail_acquire)

    result = await client.resolve_xuid_to_gamertag("123")
    assert result == "CachedTag"


@pytest.mark.asyncio
async def test_resolve_xuid_to_gamertag_api_success(monkeypatch):
    client = HaloAPIClient()
    from src.api import client as client_module

    saved = {}
    monkeypatch.setattr(client_module, "load_xuid_cache", lambda: {})
    monkeypatch.setattr(client_module, "save_xuid_cache", lambda payload: saved.update(payload))
    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)
    monkeypatch.setattr(
        client_module,
        "safe_read_json",
        lambda *args, **kwargs: {"xsts_xbox": {"token": "token", "uhs": "uhs"}},
    )

    async def _acquire(*args, **kwargs):
        return 0

    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "acquire", _acquire)
    monkeypatch.setattr(client_module.xbox_profile_rate_limiter, "release", lambda: None)

    class _FakeResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return {
                "profileUsers": [
                    {
                        "settings": [
                            {"id": "Gamertag", "value": "ResolvedTag"},
                        ]
                    }
                ]
            }

        async def text(self):
            return ""

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, *args, **kwargs):
            return _FakeResponse()

    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())

    result = await client.resolve_xuid_to_gamertag("123")
    assert result == "ResolvedTag"
    assert saved == {"123": "ResolvedTag"}


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_incremental_topup(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(200, {"TotalCount": 2, "Results": [{"MatchId": "new-1"}, {"MatchId": "old-1"}]})
            return _FakeResponse(200, {"TotalCount": 2, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {
            "processed_matches": [
                {
                    "match_id": "old-1",
                    "kills": 3,
                    "deaths": 2,
                    "assists": 1,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                }
            ]
        },
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["new_matches"] == 1
    assert result["stats"]["games_played"] == 2
    assert 0 in starts


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_without_total_hint_uses_incremental_boundary(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 3,
            "deaths": 2,
            "assists": 1,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 26)
    ]
    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                # Deliberately omit TotalCount metadata; completeness should be proven by boundary + probe.
                return _FakeResponse(200, {"Results": first_page_results})
            if start == 25:
                # Probe at one position beyond cached 25 finds no additional history.
                return _FakeResponse(200, {"Results": []})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 25
    # Incremental path fetches start=0 for page scan and first-page metadata probe.
    assert starts.count(0) >= 2
    # Probe should run exactly once at one position beyond cached coverage.
    assert starts.count(25) == 1


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_probe_detects_uncached_older_match(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 26)
    ]
    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(200, {"Results": first_page_results})
            if start == 25:
                # Boundary probe sees an uncached older match, forcing full list traversal.
                return _FakeResponse(200, {"Results": [{"MatchId": "old-26"}]})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-02-01T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 26
    # Probe + full traversal should touch start=25 at least twice.
    assert starts.count(25) >= 2
    # Fallback must still fetch details only for uncached matches.
    assert detail_calls == ["old-26"]


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_ignores_ambiguous_count_hint_and_probes(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    starts = []
    cached_matches = [
        {
            "match_id": f"old-{idx}",
            "kills": 1,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": f"2026-01-{((idx - 1) % 28) + 1:02d}T00:00:00",
        }
        for idx in range(1, 27)
    ]

    first_page_results = [{"MatchId": f"old-{idx}"} for idx in range(1, 26)]
    second_page_results = [{"MatchId": "old-26"}, {"MatchId": "old-27"}]

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                # Count is intentionally ambiguous (can be page-size), not a reliable lifetime total.
                return _FakeResponse(200, {"Count": 25, "Results": first_page_results})
            if start == 25:
                return _FakeResponse(200, {"Count": 2, "Results": second_page_results})
            if start == 26:
                # Boundary probe should detect older uncached history.
                return _FakeResponse(200, {"Count": 1, "Results": [{"MatchId": "old-27"}]})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {"processed_matches": cached_matches},
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-02-01T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 27
    # Probe position should be queried despite Count being present.
    assert 26 in starts
    # Full fallback should still fetch details only for uncached IDs.
    assert detail_calls == ["old-27"]


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_falls_back_when_gap_not_converged(monkeypatch):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            starts.append(start)
            if start == 0:
                return _FakeResponse(
                    200,
                    {
                        "TotalCount": 4,
                        "Results": [
                            {"MatchId": "old-1"},
                            {"MatchId": "old-2"},
                            {"MatchId": "new-1"},
                            {"MatchId": "new-2"},
                        ],
                    },
                )
            return _FakeResponse(200, {"TotalCount": 4, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    monkeypatch.setattr(
        client,
        "load_cached_stats",
        lambda *args, **kwargs: {
            "processed_matches": [
                {
                    "match_id": "old-1",
                    "kills": 1,
                    "deaths": 1,
                    "assists": 0,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                },
                {
                    "match_id": "old-2",
                    "kills": 1,
                    "deaths": 1,
                    "assists": 0,
                    "outcome": 2,
                    "is_ranked": False,
                    "start_time": "2026-01-01T00:00:00",
                },
            ]
        },
    )
    monkeypatch.setattr(client, "save_stats_cache", lambda *args, **kwargs: None)
    # Isolate from the real (global-singleton) stats_cache DB: without this,
    # calculate_comprehensive_stats' precomputed-summary lookup would query
    # whatever real database happens to be configured on this machine.
    monkeypatch.setattr(client.stats_cache, "get_player_mode_summary", lambda *args, **kwargs: None)

    detail_calls = []

    async def _fake_match_detail(match_id, player_xuid, session):
        detail_calls.append(match_id)
        return {
            "match_id": match_id,
            "kills": 2,
            "deaths": 1,
            "assists": 0,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-02T00:00:00",
            "csr": None,
            "csr_tier": None,
            "players": [player_xuid],
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)

    result = await client.calculate_comprehensive_stats(
        xuid="test-xuid",
        stat_type="overall",
        gamertag="Tester",
        matches_to_process=None,
        force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 4
    # start=0 is requested once for incremental check and again for full-fetch fallback.
    assert starts.count(0) >= 2
    assert set(detail_calls) == {"new-1", "new-2"}


def _seed_one_match_with_diverged_summary(client, tmp_path, xuid, match_id="m1", kills=5):
    """Seed a real (tmp_path-backed) stats_cache with one real match, then
    hand-diverge the precomputed player_mode_stats.total_kills so it disagrees
    with the raw match's kills. If a test then observes the diverged value, it
    proves the code read the precomputed summary rather than recomputing from
    the raw cached/fetched matches."""
    from src.database.cache import PlayerStatsCacheV2

    client.stats_cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = client.stats_cache.db
    db.insert_or_update_player(xuid, "PrecomputedPlayer", "2026-01-01T00:00:00")
    match = {
        "match_id": match_id, "kills": kills, "deaths": 1, "assists": 0, "outcome": 2,
        "duration": "PT1M", "start_time": "2026-01-01T00:00:00", "is_ranked": False,
        "match_category": "social", "category_source": "test", "playlist_id": "p",
        "map_id": "m", "map_version": "v1", "medals": [],
    }
    db.insert_match(match)
    db.insert_player_match(xuid, match)

    conn = db._get_connection()
    conn.execute(
        "UPDATE player_mode_stats SET total_kills = 999 WHERE xuid = ? AND game_mode = 'overall'",
        (xuid,),
    )
    conn.commit()
    return client.stats_cache


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_bounded_cache_sufficient_uses_precomputed_summary(tmp_path):
    client = HaloAPIClient()
    xuid = "xuid-precomputed-bounded"
    _seed_one_match_with_diverged_summary(client, tmp_path, xuid)

    result = await client.calculate_comprehensive_stats(
        xuid=xuid, stat_type="overall", gamertag="PrecomputedPlayer",
        matches_to_process=1, force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["new_matches"] == 0
    # Proves the precomputed player_mode_stats row was read, not a rescan of
    # the raw cached match (which would report kills=5, not 999).
    assert result["stats"]["total_kills"] == 999


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_bounded_cache_falls_back_without_summary(tmp_path):
    """Without a player_mode_stats row (e.g. pre-backfill legacy player), the
    bounded/cache-sufficient path must still work via the old Python rescan."""
    client = HaloAPIClient()
    xuid = "xuid-no-summary"
    from src.database.cache import PlayerStatsCacheV2

    client.stats_cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = client.stats_cache.db
    db.insert_or_update_player(xuid, "NoSummaryPlayer", "2026-01-01T00:00:00")
    match = {
        "match_id": "m1", "kills": 7, "deaths": 1, "assists": 0, "outcome": 2,
        "duration": "PT1M", "start_time": "2026-01-01T00:00:00", "is_ranked": False,
        "match_category": "social", "category_source": "test", "playlist_id": "p",
        "map_id": "m", "map_version": "v1", "medals": [],
    }
    db.insert_match(match)
    db.insert_player_match(xuid, match)
    conn = db._get_connection()
    conn.execute("DELETE FROM player_mode_stats WHERE xuid = ?", (xuid,))
    conn.commit()

    result = await client.calculate_comprehensive_stats(
        xuid=xuid, stat_type="overall", gamertag="NoSummaryPlayer",
        matches_to_process=1, force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["total_kills"] == 7


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_full_history_no_new_matches_uses_precomputed_summary(monkeypatch, tmp_path):
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"
    xuid = "xuid-precomputed-full"
    _seed_one_match_with_diverged_summary(client, tmp_path, xuid)

    from src.api import client as client_module

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            if start == 0:
                # Only match returned is the already-cached one: no new
                # matches, boundary hit immediately, TotalCount matches
                # cache exactly so completeness is provable without a probe.
                return _FakeResponse(200, {"TotalCount": 1, "Results": [{"MatchId": "m1"}]})
            return _FakeResponse(200, {"TotalCount": 1, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    result = await client.calculate_comprehensive_stats(
        xuid=xuid, stat_type="overall", gamertag="PrecomputedPlayer",
        matches_to_process=None, force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["new_matches"] == 0
    assert result["stats"]["total_kills"] == 999


@pytest.mark.asyncio
async def test_calculate_comprehensive_stats_new_matches_reads_summary_after_save(monkeypatch, tmp_path):
    """Once new matches are fetched and saved, the returned stats must come
    from the freshly-updated player_mode_stats summary (which insert_player_match
    just updated), including estimated_csr/csr_tier still being populated from
    the raw matches even though the numeric fields come from the DB."""
    client = HaloAPIClient()
    client.spartan_token = "spartan-token"
    xuid = "xuid-new-matches"

    from src.database.cache import PlayerStatsCacheV2
    from src.api import client as client_module

    client.stats_cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            if start == 0:
                return _FakeResponse(200, {"Results": [{"MatchId": "new-1"}]})
            return _FakeResponse(200, {"Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    async def _fake_match_detail(match_id, player_xuid, session):
        return {
            "match_id": match_id,
            "kills": 4,
            "deaths": 2,
            "assists": 1,
            "outcome": 2,
            "is_ranked": False,
            "start_time": "2026-01-05T00:00:00",
            "csr": 1500,
            "csr_tier": "Gold",
        }

    monkeypatch.setattr(client, "get_match_stats_for_match", _fake_match_detail)
    monkeypatch.setattr(
        client, "load_cached_stats", lambda *args, **kwargs: None,
    )

    result = await client.calculate_comprehensive_stats(
        xuid=xuid, stat_type="overall", gamertag="NewMatchesPlayer",
        matches_to_process=1, force_full_fetch=False,
    )

    assert result["error"] == 0
    assert result["stats"]["games_played"] == 1
    assert result["stats"]["total_kills"] == 4
    assert result["stats"]["estimated_csr"] == 1500
    assert result["stats"]["csr_tier"] == "Gold"

    # Confirm the real DB write actually happened (this is what makes the
    # summary read-back correct rather than accidentally passing).
    summary = client.stats_cache.get_player_mode_summary(xuid, "overall")
    assert summary is not None
    assert summary["total_kills"] == 4


@pytest.mark.asyncio
async def test_start_background_full_collect_dedupes_per_xuid(monkeypatch):
    client = HaloAPIClient()
    calls = []
    gate = asyncio.Event()

    async def fake_calculate_comprehensive_stats(xuid, stat_type, gamertag=None,
                                                   matches_to_process=None,
                                                   force_full_fetch=False, _retry_count=0):
        calls.append(xuid)
        await gate.wait()
        return {"error": 0, "matches_processed": 5}

    monkeypatch.setattr(client, "calculate_comprehensive_stats", fake_calculate_comprehensive_stats)

    completions = []

    async def on_complete(result):
        completions.append(result)

    client.start_background_full_collect("xuid-a", "GamerA", on_complete=on_complete)
    client.start_background_full_collect("xuid-a", "GamerA", on_complete=on_complete)  # dedup no-op
    client.start_background_full_collect("xuid-b", "GamerB", on_complete=on_complete)  # different player, concurrent

    await asyncio.sleep(0)
    assert calls.count("xuid-a") == 1
    assert calls.count("xuid-b") == 1

    gate.set()
    for _ in range(5):
        await asyncio.sleep(0)

    assert len(completions) == 2
    assert client._full_collect_tasks == {}


@pytest.mark.asyncio
async def test_incomplete_cache_forces_refetch_instead_of_bounded_short_circuit(monkeypatch, tmp_path):
    """A player whose cache is marked incomplete must not take the bounded
    'cache has enough matches' early return, even if it numerically has
    enough - previously incomplete_data/failed_match_count were always
    hardcoded False/0 on load, so this safety net could never engage."""
    from src.database.cache import PlayerStatsCacheV2
    from src.api import client as client_module

    client = HaloAPIClient()
    client.spartan_token = "spartan-token"
    xuid = "xuid-incomplete"

    client.stats_cache = PlayerStatsCacheV2(str(tmp_path / "stats.db"))
    db = client.stats_cache.db
    db.insert_or_update_player(xuid, "IncompletePlayer", "2026-01-01T00:00:00",
                                incomplete_data=True, failed_match_count=2)
    match = {
        "match_id": "m1", "kills": 1, "deaths": 1, "assists": 0, "outcome": 2,
        "duration": "PT1M", "start_time": "2026-01-01T00:00:00", "is_ranked": False,
        "match_category": "social", "category_source": "test", "playlist_id": "p",
        "map_id": "m", "map_version": "v1", "medals": [],
    }
    db.insert_match(match)
    db.insert_player_match(xuid, match)

    class _FakeRateLimiter:
        async def wait_if_needed(self, force_account=None):
            return 0

        def set_backoff(self, seconds, account_index=None):
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

    requested_starts = []

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, headers=None):
            start = int(url.split("start=")[1].split("&")[0])
            requested_starts.append(start)
            if start == 0:
                # Same single cached match: no new matches, but a request
                # must still have been made at all (proving the bounded
                # early-return was skipped because of the incomplete flag).
                return _FakeResponse(200, {"TotalCount": 1, "Results": [{"MatchId": "m1"}]})
            return _FakeResponse(200, {"TotalCount": 1, "Results": []})

    monkeypatch.setattr(client_module, "halo_stats_rate_limiter", _FakeRateLimiter())
    monkeypatch.setattr(client_module.aiohttp, "ClientSession", lambda *args, **kwargs: _FakeSession())
    monkeypatch.setattr(client_module.aiohttp, "TCPConnector", lambda *args, **kwargs: object())
    monkeypatch.setattr(client_module.aiohttp, "ClientTimeout", lambda *args, **kwargs: object())

    result = await client.calculate_comprehensive_stats(
        xuid=xuid, stat_type="overall", gamertag="IncompletePlayer",
        matches_to_process=1, force_full_fetch=False,
    )

    assert result["error"] == 0
    # The whole point: at least one HTTP request happened, proving the
    # bounded "cache has enough matches" short-circuit was NOT taken.
    assert len(requested_starts) > 0
