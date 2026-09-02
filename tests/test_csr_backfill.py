"""The CSR backfill and its merge.

The properties worth defending are the ones a re-run depends on:

  - a completed chunk is never re-issued, and "completed" is durable because
    the progress marker is written in the same transaction as its rows
  - chunk_index only identifies the same work across runs if the player list
    is ordered, so the ordering is asserted rather than assumed
  - the merge is idempotent and refuses to insert rows that would violate the
    foreign key to players(xuid), which under PRAGMA foreign_keys=ON would
    abort the entire transaction
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from src.api.client import HaloAPIClient
from src.jobs import csr_backfill, csr_merge
from src.jobs.csr_backfill import (
    CURRENT_SEASON, _chunks, _completed_units, _open_output, _persist,
    _ranked_players, _ranked_playlists,
)
from src.database.schema import HaloStatsDBv2


@pytest.fixture
def live_db(tmp_path):
    """A miniature live DB: ranked players, a ranked playlist, a social one."""
    path = tmp_path / "stats.db"
    db = HaloStatsDBv2(str(path))
    for i in range(70):
        xuid = f"{2533274800000000 + i}"
        db.insert_or_update_player(xuid, f"Player{i}")
        conn = db._get_connection()
        conn.execute("""INSERT OR REPLACE INTO player_mode_stats
            (xuid, game_mode, games_played, last_updated)
            VALUES (?, 'ranked', 10, '2026-01-01')""", (xuid,))
    db.upsert_playlist_metadata("ranked-1", "Ranked Slayer", True, "resolved")
    db.upsert_playlist_metadata("social-1", "Quick Play", False, "resolved")
    db._get_connection().commit()
    return str(path)


def test_chunks_never_exceed_the_endpoint_ceiling():
    out = _chunks([str(i) for i in range(70)], HaloAPIClient.PLAYLIST_CSR_BATCH_MAX)
    assert [len(c) for c in out] == [32, 32, 6]
    assert all(len(c) <= 32 for c in out)


def test_player_list_is_ordered_so_chunk_index_is_stable(live_db):
    # chunk_index is the resume key. If the list order varied between runs,
    # resuming would skip work it had not actually done.
    first = _ranked_players(live_db, None)
    second = _ranked_players(live_db, None)
    assert first == second == sorted(first)
    assert len(first) == 70


def test_only_ranked_playlists_are_selected(live_db):
    ids = _ranked_playlists(live_db)
    assert "ranked-1" in ids
    assert "social-1" not in ids
    # The two hardcoded ids are always included; they never get a metadata row.
    assert set(HaloAPIClient.RANKED_PLAYLIST_IDS) <= set(ids)


def test_limit_players_caps_the_dry_run(live_db):
    assert len(_ranked_players(live_db, 32)) == 32


# --------------------------------------------------------------- persistence

def test_scope_pass_records_players_with_history(tmp_path):
    out = _open_output(str(tmp_path / "csr.db"))
    found = {
        "1": {"csr": 1714, "tier": "Onyx", "sub_tier": 0,
              "season_max": 1777, "all_time_max": 1897},
        # No rank now, but has been ranked here before - must be kept, because
        # this is exactly the pair the sweep needs to visit.
        "2": {"csr": None, "tier": None, "sub_tier": None,
              "season_max": None, "all_time_max": 1500},
        # Never ranked here at all - nothing to store.
        "3": {"csr": None, "tier": None, "sub_tier": None,
              "season_max": None, "all_time_max": None},
    }

    p_rows, s_rows = _persist(out, "pl-1", CURRENT_SEASON, 0, found)

    assert (p_rows, s_rows) == (2, 0)
    stored = {r["xuid"]: r for r in out.execute("SELECT * FROM player_playlist_csr")}
    assert set(stored) == {"1", "2"}
    assert stored["2"]["current_csr"] is None
    assert stored["2"]["all_time_max"] == 1500


def test_sweep_pass_stores_only_seasons_actually_played(tmp_path):
    out = _open_output(str(tmp_path / "csr.db"))
    found = {
        "1": {"csr": 1483, "tier": "Diamond", "sub_tier": 2,
              "season_max": 1488, "all_time_max": 1897},
        "2": {"csr": None, "tier": None, "sub_tier": None,
              "season_max": None, "all_time_max": 1500},
    }

    p_rows, s_rows = _persist(out, "pl-1", "CsrSeason8-1", 0, found)

    assert (p_rows, s_rows) == (0, 1)
    rows = out.execute("SELECT * FROM player_csr_season").fetchall()
    assert len(rows) == 1 and rows[0]["xuid"] == "1"
    assert rows[0]["season_id"] == "CsrSeason8-1"


def test_progress_is_written_with_the_rows_not_after(tmp_path):
    out = _open_output(str(tmp_path / "csr.db"))
    _persist(out, "pl-1", CURRENT_SEASON, 3,
             {"1": {"csr": 1500, "tier": "Onyx", "sub_tier": 0,
                    "season_max": 1500, "all_time_max": 1600}})

    assert ("pl-1", CURRENT_SEASON, 3) in _completed_units(out)
    # Re-opening proves it was committed, not merely buffered.
    reopened = sqlite3.connect(str(tmp_path / "csr.db"))
    reopened.row_factory = sqlite3.Row
    assert reopened.execute("SELECT COUNT(*) FROM csr_progress").fetchone()[0] == 1
    assert reopened.execute("SELECT COUNT(*) FROM player_playlist_csr").fetchone()[0] == 1


def test_persist_is_idempotent(tmp_path):
    out = _open_output(str(tmp_path / "csr.db"))
    found = {"1": {"csr": 1500, "tier": "Onyx", "sub_tier": 0,
                   "season_max": 1500, "all_time_max": 1600}}

    _persist(out, "pl-1", "CsrSeason13-3", 0, found)
    _persist(out, "pl-1", "CsrSeason13-3", 0, found)

    assert out.execute("SELECT COUNT(*) FROM player_csr_season").fetchone()[0] == 1
    assert out.execute("SELECT COUNT(*) FROM csr_progress").fetchone()[0] == 1


def test_completed_units_are_skipped_on_a_rerun(tmp_path, live_db, monkeypatch):
    out_path = str(tmp_path / "csr.db")
    calls = []

    async def fake_get_playlist_csr(self, playlist, players, season_id=None,
                                    include_unranked=False, strict=False):
        calls.append((playlist, season_id, len(players)))
        return {players[0]: {"csr": 1500, "tier": "Onyx", "sub_tier": 0,
                             "season_max": 1500, "all_time_max": 1600}}

    async def fake_discover(client, out, playlist_id, sample):
        return ["CsrSeason13-3"]

    monkeypatch.setattr(HaloAPIClient, "get_playlist_csr", fake_get_playlist_csr)
    monkeypatch.setattr(csr_backfill, "_discover_seasons", fake_discover)
    monkeypatch.setattr(csr_backfill, "_load_cached_spartan_accounts",
                        lambda: [{"id": "a1", "token": "t", "name": "A1"}])

    first = asyncio.run(csr_backfill.backfill_csr(
        db_path=live_db, out_path=out_path, playlists=["pl-1"]))
    assert first.requests > 0
    assert first.units_skipped == 0
    issued = len(calls)

    calls.clear()
    second = asyncio.run(csr_backfill.backfill_csr(
        db_path=live_db, out_path=out_path, playlists=["pl-1"]))

    assert calls == [], "re-run re-issued work it had already completed"
    assert second.units_skipped == issued
    assert second.requests == 0


# --------------------------------------------------------------------- merge

def _seed_backfill(path, xuids=("2533274800000000",)):
    out = _open_output(str(path))
    for x in xuids:
        _persist(out, "pl-1", CURRENT_SEASON, 0,
                 {x: {"csr": 1714, "tier": "Onyx", "sub_tier": 0,
                      "season_max": 1777, "all_time_max": 1897}})
        _persist(out, "pl-1", "CsrSeason8-1", 0,
                 {x: {"csr": 1483, "tier": "Diamond", "sub_tier": 2,
                      "season_max": 1488, "all_time_max": 1897}})
    out.close()


def test_merge_moves_rows_into_the_live_tables(tmp_path, live_db):
    src = tmp_path / "csr.db"
    _seed_backfill(src)

    r = csr_merge.merge_csr(db_path=live_db, in_path=str(src))

    assert r.playlist_csr_rows == 1
    assert r.season_rows == 1
    db = HaloStatsDBv2(live_db)
    assert dict(db.get_player_csr("2533274800000000")[0])["current_csr"] == 1714
    assert dict(db.get_player_csr_seasons("2533274800000000")[0])["csr"] == 1483


def test_merge_is_idempotent(tmp_path, live_db):
    src = tmp_path / "csr.db"
    _seed_backfill(src)

    csr_merge.merge_csr(db_path=live_db, in_path=str(src))
    csr_merge.merge_csr(db_path=live_db, in_path=str(src))

    db = HaloStatsDBv2(live_db)
    assert len(db.get_player_csr("2533274800000000")) == 1
    assert len(db.get_player_csr_seasons("2533274800000000")) == 1


def test_merge_skips_rows_with_no_matching_player(tmp_path, live_db):
    # foreign_keys=ON means one orphan would abort the whole transaction, so
    # they are excluded by the query rather than allowed to fail the merge.
    src = tmp_path / "csr.db"
    _seed_backfill(src, xuids=("2533274800000000", "9999999999999999"))

    r = csr_merge.merge_csr(db_path=live_db, in_path=str(src))

    assert r.orphans_skipped == 2   # one row in each table
    assert r.playlist_csr_rows == 1
    db = HaloStatsDBv2(live_db)
    assert len(db.get_player_csr("9999999999999999")) == 0


def test_merge_dry_run_writes_nothing(tmp_path, live_db):
    src = tmp_path / "csr.db"
    _seed_backfill(src)

    r = csr_merge.merge_csr(db_path=live_db, in_path=str(src), dry_run=True)

    assert r.dry_run is True
    db = HaloStatsDBv2(live_db)
    assert len(db.get_player_csr("2533274800000000")) == 0


def test_merge_refuses_a_missing_backfill_file(tmp_path, live_db):
    with pytest.raises(FileNotFoundError):
        csr_merge.merge_csr(db_path=live_db, in_path=str(tmp_path / "nope.db"))


def test_merge_names_the_short_circuited_ranked_playlists(tmp_path, live_db):
    # edfef3ac is Ranked Arena and the busiest ranked playlist, but the
    # classifier short-circuits it so it never gets a metadata row. Without a
    # name it would render as a raw GUID on the site.
    from src.jobs.csr_merge import HARDCODED_PLAYLIST_NAMES

    src = tmp_path / "csr.db"
    _seed_backfill(src)
    r = csr_merge.merge_csr(db_path=live_db, in_path=str(src))

    assert r.playlist_names_seeded == len(HARDCODED_PLAYLIST_NAMES)
    db = HaloStatsDBv2(live_db)
    for asset_id, name in HARDCODED_PLAYLIST_NAMES.items():
        assert dict(db.get_playlist_metadata(asset_id))["public_name"] == name


def test_merge_does_not_overwrite_a_resolved_playlist_name(tmp_path, live_db):
    # A name the resolver discovered for itself must win over the fallback.
    asset_id = next(iter(csr_merge.HARDCODED_PLAYLIST_NAMES))
    db = HaloStatsDBv2(live_db)
    db.upsert_playlist_metadata(asset_id, "Ranked Arena (2029 rework)", True, "resolved")

    src = tmp_path / "csr.db"
    _seed_backfill(src)
    csr_merge.merge_csr(db_path=live_db, in_path=str(src))

    assert dict(db.get_playlist_metadata(asset_id))["public_name"] == "Ranked Arena (2029 rework)"
