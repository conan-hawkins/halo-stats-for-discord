"""The history-driven harvest: who it targets, and what it asks about.

These defend the two facts that made the roster-driven harvest miss 25% of
everyone ranked in Arena:

  - the population is "holds a playlist CSR record but has NO season series",
    which is exactly what the site renders as "ranked here before, but not in
    any season we have data for"
  - the matches come from player_match, which we already hold for those
    players, rather than from match_participants, which covers 8,278 of
    6,515,918 launch-era matches
"""
from __future__ import annotations

import sqlite3

import pytest

from src.api.client import HaloAPIClient
from src.jobs.csr_reconstruct import (
    ARENA_LIVE,
    _affected_players,
    _history_matches,
    _recap_rows,
)

OTHER_ARENA = "f7f30787-f607-436b-bdec-44c65bc2ecef"
SOCIAL = "dc4929de-216c-43bc-b207-1702253f4576"


def _live_db(tmp_path, with_derived=True):
    """A cut-down live DB holding only what these two queries read."""
    path = tmp_path / "live.db"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE player_playlist_csr (xuid TEXT, playlist_asset_id TEXT,
            current_csr INTEGER, all_time_max INTEGER);
        CREATE TABLE player_csr_season (xuid TEXT, playlist_asset_id TEXT,
            season_id TEXT);
        CREATE TABLE player_match (xuid TEXT, match_id TEXT);
        CREATE TABLE matches (match_id TEXT PRIMARY KEY, playlist_id TEXT,
            start_time TEXT);
    """)
    if with_derived:
        db.execute("""CREATE TABLE player_csr_season_derived (xuid TEXT,
            playlist_asset_id TEXT, season_id TEXT)""")
    db.commit()
    db.close()
    return str(path)


def test_targets_players_with_a_csr_record_but_no_season_series(tmp_path):
    path = _live_db(tmp_path)
    db = sqlite3.connect(path)
    # needy: the reported shape - a peak on Arena, no season rows anywhere.
    db.execute("INSERT INTO player_playlist_csr VALUES ('needy', ?, NULL, 878)",
               (ARENA_LIVE,))
    # official: already has a season 343 still serves, so nothing to rebuild.
    db.execute("INSERT INTO player_playlist_csr VALUES ('official', ?, 1400, 1500)",
               (ARENA_LIVE,))
    db.execute("INSERT INTO player_csr_season VALUES ('official', ?, 'CsrSeason13-3')",
               (ARENA_LIVE,))
    # already: a previous run reconstructed them; must not be re-harvested.
    db.execute("INSERT INTO player_playlist_csr VALUES ('already', ?, NULL, 1200)",
               (ARENA_LIVE,))
    db.execute("INSERT INTO player_csr_season_derived VALUES ('already', ?, 'CsrSeason1-1')",
               (ARENA_LIVE,))
    # social: never ranked on a harvested playlist at all.
    db.execute("INSERT INTO player_playlist_csr VALUES ('social', ?, NULL, 500)",
               (SOCIAL,))
    db.commit()
    db.close()

    assert _affected_players(path) == ["needy"]


def test_a_db_without_the_derived_table_excludes_nobody(tmp_path):
    # The first ever run predates --merge creating that table, and must not 500.
    path = _live_db(tmp_path, with_derived=False)
    db = sqlite3.connect(path)
    db.execute("INSERT INTO player_playlist_csr VALUES ('needy', ?, NULL, 878)",
               (ARENA_LIVE,))
    db.commit()
    db.close()

    assert _affected_players(path) == ["needy"]


def test_matches_come_from_player_match_and_group_by_match(tmp_path):
    path = _live_db(tmp_path)
    db = sqlite3.connect(path)
    db.executemany("INSERT INTO matches VALUES (?,?,?)", [
        ("m1", ARENA_LIVE, "2021-11-17T22:56:53Z"),   # in window, harvested id
        ("m2", OTHER_ARENA, "2021-12-04T07:10:00Z"),  # a launch-era Arena id
        ("m3", SOCIAL, "2021-11-20T00:00:00Z"),       # not a ranked ladder
        ("m4", ARENA_LIVE, "2022-09-01T00:00:00Z"),   # after the window
    ])
    # a and b shared m1; only a played m2.
    db.executemany("INSERT INTO player_match VALUES (?,?)", [
        ("a", "m1"), ("b", "m1"), ("a", "m2"),
        ("a", "m3"), ("a", "m4"),
    ])
    db.commit()
    db.close()

    got = _history_matches(path, ["a", "b"], "2021-11-01", "2022-03-01")

    assert set(got) == {"m1", "m2"}, "only harvested ladders inside the window"
    assert got["m1"][0] == ARENA_LIVE
    assert sorted(got["m1"][2]) == ["a", "b"], "one request covers both players"
    assert got["m2"][2] == ["a"]


def _entry(xuid, pre=None, post=None, left=None, total=None):
    return {
        "Id": f"xuid({xuid})",
        "Result": {"RankRecap": {
            "PreMatchCsr": {"Value": pre},
            "PostMatchCsr": {"Value": post,
                             "MeasurementMatchesRemaining": left,
                             "InitialMeasurementMatches": total},
        }},
    }


@pytest.mark.parametrize("entry, expect", [
    # A ranked recap keeps both sides of the match.
    (_entry("1", pre=850, post=878), (850, 878, None, None)),
    # -1 is not a rank, but the placement counters say WHY, so the row stays.
    (_entry("1", pre=-1, post=-1, left=4, total=10), (None, None, 4, 10)),
    # 0/0 is an unranked match and says nothing - no row at all.
    (_entry("1", pre=-1, post=-1, left=0, total=0), None),
])
def test_recap_rows_reads_a_recap(entry, expect):
    rows = _recap_rows(HaloAPIClient, [entry], "ladder", "m1", "2021-11-17")

    if expect is None:
        assert rows == []
        return
    assert len(rows) == 1
    xuid, ladder, mid, started, pre, post, left, total = rows[0]
    assert (xuid, ladder, mid, started) == ("1", "ladder", "m1", "2021-11-17")
    assert (pre, post, left, total) == expect


def test_recap_rows_skips_entries_with_no_usable_result():
    rows = _recap_rows(HaloAPIClient, [
        {"Id": "xuid(1)", "Result": None},   # no result block
        {"Id": None, "Result": {}},          # no player id
        {},                                  # nothing at all
    ], "ladder", "m1", "2021-11-17")

    assert rows == []
