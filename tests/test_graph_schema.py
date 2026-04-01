from src.database.graph_schema import HaloSocialGraphDB


def test_insert_and_get_player(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))

    assert db.insert_or_update_player("x1", gamertag="Alpha", halo_active=True, crawl_depth=1)
    player = db.get_player("x1")
    by_gt = db.get_player_by_gamertag("alpha")

    assert player["xuid"] == "x1"
    assert player["halo_active"] == 1
    assert by_gt["xuid"] == "x1"

    db.close()


def test_friend_edges_and_counts(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    db.insert_or_update_player("x1", gamertag="Alpha")
    db.insert_or_update_player("x2", gamertag="Beta", halo_active=True)

    assert db.insert_friend_edge("x1", "x2", is_mutual=True, depth=1)
    assert db.edge_exists("x1", "x2") is True
    assert db.get_friend_count("x1") == 1

    friends = db.get_friends("x1", mutual_only=True)
    assert len(friends) == 1
    assert friends[0]["dst_xuid"] == "x2"
    assert friends[0]["halo_active"] == 1

    db.close()


def test_crawl_queue_lifecycle(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))

    assert db.add_to_crawl_queue("x1", priority=50, depth=0)
    assert db.add_to_crawl_queue("x2", priority=10, depth=1)

    batch = db.get_next_from_queue(batch_size=1)
    assert len(batch) == 1
    assert batch[0]["xuid"] == "x1"

    assert db.mark_queue_item_complete("x1")

    stats = db.get_queue_stats()
    assert stats["total"] == 2
    assert stats["completed"] == 1

    db.close()


def test_halo_features_and_knn(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    db.insert_or_update_player("x1", gamertag="Alpha", halo_active=True)
    db.insert_or_update_player("x2", gamertag="Beta", halo_active=True)
    db.insert_or_update_player("x3", gamertag="Gamma", halo_active=True)

    db.insert_or_update_halo_features("x1", gamertag="Alpha", csr=1200, kd_ratio=1.4, win_rate=55, matches_played=30)
    db.insert_or_update_halo_features("x2", gamertag="Beta", csr=1180, kd_ratio=1.35, win_rate=53, matches_played=28)
    db.insert_or_update_halo_features("x3", gamertag="Gamma", csr=800, kd_ratio=0.9, win_rate=42, matches_played=40)

    features = db.get_halo_features("x1")
    neighbors = db.get_similar_players_knn("x1", k=2)

    assert features["csr"] == 1200
    assert len(neighbors) == 2
    assert neighbors[0]["xuid"] == "x2"

    db.close()


def test_connected_component(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    for xuid in ["x1", "x2", "x3", "x4"]:
        db.insert_or_update_player(xuid)

    db.insert_friend_edge("x1", "x2")
    db.insert_friend_edge("x2", "x3")
    db.insert_friend_edge("x4", "x4")

    component = db.get_connected_component("x1")

    assert "x1" in component
    assert "x2" in component
    assert "x3" in component
    assert "x4" not in component

    db.close()


def test_refresh_inferred_snapshot_persists_metadata_and_partner_rows(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))

    db.insert_or_update_player("owner", gamertag="Owner", halo_active=True)
    db.insert_or_update_player("friend1", gamertag="Friend1", halo_active=True)
    db.insert_or_update_player("friend2", gamertag="Friend2", halo_active=True)

    # Incoming verified friends to owner (owner has no outgoing verified edges).
    db.insert_friend_edge("friend1", "owner", is_mutual=False, depth=1)
    db.insert_friend_edge("friend2", "owner", is_mutual=False, depth=1)
    db.insert_or_update_halo_features("friend1", gamertag="Friend1", matches_played=10)
    db.insert_or_update_halo_features("friend2", gamertag="Friend2", matches_played=20)

    snapshot = db.refresh_inferred_group_snapshot("owner")

    assert snapshot["social_group_size"] == 2
    assert snapshot["social_group_size_inferred"] is True
    assert snapshot["social_group_source"] == "inferred-reciprocal"

    owner = db.get_player("owner")
    assert owner["social_group_size"] == 2
    assert owner["social_group_size_inferred"] == 1
    assert owner["social_group_source"] == "inferred-reciprocal"
    assert owner["inference_updated_at"] is not None

    partners = db.get_inferred_partners("owner")
    partner_xuids = sorted(row["inferred_xuid"] for row in partners)
    assert partner_xuids == ["friend1", "friend2"]

    # Replace with empty inferred partners after direct verified edge appears.
    db.insert_friend_edge("owner", "friend1", is_mutual=False, depth=1)
    db.insert_or_update_halo_features("friend1", gamertag="Friend1", matches_played=30)
    snapshot2 = db.refresh_inferred_group_snapshot("owner")
    assert snapshot2["social_group_source"] == "direct"
    assert db.get_inferred_partners("owner") == []

    db.close()


def test_get_coplay_neighbors_is_direction_agnostic(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    db.insert_or_update_player("x1", gamertag="Alpha")
    db.insert_or_update_player("x2", gamertag="Beta")
    db.insert_or_update_player("x3", gamertag="Gamma")

    db.insert_or_update_coplay("x1", "x2", matches_together=2)
    db.insert_or_update_coplay("x3", "x1", matches_together=5)

    neighbors = db.get_coplay_neighbors("x1", min_matches=2)
    by_partner = {row["partner_xuid"]: row for row in neighbors}

    assert by_partner["x2"]["matches_together"] == 2
    assert by_partner["x3"]["matches_together"] == 5

    db.close()


def test_get_coplay_edges_within_set_filters_min_matches(tmp_path):
    db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    for xuid in ["x1", "x2", "x3"]:
        db.insert_or_update_player(xuid)

    db.insert_or_update_coplay("x1", "x2", matches_together=1)
    db.insert_or_update_coplay("x2", "x3", matches_together=4)
    db.insert_or_update_coplay("x1", "x3", matches_together=6)

    edges = db.get_coplay_edges_within_set(["x1", "x2", "x3"], min_matches=4)
    pairs = {tuple(sorted((row["src_xuid"], row["dst_xuid"]))) for row in edges}

    assert ("x1", "x2") not in pairs
    assert ("x2", "x3") in pairs
    assert ("x1", "x3") in pairs

    db.close()


def test_coplay_quality_flags_and_coverage_from_backfill_logic(tmp_path, monkeypatch):
    import one_time_backfill_graph_coplay as backfill_module
    from src.database.schema import HaloStatsDBv2

    graph_db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    stats_db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    graph_db.insert_or_update_player("x1", gamertag="Alpha", halo_active=True)
    graph_db.insert_or_update_player("x2", gamertag="Bravo", halo_active=True)

    # Match 1: explicit same team (complete)
    stats_db.insert_match(
        {
            "match_id": "m1",
            "duration": "PT10M",
            "start_time": "2026-01-01T00:00:00",
            "is_ranked": False,
            "playlist_id": "playlist-social",
            "match_category": "social",
            "category_source": "default_non_ranked",
            "map_id": "map-1",
            "map_version": "v1",
        }
    )
    stats_db.insert_match_participants(
        "m1",
        [
            {"xuid": "x1", "outcome": 2, "team_id": "A", "kills": 10, "deaths": 4, "assists": 2},
            {"xuid": "x2", "outcome": 2, "team_id": "A", "kills": 8, "deaths": 5, "assists": 4},
        ],
    )

    # Match 2: inferred team ids only (inferred + complete)
    stats_db.insert_match(
        {
            "match_id": "m2",
            "duration": "PT10M",
            "start_time": "2026-01-02T00:00:00",
            "is_ranked": False,
            "playlist_id": "playlist-social",
            "match_category": "social",
            "category_source": "default_non_ranked",
            "map_id": "map-1",
            "map_version": "v1",
        }
    )
    stats_db.insert_match_participants(
        "m2",
        [
            {"xuid": "x1", "outcome": 2, "inferred_team_id": "outcome:WIN", "kills": 11, "deaths": 6, "assists": 3},
            {"xuid": "x2", "outcome": 2, "inferred_team_id": "outcome:WIN", "kills": 9, "deaths": 6, "assists": 1},
        ],
    )

    # Match 3: one side missing team data (partial)
    stats_db.insert_match(
        {
            "match_id": "m3",
            "duration": "PT10M",
            "start_time": "2026-01-03T00:00:00",
            "is_ranked": False,
            "playlist_id": "playlist-social",
            "match_category": "social",
            "category_source": "default_non_ranked",
            "map_id": "map-1",
            "map_version": "v1",
        }
    )
    stats_db.insert_match_participants(
        "m3",
        [
            {"xuid": "x1", "outcome": 2, "team_id": "A", "kills": 7, "deaths": 2, "assists": 1},
            {"xuid": "x2", "outcome": 2, "kills": 5, "deaths": 3, "assists": 0},
        ],
    )

    class _CacheShim:
        def __init__(self, db):
            self.db = db

        def resolve_xuid_by_gamertag(self, _gamertag):
            return None

    monkeypatch.setattr(backfill_module, "get_graph_db", lambda: graph_db)
    monkeypatch.setattr(backfill_module, "get_cache", lambda: _CacheShim(stats_db))

    result = backfill_module.run_backfill(
        dry_run=False,
        batch_size=10,
        limit_matches=None,
        seed_xuid=None,
        seed_gamertag=None,
        depth=0,
        reset_target=True,
    )

    assert result.pairs_built >= 1

    conn = graph_db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT matches_together, is_inferred, is_partial, coverage_ratio
        FROM graph_coplay
        WHERE src_xuid = ? AND dst_xuid = ?
        """,
        ("x1", "x2"),
    )
    row = cursor.fetchone()

    assert row is not None
    assert row["matches_together"] == 3
    assert row["is_inferred"] == 1
    assert row["is_partial"] == 1
    assert abs(float(row["coverage_ratio"]) - (2.0 / 3.0)) < 1e-9

    stats_db.close()
    graph_db.close()


def test_coplay_backfill_rerun_idempotency(tmp_path, monkeypatch):
    import one_time_backfill_graph_coplay as backfill_module
    from src.database.schema import HaloStatsDBv2

    graph_db = HaloSocialGraphDB(str(tmp_path / "graph.db"))
    stats_db = HaloStatsDBv2(str(tmp_path / "stats.db"))

    graph_db.insert_or_update_player("x1", gamertag="Alpha", halo_active=True)
    graph_db.insert_or_update_player("x2", gamertag="Bravo", halo_active=True)

    stats_db.insert_match(
        {
            "match_id": "idempotent-m1",
            "duration": "PT10M",
            "start_time": "2026-01-01T00:00:00",
            "is_ranked": False,
            "playlist_id": "playlist-social",
            "match_category": "social",
            "category_source": "default_non_ranked",
            "map_id": "map-1",
            "map_version": "v1",
        }
    )
    stats_db.insert_match_participants(
        "idempotent-m1",
        [
            {"xuid": "x1", "outcome": 2, "team_id": "A", "kills": 10, "deaths": 4, "assists": 2},
            {"xuid": "x2", "outcome": 2, "team_id": "A", "kills": 8, "deaths": 5, "assists": 4},
        ],
    )

    class _CacheShim:
        def __init__(self, db):
            self.db = db

        def resolve_xuid_by_gamertag(self, _gamertag):
            return None

    monkeypatch.setattr(backfill_module, "get_graph_db", lambda: graph_db)
    monkeypatch.setattr(backfill_module, "get_cache", lambda: _CacheShim(stats_db))

    first = backfill_module.run_backfill(
        dry_run=False,
        batch_size=10,
        limit_matches=None,
        seed_xuid=None,
        seed_gamertag=None,
        depth=0,
        reset_target=True,
    )
    second = backfill_module.run_backfill(
        dry_run=False,
        batch_size=10,
        limit_matches=None,
        seed_xuid=None,
        seed_gamertag=None,
        depth=0,
        reset_target=False,
    )

    conn = graph_db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS row_count
        FROM graph_coplay
        WHERE source_type = ?
        """,
        ("participants",),
    )
    row_count = cursor.fetchone()["row_count"]

    cursor.execute(
        """
        SELECT matches_together, wins_together, same_team_count, opposing_team_count, coverage_ratio
        FROM graph_coplay
        WHERE src_xuid = ? AND dst_xuid = ?
        """,
        ("x1", "x2"),
    )
    snapshot_a = dict(cursor.fetchone())

    # One more run to confirm values remain stable.
    backfill_module.run_backfill(
        dry_run=False,
        batch_size=10,
        limit_matches=None,
        seed_xuid=None,
        seed_gamertag=None,
        depth=0,
        reset_target=False,
    )
    cursor.execute(
        """
        SELECT matches_together, wins_together, same_team_count, opposing_team_count, coverage_ratio
        FROM graph_coplay
        WHERE src_xuid = ? AND dst_xuid = ?
        """,
        ("x1", "x2"),
    )
    snapshot_b = dict(cursor.fetchone())

    assert first.rows_written == second.rows_written
    assert row_count == 2  # x1->x2 and x2->x1
    assert snapshot_a == snapshot_b

    stats_db.close()
    graph_db.close()
