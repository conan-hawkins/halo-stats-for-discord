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
