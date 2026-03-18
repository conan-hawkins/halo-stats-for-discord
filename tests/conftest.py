import pytest


@pytest.fixture
def sample_match_data():
    return {
        "match_id": "match-1",
        "kills": 10,
        "deaths": 5,
        "assists": 7,
        "outcome": 2,
        "duration": "PT12M",
        "start_time": "2026-01-01T12:00:00",
        "is_ranked": True,
        "playlist_id": "playlist-ranked",
        "map_id": "map-1",
        "map_version": "v1",
        "medals": [
            {"NameId": 622331684, "Count": 2, "TotalPersonalScoreAwarded": 0},
            {"NameId": 2758320809, "Count": 1, "TotalPersonalScoreAwarded": 0},
        ],
    }
