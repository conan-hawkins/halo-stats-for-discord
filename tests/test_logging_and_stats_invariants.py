import random

import pytest

from src.api.client import HaloAPIClient


def test_calculate_stats_randomized_invariants():
    random.seed(1337)
    client = HaloAPIClient()

    for _ in range(40):
        matches = []
        for _ in range(random.randint(0, 30)):
            matches.append(
                {
                    "kills": random.randint(0, 30),
                    "deaths": random.randint(0, 30),
                    "assists": random.randint(0, 20),
                    "outcome": random.choice([1, 2, 3, 4]),
                    "is_ranked": random.choice([True, False]),
                }
            )

        stats = client._calculate_stats_from_matches(matches, "overall")

        assert stats["games_played"] == len(matches)
        assert stats["wins"] + stats["losses"] + stats["ties"] + stats["dnf"] == len(matches)
        assert stats["total_kills"] >= 0
        assert stats["total_deaths"] >= 0
        assert stats["total_assists"] >= 0
        assert stats["win_rate"].endswith("%")


@pytest.mark.asyncio
async def test_get_clearance_token_logs_do_not_expose_token_value(monkeypatch):
    client = HaloAPIClient()

    from src.api import client as client_module

    secret_token = "SECRET-SPARTAN-TOKEN"
    token_data = {
        "spartan": {"token": secret_token, "expires_at": 9999999999},
        "xsts_xbox": {"token": "xbox-token", "expires_at": 9999999999},
    }

    monkeypatch.setattr(client_module.os.path, "exists", lambda path: True)
    monkeypatch.setattr(client_module, "safe_read_json", lambda *args, **kwargs: token_data)
    monkeypatch.setattr(client_module, "is_token_valid", lambda info: True)

    logs = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: logs.append(" ".join(str(x) for x in args)))

    ok = await client.get_clearance_token()

    assert ok is True
    assert secret_token not in "\n".join(logs)
