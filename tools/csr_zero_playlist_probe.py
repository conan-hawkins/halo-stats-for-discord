#!/usr/bin/env python3
"""Why do some ranked playlists have match history but no CSR records?

After the August 2026 CSR backfill, 8 of the 16 ranked playlist asset ids ended
up with zero rows in player_playlist_csr - despite some of them having tens of
thousands of ranked matches in the database. This asks the API directly whether
those ladders are reachable at all.

    docker compose run --rm --no-deps \
      -v ~/halo-stats-for-discord/src:/app/src:ro \
      bot python /app/src/../tools/csr_zero_playlist_probe.py

(or copy it in and run: docker exec -i halo-bot python - < tools/csr_zero_playlist_probe.py)

THE POINT OF THIS SCRIPT is the RAW HTTP STATUS. HaloAPIClient._fetch_skill
collapses 404 and transport errors into the same None, so "this ladder does not
exist" and "the request failed" are indistinguishable through the normal client
path. That distinction is the whole question, so this bypasses it.

Three outcomes, three different conclusions:

  404               skill.svc only serves currently-configured playlists.
                    Retired ladders are unreachable; nothing to recover.

  200, all csr=-1   The playlist existed but never had a CSR ladder, despite
                    is_ranked=1 in playlist_metadata. That would mean the ranked
                    classification is over-broad, which is worth knowing
                    independently of CSR.

  200, real values  The backfill MISSED them. Recover with a targeted re-run:
                        python -m src.database.csr_backfill --playlists <id> ...
                        python -m src.database.csr_merge     (bot stopped)
"""
import asyncio
import json
import sqlite3
import sys

import aiohttp

from src.api.client import HaloAPIClient

DB = "/app/data/halo_stats_v2.db"

# Playlists with matches in the DB but zero CSR rows after the backfill.
# Split deliberately: the first group has a live successor and is probably
# explained by ladder consolidation; the second group has no successor at all
# and is the genuinely open question.
ROTATED_ID = [
    ("f7f30787-f607-436b-bdec-44c65bc2ecef", "Ranked Arena (launch-era)"),
    ("f7eb8c71-fedb-4696-8c0f-96025e285ffd", "Ranked Arena (launch-era 2)"),
    ("6e4e9372-5d49-4f87-b0a7-4489b5e96a0b", "Ranked Arena (hardcoded, no matches)"),
    ("7c60fb3e-656c-4ada-a085-293562642e50", "Ranked Tactical (old id)"),
    ("a883e7e1-9aca-4296-9009-3733a0ca8081", "Ranked Snipers (old id)"),
]
DISCONTINUED = [
    ("71734db4-4b8e-4682-9206-62b6eff92582", "Ranked FFA"),
    ("f3738fae-bd09-4fd1-9dea-e32f546bbbfd", "Ranked Survivors"),
    ("6dc5f699-d6d9-41c4-bdf8-7ae11dec2d1b", "Ranked Squad Battle"),
]
CONTROL = [
    ("edfef3ac-9cbe-4fa2-b949-8f29deafd483", "Ranked Arena (LIVE - control)"),
]


def players_who_played(db, pid, n=8):
    """Real participants of that playlist's matches.

    Both lookups are index-backed (idx_matches_playlist_start, then
    match_participants by match_id), so this stays cheap even on the 60GB file.
    Uses real players because a synthetic xuid would make a 404 ambiguous.
    """
    out = []
    for (mid,) in db.execute(
            "select match_id from matches where playlist_id=? limit 400", (pid,)):
        for (x,) in db.execute(
                "select xuid from match_participants where match_id=? limit 4", (mid,)):
            out.append(str(x))
            if len(out) >= n:
                return out
    return out


async def probe(session, client, db, pid, name):
    players = players_who_played(db, pid)
    if not players:
        print(f"  {name:<38} no participants recorded locally - cannot test")
        return

    token = client.get_next_spartan_token()
    headers = {"x-343-authorization-spartan": token,
               "User-Agent": client.USER_AGENT, "Accept": "application/json"}
    query = "&".join(f"players=xuid({p})" for p in players)
    url = f"{client.SKILL_URL}/hi/playlist/{pid}/csrs?{query}"

    async with session.get(url, headers=headers) as resp:
        body = await resp.text()
        if resp.status != 200:
            print(f"  {name:<38} HTTP {resp.status}   {body[:60]}")
            return
        entries = json.loads(body).get("Value", [])
        values = [((e.get("Result") or {}).get("Current") or {}).get("Value")
                  for e in entries]
        peaks = [((e.get("Result") or {}).get("AllTimeMax") or {}).get("Value")
                 for e in entries]
        ranked = [v for v in values if isinstance(v, (int, float)) and v > 0]
        # AllTimeMax survives even when the season has no rank, so it is the
        # better signal for "was this ladder ever real for these players".
        ever = [v for v in peaks if isinstance(v, (int, float)) and v > 0]
        print(f"  {name:<38} HTTP 200  sent={len(players)} got={len(entries)} "
              f"ranked_now={len(ranked)} ever_ranked={len(ever)} {ranked[:3]}")
    await asyncio.sleep(0.5)


async def main():
    client = HaloAPIClient()
    if not await client.ensure_valid_tokens():
        print("No valid tokens - start the bot once so it refreshes them.")
        return 2

    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    db.execute("pragma query_only=ON")

    async with aiohttp.ClientSession() as session:
        for title, group in (("ROTATED ASSET ID (has a live successor)", ROTATED_ID),
                             ("DISCONTINUED (no successor - the real question)", DISCONTINUED),
                             ("CONTROL (known good)", CONTROL)):
            print(f"\n== {title} ==")
            for pid, name in group:
                await probe(session, client, db, pid, name)

    print("\nReminder: 'ever_ranked > 0' with 'ranked_now == 0' means the ladder"
          "\nexists and the backfill should have found it - see the module docstring.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
