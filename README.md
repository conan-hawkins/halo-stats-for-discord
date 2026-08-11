# Halo Infinite Discord Stats Bot

A Discord bot for fetching and displaying Halo Infinite player statistics and social graph analysis using the official Halo Waypoint API.

**Made by Conan Hawkins**

## Features

- 📊 **Player Stats** - Get comprehensive statistics from match history
- 🕸️ **Social Graph Crawler** - Map friend networks and find active Halo players
- 🔄 **Multi-Account Support** - Up to 5 accounts for increased API rate limits
- 💾 **SQLite Caching** - Efficient normalized database for fast lookups
- 🔐 **Automatic Token Refresh** - Seamless authentication with weekly proactive refresh
- 📈 **Rate Limiting** - Conservative API usage to prevent bans

## Project Structure

```
halo-stats-for-discord/
├── run.py                    # Main entry point
├── src/                      # Main source package
│   ├── api/                  # Halo API client
│   │   ├── client.py         # API client wrapper
│   │   ├── rate_limiters.py  # Rate limiting classes
│   │   ├── xuid_cache.py     # XUID/Gamertag cache
│   │   └── utils.py          # Utility functions
│   ├── auth/                 # Authentication
│   │   ├── tokens.py         # Token management
│   │   └── setup_account.py  # Multi-account setup
│   ├── bot/                  # Discord bot
│   │   ├── main.py           # Bot setup
│   │   ├── tasks.py          # Background tasks
│   │   └── cogs/             # Command cogs
│   │       ├── stats.py      # Stats commands
│   │       └── graph.py      # Graph crawler commands
│   ├── config/               # Configuration
│   │   └── settings.py       # Centralized settings
│   ├── database/             # Database layer
│   │   ├── cache.py          # Stats cache
│   │   ├── schema.py         # Stats schema
│   │   └── graph_schema.py   # Social graph schema
│   ├── graph/                # Social graph
│   │   └── crawler.py        # BFS graph crawler
│   └── web/                  # Internal HTTP API for the stats website
│       └── internal_api.py   # POST /internal/refresh-player
├── docker/
│   └── healthcheck.py        # Container healthcheck (probes the internal API)
├── Dockerfile                # Image used by the halo-stack deployment
├── requirements.txt          # Runtime deps (TRACKED - see note in Quick Start)
├── requirements-test.txt     # Test deps
└── data/                     # Data files (bind-mounted in Docker)
    ├── auth/                 # Token cache files
    ├── halo_stats_v2.db      # Stats database
    ├── halo_social_graph.db  # Social graph database
    ├── medal_icons/          # Cropped medal PNGs, served by halo-stats-api
    └── xuid_gamertag_cache.json  # XUID cache (86k+ entries)
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

> `bot_docs/` is gitignored, so `bot_docs/requirements.txt` does not survive a
> clone and must not be used. The tracked `requirements.txt` in the project root
> is the source of truth, and is what the Docker image installs from.

### 2. Configure

Create a `.env` file in the project root:

```env
DISCORD_TOKEN=your_discord_bot_token_here
client_id=your_azure_app_client_id
client_secret=your_azure_app_client_secret
```

### 3. Authenticate Account 1

```bash
python -m src.auth.tokens
```

### 4. (Optional) Add More Accounts

For faster API access, add up to 4 additional Xbox accounts:

```bash
python -m src.auth.setup_account 2
python -m src.auth.setup_account 3
python -m src.auth.setup_account 4
python -m src.auth.setup_account 5
```

Each opens an incognito browser window - sign in with a different Microsoft account.

### 5. Run

```bash
python run.py
```

## Testing

This repository now uses a pytest-based unit test suite with coverage reporting.

### Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### Run Unit Tests

```bash
pytest
```

### Coverage Output

Running `pytest` generates:

- terminal coverage report with missing lines
- `coverage.xml` for CI integrations

## Commands

### Stats Commands
| Command | Description |
|---------|-------------|
| `#stats <gamertag>` | Get player stats (cached) |
| `#full <gamertag>` | Get stats from ALL match history |

### Graph Commands (Admin)
| Command | Description |
|---------|-------------|
| `#crawlfriends <gamertag> [depth]` | Start Halo-friends graph crawl (default depth 2) |
| `#crawlgames <gamertag> [depth]` | Build weighted co-play edges from shared match history |
| `#crawlstop` | Stop the current crawl |
| `#graphstats` | Show graph database statistics |

### Graph CLI Migration

`run_graph_crawler.py` has been removed in favor of Discord-first graph workflows.

Use these Discord replacements:

- `#crawlfriends <gamertag> [depth]` instead of CLI seed/resume crawl workflows
- `#crawlgames <gamertag> [depth] [--global]` for co-play edge backfill
- `#graphstats` for graph health and totals
- `#crawlstop` to cancel active background crawl jobs

If you still depend on CLI-only maintenance operations (CSV export, missing-stats backfill), add equivalent admin Discord commands.

## Match Categories

Matches now store category metadata in the stats DB:

- `match_category`: `ranked`, `social`, `custom`, or `unknown`
- `category_source`: classifier provenance (for example `playlist_map`, `text_heuristic`, `default_non_ranked`)

Historical migration and one-time backfill scripts are no longer part of this repository.

## Social Graph Crawler

The crawler builds a social network of Halo players using BFS traversal:

1. Starts from a seed player
2. Fetches their Xbox friends list
3. Checks each friend for recent Halo activity (since Sept 2025)
4. Recursively discovers friends-of-friends up to specified depth
5. Collects match statistics for active players

### Sample Size Guidelines

| Use Case | Players Needed |
|----------|---------------|
| Basic analytics | 1,000-2,000 |
| Social graph analysis | 5,000-10,000 |
| Comprehensive study | 10,000-25,000 |

A depth-2 crawl from a well-connected player typically finds 10,000-30,000 unique players.

## Multi-Account Setup

The bot supports up to 5 Xbox accounts for parallel API requests:

- **Account 1**: Primary account (set up with `python -m src.auth.tokens`)
- **Accounts 2-5**: Additional accounts (set up with `python -m src.auth.setup_account N`)

Benefits:
- 5x faster match history fetching
- Distributed rate limiting across accounts
- Automatic token refresh keeps accounts active

### Token Expiration

Microsoft refresh tokens expire after 90 days of inactivity. The bot includes:
- **Hourly token validation** - Checks and refreshes as needed
- **Weekly proactive refresh** - Prevents 90-day expiration

If tokens expire, re-authenticate manually:
```bash
python -m src.auth.setup_account N  # Where N is 2-5
```

## Stats website integration

The bot is one of three services behind the stats website. It is the **single
writer** of `halo_stats_v2.db`; the website's API reads that same file read-only
and cannot write to it. Anything that needs fresh data from Halo goes through
the bot, because the bot owns the Halo credentials, the rate limiters and the
freshness signal shared with Discord commands.

| Repo | Role |
|---|---|
| `halo-stats-for-discord` | this bot — Halo fetching, the only DB writer |
| `halo-stats-api` | read-only HTTP API over the DB + a refresh proxy |
| `halo-stats-web` | the React frontend |
| `halo-stack` | Docker Compose, nginx and the deployment runbook |

### The internal refresh endpoint

`src/web/internal_api.py` runs an aiohttp server on the bot's own event loop,
started from `on_ready()`. It exposes exactly one route:

```
POST /internal/refresh-player     header: X-Internal-Token     body: {"gamertag": "..."}
```

It is **a no-op unless `INTERNAL_STATS_REFRESH_TOKEN` is set** — if the token is
unset the server never starts, and the website simply cannot trigger refreshes.

| Setting | Default | Purpose |
|---|---|---|
| `INTERNAL_STATS_REFRESH_TOKEN` | *(unset)* | Shared secret. Must match the API's `BOT_INTERNAL_TOKEN` exactly |
| `INTERNAL_API_HOST` | `127.0.0.1` | Bind address. See the warning below |
| `INTERNAL_API_PORT` | `8787` | |
| `REFRESH_MAX_CONCURRENCY` | `2` | Concurrent live fetches triggered from the web |
| `WEB_AUTOREFRESH_FRESHNESS_SECONDS` | `270` | Coalescing window — skip if checked this recently |
| `WEB_REFRESH_MAX_FETCHES_PER_MINUTE` | `20` | Global ceiling on actual Halo fetches from the web |

> **`INTERNAL_API_HOST` defaults to loopback deliberately.** Only widen it to
> `0.0.0.0` when the port is confined to a private container network and is
> never published to the host — which is exactly what the `halo-stack` compose
> file does. Widening it on a machine where 8787 is reachable would expose a
> refresh trigger to anything that can reach that port.

### Why public auto-refresh is safe

The website calls this endpoint with **no user token**, on every player page
open and roughly every 5 minutes per open tab. That is safe because the bot, not
the caller, holds the throttle:

- **Freshness gate** — a player checked by *anyone* (web or a Discord command)
  within `WEB_AUTOREFRESH_FRESHNESS_SECONDS` returns immediately from cache,
  costing zero Halo calls.
- **Per-gamertag coalescing** — concurrent requests for the same player await
  one shared fetch instead of stacking up.
- **Global fetch cap** — `WEB_REFRESH_MAX_FETCHES_PER_MINUTE` bounds real Halo
  calls per rolling minute, regardless of traffic.

Together these bound Halo usage by *distinct stale players* rather than by
viewer count. Both "skipped" outcomes return **HTTP 200** with `ok: true` on
purpose — the page keeps showing cached data rather than erroring.

## Docker

The `Dockerfile` here builds the image; orchestration lives in `halo-stack`.

```bash
cd ../halo-stack && docker compose up -d bot
```

Notes that matter if you change it:

- **`data/` is a bind mount**, not a volume — on the deployed box it points at a
  directory on a large disk, not at the repo checkout. Never add a `VOLUME`
  instruction for it; an anonymous volume would silently mask a missing mount
  and the bot would start writing a brand-new empty database.
- **The container runs as UID 1000**, matching the API container. This is not
  cosmetic: a read-only SQLite reader still has to write the WAL `-shm`/`-wal`
  files that this process creates, so mismatched UIDs break the website's reads.
- **`.dockerignore` is mandatory.** The data directory can hold a 60GB database;
  without it, `docker build` streams the lot to the daemon.

## Rate Limiting

The bot uses conservative rate limiting to avoid API bans:

- **8 requests/second per account** (`REQUESTS_PER_SECOND_PER_ACCOUNT`)
- **5 max concurrent requests per account**
- **Exponential backoff** on 429 errors (30s, 60s, 120s, 240s, 480s)
- **Global backoff** when all accounts hit limits

## Requirements

- **Python 3.11** — what the Docker image runs and what the pins are tested against
- discord.py, aiohttp, requests, python-dotenv
- **portalocker** — imported at module load in `src/api/utils.py`; without it the
  bot tries to `pip install` at runtime, which fails inside a container
- **Pillow** — medal sprite-sheet cropping. Imported lazily, so its absence is
  silent: icon warming just raises `ImportError` on every startup and
  `data/medal_icons/` never fills, leaving the website's medals page text-only

See `requirements.txt` (tracked, exact pins) for the full list. Two packages are
deliberately **absent** because the deployed bot has never had them, and both are
lazily imported so the bot runs fine without them: `matplotlib` and `networkx`,
used only by the social-graph plots. Add them if you want those features.

## License

Private project by Conan Hawkins.
