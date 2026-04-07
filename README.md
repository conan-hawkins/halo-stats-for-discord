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
│   └── graph/                # Social graph
│       └── crawler.py        # BFS graph crawler
├── data/                     # Data files
│   ├── auth/                 # Token cache files
│   ├── halo_stats_v2.db      # Stats database
│   ├── halo_social_graph.db  # Social graph database
│   └── xuid_gamertag_cache.json  # XUID cache (86k+ entries)
└── bot_docs/                 # Documentation
    └── requirements.txt
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r bot_docs/requirements.txt
```

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

## Rate Limiting

The bot uses conservative rate limiting to avoid API bans:

- **3 requests/second per account** (Halo Stats API)
- **5 max concurrent requests per account**
- **Exponential backoff** on 429 errors (30s, 60s, 120s, 240s, 480s)
- **Global backoff** when all accounts hit limits

## Requirements

- Python 3.9+
- discord.py 2.0+
- aiohttp
- python-dotenv
- portalocker

See `bot_docs/requirements.txt` for full list.

## License

Private project by Conan Hawkins.
