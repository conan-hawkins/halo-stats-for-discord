# Halo Infinite Discord Stats Bot

A Discord bot that retrieves Halo Infinite player statistics using the official Halo Waypoint API. Features high-performance concurrent data processing and server-wide leaderboard generation.

## Commands

**Command prefix:** `#`

| Command | Description | Example |
|---------|-------------|---------|
| `#full [gamertag]` | Get complete match history stats (ALL matches) | `#full XxUK D3STROYxX` |
| `#server` | Generate server-wide leaderboard from all members | `#server` |
| `#populate [gamertag]` | Cache gamertags from player's match history | `#populate XxUK D3STROYxX` |
| `#help` | Show bot information | `#help` |

### Performance Features
- Connection pooling with 50 concurrent connections
- Shared HTTP session across all requests
- Concurrent page fetching for parallel data retrieval
- Batch processing of 50 matches per batch
- Smart pagination that automatically retrieves all pages
- Intelligent caching to avoid redundant API calls
- Processes 40+ matches per second

## Installation & Setup

### Prerequisites
- Python 3.13+
- Discord Bot Token
- Xbox Live/Microsoft Account (for Halo API access)

### 1. Install Dependencies
```bash
pip install discord.py python-dotenv aiohttp
```

### 2. Environment Setup
Create a `.env` file in the project root:
```env
DISCORD_TOKEN=your_discord_bot_token_here
```

### 3. Xbox Authentication
Run the authentication setup script:
```bash
python get_auth_tokens.py
```

This will:
1. Open your browser for Microsoft account login
2. Cache authentication tokens in `token_cache.json`
3. Enable automatic token refresh

### 4. Run the Bot
```bash
python bot.py
```

The bot will automatically validate tokens on startup and refresh them hourly.

## Project Structure

```
├── bot.py              # Main Discord bot entry point
├── commands.py         # Discord command handlers
├── halo_api.py         # Halo Waypoint API client
├── get_auth_tokens.py  # Xbox Live OAuth authentication
├── embed_formatter.py  # Discord embed formatting
├── discord_utils.py    # Discord utility functions
├── setup_account2.py   # Account setup utilities
├── token_cache.json    # Cached auth tokens (auto-generated)
└── .env                # Environment variables (not in repo)
```

## Technical Details

### API Architecture
- Uses official Halo Waypoint API with Xbox Live authentication
- Implements OAuth 2.0 flow for secure API access
- Automatic token refresh to maintain continuous operation
- Background task that validates tokens hourly

### Optimization Features
- Asynchronous HTTP requests using aiohttp
- Connection pooling to reduce overhead
- Concurrent batch processing of match data
- XUID caching system for fast gamertag resolution
- Efficient pagination handling for large datasets

### Error Handling
The bot handles common scenarios:
- Player not found
- Private profiles
- API authentication failures
- Network connectivity issues
- Rate limiting and retry logic

## Commands in Detail

### #full
Processes the player's entire match history and displays comprehensive statistics including:
- Total kills, deaths, and K/D ratio
- Accuracy percentage
- Win/loss record
- Match count and performance trends

### #server
Generates a server-wide leaderboard by:
1. Scanning all Discord server members
2. Attempting to match Discord names with Halo gamertags
3. Fetching stats for each player
4. Ranking by performance metrics

### #populate
Pre-caches gamertags from a player's recent matches:
- Resolves gamertags to XUIDs
- Stores mappings for fast future lookups
- Useful for preparing data before running server stats

## Troubleshooting

### Authentication Issues
If you encounter authentication errors:
1. Delete `token_cache.json`
2. Run `python get_auth_tokens.py`
3. Complete the Microsoft login flow

### Player Not Found
Verify:
- Gamertag spelling is correct
- Player has played Halo Infinite
- Profile privacy settings allow stats viewing

### API Errors
- Check internet connectivity
- Verify Xbox Live services are operational
- Wait a few minutes if rate limited
- Check console output for detailed error messages

## Notes

- Created by Conan Hawkins
- Project codename: "Project Goliath"
- Token cache is automatically maintained and refreshed
- Background tasks run to ensure continuous authentication
- All stats are retrieved directly from official Halo API endpoints
