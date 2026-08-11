"""Container healthcheck for the bot.

The bot has no general-purpose health surface, so we probe the one thing the
stats website actually depends on: the internal refresh endpoint. Posting with
no token must return 401, which proves two things at once - the aiohttp site is
listening, and auth is being enforced. Auth is checked before anything else in
handle_refresh, so this costs no Halo call and no DB work.

The endpoint only binds after the bot has connected to Discord (and not at all
if INTERNAL_STATS_REFRESH_TOKEN is unset), hence the long start period on the
HEALTHCHECK instruction. An unhealthy mark here is informational: Docker's
restart policy does not act on it, so a missing token shows up in `docker ps`
rather than putting Discord into a restart loop.
"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

PORT = os.getenv("INTERNAL_API_PORT", "8787")
URL = f"http://127.0.0.1:{PORT}/internal/refresh-player"


def main() -> int:
    request = urllib.request.Request(
        URL,
        data=b"{}",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=5)
    except urllib.error.HTTPError as e:
        # 401 is the expected, healthy answer to an unauthenticated probe.
        return 0 if e.code == 401 else 1
    except Exception:
        # Connection refused / DNS / timeout: not listening.
        return 1
    # A 2xx without a token would mean auth is not being enforced at all.
    return 1


if __name__ == "__main__":
    sys.exit(main())
