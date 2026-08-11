# syntax=docker/dockerfile:1
#
# Tracked version of the Dockerfile that has been running this bot in
# production. Two deliberate changes from the untracked original:
#   1. installs from ./requirements.txt (tracked) instead of
#      bot_docs/requirements.txt - bot_docs/ is gitignored, so that file does
#      not survive a clone, and it is missing Pillow.
#   2. adds a HEALTHCHECK for the internal refresh endpoint the stats API needs.
#
# Python stays on 3.11 to match the image that is running today. Changing the
# runtime at the same time as the infrastructure would make a rollback
# ambiguous - if something breaks you want to know which change did it.
FROM python:3.11-slim

# Prevents Python from writing .pyc files and buffers stdout (better for docker logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
# Harmless today (matplotlib is not installed), but if the social-graph plots
# are ever enabled this stops them trying to open a display on a headless box.
ENV MPLBACKEND=Agg

WORKDIR /app

# Install dependencies first (better layer caching - only reinstalls if requirements change).
# --only-binary makes the wheel-only assumption explicit: if a wheel ever
# disappears the build fails loudly instead of silently pulling in a compiler.
COPY --chown=1000:1000 requirements.txt requirements.txt
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# Copy the rest of the project
COPY --chown=1000:1000 . .

# The container runs as 1000:1000 via `user:` in docker-compose.yml, matching
# the owner of the data directory on the host. That matters for more than file
# permissions now: the stats API opens the same SQLite database read-only, and
# a WAL reader has to be able to write the -shm/-wal files that this process
# creates. Different UIDs on the two containers would break reads.

# The internal refresh endpoint, reachable only on the private compose network.
# Never published to the host - see halo-stack/docker-compose.yml.
EXPOSE 8787

# Probes that endpoint: an unauthenticated POST must return 401, which proves it
# is listening AND enforcing auth, at no Halo or DB cost. It only binds after
# the bot has connected to Discord, hence the long start period. An unhealthy
# mark here is informational - Docker's restart policy does not act on it, so a
# missing token shows up in `docker ps` rather than looping Discord.
HEALTHCHECK --interval=60s --timeout=10s --start-period=240s --retries=3 \
    CMD ["python", "docker/healthcheck.py"]

# data/ is mounted as a volume at runtime (see docker-compose.yml) so it's not baked into the image
CMD ["python", "run.py"]
