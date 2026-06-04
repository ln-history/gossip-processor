# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Install dependencies
pip install -r requirements.txt

# Run locally (requires .env with POSTGRES_URI and ZMQ_SOURCES)
python main.py

# Run in Docker (primary development workflow)
docker compose up -d --build gossip-processor

# View logs
docker logs -f gossip-processor

# Lint
ruff check main.py
ruff format main.py

# Type check
mypy main.py

# Build and publish image
docker buildx build --platform linux/amd64 -t ghcr.io/ln-history/gossip-processor:<TAG> --push .
```

## Architecture

The entire service lives in a single file: `main.py`. It is part of the broader `ln-history` platform.

**Threading model**: One `zmq_worker` thread per ZMQ source subscribes to a Core Lightning node PUB socket and enqueues raw JSON messages. A single `db_worker` thread drains the queue in batches (default 200 msgs / 1 sec timeout) and executes each batch in one PostgreSQL transaction.

**Two-class design**:
- `Database` — all SQL. Methods are called from within an already-open cursor/connection, so callers own the transaction boundary.
- `GossipProcessor` — threading, ZMQ, queue management, metrics, and orchestration.

**Processing pipeline per message** (`process_msg_in_batch`):
1. Hash the raw bytes → `gossip_id` (SHA-256)
2. Strip Bitcoin-style VarInt length prefix, then the 2-byte BOLT message type
3. Parse binary payload via `lnhistoryclient.parser.parser_factory`
4. `register_collector` — UPSERT collector stats
5. `insert_content` — INSERT INTO `gossip_inventory` ON CONFLICT DO NOTHING; if `rowcount == 1` (new), write to the content table (`channels`, `node_announcements`, `channel_updates`)
6. `insert_observation` — always record that this collector saw this hash

**SCD Type 2 (Slowly Changing Dimension)** is applied to `node_announcements` and `channel_updates`. On insert, the previous row where `valid_to IS NULL` is closed by setting `valid_to = now`. Out-of-order messages (older than the current tip) are inserted as historical records with `valid_to` set to the current tip's `valid_from`.

**External dependency**: `lnhistoryclient` (PyPI) provides the BOLT #7 binary parser and typed model classes (`ChannelAnnouncement`, `NodeAnnouncement`, `ChannelUpdate`).

**Key environment variables**:
| Variable | Default | Description |
|---|---|---|
| `ZMQ_SOURCES` | `tcp://host.docker.internal:5675,...` | Comma-separated ZMQ endpoints |
| `POSTGRES_URI` | — | psycopg3-compatible connection string |
| `BATCH_SIZE` | `200` | Max messages per DB transaction |
| `BATCH_TIMEOUT` | `1.0` | Seconds to wait before flushing partial batch |
| `LOG_LEVEL` | `INFO` | Python logging level |

**Observability**: Prometheus metrics exposed on port `8000`. Grafana + Loki + Promtail stack defined in `docker-compose.yaml` under `monitoring/`.

## Versioning

Uses [Commitizen](https://commitizen-tools.github.io/commitizen/) with conventional commits. Version is in `pyproject.toml` under `[project].version`.
