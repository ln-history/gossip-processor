# Gossip Processor

![Status](https://img.shields.io/badge/status-active-success) ![Python](https://img.shields.io/badge/python-3.13-blue) ![Postgres](https://img.shields.io/badge/postgres-18-blue)

The **Gossip Processor** is the central ingestion engine of the `ln-history` platform. It subscribes to multiple ZeroMQ `PUB` sockets (from Core Lightning nodes), deduplicates incoming gossip messages in real-time, and persists them into a PostgreSQL database.

## 🧠 Architecture

The service operates on a **Linear Pipeline** model:
`ZMQ Source` $\rightarrow$ `Deduplication` $\rightarrow$ `Observation Logging` $\rightarrow$ `Parsing` $\rightarrow$ `Persistence`.

### Key Features
* **Multi-Source Ingestion:** Consumes gossip from multiple nodes (Alice, Bob) simultaneously.
* **Global Deduplication:** Uses a lightweight "Master Inventory" table to ensure we only process unique messages once.
* **Provenance Tracking:** Records exactly *which* collector saw *which* message and *when* (N:M relationship).
* **Snapshot Optimized:** Raw gossip payloads are stored directly in their respective content tables (`channels`, `updates`, `node_announcements`) with Validity Ranges (SCD Type 2), allowing for join-free snapshot queries.

---

## 📊 Database Schema

The processor writes to two main categories of tables: **Statistics** (Observation Data) and **Content** (The Gossip Payloads).

### 1. Statistics & Observations
These tables answer the question: *"Who saw what, and when?"*

| Table | Description | Key Columns |
| :--- | :--- | :--- |
| **`collectors`** | Metadata about our infrastructure nodes (Alice, Bob). Tracks performance metrics. | `node_id`, `last_collection_at`, `total_messages_collected` |
| **`gossip_inventory`** | The global registry of all unique gossip hashes. Acts as the "Gatekeeper" for deduplication. | `gossip_id` (PK), `type`, `first_seen_at` |
| **`gossip_observations`** | The N:M link table. Stores the proof that a specific collector saw a specific hash. | `gossip_id`, `collector_node_id`, `seen_at` |

### 2. Persisting Gossip Messages (Content)
These tables store the parsed BOLT #7 data **and** the raw binary blob. They use **SCD Type 2** (Validity Ranges) to preserve history.

| Table | Description | Key Columns |
| :--- | :--- | :--- |
| **`nodes`** | Minimal record of public node existence. | `node_id`, `first_seen`, `last_seen` |
| **`node_announcements`** | Stores node metadata and the **raw node announcement blob**. | `gossip_id`, `alias`, `raw_gossip`, `valid_from`, `valid_to` |
| **`channels`** | Stores static channel definitions and the **raw channel announcement blob**. | `scid`, `source_node_id`, `target_node_id`, `raw_gossip` |
| **`channel_updates`** | Stores dynamic routing fees and the **raw channel update blob**. | `scid`, `direction`, `fee_base_msat`, `raw_gossip`, `valid_from`, `valid_to` |
| **`node_addresses`** | Normalized table for IP/Tor addresses linked to announcements. | `gossip_id`, `type_id`, `address`, `port` |

---

## 🔄 Control Flow

The following diagram illustrates the exact lifecycle of an incoming gossip message.

```mermaid
sequenceDiagram
    participant ZMQ as ZMQ Source (Alice)
    participant GP as Gossip Processor
    participant DB as Postgres DB

    ZMQ->>GP: Send Raw Gossip (JSON)
    activate GP
    
    GP->>GP: Calculate Hash (Gossip ID)
    
    %% Step 1: Update Collector Stats
    GP->>DB: UPSERT into collectors<br/>(Update last_collection_at)
    
    %% Step 2: Inventory Check (Fast Path)
    GP->>DB: INSERT into gossip_inventory<br/>ON CONFLICT DO NOTHING
    
    alt is newly inserted?
        DB-->>GP: True (New Global Message)
    else
        DB-->>GP: False (Duplicate)
    end
    
    %% Step 3: Log Observation (Always happens)
    GP->>DB: INSERT into gossip_observations<br/>(Alice saw GossipID at Now)
    
    %% Step 4: Parse & Persist Content (Slow Path)
    opt If Message is Globally New
        GP->>GP: Parse Binary Blob (BOLT #7)
        
        alt Type 256 (Channel Ann)
            GP->>DB: INSERT into channels (with raw_gossip)
        else Type 257 (Node Ann)
            GP->>DB: INSERT into nodes (Ensure existence)<br/>INSERT into node_announcements (with raw_gossip)<br/>INSERT into node_addresses
        else Type 258 (Channel Update)
             GP->>DB: UPDATE older channel_updates SET valid_to = Now<br/>INSERT into channel_updates (with raw_gossip)
        end
    end
    
    deactivate GP
```
    
    
## ⚙️ Detailed Logic by Message Type
1. Channel Announcement (type: `256`)

    **Trigger**: Received a channel_announcement.

    **Action**:
    - Parse the message to extract short_channel_id (`scid`), `node_id_1`, and `node_id_2`.
    - Ensure both `node_id_1` and `node_id_2` exist in the `nodes` table (INSERT IGNORE).
    - Insert the row into the `channels` table.
    - Note: Chain data (Funding Block, Capacity) is not in the gossip message. This field is left NULL to be filled by the separate chain-enricher service later.

2. Node Announcement (type: `257`)

    **Trigger**: Received a node_announcement.

    **Action**:
    - Parse the message to extract `alias`, `color`, and `addresses`.
    - **SCD Logic**: Find the previous announcement for this `node_id` where `valid_to IS NULL`.
    - Update the previous row: set `valid_to` = `current_timestamp`.
    - Insert the new row into `node_announcements` with `valid_from` = `current_timestamp` and `valid_to` = `NULL`.
    - Address Normalization: Iterate through the `addresses` (`IPv4`, `Tor`, etc.) and insert them into the `node_addresses` table, linking them to this specific `gossip_id`.

3. Channel Update (type: `258`)

    **Trigger**: Received a channel_update (Fee change, disable/enable).

    **Action**: 
    - Parse the message to extract `scid`, `direction`, and `fee policies`.
    - Validation: Check if the `scid` exists in the `channels` table.
        - If Yes: Proceed.
        - If No: Insert anyway (Orphan Update). This is common in gossip; the announcement might arrive later.
    - **SCD Logic**: Find the previous active update for this (`scid`, `direction`) tuple.
    - Update the previous row: `set valid_to = current_timestamp`.
    - Insert the new row into `channel_updates` with `valid_from` = `current_timestamp`.

## 📈 Monitoring & Metrics
The service exposes real-time metrics for Grafana visualization.

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `ln_gossip_received_total` | Counter | Total raw messages received (labeled by `source_node`). |
| `ln_gossip_new_total` | Counter | Total **globally new** messages (Inventory inserts). |
| `ln_db_insert_latency_seconds` | Histogram | Time taken to perform DB transactions. |
| `ln_orphan_updates_total` | Counter | Number of channel updates received for unknown channels. |

### Insights Goals
This setup allows us to answer complex queries via Grafana/SQL:
- *"Which collector (Alice/Bob) hears about fee changes faster?"*
- *"What is the average propagation delay of a block-mined channel?"*
- *"Show me the fee history of Channel X over the last year."*

## 🚀 Running the ServiceThe service is containerized and managed via Docker Compose.
```sh
# Build and Run
docker compose up -d --build gossip-processor
```

# Check Logs
```sh
docker logs -f gossip-processor
```
| Variable | Default | Description |
| :--- | :--- | :--- |
| `ZMQ_SOURCES` | `tcp://127.0.0.1:5675` | Comma-separated list of ZMQ endpoints. |
| `POSTGRES_URI` | `postgresql://...` | Connection string for the database. |
| `LOG_LEVEL` | `INFO` | Verbosity of logs (`DEBUG`, `INFO`, `WARN`). |


## Deploy and Publish
Run this command from inside this projects root directory. 
Please make sure to choose an appropriate <TAG>
```sh
docker buildx build --platform linux/amd64 -t ghcr.io/ln-history/gossip-processor:<TAG> --push .
```
Push it to the GitHub container registry:
```sh
docker push ghcr.io/ln-history/gossip-processor:<TAG> 
```
