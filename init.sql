-- ==========================================
-- 1. MASTER REGISTRY
-- ==========================================
CREATE TABLE IF NOT EXISTS gossip_inventory (
    gossip_id VARCHAR(64) PRIMARY KEY,
    type INT NOT NULL, -- 256, 257, 258
    first_seen_at TIMESTAMPTZ DEFAULT NOW()
    -- NOTE: raw_gossip moved to content tables for snapshot optimization
);

-- ==========================================
-- 2. INFRASTRUCTURE & NODES
-- ==========================================
CREATE TABLE IF NOT EXISTS nodes (
    node_id VARCHAR(66) PRIMARY KEY,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS collectors (
    node_id VARCHAR(66) PRIMARY KEY,
    alias VARCHAR(100),
    first_collection_at TIMESTAMPTZ DEFAULT NOW(),
    last_collection_at TIMESTAMPTZ,
    total_messages_collected BIGINT DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS gossip_observations (
    gossip_id VARCHAR(64) REFERENCES gossip_inventory(gossip_id),
    collector_node_id VARCHAR(66) REFERENCES collectors(node_id),
    seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (gossip_id, collector_node_id)
);

-- ==========================================
-- 3. ADDRESS NORMALIZATION
-- ==========================================
CREATE TABLE IF NOT EXISTS address_types (
    id INT PRIMARY KEY,
    name VARCHAR(20),
    description VARCHAR(100)
);

INSERT INTO address_types (id, name, description) VALUES
(1, 'IPv4', 'Standard IPv4 address'),
(2, 'IPv6', 'Standard IPv6 address'),
(3, 'TorV2', 'Deprecated Tor v2 onion service'),
(4, 'TorV3', 'Tor v3 onion service'),
(5, 'DNS', 'DNS hostname')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS node_addresses (
    id BIGSERIAL PRIMARY KEY,
    gossip_id VARCHAR(64), -- FK added at bottom
    type_id INT REFERENCES address_types(id),
    address VARCHAR(255),
    port INT
);

-- ==========================================
-- 4. CONTENT TABLES (Time-Travel Enabled)
-- ==========================================

-- Type 257: Node Announcement
CREATE TABLE IF NOT EXISTS node_announcements (
    gossip_id VARCHAR(64) PRIMARY KEY REFERENCES gossip_inventory(gossip_id),
    node_id VARCHAR(66) REFERENCES nodes(node_id),
    
    -- Validity Range (SCD Type 2)
    valid_from TIMESTAMPTZ NOT NULL, 
    valid_to TIMESTAMPTZ,            -- NULL means "Current"
    
    signature VARCHAR(144),
    features BYTEA,
    rgb_color VARCHAR(6),
    alias VARCHAR(32),
    
    raw_gossip BYTEA -- Stored here for fast snapshots
);

-- Type 256: Channel Announcement
CREATE TABLE IF NOT EXISTS channels (
    gossip_id VARCHAR(64) PRIMARY KEY REFERENCES gossip_inventory(gossip_id),
    scid BIGINT UNIQUE, -- BigInt for performance (800000 << 40 ...)
    
    -- Chain Data (Enriched by external service)
    funding_timestamp TIMESTAMPTZ, 
    closing_timestamp TIMESTAMPTZ, -- NULL means "Open"
    capacity_sat BIGINT,

    source_node_id VARCHAR(66) REFERENCES nodes(node_id),
    target_node_id VARCHAR(66) REFERENCES nodes(node_id),
    
    node_signature_1 VARCHAR(144),
    node_signature_2 VARCHAR(144),
    bitcoin_signature_1 VARCHAR(144),
    bitcoin_signature_2 VARCHAR(144),
    features BYTEA,
    chain_hash VARCHAR(64),
    bitcoin_key_1 VARCHAR(66),
    bitcoin_key_2 VARCHAR(66),
    
    raw_gossip BYTEA -- Stored here for fast snapshots
);

-- Type 258: Channel Update
CREATE TABLE IF NOT EXISTS channel_updates (
    gossip_id VARCHAR(64) PRIMARY KEY REFERENCES gossip_inventory(gossip_id),
    scid BIGINT REFERENCES channels(scid), -- BigInt match
    direction BIT,
    
    -- Validity Range (SCD Type 2)
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,            -- NULL means "Current"
    
    signature VARCHAR(144),
    chain_hash VARCHAR(64),
    message_flags INT,
    channel_flags INT,
    cltv_expiry_delta INT,
    htlc_minimum_msat BIGINT,
    fee_base_msat BIGINT,
    fee_proportional_millionths BIGINT,
    htlc_maximum_msat BIGINT,
    
    raw_gossip BYTEA -- Stored here for fast snapshots
);

-- Add deferred FK for addresses
ALTER TABLE node_addresses 
ADD CONSTRAINT fk_addr_gossip 
FOREIGN KEY (gossip_id) REFERENCES node_announcements(gossip_id);

-- ==========================================
-- 5. INDICES (Optimized for Snapshots)
-- ==========================================

-- Fast Observation Stats
CREATE INDEX IF NOT EXISTS idx_obs_time ON gossip_observations USING btree (seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_obs_collector ON gossip_observations USING btree (collector_node_id);

-- Time-Travel Indices
-- Query: "Give me raw_gossip for updates active at Time T"
-- SQL: SELECT raw_gossip FROM channel_updates WHERE valid_from <= T AND (valid_to > T OR valid_to IS NULL)
CREATE INDEX IF NOT EXISTS idx_chan_upd_validity ON channel_updates USING btree (valid_from DESC, valid_to);
CREATE INDEX IF NOT EXISTS idx_node_ann_validity ON node_announcements USING btree (valid_from DESC, valid_to);

-- Channel Indices
CREATE INDEX IF NOT EXISTS idx_chan_validity ON channels USING btree (funding_timestamp, closing_timestamp);
CREATE INDEX IF NOT EXISTS idx_chan_scid ON channels USING btree (scid);

-- Feature specific indices
CREATE INDEX IF NOT EXISTS idx_chan_upd_fees ON channel_updates USING btree (fee_base_msat, fee_proportional_millionths);
CREATE INDEX IF NOT EXISTS idx_node_ann_alias ON node_announcements USING btree (alias);
CREATE INDEX IF NOT EXISTS idx_addr_lookup ON node_addresses USING btree (address);