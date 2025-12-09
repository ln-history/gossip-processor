import os
import json
import logging
import hashlib
import threading
import time
import signal
import sys
from queue import Queue, Empty
from datetime import datetime, timezone

import zmq
from psycopg_pool import ConnectionPool
from prometheus_client import start_http_server, Counter

from lnhistoryclient.parser import parser_factory
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI")
ZMQ_SOURCES = os.getenv("ZMQ_SOURCES", "tcp://host.docker.internal:5675,tcp://host.docker.internal:5676").split(",")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("processor")

# Define Metrics
MSG_COUNTER = Counter('gossip_messages_total', 'Total gossip messages', ['type', 'source'])

class Database:
    def __init__(self, conn_str):
        self.pool = ConnectionPool(conn_str, min_size=4, max_size=20)
        self.pool.wait()
        logger.info("✅ Connected to PostgreSQL")

    def register_collector(self, collector_node_id, seen_at_dt):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO collectors (node_id, last_collection_at, total_messages_collected)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (node_id) DO UPDATE 
                    SET last_collection_at = EXCLUDED.last_collection_at,
                        total_messages_collected = collectors.total_messages_collected + 1
                """, (collector_node_id, seen_at_dt))

    def insert_observation(self, gossip_id, collector_node_id, seen_at_dt):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO gossip_observations (gossip_id, collector_node_id, seen_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (gossip_id, collector_node_id) DO NOTHING
                """, (gossip_id, collector_node_id, seen_at_dt))

    def insert_content(self, gossip_id, msg_type, raw_bytes, parsed_obj, timestamp_int):
        """
        Inserts content using the parsed object from lnhistoryclient.
        """
        dt = datetime.fromtimestamp(timestamp_int, timezone.utc)
        
        # Convert dataclass to dict for easy access
        data = parsed_obj.to_dict()

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                # 1. Inventory Check
                cur.execute("""
                    INSERT INTO gossip_inventory (gossip_id, type, first_seen_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (gossip_id) DO NOTHING
                """, (gossip_id, msg_type, dt))
                
                if cur.rowcount == 0:
                    return False # Duplicate

                # 2. Insert Content
                if msg_type == 257: # Node Announcement
                    self._handle_node_announcement(cur, gossip_id, dt, raw_bytes, data)

                elif msg_type == 256: # Channel Announcement
                    self._handle_channel_announcement(cur, gossip_id, dt, raw_bytes, data)

                elif msg_type == 258: # Channel Update
                    self._handle_channel_update(cur, gossip_id, dt, raw_bytes, data)

                return True
            
    def insert_node_addresses(self, cur, gossip_id, addresses):
        """
        Parses the list of address dicts from lnhistoryclient and inserts them.
        """
        if not addresses:
            return

        for addr_dict in addresses:
            # 1. Extract Nested Type ID
            type_obj = addr_dict.get('typ')
            type_id = None
            
            if isinstance(type_obj, dict):
                type_id = type_obj.get('id')
            elif isinstance(type_obj, int):
                # Fallback just in case raw int is passed
                type_id = type_obj
            
            address_str = addr_dict.get('addr')
            port = addr_dict.get('port')

            # Only insert if we have valid data
            if type_id is not None and address_str:
                cur.execute("""
                    INSERT INTO node_addresses (gossip_id, type_id, address, port)
                    VALUES (%s, %s, %s, %s)
                """, (gossip_id, type_id, address_str, port))

    def _handle_channel_announcement(self, cur, gossip_id, dt, raw_bytes, data):
        """
        Handles Channel Announcement (Type 256).
        Edge Case: Nodes 1 & 2 might not exist yet. We must create placeholder entries for them.
        """
        # 1. Parse SCID to Integer (Critical for DB performance)
        scid_str = data.get('scid') 
        scid_int = self._parse_scid_to_int(scid_str)
        
        node1 = data.get('node_id_1')
        node2 = data.get('node_id_2')

        # 2. EDGE CASE: Ensure both nodes exist in the 'nodes' table.
        # We use ON CONFLICT DO NOTHING because we only want to ensure the ID exists.
        # We do NOT insert into 'node_announcements' (raw_gossip) because we haven't seen their metadata yet.
        # This effectively creates a "Ghost Node" or "Stub" that satisfies the Foreign Key.
        for nid in [node1, node2]:
            if nid:
                cur.execute("""
                    INSERT INTO nodes (node_id, first_seen, last_seen) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (node_id) DO UPDATE 
                    SET last_seen = GREATEST(nodes.last_seen, EXCLUDED.last_seen)
                """, (nid, dt, dt))

        # 3. Insert the Channel
        cur.execute("""
            INSERT INTO channels 
            (gossip_id, scid, source_node_id, target_node_id, 
             node_signature_1, node_signature_2, bitcoin_signature_1, bitcoin_signature_2,
             features, chain_hash, bitcoin_key_1, bitcoin_key_2, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scid) DO NOTHING
        """, (
            gossip_id, scid_int,
            node1, node2,
            data.get('node_signature_1'), data.get('node_signature_2'),
            data.get('bitcoin_signature_1'), data.get('bitcoin_signature_2'),
            bytes.fromhex(data.get('features', '')) if data.get('features') else None,
            data.get('chain_hash'),
            data.get('bitcoin_key_1'), data.get('bitcoin_key_2'),
            raw_bytes
        ))

    def _handle_node_announcement(self, cur, gossip_id, dt, raw_bytes, data):
        """
        Handles Node Announcement (Type 257).
        Update logic: Closes previous validity ranges (SCD Type 2).
        """
        node_id = data.get('node_id')
        
        if not node_id:
            logger.warning(f"Node Announcement missing node_id. Gossip ID: {gossip_id}")
            return

        # 1. Ensure public node exists (or update 'last_seen' if it was just a stub from a channel)
        cur.execute("""
            INSERT INTO nodes (node_id, first_seen, last_seen)
            VALUES (%s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET last_seen = EXCLUDED.last_seen
        """, (node_id, dt, dt))

        # 2. SCD Type 2 Logic: "Retire" the previous active record
        # We find the record for this node that is currently open (valid_to IS NULL) and close it.
        cur.execute("""
            UPDATE node_announcements SET valid_to = %s 
            WHERE node_id = %s AND valid_to IS NULL
        """, (dt, node_id))

        # 3. Insert the new active record
        cur.execute("""
            INSERT INTO node_announcements 
            (gossip_id, node_id, valid_from, signature, features, rgb_color, alias, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, node_id, dt,
            data.get('signature'),
            bytes.fromhex(data.get('features', '')) if data.get('features') else None,
            data.get('rgb_color'),
            data.get('alias'),
            raw_bytes
        ))
        
        # 4. Handle Addresses (Normalized Table)
        # We call the helper to insert into 'node_addresses' linking to THIS gossip_id
        self.insert_node_addresses(cur, gossip_id, data.get('addresses', []))

    def _handle_channel_update(self, cur, gossip_id, dt, raw_bytes, data):
        """
        Handles Channel Update (Type 258).
        Edge Case: Orphan Updates (Channel doesn't exist yet).
        """
        scid_str = data.get('scid')
        scid_int = self._parse_scid_to_int(scid_str)
        
        # Channel flags bit 0: 0 = Node1, 1 = Node2
        flags = int(data.get('channel_flags', 0))
        direction = flags & 1
        
        # 1. ORPHAN CHECK
        # We check if the channel exists. If not, we log a warning but still insert 
        cur.execute("SELECT 1 FROM channels WHERE scid = %s", (scid_int,))
        if cur.fetchone() is None:
            # Log it so you can track how often this happens via Grafana (logs panel)
            logger.warning(f"Orphan Update: SCID {scid_int} not found in channels table. Storing anyway.")

        # 2. SCD Type 2 Logic: Close previous update for THIS direction
        # We only close the update one matching our direction.
        cur.execute("""
            UPDATE channel_updates SET valid_to = %s 
            WHERE scid = %s AND direction = %s::bit AND valid_to IS NULL
        """, (dt, scid_int, str(direction)))

        # 3. Insert new update
        cur.execute("""
            INSERT INTO channel_updates 
            (gossip_id, scid, direction, valid_from,
             signature, chain_hash, message_flags, channel_flags,
             cltv_expiry_delta, htlc_minimum_msat, fee_base_msat, 
             fee_proportional_millionths, htlc_maximum_msat, raw_gossip)
            VALUES (%s, %s, %s::bit, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, scid_int, str(direction), dt,
            data.get('signature'), data.get('chain_hash'),
            data.get('message_flags'), data.get('channel_flags'),
            data.get('cltv_expiry_delta'), data.get('htlc_minimum_msat'),
            data.get('fee_base_msat'), data.get('fee_proportional_millionths'),
            data.get('htlc_maximum_msat'),
            raw_bytes
        ))

    def _parse_scid_to_int(self, scid):
        """Handles various formats your lib might return"""
        if isinstance(scid, int): return scid
        if not scid or 'x' not in str(scid): return None
        try:
            parts = str(scid).split('x')
            return (int(parts[0]) << 40) | (int(parts[1]) << 16) | int(parts[2])
        except:
            return None

class GossipProcessor:
    def __init__(self):
        self.db = Database(POSTGRES_URI)
        self.running = True
        self.queue = Queue()
        self.processed_count = 0

    def _strip_varint_len(self, data: bytes) -> bytes:
        """
        Detects and strips the Bitcoin-style VarInt length prefix.
        Returns the remaining data (which starts with the 2-byte msg type).
        """
        if len(data) < 1:
            return data
            
        first = data[0]
        
        # Determine VarInt size based on the first byte prefix
        if first < 0xfd:
            # 1 byte VarInt (values < 0xfd)
            return data[1:]
        elif first == 0xfd:
            # 3 bytes total (prefix + uint16)
            return data[3:]
        elif first == 0xfe:
            # 5 bytes total (prefix + uint32)
            return data[5:]
        elif first == 0xff:
            # 9 bytes total (prefix + uint64)
            return data[9:]
        
        return data

    def calculate_gossip_id(self, raw_bytes: bytes) -> str:
        return hashlib.sha256(raw_bytes).hexdigest()

    def process_msg(self, msg):
        try:
            metadata = msg.get('metadata', {})
            raw_hex = msg.get('raw_hex')
            
            if not raw_hex or not metadata:
                return

            # 1. Decode Hex to Bytes
            raw_bytes_full = bytes.fromhex(raw_hex)
            
            # 2. Calculate ID from the FULL RAW MESSAGE
            gossip_id = self.calculate_gossip_id(raw_bytes_full)

            # 3. Prepare data for the Parser
            raw_payload_with_type = self._strip_varint_len(raw_bytes_full)
            payload_body_only = strip_known_message_type(raw_payload_with_type)
            
            # 4. Parse using lnhistoryclient
            msg_type = metadata.get('type')
            parsed_obj = None
            parsed_obj: ChannelUpdate | ChannelAnnouncement | NodeAnnouncement
            
            try:
                parser_func = parser_factory.get_parser_by_message_type(msg_type)
                parsed_obj = parser_func(payload_body_only)
            except Exception as e:
                logger.warning(f"Failed to parse message type {msg_type}: {e}")
                return

            collector_node_id = metadata.get('sender_node_id')
            
            # Use the timestamp from the MESSAGE payload if available (more accurate for "Gossip Time")
            # Fallback to metadata timestamp (Receipt Time)
            gossip_ts = metadata.get('timestamp')
            
            # Try to extract actual gossip timestamp from parsed object for better logging
            if hasattr(parsed_obj, 'timestamp'):
                gossip_ts = parsed_obj.timestamp
            
            dt = datetime.fromtimestamp(gossip_ts, timezone.utc)
            
            # Calculate Lag (How old is this message?)
            now = datetime.now(timezone.utc)
            lag_seconds = (now - dt).total_seconds()

            MSG_COUNTER.labels(type=msg_type, source=collector_node_id).inc()
            
            # 5. DB Operations
            # Note: We register collector using 'now' (receipt time) to track aliveness, 
            # but we store content using 'dt' (gossip time).
            receipt_dt = datetime.fromtimestamp(metadata.get('timestamp'), timezone.utc)
            
            self.db.register_collector(collector_node_id, receipt_dt)
            
            # Insert Content
            is_new = self.db.insert_content(gossip_id, msg_type, raw_bytes_full, parsed_obj, gossip_ts)
            
            # Log Observation
            self.db.insert_observation(gossip_id, collector_node_id, receipt_dt)

            if is_new:
                self.processed_count += 1
                
                # ENHANCED LOGGING
                # Log detailed info every 50 messages OR if lag is > 1 hour (catching up)
                if self.processed_count % 50 == 0 or lag_seconds > 3600:
                    type_name = "Unknown"
                    if msg_type == 256: type_name = "channel_announcement"
                    elif msg_type == 257: type_name = "node_announcement"
                    elif msg_type == 258: type_name = "channel_update"
                    
                    logger.info(
                        f"Processed {self.processed_count} | "
                        f"Type: {type_name} ({msg_type}) | "
                        f"Time: {dt.strftime('%H:%M:%S')} (Lag: {lag_seconds:.0f}s) | "
                        f"Source: {collector_node_id[:8]}..."
                    )
            
        except Exception as e:
            logger.error(f"Processing Error: {e}", exc_info=True)


    def zmq_worker(self, zmq_uri):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        try:
            sock.connect(zmq_uri)
            sock.setsockopt_string(zmq.SUBSCRIBE, "")
            logger.info(f"🎧 Connected to ZMQ: {zmq_uri}")
            
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLIN)

            while self.running:
                events = dict(poller.poll(1000))
                if sock in events:
                    topic, msg_bytes = sock.recv_multipart()
                    self.queue.put(json.loads(msg_bytes.decode('utf-8')))
        except Exception as e:
            logger.error(f"ZMQ Error {zmq_uri}: {e}")
        finally:
            sock.close()

    def db_worker(self):
        logger.info("💾 DB Worker started")
        while self.running:
            try:
                msg = self.queue.get(timeout=1)
                self.process_msg(msg)
                self.queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error(f"DB Worker Error: {e}")

    def start(self):
        # Start Metrics Server on Port 8000
        start_http_server(8000)
        logger.info("--- Metrics server started on port 8000 ---")

        threads = []
        for src in ZMQ_SOURCES:
            t = threading.Thread(target=self.zmq_worker, args=(src,), daemon=True)
            t.start()
            threads.append(t)

        db_t = threading.Thread(target=self.db_worker, daemon=True)
        db_t.start()
        threads.append(db_t)

        def signal_handler(sig, frame):
            logger.info("Stopping...")
            self.running = False
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while self.running:
            time.sleep(1)

if __name__ == "__main__":
    processor = GossipProcessor()
    processor.start()