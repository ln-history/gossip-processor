import os
import json
import logging
import hashlib
import threading
import time
import signal
import sys
import codecs
from queue import Queue, Empty
from datetime import datetime, timezone
from typing import List, TypedDict, cast

import zmq
from psycopg_pool import ConnectionPool
from prometheus_client import start_http_server, Counter, Gauge, Histogram

from lnhistoryclient.parser import parser_factory
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate, ChannelUpdateDict
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement, NodeAnnouncementDict, Address
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement, ChannelAnnouncementDict

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI")
ZMQ_SOURCES = os.getenv("ZMQ_SOURCES", "tcp://host.docker.internal:5675,tcp://host.docker.internal:5676").split(",")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gossip-processor")

# --- TYPEDEFS (Documentation-as-Code) ---

class PluginEventMetadata(TypedDict):
    type: int
    name: str
    timestamp: int
    sender_node_id: str
    length: int

class BasePluginEvent(TypedDict):
    metadata: PluginEventMetadata
    raw_hex: str

# Helper to decode aliases safely
def decode_alias(alias_bytes: bytes) -> str:
    try:
        return alias_bytes.decode("utf-8").strip("\x00")
    except UnicodeDecodeError:
        try:
            cleaned = alias_bytes.strip(b"\x00")
            return codecs.decode(cleaned, "punycode")
        except Exception:
            return alias_bytes.hex()

# --- METRICS DEFINITIONS ---
MSG_COUNTER = Counter('gossip_messages_total', 'Total gossip messages received', ['type', 'source'])
UNIQUE_MSG_COUNTER = Counter('gossip_unique_total', 'Number of unique messages (first time seen)', ['type'])
DUPLICATE_MSG_COUNTER = Counter('gossip_duplicates_total', 'Number of duplicate messages (already in DB)', ['type', 'source'])

LAG_HISTOGRAM = Histogram(
    'gossip_processing_lag_seconds', 
    'Time difference between Gossip Timestamp and Processing Time',
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0, 1800.0, 3600.0, 86400.0)
)

DB_DURATION = Histogram(
    'gossip_db_duration_seconds', 
    'Time spent executing database insertions',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0)
)

QUEUE_SIZE = Gauge('gossip_queue_depth', 'Current number of messages waiting in the internal queue')
SCD_CLOSURES = Counter('gossip_scd_closures_total', 'Number of historical records closed (SCD Type 2)', ['table'])
ORPHANS = Counter('gossip_orphans_total', 'Number of updates received for unknown channels')


class Database:
    def __init__(self, conn_str):
        self.pool = ConnectionPool(conn_str, min_size=4, max_size=20)
        self.pool.wait()
        logger.info("--- Connected to PostgreSQL ---")

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
        Parses the list of address dicts using lnhistoryclient and inserts them.
        """
        dt = datetime.fromtimestamp(timestamp_int, timezone.utc)

        with DB_DURATION.time():
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
                    # parsed_obj is a strongly typed dataclass here
                    if msg_type == 257: 
                        self._handle_node_announcement(cur, gossip_id, dt, raw_bytes, parsed_obj)
                    elif msg_type == 256:
                        self._handle_channel_announcement(cur, gossip_id, dt, raw_bytes, parsed_obj)
                    elif msg_type == 258:
                        self._handle_channel_update(cur, gossip_id, dt, raw_bytes, parsed_obj)

                    return True
            
    def insert_node_addresses(self, cur, gossip_id, addresses: List[Address]):
        if not addresses: return
        
        for addr in addresses:
            type_id = None
            if addr.typ:
                if isinstance(addr.typ, int):
                    type_id = addr.typ
                elif hasattr(addr.typ, 'id'):
                    type_id = addr.typ.id
            
            address_str = addr.addr
            port = addr.port

            if type_id is not None and address_str:
                cur.execute("""
                    INSERT INTO node_addresses (gossip_id, type_id, address, port)
                    VALUES (%s, %s, %s, %s)
                """, (gossip_id, type_id, address_str, port))

    def _handle_channel_announcement(self, cur, gossip_id, dt, raw_bytes, data: ChannelAnnouncement):
        scid_int = data.scid
        node1 = data.node_id_1.hex()
        node2 = data.node_id_2.hex()

        # 1. EDGE CASE: Ensure both nodes exist in the 'nodes' table.
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

         # 2. Insert the Channel
        cur.execute("""
            INSERT INTO channels 
            (gossip_id, scid, source_node_id, target_node_id, 
             node_signature_1, node_signature_2, bitcoin_signature_1, bitcoin_signature_2,
             features, chain_hash, bitcoin_key_1, bitcoin_key_2, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scid) DO NOTHING
        """, (
            gossip_id, scid_int, node1, node2,
            data.node_signature_1.hex(), data.node_signature_2.hex(),
            data.bitcoin_signature_1.hex(), data.bitcoin_signature_2.hex(),
            data.features, # BYTEA
            data.chain_hash.hex(),
            data.bitcoin_key_1.hex(), data.bitcoin_key_2.hex(),
            raw_bytes
        ))

    def _handle_node_announcement(self, cur, gossip_id, dt, raw_bytes, data: NodeAnnouncement):
        node_id = data.node_id.hex()
        if not node_id: return

        # 1. Ensure public node stub exists
        cur.execute("""
            INSERT INTO nodes (node_id, first_seen, last_seen)
            VALUES (%s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET last_seen = EXCLUDED.last_seen
        """, (node_id, dt, dt))

        # 2. SCD Logic: Try to close the PREVIOUS active record
        # Added 'AND valid_from < %s' to prevent the "Range Error" crash.
        # We only close the active record if we are actually newer than it.
        cur.execute("""
            UPDATE node_announcements SET valid_to = %s 
            WHERE node_id = %s AND valid_to IS NULL AND valid_from < %s
        """, (dt, node_id, dt))
        
        # Determine valid_to for the NEW record
        if cur.rowcount > 0:
            SCD_CLOSURES.labels(table='node_announcements').inc()
            # We successfully closed the old tip. We are now the new tip.
            valid_to = None
        else:
            # We failed to update. Either:
            # A) No previous record exists (First time seeing node) -> valid_to = None
            # B) We are OLDER than the current tip (Out-of-order) -> valid_to = tip.valid_from
            
            cur.execute("SELECT valid_from FROM node_announcements WHERE node_id = %s AND valid_to IS NULL", (node_id,))
            current_tip = cur.fetchone()
            
            if current_tip:
                # Case B: We are history. Fill the gap before the current tip starts.
                valid_to = current_tip[0]
                
                # Edge case: If we are effectively simultaneous or older than existing history
                if dt >= valid_to:
                    return # Skip this message to avoid unique constraint violations or zero-length ranges
            else:
                # Case A: Brand new node
                valid_to = None

        # 3. Insert the record with the calculated valid_to
        cur.execute("""
            INSERT INTO node_announcements 
            (gossip_id, node_id, valid_from, valid_to, signature, features, rgb_color, alias, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, 
            node_id, 
            dt, 
            valid_to,
            data.signature.hex(),
            data.features, 
            data.rgb_color.hex(),
            decode_alias(data.alias),
            raw_bytes
        ))
        
        self.insert_node_addresses(cur, gossip_id, data._parse_addresses)

    def _handle_channel_update(self, cur, gossip_id, dt, raw_bytes, data: ChannelUpdate):
        scid_int = data.scid
        direction = data.direction
        
        # 1. Orphan Check (This is just a warning, not an error)
        cur.execute("SELECT 1 FROM channels WHERE scid = %s", (scid_int,))
        if cur.fetchone() is None:
            ORPHANS.inc()
            logger.warning(f"Orphan Update: SCID {scid_int} not found. Storing anyway.")

        # 2. SCD Logic: Try to close previous update
        # Added 'AND valid_from < %s'
        cur.execute("""
            UPDATE channel_updates SET valid_to = %s 
            WHERE scid = %s AND direction = %s::bit AND valid_to IS NULL AND valid_from < %s
        """, (dt, scid_int, str(direction), dt))

        if cur.rowcount > 0:
            SCD_CLOSURES.labels(table='channel_updates').inc()
            valid_to = None
        else:
            # Out-of-order handling
            cur.execute("""
                SELECT valid_from FROM channel_updates 
                WHERE scid = %s AND direction = %s::bit AND valid_to IS NULL
            """, (scid_int, str(direction)))
            current_tip = cur.fetchone()
            
            if current_tip:
                valid_to = current_tip[0]
                if dt >= valid_to: return
            else:
                valid_to = None

        # 3. Insert
        cur.execute("""
            INSERT INTO channel_updates 
            (gossip_id, scid, direction, valid_from, valid_to,
             signature, chain_hash, message_flags, channel_flags,
             cltv_expiry_delta, htlc_minimum_msat, fee_base_msat, 
             fee_proportional_millionths, htlc_maximum_msat, raw_gossip)
            VALUES (%s, %s, %s::bit, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, 
            scid_int, 
            str(direction), 
            dt, 
            valid_to,
            data.signature.hex(), 
            data.chain_hash.hex(),
            int.from_bytes(data.message_flags, 'big'), 
            int.from_bytes(data.channel_flags, 'big'),
            data.cltv_expiry_delta, 
            data.htlc_minimum_msat,
            data.fee_base_msat, 
            data.fee_proportional_millionths,
            data.htlc_maximum_msat,
            raw_bytes
        ))


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

    def process_msg(self, msg: BasePluginEvent):
        """
        Process a single gossip message.
        Expected format: BasePluginEvent (Metadata + Hex String)
        """
        try:
            # Type-Safe Access
            raw_hex = msg['raw_hex']
            metadata = msg['metadata']
            
            if not raw_hex: return

            raw_bytes_full = bytes.fromhex(raw_hex)
            gossip_id = self.calculate_gossip_id(raw_bytes_full)
            raw_payload_with_type = self._strip_varint_len(raw_bytes_full)
            payload_body_only = strip_known_message_type(raw_payload_with_type)
            msg_type = metadata['type']
            
            parsed_obj = None
            parsed_obj: ChannelUpdate | ChannelAnnouncement | NodeAnnouncement
            try:
                parser_func = parser_factory.get_parser_by_message_type(msg_type)
                parsed_obj = parser_func(payload_body_only)
            except Exception as e:
                # Silence known internal messages if parser fails, else warn
                if msg_type not in [256, 257, 258]: return
                logger.warning(f"Failed to parse message type {msg_type}: {e}")
                return

            collector_node_id = metadata['sender_node_id']
            
            # --- METRICS & TIMESTAMP LOGIC ---
            receipt_ts_int = metadata['timestamp']
            receipt_dt = datetime.fromtimestamp(receipt_ts_int, timezone.utc)
            
            gossip_ts_int = receipt_ts_int
            if hasattr(parsed_obj, 'timestamp'):
                gossip_ts_int = parsed_obj.timestamp
            
            # Calculate Lag (How old is this message?)
            now = datetime.now(timezone.utc)
            lag_seconds = (now - receipt_dt).total_seconds()

            MSG_COUNTER.labels(type=msg_type, source=collector_node_id).inc()
            LAG_HISTOGRAM.observe(lag_seconds)

            # FILTER: IGNORE INTERNAL MESSAGES
            if msg_type not in [256, 257, 258]:
                return

            # DB Operations
            self.db.register_collector(collector_node_id, receipt_dt)
            is_new = self.db.insert_content(gossip_id, msg_type, raw_bytes_full, parsed_obj, gossip_ts_int)
            self.db.insert_observation(gossip_id, collector_node_id, receipt_dt)

            if is_new:
                self.processed_count += 1
                UNIQUE_MSG_COUNTER.labels(type=msg_type).inc()
                
                if self.processed_count % 50 == 0 or lag_seconds > 600:
                    type_name = "Unknown"
                    if msg_type == 256: type_name = "channel_announcement"
                    elif msg_type == 257: type_name = "node_announcement"
                    elif msg_type == 258: type_name = "channel_update"
                    
                    logger.info(
                        f"Processed {self.processed_count} | "
                        f"Type: {type_name} ({msg_type}) | "
                        f"Receipt: {receipt_dt.strftime("%d/%m/%Y, %H:%M:%S")} (Lag: {lag_seconds:.2f}s) | "
                        f"Src: {collector_node_id[:8]}..."
                    )
            else:
                DUPLICATE_MSG_COUNTER.labels(type=msg_type, source=collector_node_id).inc()
            
        except Exception as e:
            logger.error(f"Processing Error: {e}", exc_info=True)
            
    def zmq_worker(self, zmq_uri):
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        try:
            sock.connect(zmq_uri)
            sock.setsockopt_string(zmq.SUBSCRIBE, "")
            logger.info(f"--- Connected to ZMQ: {zmq_uri} ---")
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLIN)

            while self.running:
                events = dict(poller.poll(1000))
                if sock in events:
                    topic, msg_bytes = sock.recv_multipart()
                    msg_dict = json.loads(msg_bytes.decode('utf-8'))
                    self.queue.put(cast(BasePluginEvent, msg_dict))
        except Exception as e:
            logger.error(f"ZMQ Error {zmq_uri}: {e}")
        finally:
            sock.close()

    def db_worker(self):
        logger.info("--- DB Worker started ---")
        while self.running:
            QUEUE_SIZE.set(self.queue.qsize())
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
            logger.info("--- Stopping... ---")
            self.running = False
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while self.running:
            time.sleep(1)

if __name__ == "__main__":
    processor = GossipProcessor()
    processor.start()