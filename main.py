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

from lnhistoryclient.parser import parser_factory
from lnhistoryclient.parser.common import strip_known_message_type, varint_decode

# --- CONFIGURATION ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POSTGRES_URI = os.getenv("POSTGRES_URI")
ZMQ_SOURCES = os.getenv("ZMQ_SOURCES", "tcp://host.docker.internal:5675,tcp://host.docker.internal:5676").split(",")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("processor")

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

    def _handle_node_announcement(self, cur, gossip_id, dt, raw_bytes, data):
        node_id = data['node_id']
        
        # Ensure public node exists
        cur.execute("""
            INSERT INTO nodes (node_id, first_seen, last_seen)
            VALUES (%s, %s, %s)
            ON CONFLICT (node_id) DO UPDATE SET last_seen = EXCLUDED.last_seen
        """, (node_id, dt, dt))

        # SCD Type 2: Close previous
        cur.execute("""
            UPDATE node_announcements SET valid_to = %s 
            WHERE node_id = %s AND valid_to IS NULL
        """, (dt, node_id))

        # Insert new
        cur.execute("""
            INSERT INTO node_announcements 
            (gossip_id, node_id, valid_from, signature, features, rgb_color, alias, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, node_id, dt,
            data['signature'],
            bytes.fromhex(data['features']),
            data['rgb_color'],
            data['alias'],
            raw_bytes
        ))

    def _handle_channel_announcement(self, cur, gossip_id, dt, raw_bytes, data):
        # Your library likely returns scid as a string or parsed object
        # We need to convert it to int for our DB optimization
        # Assuming your lib might return "800000x12x1" or similar
        scid_str = data.get('scid')
        scid_int = self._parse_scid_to_int(scid_str)
        
        # Ensure both nodes exist
        for nid in [data['node_id_1'], data['node_id_2']]:
            cur.execute("INSERT INTO nodes (node_id, first_seen, last_seen) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (nid, dt, dt))

        cur.execute("""
            INSERT INTO channels 
            (gossip_id, scid, source_node_id, target_node_id, 
             node_signature_1, node_signature_2, bitcoin_signature_1, bitcoin_signature_2,
             features, chain_hash, bitcoin_key_1, bitcoin_key_2, raw_gossip)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scid) DO NOTHING
        """, (
            gossip_id, scid_int,
            data['node_id_1'], data['node_id_2'],
            data['node_signature_1'], data['node_signature_2'],
            data['bitcoin_signature_1'], data['bitcoin_signature_2'],
            bytes.fromhex(data['features']),
            data['chain_hash'],
            data['bitcoin_key_1'], data['bitcoin_key_2'],
            raw_bytes
        ))

    def _handle_channel_update(self, cur, gossip_id, dt, raw_bytes, data):
        scid_str = data.get('scid')
        scid_int = self._parse_scid_to_int(scid_str)
        
        # Channel flags: bit 0 indicates direction (0=Node1, 1=Node2)
        flags = int(data.get('channel_flags', 0))
        direction = flags & 1
        
        # SCD Type 2
        cur.execute("""
            UPDATE channel_updates SET valid_to = %s 
            WHERE scid = %s AND direction = %s::bit AND valid_to IS NULL
        """, (dt, scid_int, str(direction)))

        cur.execute("""
            INSERT INTO channel_updates 
            (gossip_id, scid, direction, valid_from,
             signature, chain_hash, message_flags, channel_flags,
             cltv_expiry_delta, htlc_minimum_msat, fee_base_msat, 
             fee_proportional_millionths, htlc_maximum_msat, raw_gossip)
            VALUES (%s, %s, %s::bit, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gossip_id, scid_int, str(direction), dt,
            data['signature'], data['chain_hash'],
            data['message_flags'], data['channel_flags'],
            data['cltv_expiry_delta'], data['htlc_minimum_msat'],
            data['fee_base_msat'], data['fee_proportional_millionths'],
            data['htlc_maximum_msat'],
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
            # This contains the full sequence: [VarInt Length] + [Type] + [Body]
            raw_bytes_full = bytes.fromhex(raw_hex)
            
            # 2. Calculate ID from the FULL RAW MESSAGE
            # We hash exactly what we received and what we will store.
            gossip_id = self.calculate_gossip_id(raw_bytes_full)

            # 3. Prepare data for the Parser (Needs [Body] only)
            # First, strip the VarInt to get [Type + Body]
            raw_payload_with_type = self._strip_varint_len(raw_bytes_full)
            
            # Second, strip the 2-byte Type to get [Body]
            # This is what lnhistoryclient expects
            payload_body_only = strip_known_message_type(raw_payload_with_type)
            
            # 4. Parse using lnhistoryclient
            msg_type = metadata.get('type')
            parsed_obj = None
            
            try:
                parser_func = parser_factory.get_parser_by_message_type(msg_type)
                parsed_obj = parser_func(payload_body_only)
            except Exception as e:
                # If parsing fails, we log it but proceed to store the raw blob
                logger.warning(f"Failed to parse message type {msg_type}: {e}")
                return
            
            collector_node_id = metadata.get('sender_node_id')
            timestamp = metadata.get('timestamp')
            dt = datetime.fromtimestamp(timestamp, timezone.utc)
            
            # 5. DB Operations
            # We pass 'raw_bytes_full' to insert_content so the DB stores [VarInt + Type + Body]
            self.db.register_collector(collector_node_id, dt)
            
            is_new = self.db.insert_content(gossip_id, msg_type, raw_bytes_full, parsed_obj, timestamp)
            
            self.db.insert_observation(gossip_id, collector_node_id, dt)

            if is_new:
                self.processed_count += 1
                if self.processed_count % 50 == 0:
                    logger.info(f"Processed {self.processed_count} new messages. Last type: {msg_type}")
            
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