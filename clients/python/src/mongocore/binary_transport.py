"""Binary UDS transport for MongoCore — high-performance local protocol."""
import os
import struct
import socket
from typing import Optional, Any

import bson

HEADER_SIZE = 10
DEFAULT_SOCKET_PATH = "/tmp/mongocore.bin.sock"
DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024

# Opcodes (bits 0-5 of flags)
OP_FIND = 0x01
OP_FIND_ONE = 0x02
OP_INSERT_ONE = 0x03
OP_INSERT_MANY = 0x04
OP_UPDATE_ONE = 0x05
OP_UPDATE_MANY = 0x06
OP_DELETE_ONE = 0x07
OP_DELETE_MANY = 0x08
OP_AGGREGATE = 0x09
OP_COUNT = 0x0A
OP_RUN_COMMAND = 0x0D
OP_ERROR = 0x3E
OP_HANDSHAKE = 0x3F

# Flag bits (bit 0 = LSB)
FLAG_EOS = 1 << 6
FLAG_NO_REPLY = 1 << 7
FLAG_BATCH_FOLLOWS = 1 << 8


class BinaryTransportError(Exception):
    """Error from the binary transport layer."""
    pass


class BinaryTransport:
    """High-performance binary transport over Unix domain sockets.

    Connects to MongoCore's binary UDS listener for lower-latency,
    lower-overhead communication compared to gRPC.

    Usage:
        transport = BinaryTransport()
        transport.connect()
        doc = transport.find_one("mydb", "mycoll", {"name": "Alice"})
        transport.close()
    """

    def __init__(self, socket_path: Optional[str] = None):
        """Initialize the binary transport.

        Args:
            socket_path: Path to the Unix domain socket. Falls back to
                MONGOCORE_BINARY_SOCKET_PATH env var, then default path.
        """
        self._socket_path = (
            socket_path
            or os.environ.get("MONGOCORE_BINARY_SOCKET_PATH")
            or DEFAULT_SOCKET_PATH
        )
        self._sock: Optional[socket.socket] = None
        self._req_counter = 0

    def connect(self):
        """Connect to the MongoCore binary UDS and perform handshake.

        Raises:
            BinaryTransportError: If connection or handshake fails.
        """
        try:
            self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._sock.connect(self._socket_path)
        except (OSError, ConnectionRefusedError) as e:
            self._sock = None
            raise BinaryTransportError(f"Failed to connect to {self._socket_path}: {e}") from e

        self._handshake()
        return self

    def close(self):
        """Close the socket connection."""
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def find(
        self,
        db: str,
        collection: str,
        filter: Optional[dict] = None,
        limit: int = 0,
    ) -> list[dict]:
        """Find multiple documents matching the filter.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document (matches all if None/empty).
            limit: Maximum number of documents to return (0 = no limit).

        Returns:
            List of matched documents.
        """
        envelope: dict[str, Any] = {"db": db, "coll": collection, "filter": filter or {}}
        if limit:
            envelope["limit"] = limit

        self._send_frame(OP_FIND, envelope)
        resp_envelope, raw_docs = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])

        # Parse concatenated BSON docs from raw_docs
        docs = []
        offset = 0
        while offset < len(raw_docs):
            doc_len = struct.unpack("<i", raw_docs[offset:offset + 4])[0]
            docs.append(bson.decode(raw_docs[offset:offset + doc_len]))
            offset += doc_len
        return docs

    def find_one(
        self,
        db: str,
        collection: str,
        filter: dict,
        projection: Optional[dict] = None,
    ) -> Optional[dict]:
        """Find a single document matching the filter.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document.
            projection: Optional projection document.

        Returns:
            The matched document, or None if no match.
        """
        envelope = {"db": db, "coll": collection, "filter": filter}
        if projection:
            envelope["projection"] = projection

        self._send_frame(OP_FIND_ONE, envelope)
        resp_envelope, raw_docs = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])

        if not raw_docs:
            return None
        return bson.decode(raw_docs)

    def insert_one(self, db: str, collection: str, document: dict) -> str:
        """Insert a single document.

        Args:
            db: Database name.
            collection: Collection name.
            document: The document to insert.

        Returns:
            The inserted document's _id as a string.
        """
        raw_doc = bson.encode(document)
        envelope = {"db": db, "coll": collection}

        self._send_frame(OP_INSERT_ONE, envelope, raw_docs=raw_doc)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return str(resp.get("inserted_id", ""))

    def insert_many(
        self,
        db: str,
        collection: str,
        documents: list[dict],
        ordered: bool = True,
    ) -> int:
        """Insert multiple documents.

        Args:
            db: Database name.
            collection: Collection name.
            documents: List of documents to insert.
            ordered: Whether to stop on first error.

        Returns:
            The number of documents inserted.
        """
        raw_docs = b"".join(bson.encode(doc) for doc in documents)
        envelope = {"db": db, "coll": collection, "ordered": ordered, "count": len(documents)}

        self._send_frame(OP_INSERT_MANY, envelope, raw_docs=raw_docs)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return resp.get("inserted_count", 0)

    def update_one(self, db: str, collection: str, filter: dict, update: dict) -> dict:
        """Update a single document.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document.
            update: Update operations document.

        Returns:
            Dict with matched_count and modified_count.
        """
        envelope = {"db": db, "coll": collection, "filter": filter, "update": update}

        self._send_frame(OP_UPDATE_ONE, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return {
            "matched_count": resp.get("matched_count", 0),
            "modified_count": resp.get("modified_count", 0),
        }

    def update_many(self, db: str, collection: str, filter: dict, update: dict) -> dict:
        """Update multiple documents.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document.
            update: Update operations document.

        Returns:
            Dict with matched_count and modified_count.
        """
        envelope = {"db": db, "coll": collection, "filter": filter, "update": update}

        self._send_frame(OP_UPDATE_MANY, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return {
            "matched_count": resp.get("matched_count", 0),
            "modified_count": resp.get("modified_count", 0),
        }

    def delete_one(self, db: str, collection: str, filter: dict) -> int:
        """Delete a single document.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document.

        Returns:
            Number of documents deleted (0 or 1).
        """
        envelope = {"db": db, "coll": collection, "filter": filter}

        self._send_frame(OP_DELETE_ONE, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return resp.get("deleted_count", 0)

    def delete_many(self, db: str, collection: str, filter: dict) -> int:
        """Delete multiple documents.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Query filter document.

        Returns:
            Number of documents deleted.
        """
        envelope = {"db": db, "coll": collection, "filter": filter}

        self._send_frame(OP_DELETE_MANY, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return resp.get("deleted_count", 0)

    def count_documents(self, db: str, collection: str, filter: Optional[dict] = None) -> int:
        """Count documents matching a filter.

        Args:
            db: Database name.
            collection: Collection name.
            filter: Optional query filter (counts all if None).

        Returns:
            The document count.
        """
        envelope = {"db": db, "coll": collection, "filter": filter or {}}

        self._send_frame(OP_COUNT, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])
        return resp.get("count", 0)

    def run_command(self, db: str, command: dict) -> dict:
        """Run an arbitrary database command.

        Args:
            db: Database name.
            command: The command document.

        Returns:
            The command response document.
        """
        envelope = {"db": db, "command": command}

        self._send_frame(OP_RUN_COMMAND, envelope)
        resp_envelope, raw_docs = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(resp["error"])

        if raw_docs:
            return bson.decode(raw_docs)
        return resp

    @staticmethod
    def is_available(socket_path: Optional[str] = None) -> bool:
        """Check if the binary transport socket exists and is connectable.

        Args:
            socket_path: Path to check. Falls back to env var, then default.

        Returns:
            True if the socket exists and accepts connections.
        """
        path = (
            socket_path
            or os.environ.get("MONGOCORE_BINARY_SOCKET_PATH")
            or DEFAULT_SOCKET_PATH
        )
        if not os.path.exists(path):
            return False

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(path)
            sock.close()
            return True
        except (OSError, ConnectionRefusedError):
            return False

    # --- Internal methods ---

    def _handshake(self):
        """Perform protocol handshake with the server."""
        envelope = {"client_language": "python"}
        self._send_frame(OP_HANDSHAKE, envelope)
        resp_envelope, _ = self._read_frame()

        resp = bson.decode(resp_envelope)
        if resp.get("error"):
            raise BinaryTransportError(f"Handshake failed: {resp['error']}")

    def _next_req_id(self) -> int:
        """Generate the next monotonic request ID."""
        self._req_counter += 1
        return self._req_counter

    def _send_frame(
        self,
        opcode: int,
        envelope_dict: dict,
        raw_docs: bytes = b"",
        no_reply: bool = False,
    ) -> int:
        """Send a framed message to the server.

        Args:
            opcode: Operation code (bits 0-5 of flags).
            envelope_dict: Envelope to BSON-encode.
            raw_docs: Raw BSON document bytes to append after envelope.
            no_reply: If True, set the no-reply flag.

        Returns:
            The request ID used for this frame.
        """
        if self._sock is None:
            raise BinaryTransportError("Not connected. Call connect() first.")

        req_id = self._next_req_id()
        envelope_bytes = bson.encode(envelope_dict)

        # flags: bits[0:5] = opcode, bit6 = EOS (always set for single frame),
        #         bit7 = no-reply, bit8 = batch-follows
        flags = (opcode & 0x3F) | FLAG_EOS
        if no_reply:
            flags |= FLAG_NO_REPLY

        # msg_len = flags(2) + req_id(4) + envelope + raw_docs
        msg_len = 2 + 4 + len(envelope_bytes) + len(raw_docs)

        # Header: 4B msg_len (BE) + 2B flags (BE) + 4B req_id (BE)
        header = struct.pack(">IHI", msg_len, flags, req_id)
        self._sock.sendall(header + envelope_bytes + raw_docs)

        return req_id

    def _read_frame(self) -> tuple[bytes, bytes]:
        """Read a complete frame from the server.

        Returns:
            Tuple of (envelope_bytes, raw_doc_bytes).

        Raises:
            BinaryTransportError: On protocol errors or disconnection.
        """
        # Read header
        header = self._recv_exact(HEADER_SIZE)
        msg_len, flags, req_id = struct.unpack(">IHI", header)

        # Validate
        payload_len = msg_len - 2 - 4  # subtract flags + req_id sizes
        if payload_len < 0:
            raise BinaryTransportError(f"Invalid frame: msg_len={msg_len} too small")
        if payload_len > DEFAULT_MAX_FRAME_SIZE:
            raise BinaryTransportError(f"Frame too large: {payload_len} bytes")

        opcode = flags & 0x3F
        if opcode == OP_ERROR:
            # Read error payload
            payload = self._recv_exact(payload_len) if payload_len > 0 else b""
            if payload:
                bson_len = struct.unpack("<i", payload[:4])[0]
                err = bson.decode(payload[:bson_len])
                raise BinaryTransportError(err.get("message", err.get("error", "Unknown server error")))
            raise BinaryTransportError("Unknown server error")

        # Read payload
        if payload_len == 0:
            return b"", b""

        payload = self._recv_exact(payload_len)

        # Split envelope from raw docs using BSON document length prefix
        if len(payload) < 4:
            return payload, b""

        # First 4 bytes of BSON doc is its length (little-endian i32)
        envelope_len = struct.unpack("<i", payload[:4])[0]
        if envelope_len < 5 or envelope_len > len(payload):
            raise BinaryTransportError(
                f"Invalid envelope length: {envelope_len} (payload={len(payload)})"
            )

        envelope_bytes = payload[:envelope_len]
        raw_doc_bytes = payload[envelope_len:]
        return envelope_bytes, raw_doc_bytes

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly n bytes from the socket.

        Args:
            n: Number of bytes to read.

        Returns:
            Exactly n bytes.

        Raises:
            BinaryTransportError: If connection closed before all bytes read.
        """
        if self._sock is None:
            raise BinaryTransportError("Not connected.")

        chunks = []
        remaining = n
        while remaining > 0:
            chunk = self._sock.recv(min(remaining, 65536))
            if not chunk:
                raise BinaryTransportError(
                    f"Connection closed (read {n - remaining}/{n} bytes)"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()


class BinaryTransportPool:
    """Connection pool for binary transport — distributes operations across multiple connections.

    Maintains a pool of BinaryTransport connections and distributes requests
    using round-robin selection. Default pool size of 4 connections supports
    high-throughput CPU-bound workloads.

    Usage:
        with BinaryTransportPool(pool_size=4) as pool:
            doc = pool.find_one("mydb", "mycoll", {"name": "Alice"})
    """

    def __init__(self, socket_path: Optional[str] = None, pool_size: int = 4):
        """Initialize the connection pool.

        Args:
            socket_path: Path to the Unix domain socket. Falls back to
                MONGOCORE_BINARY_SOCKET_PATH env var, then default path.
            pool_size: Number of connections to maintain (default: 4).
        """
        self._socket_path = (
            socket_path
            or os.environ.get("MONGOCORE_BINARY_SOCKET_PATH")
            or DEFAULT_SOCKET_PATH
        )
        self._pool_size = pool_size
        self._connections: list[BinaryTransport] = []
        self._index = 0

    def connect(self):
        """Open all connections in the pool.

        Raises:
            BinaryTransportError: If any connection fails (all opened
                connections are closed on failure).
        """
        try:
            for _ in range(self._pool_size):
                conn = BinaryTransport(self._socket_path)
                conn.connect()
                self._connections.append(conn)
        except BinaryTransportError:
            self.close()
            raise
        return self

    def close(self):
        """Close all connections in the pool."""
        for conn in self._connections:
            conn.close()
        self._connections.clear()
        self._index = 0

    def _next(self) -> BinaryTransport:
        """Round-robin connection selection."""
        if not self._connections:
            raise BinaryTransportError("Pool not connected. Call connect() first.")
        conn = self._connections[self._index % len(self._connections)]
        self._index += 1
        return conn

    def find(
        self,
        db: str,
        collection: str,
        filter: Optional[dict] = None,
        limit: int = 0,
    ) -> list[dict]:
        """Find multiple documents matching the filter."""
        return self._next().find(db, collection, filter, limit)

    def find_one(
        self,
        db: str,
        collection: str,
        filter: dict,
        projection: Optional[dict] = None,
    ) -> Optional[dict]:
        """Find a single document matching the filter."""
        return self._next().find_one(db, collection, filter, projection)

    def insert_one(self, db: str, collection: str, document: dict) -> str:
        """Insert a single document."""
        return self._next().insert_one(db, collection, document)

    def insert_many(
        self,
        db: str,
        collection: str,
        documents: list[dict],
        ordered: bool = True,
    ) -> int:
        """Insert multiple documents."""
        return self._next().insert_many(db, collection, documents, ordered)

    def update_one(self, db: str, collection: str, filter: dict, update: dict) -> dict:
        """Update a single document."""
        return self._next().update_one(db, collection, filter, update)

    def update_many(self, db: str, collection: str, filter: dict, update: dict) -> dict:
        """Update multiple documents."""
        return self._next().update_many(db, collection, filter, update)

    def delete_one(self, db: str, collection: str, filter: dict) -> int:
        """Delete a single document."""
        return self._next().delete_one(db, collection, filter)

    def delete_many(self, db: str, collection: str, filter: dict) -> int:
        """Delete multiple documents."""
        return self._next().delete_many(db, collection, filter)

    def count_documents(self, db: str, collection: str, filter: Optional[dict] = None) -> int:
        """Count documents matching a filter."""
        return self._next().count_documents(db, collection, filter)

    def run_command(self, db: str, command: dict) -> dict:
        """Run an arbitrary database command."""
        return self._next().run_command(db, command)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
