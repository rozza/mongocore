# Binary UDS Transport — Design Spec

> **For implementers:** Read and follow `AGENTS.md` at the project root.
> Before committing: `cargo build` must produce ZERO warnings AND `cargo test --lib` must pass.
> If modifying client libraries: verify imports work and run `just test-clients`.
> If modifying shared structs (like `Config`): update ALL struct literals in `src/` AND `tests/`.

## Summary

Add a high-performance binary transport over Unix Domain Sockets that bypasses HTTP/2 and protobuf overhead entirely. This coexists permanently alongside gRPC (which remains for remote/cross-network use) and gives local clients the fastest possible path to MongoCore operations.

## Goals

- Minimize per-call latency for AI agent hot-loops (MCP tool calls)
- Maximize throughput for bulk operations (ingestion, large result sets)
- Feel like a native in-process driver for local connections
- Keep the protocol simple and evolvable (no ceremony — just change it when needed)

## Non-Goals

- Replacing gRPC for remote/cloud connections
- Windows named pipe support (future work)
- TLS on the local socket (UDS permissions provide access control)
- Backwards compatibility / version negotiation (server and clients always ship together)

## Versioning Philosophy

MongoCore and its client libraries are always released and deployed as a unit. There is no scenario where a v2 client talks to a v1 server or vice versa. This means:

- The wire format can change freely between releases with no migration path.
- No version fields in the handshake, no feature flags, no compatibility shims.
- New opcodes, new flag bits, and frame format changes are just code changes — update server and clients in the same commit.
- The handshake advertises runtime limits (max_frame_size, max_concurrent) — this is configuration exchange, not version negotiation.
- This keeps the protocol dead simple and the implementation lean.

## Frame Format

```
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────────────┐
│ 4B msg_len   │ 2B flags     │ 4B req_id    │ N bytes BSON │ M bytes raw docs     │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────────────┘
  big-endian u32   see below    big-endian u32   envelope       passthrough bytes
```

- **msg_len**: Total frame size excluding the 4-byte length prefix (i.e., `2 + 4 + N + M`). Max frame size: 64 MiB (configurable).
- **flags** (2 bytes, big-endian u16, bit 0 = LSB):
  - `bits[0:5]` = opcode (64 operations; 0x00 = invalid/reserved)
  - `bit 6` = end-of-stream (always set on non-streaming responses; signals final frame for streaming ops)
  - `bit 7` = no-reply (fire-and-forget, server skips response; request direction only)
  - `bit 8` = batch-follows (more frames in this batch, defer flush; **bidirectional** — clients use it to batch requests, server uses it to batch streaming response frames)
  - `bits[9:10]` = priority (0=normal, 1=low, 2=high, 3=critical — see Priority Semantics below)
  - `bits[11:15]` = unused
- **req_id**: Client-assigned monotonically increasing u32. Server echoes back on responses. Enables multiplexing multiple in-flight requests on a single connection.
- **BSON envelope** (N bytes): Operation metadata (db, collection, filter, options). MUST include `doc_bytes_len: u32` field when raw document bytes follow (0 when none). Does NOT contain document payloads for write operations — those go in the raw section.
- **Raw document bytes** (M bytes): Concatenated raw BSON documents, no wrapping. Present when `doc_bytes_len > 0` in the envelope. For writes: documents to insert/update. For responses: result documents from cursor.

### Envelope/Raw Split — How the Parser Knows

The frame parser works as follows:
1. Read 10-byte header → know total `msg_len`
2. Read remaining `msg_len - 6` bytes into pooled buffer (using `read_exact` — UDS is stream-oriented, short reads are handled by reading until the buffer is full)
3. Read the first 4 bytes of the buffer — this is the BSON document's self-declared size (BSON encodes its total byte length in its first 4 bytes as a little-endian i32). This gives N.
4. Parse N bytes as the BSON envelope.
5. Everything after byte N is raw document bytes: M = (buffer size) - N.
6. Validate: `M == doc_bytes_len` from the envelope. If mismatch, reject the frame.

The `doc_bytes_len` field in the envelope is a **validation redundancy** — the parser determines M from BSON self-delimiting, then cross-checks against `doc_bytes_len`. This catches corruption without requiring the parser to trust either source alone.

### Bit Ordering

Flags are a big-endian u16. **Bit 0 is the least significant bit.** So for a flags value of `0x0041`:
- bits[0:5] = 0x01 (opcode: Find)
- bit 6 = 1 (end-of-stream)
- bits 7-15 = 0

### Opcode 0x00

Opcode 0x00 is reserved as invalid. If the server receives a frame with opcode 0, it sends an error response and closes the connection. This serves as a catch for zero-initialized or corrupted frames.

### Frame Size Validation

After reading `msg_len`, the server MUST reject the frame if `msg_len > max_frame_size` (default 64 MiB). This check happens before allocating the read buffer, preventing memory exhaustion from oversized frames.

### Design Rationale

- 10-byte fixed header (4+2+4) enables single-read parsing with zero allocation for the header itself.
- Big-endian for network convention and easy debugging with tools like `xxd`.
- 2-byte flags provides room for no-reply, batching, and priority. No backwards-compat concern — server and clients ship together, so the wire format can change freely.
- Separated envelope + raw docs enables true zero-copy: document bytes pass through from client to MongoDB wire protocol without deserialization/reserialization on the MongoCore server.
- Batch-follows bit allows clients to queue multiple operations and flush once, reducing syscall count for rapid sequential calls (common in agent workloads).
- No-reply bit cuts roundtrip for unacknowledged writes (ingestion pipelines, telemetry).

## Opcodes

| Code | Operation | Server handler |
|------|-----------|----------------|
| 0x01 | Find | `operations::find` (streaming, returns cursor batches) |
| 0x02 | FindOne | `operations::find_one` (single doc, no cursor overhead) |
| 0x03 | InsertOne | `operations::insert` |
| 0x04 | InsertMany | `operations::insert_many` |
| 0x05 | UpdateOne | `operations::update` |
| 0x06 | UpdateMany | `operations::update_many` |
| 0x07 | DeleteOne | `operations::delete` |
| 0x08 | DeleteMany | `operations::delete_many` |
| 0x09 | Aggregate | `operations::aggregate` (streaming) |
| 0x0A | CountDocuments | `operations::count` |
| 0x0B | CreateIndex | `operations::create_index` |
| 0x0C | ListCollections | `operations::list_collections` |
| 0x0D | RunCommand | `operations::raw_command` |
| 0x0E | FindOneAndUpdate | `operations::find_one_and_update` |
| 0x0F | FindOneAndDelete | `operations::find_one_and_delete` |
| 0x10 | BulkWrite | `operations::bulk_write` |
| 0x11 | Distinct | `operations::distinct` |
| 0x12 | CreateCollection | `operations::create_collection` |
| 0x13 | DropCollection | `operations::drop_collection` |
| 0x3E | Error (response only) | Error envelope |
| 0x3F | Handshake/Ping | Connection setup and health |

### Handshake (0x3F)

The first frame on any new connection MUST be a handshake. Since server and clients are always shipped together, there's no version negotiation — just identification.

Client sends:

```bson
{
  "client_language": "python"
}
```

Server responds:

```bson
{
  "max_frame_size": 67108864,  // 64 MiB
  "max_concurrent": 64         // max in-flight requests this connection will service
}
```

If the handshake frame is malformed, server sends an error frame (0x3E) and closes the connection.

### Shutdown (Server-Initiated)

When the server is shutting down gracefully, it sends a special frame to each connected client:
- Opcode 0x3F (Handshake/Ping) with end-of-stream bit set
- Payload: `{ "shutdown": true, "drain_ms": 5000 }`

Clients receiving this frame should:
1. Stop sending new requests
2. Wait for in-flight responses (up to `drain_ms`)
3. Close the connection

If clients don't disconnect within `drain_ms`, the server closes the socket.

## Request Payload Conventions

The BSON envelope contains operation metadata. Raw document bytes (if any) follow immediately after the envelope in the frame.

All envelopes include these common fields:

```bson
{
  "db": "mydb",                    // required: target database
  "coll": "mycollection",         // required for collection ops
  "doc_bytes_len": 0,             // 0 = no raw bytes follow; >0 = M bytes of raw docs after envelope
  // ... operation-specific fields
}
```

### Example: Find (streaming)

Envelope only, no raw bytes:

```bson
{
  "db": "mydb",
  "coll": "users",
  "filter": { "age": { "$gt": 21 } },
  "projection": { "name": 1, "email": 1 },
  "limit": 100,
  "sort": { "name": 1 },
  "batch_size": 50,
  "doc_bytes_len": 0
}
```

### Example: FindOne

Envelope only, response is a single frame with one raw document:

```bson
{
  "db": "mydb",
  "coll": "users",
  "filter": { "_id": "abc123" },
  "projection": { "name": 1, "email": 1 },
  "doc_bytes_len": 0
}
```

### Example: InsertMany

Envelope + raw document bytes after:

```bson
// BSON envelope (N bytes):
{
  "db": "mydb",
  "coll": "events",
  "ordered": true,
  "doc_bytes_len": 48576          // total size of concatenated raw BSON docs that follow
}
// Raw document bytes (M = 48576 bytes):
// [BSON doc 1][BSON doc 2][BSON doc 3]... concatenated, no separators
// (each BSON doc is self-delimiting via its 4-byte length prefix)
```

## Response Conventions

### Single-Document Response (e.g., FindOne, InsertOne)

Response frame with matching `req_id`, end-of-stream bit set. Envelope contains metadata; raw bytes contain the result document(s):

```bson
// Envelope:
{
  "ok": 1,
  "doc_bytes_len": 256             // size of raw result document that follows
}
// Raw bytes: [single BSON document]
```

For operations with no document result (e.g., DeleteOne):

```bson
{
  "ok": 1,
  "deleted_count": 1,
  "doc_bytes_len": 0
}
```

### Streaming Response (Find, Aggregate)

Multiple response frames with the same `req_id`. Each frame contains a batch of raw documents:

```bson
// Envelope:
{
  "count": 50,                     // number of docs in this batch
  "doc_bytes_len": 24800           // total raw bytes that follow
}
// Raw bytes: [BSON doc 1][BSON doc 2]...[BSON doc 50] concatenated
```

Final frame has the end-of-stream bit (bit 7) set in flags. It may contain a final batch or be envelope-only with `count: 0`.

### Error Response

Opcode 0x3E, end-of-stream bit set:

```bson
{
  "code": 11000,           // MongoDB error code or MongoCore internal code
  "message": "duplicate key error",
  "details": { ... },      // optional additional context
  "doc_bytes_len": 0
}
```

## Server Architecture

```
src/transport/
├── mod.rs          // Public API: TransportServer, start(), shutdown()
├── frame.rs        // Frame encode/decode, read_frame(), write_frame(), writev support
├── connection.rs   // Per-connection task: read loop → dispatch → write loop
├── dispatch.rs     // Opcode → operation routing table
├── codec.rs        // BSON envelope ↔ operation params conversion
├── buffer_pool.rs  // Slab allocator: small/medium/large buffer pools
└── prefetch.rs     // Eager cursor prefetch for streaming responses
```

### TransportServer

- Binds to configurable UDS path (default: `/tmp/mongocore.bin.sock`)
- Sets socket permissions (default: `0o600`)
- Removes stale socket file on startup
- Accepts connections in a loop, spawning a tokio task per connection
- Tracks active connections for graceful shutdown

### Per-Connection Handler

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│ Read Loop   │────▶│ Dispatch     │────▶│ Write Loop  │
│ (frames in) │     │ (op routing) │     │ (frames out)│
└─────────────┘     └──────────────┘     └─────────────┘
       │                                        ▲
       │         tokio::mpsc channel            │
       └────────────────────────────────────────┘
```

- **Read loop**: Reads frames from the socket, validates header, routes to dispatch.
- **Dispatch**: Maps opcode to operation, deserializes BSON params, calls operation, serializes result.
- **Write loop**: Receives response frames via channel, writes to socket with backpressure.
- Multiple in-flight requests per connection (bounded by configurable concurrency limit, default 64).

### Integration with Existing Code

The dispatch layer calls directly into `src/operations/` — the same functions that gRPC and MCP handlers use. No new abstraction layer needed. The codec module converts between BSON envelopes and the existing Rust operation parameter structs.

## Performance Architecture

These are core design decisions, not optimizations to add later. They're what makes this transport meaningfully faster than "gRPC minus HTTP/2."

### Zero-Copy Document Passthrough

The critical insight: for most operations, MongoCore is a router — it receives BSON from the client and forwards it to MongoDB, or receives BSON from MongoDB and forwards it to the client. The server should never deserialize document bytes it doesn't need to inspect.

**Write path (client → MongoDB):**
```
Client frame: [header][envelope: {db, coll, ordered}][raw BSON docs...]
                                                      ↓
MongoCore reads envelope, passes raw doc bytes directly to mongodb driver
(no bson::Document parse, no intermediate Vec<Document>)
```

**Read path (MongoDB → client):**
```
MongoDB cursor yields raw BSON bytes
       ↓
MongoCore writes: [header][envelope: {count, has_more}][raw BSON bytes from cursor]
(no deserialization of individual documents)
```

Implementation: Use `bson::RawDocumentBuf` and MongoDB driver's raw BSON cursor mode. The `doc_bytes_len` field in the envelope tells the frame parser where metadata ends and passthrough bytes begin.

### Buffer Pool (Slab Allocator)

Per-request heap allocation is the #1 bottleneck after protocol overhead is removed. Use a pre-allocated buffer pool:

```rust
// Pool of reusable buffers, sized to common frame sizes
struct BufferPool {
    small: crossbeam::ArrayQueue<BytesMut>,   // 4 KiB buffers (metadata-only ops)
    medium: crossbeam::ArrayQueue<BytesMut>,  // 64 KiB buffers (typical reads)
    large: crossbeam::ArrayQueue<BytesMut>,   // 1 MiB buffers (bulk ops)
}
```

- Buffers are checked out on frame read, returned after response write.
- Pool sizes are configurable; default: 256 small, 64 medium, 16 large.
- Fallback to heap allocation if pool is exhausted (never blocks).
- `bytes::BytesMut` enables zero-copy slicing for sub-frame access.

### Vectored I/O (writev)

Never copy header + payload into a contiguous buffer before writing. Use `writev` (scatter-gather I/O) to send them from separate memory in a single syscall:

```rust
// Writes header and payload in one syscall, no memcpy
let header_buf = encode_header(&frame);
let iov = [
    IoSlice::new(&header_buf),        // 10 bytes (stack-allocated)
    IoSlice::new(&envelope_bytes),    // N bytes (from pool)
    IoSlice::new(&raw_doc_bytes),     // M bytes (from cursor, zero-copy)
];
socket.write_vectored(&iov).await?;
```

On the read side, `read_exact` for the 10-byte header, then a single `read_exact` for the payload into a pooled buffer.

### Batch Frame Coalescing

When the `batch-follows` flag (bit 9) is set, the server defers flushing the write buffer until it sees a frame without this flag. This enables:

1. **Client-side batching**: Agent sends 5 rapid findOne calls, marks first 4 with batch-follows. Server processes all 5, writes all 5 responses in a single `writev` call (1 syscall instead of 5).
2. **Server-side coalescing**: For streaming responses (cursor batches), the server sets batch-follows on all but the last batch frame, ensuring one flush per cursor drain.

### Fire-and-Forget (No-Reply) Writes

When flag bit 8 is set, the server does not send a response frame. The operation still executes, but:
- No response buffer allocated
- No write syscall for that request
- Client doesn't wait — immediately sends next request
- Errors are logged server-side but not reported to client

Use cases: unacknowledged inserts in telemetry/logging pipelines, analytics event recording, pre-warming caches.

### Eager Cursor Prefetch

For streaming responses (Find, Aggregate), the server prefetches the next batch while the client processes the current one:

```
Server: [send batch 1] → [prefetch batch 2 from MongoDB] → [send batch 2] → ...
Client: [receive batch 1] → [process batch 1] → [receive batch 2 (already waiting)] → ...
```

Implementation: A small prefetch buffer (default: 2 batches ahead) fills from the MongoDB cursor on a separate tokio task. The write loop drains from this buffer. Net effect: client never waits for MongoDB round-trips between batches.

### Client Connection Strategy

Multiplexing via request IDs is the default for simplicity, but clients SHOULD maintain a small connection pool (default: 4 connections) for CPU-bound workloads:

- Avoids head-of-line blocking in the write loop (one slow response doesn't delay others)
- Distributes across CPU cores on the server (each connection = separate tokio task)
- Multiplexing within each connection still reduces total socket count vs one-per-request

Recommended: 4 connections × 16 concurrent requests each = 64 total in-flight ops.

### Platform-Specific Optimizations (Post-v1)

Documented here for future implementation, not required for initial release:

- **Linux io_uring**: 30-50% faster than epoll for UDS I/O. Use `tokio-uring` or `monoio` runtime for the transport accept loop. Falls back to epoll on older kernels.
- **macOS kqueue**: Already used by tokio's default runtime. No additional work needed.
- **splice/sendfile**: For very large document transfers (>1 MiB), splice bytes directly between file descriptors without crossing user-space.

## Configuration

New fields in `CliArgs` / `FileConfig` / `Config`:

| CLI flag | Env var | Default | Description |
|----------|---------|---------|-------------|
| `--binary-socket-path` | `MONGOCORE_BINARY_SOCKET_PATH` | `/tmp/mongocore.bin.sock` | UDS path for binary transport |
| `--binary-socket-permissions` | `MONGOCORE_BINARY_SOCKET_PERMISSIONS` | `0o600` | Socket file permissions |
| `--enable-binary-transport` | `MONGOCORE_ENABLE_BINARY` | `true` | Enable/disable binary transport |
| `--binary-max-frame-size` | `MONGOCORE_BINARY_MAX_FRAME_SIZE` | `67108864` (64 MiB) | Maximum frame size |
| `--binary-max-concurrent` | `MONGOCORE_BINARY_MAX_CONCURRENT` | `64` | Max in-flight requests per connection |

## Client Auto-Discovery

Client libraries check transports in preference order:

1. `MONGOCORE_BINARY_SOCKET_PATH` env var → binary UDS at specified path
2. `/tmp/mongocore.bin.sock` exists and is a socket → binary UDS
3. Existing gRPC discovery (UDS → TCP)

Each client library adds a binary transport module alongside the existing gRPC transport:
- `clients/python/src/mongocore/binary_transport.py`
- `clients/typescript/src/binary-transport.ts`
- `clients/go/mongocore/binary_transport.go`
- `clients/java/src/main/java/com/mongocore/BinaryTransport.java`

## Priority Semantics

The 2-bit priority field (bits[10:11]) controls dispatch ordering on the server:

| Value | Level | Behavior |
|-------|-------|----------|
| 0 | Normal | Default. Dispatched in arrival order. |
| 1 | Low | Queued behind all normal/high/critical requests. Used for background prefetch, analytics. |
| 2 | High | Jumps ahead of normal/low in the dispatch queue. Used for interactive agent ops. |
| 3 | Critical | Processed immediately, bypassing the concurrency limit. Reserved for opcode 0x3F (Handshake/Ping) only. |

Implementation: The per-connection dispatch uses a priority queue (e.g., `tokio::sync::mpsc` with separate channels per priority, polled in priority order). Critical requests are never queued — they execute on a reserved slot outside the normal concurrency pool. To prevent resource exhaustion, only opcode 0x3F is permitted at critical priority; other opcodes marked critical are downgraded to high.

## Performance Targets

Relative to current gRPC-over-UDS baseline:

| Scenario | Expected speedup | Source of gains |
|----------|-----------------|-----------------|
| Ping/handshake | 3–5x | No HTTP/2, no protobuf, 10-byte header vs ~100+ bytes |
| findOne by _id | 2–3x | Zero-copy BSON passthrough, buffer pool, no proto encode |
| find 1000 docs | 4–6x | Streaming prefetch, vectored I/O, raw cursor bytes |
| insertOne | 2–3x | Buffer pool, minimal envelope |
| insertMany 10k (acknowledged) | 3–5x | Zero-copy write path, batch coalescing |
| insertMany 10k (fire-and-forget) | 5–8x | No-reply flag eliminates response path entirely |
| Agent burst (50 sequential ops) | 4–6x | Batch-follows coalescing, connection pooling |

These gains come from eliminating: HTTP/2 framing, HPACK header compression, protobuf encode/decode, per-request allocation, unnecessary BSON deserialization/reserialization, and excess syscalls.

## Error Handling

- **Malformed frame** (bad length, unknown opcode): Send error response (0x3E), close connection.
- **Operation error** (MongoDB error, validation failure): Send error response with matching req_id, keep connection open.
- **Connection drop**: Server cancels in-flight operations for that connection, cleans up cursor state.
- **Backpressure**: If write buffer exceeds threshold, server pauses reading from that connection.

## Security

- Socket file permissions (default 0o600) restrict access to the owning user.
- No authentication on the binary protocol — access control is filesystem-based (same as current UDS gRPC).
- Max frame size prevents memory exhaustion from malicious/buggy clients.
- Handshake timeout (5s) prevents connection slot exhaustion from idle openers.

## Testing Strategy

- Unit tests: frame encode/decode round-trips, opcode dispatch routing, BSON codec conversions
- Integration tests: full request/response cycle over UDS for each opcode
- Benchmark tests: latency and throughput comparisons vs gRPC-over-UDS baseline
- Client integration tests: each language client exercises binary transport path

## Benchmarking & Transport Switching

A key design requirement is the ability to benchmark both transports under identical workloads with a simple config switch.

### Client-Side Transport Selection

All client libraries expose a transport selection option:

```python
# Python
client = MongoClient(transport="binary")   # force binary UDS
client = MongoClient(transport="grpc")     # force gRPC (TCP or UDS)
client = MongoClient(transport="auto")     # default: prefer binary, fall back to gRPC
```

```go
// Go
client := mongocore.NewClient(mongocore.WithTransport("binary"))
client := mongocore.NewClient(mongocore.WithTransport("grpc"))
```

The `MONGOCORE_TRANSPORT` env var provides a global override without code changes:
```bash
MONGOCORE_TRANSPORT=grpc cargo bench    # benchmark gRPC path
MONGOCORE_TRANSPORT=binary cargo bench  # benchmark binary path
```

### Server-Side: Both Transports Always Available

When `--enable-binary-transport` is true (default), the server listens on **both** gRPC and binary sockets simultaneously. This allows A/B benchmarking without server restarts — only the client config changes.

### Benchmark Harness

A dedicated benchmark binary (`benches/transport_bench.rs`) runs identical workloads over both transports and produces comparative results:

```bash
just bench-transport               # runs both, prints comparison table
just bench-transport --ops 10000   # customize iteration count
just bench-transport --only binary # single transport
```

Benchmark scenarios:
- **Ping latency**: Handshake + empty roundtrip (measures pure protocol overhead)
- **Small read**: findOne by _id (single document, minimal payload)
- **Bulk read**: find 1000 documents (streaming throughput)
- **Small write**: insertOne (single document)
- **Bulk write**: insertMany 10k documents (write throughput)
- **Mixed workload**: 70% reads / 30% writes (realistic agent pattern)

Output format:
```
┌───────────────────┬────────────┬────────────┬──────────┐
│ Scenario          │ gRPC (μs)  │ Binary (μs)│ Speedup  │
├───────────────────┼────────────┼────────────┼──────────┤
│ Ping latency      │ 142        │ 38         │ 3.7x     │
│ Small read        │ 285        │ 112        │ 2.5x     │
│ Bulk read (1000)  │ 4,200      │ 1,050      │ 4.0x     │
│ ...               │            │            │          │
└───────────────────┴────────────┴────────────┴──────────┘
```

### Configuration Summary

| Level | Switch | Values | Purpose |
|-------|--------|--------|---------|
| Server | `--enable-binary-transport` | `true`/`false` | Enable/disable binary listener |
| Client env | `MONGOCORE_TRANSPORT` | `auto`/`binary`/`grpc` | Force transport without code change |
| Client code | `transport=` parameter | `auto`/`binary`/`grpc` | Programmatic transport selection |
| Benchmark | `just bench-transport` | `--only binary\|grpc` | Run comparative benchmarks |

## AGENTS.md Updates

The following sections of `AGENTS.md` must be updated:
- **Architecture diagram**: Add binary UDS transport path
- **Project Layout**: Add `src/transport/` directory
- **Adding a New RPC**: Add step for binary transport opcode registration
- **Configuration**: Document new config fields
- **Testing**: Add binary transport test commands

## Future Expansion (Post-v1)

- Named pipes for Windows support
- Linux io_uring runtime for the transport accept loop (30-50% faster than epoll)
- splice/sendfile for large document transfers (>1 MiB) without user-space copy
- Connection pooling on the server side (shared cursors across client reconnects)
- Request cancellation frame (client sends cancel for a specific req_id, server aborts in-flight work)
