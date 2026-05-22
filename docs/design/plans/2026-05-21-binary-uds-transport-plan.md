# Binary UDS Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a high-performance binary transport over Unix Domain Sockets that bypasses HTTP/2 and protobuf, calling directly into the existing operations layer.

**Architecture:** A new `src/transport/` module handles frame parsing, buffer pooling, and dispatch. It listens on a separate UDS (`/tmp/mongocore.bin.sock`) alongside the existing gRPC server. The dispatch layer converts BSON envelopes into operation calls using the same `Operations` struct that gRPC and MCP already use.

**Tech Stack:** Rust, tokio (UnixListener/UnixStream), bson crate, bytes crate, crossbeam (ArrayQueue for buffer pool)

**Spec:** `docs/design/specs/2026-05-21-binary-uds-transport-design.md`

---

> **For implementers:** Read and follow `AGENTS.md` at the project root.
> Before committing: `cargo build` must produce ZERO warnings AND `cargo test --lib` must pass.
> If modifying client libraries: verify imports work and run `just test-clients`.
> If modifying shared structs (like `Config`): update ALL struct literals in `src/` AND `tests/`.

---

## File Structure

```
src/transport/
├── mod.rs          — Public API: TransportServer, start_binary_transport()
├── frame.rs        — Frame encode/decode structs, read/write functions
├── buffer_pool.rs  — Slab allocator with small/medium/large tiers
├── connection.rs   — Per-connection handler (read loop, dispatch, write loop)
├── dispatch.rs     — Opcode → operation routing + BSON envelope parsing
└── codec.rs        — BSON envelope ↔ operation params conversion

src/defaults.rs     — New defaults for binary transport
src/config.rs       — New CLI args, FileConfig fields, Config fields
src/main.rs         — Start binary transport alongside gRPC/MCP
src/error.rs        — New TransportError variant

benches/transport_bench.rs — Comparative benchmark harness
```

---

## Task 1: Frame Types and Encoding (`src/transport/frame.rs`)

**Files:**
- Create: `src/transport/frame.rs`
- Create: `src/transport/mod.rs` (minimal, just `pub mod frame;`)

- [ ] **Step 1: Write failing test for frame header encoding**

```rust
// In src/transport/frame.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_header() {
        let header = FrameHeader {
            msg_len: 100,
            flags: Flags {
                opcode: Opcode::FindOne,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 42,
        };
        let bytes = header.encode();
        assert_eq!(bytes.len(), 10);
        // msg_len: 100 as big-endian u32
        assert_eq!(&bytes[0..4], &100u32.to_be_bytes());
        // flags: opcode 0x02 (FindOne) | bit 6 (EOS) = 0x0042
        assert_eq!(&bytes[4..6], &0x0042u16.to_be_bytes());
        // req_id: 42 as big-endian u32
        assert_eq!(&bytes[6..10], &42u32.to_be_bytes());
    }

    #[test]
    fn test_decode_header() {
        let mut buf = [0u8; 10];
        buf[0..4].copy_from_slice(&100u32.to_be_bytes());
        buf[4..6].copy_from_slice(&0x0042u16.to_be_bytes());
        buf[6..10].copy_from_slice(&42u32.to_be_bytes());

        let header = FrameHeader::decode(&buf).unwrap();
        assert_eq!(header.msg_len, 100);
        assert_eq!(header.flags.opcode, Opcode::FindOne);
        assert!(header.flags.end_of_stream);
        assert!(!header.flags.no_reply);
        assert_eq!(header.req_id, 42);
    }

    #[test]
    fn test_opcode_zero_is_invalid() {
        let mut buf = [0u8; 10];
        buf[0..4].copy_from_slice(&10u32.to_be_bytes());
        // flags = 0x0000 means opcode 0
        buf[4..6].copy_from_slice(&0x0000u16.to_be_bytes());
        buf[6..10].copy_from_slice(&1u32.to_be_bytes());

        let result = FrameHeader::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_flags_roundtrip_all_bits() {
        let flags = Flags {
            opcode: Opcode::Aggregate, // 0x09
            end_of_stream: true,       // bit 6
            no_reply: true,            // bit 7
            batch_follows: true,       // bit 8
            priority: Priority::High,  // bits 9:10 = 2
        };
        let encoded = flags.encode();
        let decoded = Flags::decode(encoded).unwrap();
        assert_eq!(decoded.opcode, Opcode::Aggregate);
        assert!(decoded.end_of_stream);
        assert!(decoded.no_reply);
        assert!(decoded.batch_follows);
        assert_eq!(decoded.priority, Priority::High);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib transport::frame`
Expected: FAIL — module doesn't exist yet

- [ ] **Step 3: Implement frame types and encode/decode**

```rust
// src/transport/frame.rs
use std::fmt;

pub const HEADER_SIZE: usize = 10;
pub const MAX_FRAME_SIZE_DEFAULT: u32 = 64 * 1024 * 1024; // 64 MiB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Find = 0x01,
    FindOne = 0x02,
    InsertOne = 0x03,
    InsertMany = 0x04,
    UpdateOne = 0x05,
    UpdateMany = 0x06,
    DeleteOne = 0x07,
    DeleteMany = 0x08,
    Aggregate = 0x09,
    CountDocuments = 0x0A,
    CreateIndex = 0x0B,
    ListCollections = 0x0C,
    RunCommand = 0x0D,
    FindOneAndUpdate = 0x0E,
    FindOneAndDelete = 0x0F,
    BulkWrite = 0x10,
    Distinct = 0x11,
    CreateCollection = 0x12,
    DropCollection = 0x13,
    Error = 0x3E,
    Handshake = 0x3F,
}

impl Opcode {
    pub fn from_u8(val: u8) -> Result<Self, FrameError> {
        match val {
            0x01 => Ok(Self::Find),
            0x02 => Ok(Self::FindOne),
            0x03 => Ok(Self::InsertOne),
            0x04 => Ok(Self::InsertMany),
            0x05 => Ok(Self::UpdateOne),
            0x06 => Ok(Self::UpdateMany),
            0x07 => Ok(Self::DeleteOne),
            0x08 => Ok(Self::DeleteMany),
            0x09 => Ok(Self::Aggregate),
            0x0A => Ok(Self::CountDocuments),
            0x0B => Ok(Self::CreateIndex),
            0x0C => Ok(Self::ListCollections),
            0x0D => Ok(Self::RunCommand),
            0x0E => Ok(Self::FindOneAndUpdate),
            0x0F => Ok(Self::FindOneAndDelete),
            0x10 => Ok(Self::BulkWrite),
            0x11 => Ok(Self::Distinct),
            0x12 => Ok(Self::CreateCollection),
            0x13 => Ok(Self::DropCollection),
            0x3E => Ok(Self::Error),
            0x3F => Ok(Self::Handshake),
            0x00 => Err(FrameError::InvalidOpcode(0)),
            other => Err(FrameError::InvalidOpcode(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Priority {
    Normal = 0,
    Low = 1,
    High = 2,
    Critical = 3,
}

impl Priority {
    pub fn from_u8(val: u8) -> Self {
        match val & 0x03 {
            0 => Self::Normal,
            1 => Self::Low,
            2 => Self::High,
            3 => Self::Critical,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags {
    pub opcode: Opcode,
    pub end_of_stream: bool,
    pub no_reply: bool,
    pub batch_follows: bool,
    pub priority: Priority,
}

impl Flags {
    pub fn encode(&self) -> u16 {
        let mut val: u16 = self.opcode as u16 & 0x3F;
        if self.end_of_stream { val |= 1 << 6; }
        if self.no_reply { val |= 1 << 7; }
        if self.batch_follows { val |= 1 << 8; }
        val |= ((self.priority as u16) & 0x03) << 9;
        val
    }

    pub fn decode(val: u16) -> Result<Self, FrameError> {
        let opcode_val = (val & 0x3F) as u8;
        let opcode = Opcode::from_u8(opcode_val)?;
        Ok(Self {
            opcode,
            end_of_stream: (val >> 6) & 1 == 1,
            no_reply: (val >> 7) & 1 == 1,
            batch_follows: (val >> 8) & 1 == 1,
            priority: Priority::from_u8(((val >> 9) & 0x03) as u8),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub msg_len: u32,
    pub flags: Flags,
    pub req_id: u32,
}

impl FrameHeader {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.msg_len.to_be_bytes());
        buf[4..6].copy_from_slice(&self.flags.encode().to_be_bytes());
        buf[6..10].copy_from_slice(&self.req_id.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self, FrameError> {
        let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let flags_raw = u16::from_be_bytes([buf[4], buf[5]]);
        let flags = Flags::decode(flags_raw)?;
        let req_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);
        Ok(Self { msg_len, flags, req_id })
    }
}

#[derive(Debug)]
pub struct Frame {
    pub header: FrameHeader,
    pub envelope: Vec<u8>,
    pub raw_docs: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum FrameError {
    InvalidOpcode(u8),
    FrameTooLarge { size: u32, max: u32 },
    DocBytesLenMismatch { expected: u32, actual: u32 },
    MalformedEnvelope(String),
    Io(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOpcode(op) => write!(f, "invalid opcode: 0x{op:02X}"),
            Self::FrameTooLarge { size, max } => write!(f, "frame {size} exceeds max {max}"),
            Self::DocBytesLenMismatch { expected, actual } => {
                write!(f, "doc_bytes_len mismatch: expected {expected}, got {actual}")
            }
            Self::MalformedEnvelope(msg) => write!(f, "malformed envelope: {msg}"),
            Self::Io(msg) => write!(f, "I/O error: {msg}"),
        }
    }
}

impl std::error::Error for FrameError {}
```

- [ ] **Step 4: Create minimal mod.rs**

```rust
// src/transport/mod.rs
pub mod frame;
```

And add to `src/lib.rs` or `src/main.rs`:
```rust
pub mod transport;
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib transport::frame`
Expected: All 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/transport/
git commit -m "feat(transport): add frame types and header encode/decode"
```

---

## Task 2: Frame I/O — Read and Write Over UnixStream (`src/transport/frame.rs`)

**Files:**
- Modify: `src/transport/frame.rs` (add async read/write functions)

- [ ] **Step 1: Write failing test for frame read/write roundtrip**

```rust
// Add to src/transport/frame.rs tests module
#[tokio::test]
async fn test_frame_write_read_roundtrip() {
    use tokio::net::UnixStream;

    let (mut client, mut server) = UnixStream::pair().unwrap();

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0, // will be computed by write_frame
            flags: Flags {
                opcode: Opcode::InsertOne,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 7,
        },
        envelope: b"\x10\x00\x00\x00\x02ok\x00\x02\x00\x00\x00\x31\x00\x00".to_vec(), // minimal BSON
        raw_docs: vec![1, 2, 3, 4, 5],
    };

    write_frame(&mut client, &frame).await.unwrap();
    let read_back = read_frame(&mut server, MAX_FRAME_SIZE_DEFAULT).await.unwrap();

    assert_eq!(read_back.header.flags.opcode, Opcode::InsertOne);
    assert_eq!(read_back.header.req_id, 7);
    assert_eq!(read_back.raw_docs, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_frame_read_rejects_oversized() {
    use tokio::net::UnixStream;

    let (mut client, mut server) = UnixStream::pair().unwrap();

    // Write a header claiming msg_len of 100MB
    let header = FrameHeader {
        msg_len: 100 * 1024 * 1024,
        flags: Flags {
            opcode: Opcode::Handshake,
            end_of_stream: false,
            no_reply: false,
            batch_follows: false,
            priority: Priority::Normal,
        },
        req_id: 1,
    };
    use tokio::io::AsyncWriteExt;
    client.write_all(&header.encode()).await.unwrap();

    let result = read_frame(&mut server, MAX_FRAME_SIZE_DEFAULT).await;
    assert!(result.is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib transport::frame`
Expected: FAIL — `write_frame` and `read_frame` not defined

- [ ] **Step 3: Implement async read/write functions**

```rust
// Add to src/transport/frame.rs
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use std::io::IoSlice;

pub async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), FrameError> {
    let payload_len = frame.envelope.len() + frame.raw_docs.len();
    let msg_len = (6 + payload_len) as u32; // 2 (flags) + 4 (req_id) + payload

    let mut header_buf = [0u8; HEADER_SIZE];
    header_buf[0..4].copy_from_slice(&msg_len.to_be_bytes());
    header_buf[4..6].copy_from_slice(&frame.header.flags.encode().to_be_bytes());
    header_buf[6..10].copy_from_slice(&frame.header.req_id.to_be_bytes());

    let bufs = &[
        IoSlice::new(&header_buf),
        IoSlice::new(&frame.envelope),
        IoSlice::new(&frame.raw_docs),
    ];
    stream.write_vectored(bufs).await.map_err(|e| FrameError::Io(e.to_string()))?;
    Ok(())
}

pub async fn read_frame(stream: &mut UnixStream, max_frame_size: u32) -> Result<Frame, FrameError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header_buf).await.map_err(|e| FrameError::Io(e.to_string()))?;

    let header = FrameHeader::decode(&header_buf)?;

    if header.msg_len > max_frame_size {
        return Err(FrameError::FrameTooLarge {
            size: header.msg_len,
            max: max_frame_size,
        });
    }

    // msg_len = 2 (flags) + 4 (req_id) + payload_len
    let payload_len = header.msg_len.checked_sub(6)
        .ok_or_else(|| FrameError::MalformedEnvelope("msg_len too small".into()))? as usize;

    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await.map_err(|e| FrameError::Io(e.to_string()))?;

    // Split envelope from raw docs using BSON self-delimiting
    let (envelope, raw_docs) = if payload.len() >= 4 {
        let bson_len = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if bson_len > payload.len() || bson_len < 5 {
            return Err(FrameError::MalformedEnvelope(
                format!("BSON declares size {bson_len} but payload is {}", payload.len())
            ));
        }
        let raw_docs = payload[bson_len..].to_vec();
        payload.truncate(bson_len);
        (payload, raw_docs)
    } else if payload.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        return Err(FrameError::MalformedEnvelope("payload too short for BSON".into()));
    };

    Ok(Frame {
        header,
        envelope,
        raw_docs,
    })
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib transport::frame`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/transport/frame.rs
git commit -m "feat(transport): add async frame read/write with vectored I/O"
```

---

## Task 3: Buffer Pool (`src/transport/buffer_pool.rs`)

**Files:**
- Create: `src/transport/buffer_pool.rs`
- Modify: `Cargo.toml` (add `crossbeam-queue` dependency)
- Modify: `src/transport/mod.rs` (add `pub mod buffer_pool;`)

- [ ] **Step 1: Add dependency**

Add to `Cargo.toml` under `[dependencies]`:
```toml
crossbeam-queue = "0.3"
bytes = "1"
```

- [ ] **Step 2: Write failing test**

```rust
// src/transport/buffer_pool.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkout_returns_buffer_of_correct_tier() {
        let pool = BufferPool::new(BufferPoolConfig::default());
        let buf = pool.checkout(100); // 100 bytes → small tier (4 KiB)
        assert!(buf.capacity() >= 100);
    }

    #[test]
    fn test_checkin_returns_buffer_to_pool() {
        let pool = BufferPool::new(BufferPoolConfig::default());
        let buf = pool.checkout(100);
        let cap = buf.capacity();
        pool.checkin(buf);
        let buf2 = pool.checkout(100);
        assert_eq!(buf2.capacity(), cap); // got the same buffer back
    }

    #[test]
    fn test_exhausted_pool_falls_back_to_heap() {
        let config = BufferPoolConfig { small_count: 1, medium_count: 0, large_count: 0 };
        let pool = BufferPool::new(config);
        let _b1 = pool.checkout(100);
        let b2 = pool.checkout(100); // pool exhausted, heap fallback
        assert!(b2.capacity() >= 100);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib transport::buffer_pool`
Expected: FAIL — module doesn't exist

- [ ] **Step 4: Implement buffer pool**

```rust
// src/transport/buffer_pool.rs
use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

const SMALL_SIZE: usize = 4 * 1024;       // 4 KiB
const MEDIUM_SIZE: usize = 64 * 1024;     // 64 KiB
const LARGE_SIZE: usize = 1024 * 1024;    // 1 MiB

pub struct BufferPoolConfig {
    pub small_count: usize,
    pub medium_count: usize,
    pub large_count: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            small_count: 256,
            medium_count: 64,
            large_count: 16,
        }
    }
}

#[derive(Clone)]
pub struct BufferPool {
    small: Arc<ArrayQueue<BytesMut>>,
    medium: Arc<ArrayQueue<BytesMut>>,
    large: Arc<ArrayQueue<BytesMut>>,
}

impl BufferPool {
    pub fn new(config: BufferPoolConfig) -> Self {
        let small = Arc::new(ArrayQueue::new(config.small_count.max(1)));
        let medium = Arc::new(ArrayQueue::new(config.medium_count.max(1)));
        let large = Arc::new(ArrayQueue::new(config.large_count.max(1)));

        for _ in 0..config.small_count {
            let _ = small.push(BytesMut::with_capacity(SMALL_SIZE));
        }
        for _ in 0..config.medium_count {
            let _ = medium.push(BytesMut::with_capacity(MEDIUM_SIZE));
        }
        for _ in 0..config.large_count {
            let _ = large.push(BytesMut::with_capacity(LARGE_SIZE));
        }

        Self { small, medium, large }
    }

    pub fn checkout(&self, needed: usize) -> BytesMut {
        if needed <= SMALL_SIZE {
            if let Some(mut buf) = self.small.pop() {
                buf.clear();
                return buf;
            }
        } else if needed <= MEDIUM_SIZE {
            if let Some(mut buf) = self.medium.pop() {
                buf.clear();
                return buf;
            }
        } else if needed <= LARGE_SIZE {
            if let Some(mut buf) = self.large.pop() {
                buf.clear();
                return buf;
            }
        }
        // Fallback: heap allocation
        BytesMut::with_capacity(needed)
    }

    pub fn checkin(&self, buf: BytesMut) {
        let cap = buf.capacity();
        if cap <= SMALL_SIZE {
            let _ = self.small.push(buf);
        } else if cap <= MEDIUM_SIZE {
            let _ = self.medium.push(buf);
        } else if cap <= LARGE_SIZE {
            let _ = self.large.push(buf);
        }
        // Oversized buffers are dropped (not returned to pool)
    }
}
```

- [ ] **Step 5: Update mod.rs**

```rust
// src/transport/mod.rs
pub mod buffer_pool;
pub mod frame;
```

- [ ] **Step 6: Run tests**

Run: `cargo test --lib transport::buffer_pool`
Expected: All 3 tests PASS

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src/transport/buffer_pool.rs src/transport/mod.rs
git commit -m "feat(transport): add buffer pool with tiered slab allocation"
```

---

## Task 4: Configuration (`src/config.rs`, `src/defaults.rs`)

**Files:**
- Modify: `src/defaults.rs` (add binary transport defaults)
- Modify: `src/config.rs` (add fields to CliArgs, FileConfig, Config, and Config::load)

- [ ] **Step 1: Add defaults**

Add to `src/defaults.rs`:

```rust
/// Default binary transport socket path.
pub const DEFAULT_BINARY_SOCKET_PATH: &str = "/tmp/mongocore.bin.sock";

/// Default binary transport socket permissions.
pub const DEFAULT_BINARY_SOCKET_PERMISSIONS: u32 = 0o600;

/// Whether the binary transport is enabled by default.
pub const DEFAULT_BINARY_TRANSPORT_ENABLED: bool = true;

/// Default max frame size for binary transport (64 MiB).
pub const DEFAULT_BINARY_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Default max concurrent in-flight requests per connection.
pub const DEFAULT_BINARY_MAX_CONCURRENT: usize = 64;
```

- [ ] **Step 2: Add CLI args to CliArgs struct**

Add to `CliArgs` in `src/config.rs`:

```rust
#[arg(long, env = "MONGOCORE_BINARY_SOCKET_PATH")]
pub binary_socket_path: Option<String>,

#[arg(long, env = "MONGOCORE_BINARY_SOCKET_PERMISSIONS")]
pub binary_socket_permissions: Option<u32>,

#[arg(long, env = "MONGOCORE_ENABLE_BINARY")]
pub enable_binary_transport: Option<bool>,

#[arg(long, env = "MONGOCORE_BINARY_MAX_FRAME_SIZE")]
pub binary_max_frame_size: Option<usize>,

#[arg(long, env = "MONGOCORE_BINARY_MAX_CONCURRENT")]
pub binary_max_concurrent: Option<usize>,
```

- [ ] **Step 3: Add fields to FileConfig**

Add to `FileConfig` in `src/config.rs`:

```rust
pub binary_socket_path: Option<String>,
pub binary_socket_permissions: Option<u32>,
pub enable_binary_transport: Option<bool>,
pub binary_max_frame_size: Option<usize>,
pub binary_max_concurrent: Option<usize>,
```

- [ ] **Step 4: Add fields to Config struct**

Add to `Config` in `src/config.rs`:

```rust
pub binary_socket_path: String,
pub binary_socket_permissions: u32,
pub binary_transport_enabled: bool,
pub binary_max_frame_size: usize,
pub binary_max_concurrent: usize,
```

- [ ] **Step 5: Add resolution logic in Config::load()**

Add to `Config::load()` after the existing field resolutions:

```rust
binary_socket_path: cli.binary_socket_path
    .or(file_config.binary_socket_path)
    .unwrap_or_else(|| DEFAULT_BINARY_SOCKET_PATH.to_string()),
binary_socket_permissions: cli.binary_socket_permissions
    .or(file_config.binary_socket_permissions)
    .unwrap_or(DEFAULT_BINARY_SOCKET_PERMISSIONS),
binary_transport_enabled: cli.enable_binary_transport
    .or(file_config.enable_binary_transport)
    .unwrap_or(DEFAULT_BINARY_TRANSPORT_ENABLED),
binary_max_frame_size: cli.binary_max_frame_size
    .or(file_config.binary_max_frame_size)
    .unwrap_or(DEFAULT_BINARY_MAX_FRAME_SIZE),
binary_max_concurrent: cli.binary_max_concurrent
    .or(file_config.binary_max_concurrent)
    .unwrap_or(DEFAULT_BINARY_MAX_CONCURRENT),
```

- [ ] **Step 6: Update ALL Config struct literals in tests**

Search for `Config {` in `src/` and `tests/` and add the new fields with defaults. Run:
```bash
grep -rn "Config {" src/ tests/ | grep -v "//"
```

For each struct literal, add:
```rust
binary_socket_path: "/tmp/mongocore.bin.sock".to_string(),
binary_socket_permissions: 0o600,
binary_transport_enabled: true,
binary_max_frame_size: 64 * 1024 * 1024,
binary_max_concurrent: 64,
```

- [ ] **Step 7: Build and verify zero warnings**

Run: `cargo build 2>&1 | grep "warning:"`
Expected: No output

- [ ] **Step 8: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass

- [ ] **Step 9: Commit**

```bash
git add src/config.rs src/defaults.rs tests/
git commit -m "feat(config): add binary transport configuration fields"
```

---

## Task 5: Codec — BSON Envelope to Operation Params (`src/transport/codec.rs`)

**Files:**
- Create: `src/transport/codec.rs`
- Modify: `src/transport/mod.rs`

- [ ] **Step 1: Write failing tests for envelope parsing**

```rust
// src/transport/codec.rs
#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_parse_find_one_envelope() {
        let envelope = doc! {
            "db": "testdb",
            "coll": "users",
            "filter": { "_id": "abc" },
            "projection": { "name": 1 },
            "doc_bytes_len": 0_i32
        };
        let bytes = bson::to_vec(&envelope).unwrap();
        let req = parse_envelope(&bytes, Opcode::FindOne).unwrap();

        match req {
            OperationRequest::FindOne { db, collection, filter, projection } => {
                assert_eq!(db, "testdb");
                assert_eq!(collection, "users");
                assert_eq!(filter, doc! { "_id": "abc" });
                assert_eq!(projection, Some(doc! { "name": 1 }));
            }
            _ => panic!("expected FindOne"),
        }
    }

    #[test]
    fn test_parse_insert_one_envelope() {
        let envelope = doc! {
            "db": "testdb",
            "coll": "events",
            "doc_bytes_len": 50_i32
        };
        let bytes = bson::to_vec(&envelope).unwrap();
        let req = parse_envelope(&bytes, Opcode::InsertOne).unwrap();

        match req {
            OperationRequest::InsertOne { db, collection, doc_bytes_len } => {
                assert_eq!(db, "testdb");
                assert_eq!(collection, "events");
                assert_eq!(doc_bytes_len, 50);
            }
            _ => panic!("expected InsertOne"),
        }
    }

    #[test]
    fn test_encode_find_one_response() {
        let result_doc = doc! { "name": "Alice", "age": 30 };
        let raw_bytes = bson::to_vec(&result_doc).unwrap();
        let (envelope_bytes, doc_bytes) = encode_response(
            &OperationResponse::SingleDoc {
                ok: true,
                doc: Some(raw_bytes.clone()),
            }
        ).unwrap();

        let env: bson::Document = bson::from_slice(&envelope_bytes).unwrap();
        assert_eq!(env.get_i32("ok").unwrap(), 1);
        assert_eq!(env.get_i32("doc_bytes_len").unwrap(), raw_bytes.len() as i32);
        assert_eq!(doc_bytes, raw_bytes);
    }

    #[test]
    fn test_encode_error_response() {
        let (envelope_bytes, doc_bytes) = encode_response(
            &OperationResponse::Error {
                code: 11000,
                message: "duplicate key".to_string(),
            }
        ).unwrap();

        let env: bson::Document = bson::from_slice(&envelope_bytes).unwrap();
        assert_eq!(env.get_i32("code").unwrap(), 11000);
        assert_eq!(env.get_str("message").unwrap(), "duplicate key");
        assert!(doc_bytes.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib transport::codec`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement codec**

```rust
// src/transport/codec.rs
use bson::{doc, Document};
use crate::transport::frame::Opcode;
use crate::operations::FindOptions;

#[derive(Debug)]
pub enum OperationRequest {
    Handshake { client_language: String },
    Find {
        db: String,
        collection: String,
        filter: Document,
        options: Option<FindOptions>,
        batch_size: Option<u32>,
        doc_bytes_len: u32,
    },
    FindOne {
        db: String,
        collection: String,
        filter: Document,
        projection: Option<Document>,
    },
    InsertOne {
        db: String,
        collection: String,
        doc_bytes_len: u32,
    },
    InsertMany {
        db: String,
        collection: String,
        ordered: bool,
        doc_bytes_len: u32,
    },
    UpdateOne {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    UpdateMany {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    DeleteOne {
        db: String,
        collection: String,
        filter: Document,
    },
    DeleteMany {
        db: String,
        collection: String,
        filter: Document,
    },
    Aggregate {
        db: String,
        collection: String,
        pipeline: Vec<Document>,
        batch_size: Option<u32>,
    },
    CountDocuments {
        db: String,
        collection: String,
        filter: Document,
    },
    RunCommand {
        db: String,
        command: Document,
    },
    CreateIndex {
        db: String,
        collection: String,
        keys: Document,
        name: Option<String>,
        unique: Option<bool>,
    },
    ListCollections {
        db: String,
    },
    CreateCollection {
        db: String,
        name: String,
    },
    DropCollection {
        db: String,
        name: String,
    },
    Distinct {
        db: String,
        collection: String,
        field: String,
        filter: Option<Document>,
    },
    FindOneAndUpdate {
        db: String,
        collection: String,
        filter: Document,
        update: Document,
    },
    FindOneAndDelete {
        db: String,
        collection: String,
        filter: Document,
    },
    BulkWrite {
        db: String,
        collection: String,
        doc_bytes_len: u32,
        ordered: bool,
    },
}

#[derive(Debug)]
pub enum OperationResponse {
    Handshake { max_frame_size: u32, max_concurrent: u32 },
    SingleDoc { ok: bool, doc: Option<Vec<u8>> },
    Batch { count: u32, doc_bytes: Vec<u8> },
    InsertOneResult { inserted_id: String },
    InsertManyResult { inserted_count: u32 },
    UpdateResult { matched_count: u64, modified_count: u64 },
    DeleteResult { deleted_count: u64 },
    CountResult { count: u64 },
    IndexResult { name: String },
    Collections { names: Vec<String> },
    CommandResult { doc: Vec<u8> },
    Error { code: i32, message: String },
    Shutdown { drain_ms: u32 },
}

pub fn parse_envelope(bytes: &[u8], opcode: Opcode) -> Result<OperationRequest, CodecError> {
    let envelope: Document = bson::from_slice(bytes)
        .map_err(|e| CodecError::InvalidBson(e.to_string()))?;

    match opcode {
        Opcode::Handshake => {
            let lang = envelope.get_str("client_language")
                .unwrap_or("unknown").to_string();
            Ok(OperationRequest::Handshake { client_language: lang })
        }
        Opcode::FindOne => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = envelope.get_document("filter").cloned().unwrap_or_default();
            let projection = envelope.get_document("projection").cloned();
            Ok(OperationRequest::FindOne { db, collection: coll, filter, projection })
        }
        Opcode::Find => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = envelope.get_document("filter").cloned().unwrap_or_default();
            let limit = envelope.get_i64("limit").ok().map(|v| v as i64);
            let skip = envelope.get_i64("skip").ok().map(|v| v as u64);
            let sort = envelope.get_document("sort").cloned();
            let projection = envelope.get_document("projection").cloned();
            let batch_size = envelope.get_i32("batch_size").ok().map(|v| v as u32);
            let doc_bytes_len = envelope.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            let options = Some(FindOptions { limit, skip, sort, projection });
            Ok(OperationRequest::Find { db, collection: coll, filter, options, batch_size, doc_bytes_len })
        }
        Opcode::InsertOne => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let doc_bytes_len = envelope.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            Ok(OperationRequest::InsertOne { db, collection: coll, doc_bytes_len })
        }
        Opcode::InsertMany => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let ordered = envelope.get_bool("ordered").unwrap_or(true);
            let doc_bytes_len = envelope.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            Ok(OperationRequest::InsertMany { db, collection: coll, ordered, doc_bytes_len })
        }
        Opcode::UpdateOne => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            let update = get_required_doc(&envelope, "update")?;
            Ok(OperationRequest::UpdateOne { db, collection: coll, filter, update })
        }
        Opcode::UpdateMany => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            let update = get_required_doc(&envelope, "update")?;
            Ok(OperationRequest::UpdateMany { db, collection: coll, filter, update })
        }
        Opcode::DeleteOne => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            Ok(OperationRequest::DeleteOne { db, collection: coll, filter })
        }
        Opcode::DeleteMany => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            Ok(OperationRequest::DeleteMany { db, collection: coll, filter })
        }
        Opcode::Aggregate => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let pipeline = envelope.get_array("pipeline")
                .map_err(|_| CodecError::MissingField("pipeline"))?
                .iter()
                .map(|v| v.as_document().cloned().ok_or(CodecError::InvalidBson("pipeline stage not a document".into())))
                .collect::<Result<Vec<_>, _>>()?;
            let batch_size = envelope.get_i32("batch_size").ok().map(|v| v as u32);
            Ok(OperationRequest::Aggregate { db, collection: coll, pipeline, batch_size })
        }
        Opcode::CountDocuments => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = envelope.get_document("filter").cloned().unwrap_or_default();
            Ok(OperationRequest::CountDocuments { db, collection: coll, filter })
        }
        Opcode::RunCommand => {
            let db = get_required_str(&envelope, "db")?;
            let command = get_required_doc(&envelope, "command")?;
            Ok(OperationRequest::RunCommand { db, command })
        }
        Opcode::CreateIndex => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let keys = get_required_doc(&envelope, "keys")?;
            let name = envelope.get_str("name").ok().map(|s| s.to_string());
            let unique = envelope.get_bool("unique").ok();
            Ok(OperationRequest::CreateIndex { db, collection: coll, keys, name, unique })
        }
        Opcode::ListCollections => {
            let db = get_required_str(&envelope, "db")?;
            Ok(OperationRequest::ListCollections { db })
        }
        Opcode::CreateCollection => {
            let db = get_required_str(&envelope, "db")?;
            let name = get_required_str(&envelope, "name")?;
            Ok(OperationRequest::CreateCollection { db, name })
        }
        Opcode::DropCollection => {
            let db = get_required_str(&envelope, "db")?;
            let name = get_required_str(&envelope, "name")?;
            Ok(OperationRequest::DropCollection { db, name })
        }
        Opcode::Distinct => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let field = get_required_str(&envelope, "field")?;
            let filter = envelope.get_document("filter").cloned();
            Ok(OperationRequest::Distinct { db, collection: coll, field, filter })
        }
        Opcode::FindOneAndUpdate => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            let update = get_required_doc(&envelope, "update")?;
            Ok(OperationRequest::FindOneAndUpdate { db, collection: coll, filter, update })
        }
        Opcode::FindOneAndDelete => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let filter = get_required_doc(&envelope, "filter")?;
            Ok(OperationRequest::FindOneAndDelete { db, collection: coll, filter })
        }
        Opcode::BulkWrite => {
            let db = get_required_str(&envelope, "db")?;
            let coll = get_required_str(&envelope, "coll")?;
            let doc_bytes_len = envelope.get_i32("doc_bytes_len").unwrap_or(0) as u32;
            let ordered = envelope.get_bool("ordered").unwrap_or(true);
            Ok(OperationRequest::BulkWrite { db, collection: coll, doc_bytes_len, ordered })
        }
        Opcode::Error => Err(CodecError::InvalidBson("client cannot send Error opcode".into())),
    }
}

pub fn encode_response(response: &OperationResponse) -> Result<(Vec<u8>, Vec<u8>), CodecError> {
    match response {
        OperationResponse::Handshake { max_frame_size, max_concurrent } => {
            let env = doc! {
                "max_frame_size": *max_frame_size as i32,
                "max_concurrent": *max_concurrent as i32,
                "doc_bytes_len": 0_i32
            };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::SingleDoc { ok, doc } => {
            let raw = doc.clone().unwrap_or_default();
            let env = doc! {
                "ok": if *ok { 1_i32 } else { 0_i32 },
                "doc_bytes_len": raw.len() as i32
            };
            Ok((bson::to_vec(&env).unwrap(), raw))
        }
        OperationResponse::Batch { count, doc_bytes } => {
            let env = doc! {
                "count": *count as i32,
                "doc_bytes_len": doc_bytes.len() as i32
            };
            Ok((bson::to_vec(&env).unwrap(), doc_bytes.clone()))
        }
        OperationResponse::InsertOneResult { inserted_id } => {
            let env = doc! { "ok": 1_i32, "inserted_id": inserted_id, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::InsertManyResult { inserted_count } => {
            let env = doc! { "ok": 1_i32, "inserted_count": *inserted_count as i32, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::UpdateResult { matched_count, modified_count } => {
            let env = doc! {
                "ok": 1_i32,
                "matched_count": *matched_count as i64,
                "modified_count": *modified_count as i64,
                "doc_bytes_len": 0_i32
            };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::DeleteResult { deleted_count } => {
            let env = doc! { "ok": 1_i32, "deleted_count": *deleted_count as i64, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::CountResult { count } => {
            let env = doc! { "ok": 1_i32, "count": *count as i64, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::IndexResult { name } => {
            let env = doc! { "ok": 1_i32, "name": name, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::Collections { names } => {
            let env = doc! { "ok": 1_i32, "names": names, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::CommandResult { doc } => {
            let env = doc! { "ok": 1_i32, "doc_bytes_len": doc.len() as i32 };
            Ok((bson::to_vec(&env).unwrap(), doc.clone()))
        }
        OperationResponse::Error { code, message } => {
            let env = doc! { "code": *code, "message": message, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
        OperationResponse::Shutdown { drain_ms } => {
            let env = doc! { "shutdown": true, "drain_ms": *drain_ms as i32, "doc_bytes_len": 0_i32 };
            Ok((bson::to_vec(&env).unwrap(), Vec::new()))
        }
    }
}

fn get_required_str(doc: &Document, field: &'static str) -> Result<String, CodecError> {
    doc.get_str(field)
        .map(|s| s.to_string())
        .map_err(|_| CodecError::MissingField(field))
}

fn get_required_doc(doc: &Document, field: &'static str) -> Result<Document, CodecError> {
    doc.get_document(field)
        .cloned()
        .map_err(|_| CodecError::MissingField(field))
}

#[derive(Debug, Clone)]
pub enum CodecError {
    MissingField(&'static str),
    InvalidBson(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "missing required field: {field}"),
            Self::InvalidBson(msg) => write!(f, "invalid BSON: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}
```

- [ ] **Step 4: Update mod.rs**

```rust
// src/transport/mod.rs
pub mod buffer_pool;
pub mod codec;
pub mod frame;
```

- [ ] **Step 5: Run tests**

Run: `cargo test --lib transport::codec`
Expected: All 4 tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/transport/codec.rs src/transport/mod.rs
git commit -m "feat(transport): add BSON envelope codec for all opcodes"
```

---

## Task 6: Dispatch — Route Opcodes to Operations (`src/transport/dispatch.rs`)

**Files:**
- Create: `src/transport/dispatch.rs`
- Modify: `src/transport/mod.rs`

- [ ] **Step 1: Write failing test**

```rust
// src/transport/dispatch.rs
#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_dispatch_handshake() {
        // Handshake doesn't need operations — it's handled inline
        let req = OperationRequest::Handshake { client_language: "rust".to_string() };
        let response = handle_handshake(&req, 64 * 1024 * 1024, 64);
        match response {
            OperationResponse::Handshake { max_frame_size, max_concurrent } => {
                assert_eq!(max_frame_size, 64 * 1024 * 1024);
                assert_eq!(max_concurrent, 64);
            }
            _ => panic!("expected Handshake response"),
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib transport::dispatch`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement dispatch**

```rust
// src/transport/dispatch.rs
use bson::Document;
use tracing::{debug, warn};

use crate::operations::{FindOptions, Operations, IndexOptions};
use crate::operations::raw::{run_command, RawCommandOptions};
use crate::connection::pool::ConnectionPool;
use crate::transport::codec::{OperationRequest, OperationResponse};
use crate::transport::frame::{Opcode, Priority};

pub fn handle_handshake(
    req: &OperationRequest,
    max_frame_size: u32,
    max_concurrent: u32,
) -> OperationResponse {
    if let OperationRequest::Handshake { client_language } = req {
        debug!(client_language, "binary transport handshake");
        OperationResponse::Handshake { max_frame_size, max_concurrent }
    } else {
        OperationResponse::Error {
            code: 1,
            message: "expected handshake".to_string(),
        }
    }
}

pub async fn dispatch(
    operations: &Operations,
    pool: &ConnectionPool,
    request: OperationRequest,
    raw_docs: &[u8],
) -> OperationResponse {
    match request {
        OperationRequest::Handshake { .. } => {
            OperationResponse::Error { code: 1, message: "handshake already completed".into() }
        }
        OperationRequest::FindOne { db, collection, filter, projection } => {
            let mut opts_filter = filter;
            // If projection provided, use find with limit 1 and projection
            match operations.find_one(&db, &collection, opts_filter).await {
                Ok(Some(doc)) => {
                    let raw = bson::to_vec(&doc).unwrap_or_default();
                    OperationResponse::SingleDoc { ok: true, doc: Some(raw) }
                }
                Ok(None) => OperationResponse::SingleDoc { ok: true, doc: None },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::Find { db, collection, filter, options, .. } => {
            match operations.find(&db, &collection, filter, options).await {
                Ok(docs) => {
                    let mut raw_bytes = Vec::new();
                    for doc in &docs {
                        raw_bytes.extend(bson::to_vec(doc).unwrap_or_default());
                    }
                    OperationResponse::Batch { count: docs.len() as u32, doc_bytes: raw_bytes }
                }
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::InsertOne { db, collection, .. } => {
            match parse_single_bson_doc(raw_docs) {
                Ok(doc) => match operations.insert(&db, &collection, doc).await {
                    Ok(result) => OperationResponse::InsertOneResult {
                        inserted_id: result.inserted_id.to_string(),
                    },
                    Err(e) => to_error_response(e),
                },
                Err(e) => OperationResponse::Error { code: 2, message: e },
            }
        }
        OperationRequest::InsertMany { db, collection, ordered, .. } => {
            match parse_bson_docs(raw_docs) {
                Ok(docs) => match operations.insert_many(&db, &collection, docs).await {
                    Ok(result) => OperationResponse::InsertManyResult {
                        inserted_count: result.inserted_ids.len() as u32,
                    },
                    Err(e) => to_error_response(e),
                },
                Err(e) => OperationResponse::Error { code: 2, message: e },
            }
        }
        OperationRequest::UpdateOne { db, collection, filter, update } => {
            match operations.update(&db, &collection, filter, update).await {
                Ok(r) => OperationResponse::UpdateResult {
                    matched_count: r.matched_count,
                    modified_count: r.modified_count,
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::UpdateMany { db, collection, filter, update } => {
            match operations.update_many(&db, &collection, filter, update).await {
                Ok(r) => OperationResponse::UpdateResult {
                    matched_count: r.matched_count,
                    modified_count: r.modified_count,
                },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::DeleteOne { db, collection, filter } => {
            match operations.delete(&db, &collection, filter).await {
                Ok(r) => OperationResponse::DeleteResult { deleted_count: r.deleted_count },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::DeleteMany { db, collection, filter } => {
            match operations.delete_many(&db, &collection, filter).await {
                Ok(r) => OperationResponse::DeleteResult { deleted_count: r.deleted_count },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::Aggregate { db, collection, pipeline, .. } => {
            match operations.aggregate(&db, &collection, pipeline).await {
                Ok(docs) => {
                    let mut raw_bytes = Vec::new();
                    for doc in &docs {
                        raw_bytes.extend(bson::to_vec(doc).unwrap_or_default());
                    }
                    OperationResponse::Batch { count: docs.len() as u32, doc_bytes: raw_bytes }
                }
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::CountDocuments { db, collection, filter } => {
            match operations.count_documents(&db, &collection, filter).await {
                Ok(count) => OperationResponse::CountResult { count },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::RunCommand { db, command } => {
            let opts = RawCommandOptions::default();
            match run_command(pool, &db, command, &opts).await {
                Ok(doc) => {
                    let raw = bson::to_vec(&doc).unwrap_or_default();
                    OperationResponse::CommandResult { doc: raw }
                }
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::CreateIndex { db, collection, keys, name, unique } => {
            let opts = if name.is_some() || unique.is_some() {
                Some(IndexOptions { name, unique })
            } else {
                None
            };
            match operations.create_index(&db, &collection, keys, opts).await {
                Ok(name) => OperationResponse::IndexResult { name },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::ListCollections { db } => {
            match operations.list_collections(&db).await {
                Ok(names) => OperationResponse::Collections { names },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::CreateCollection { db, name } => {
            match operations.create_collection(&db, &name).await {
                Ok(()) => OperationResponse::SingleDoc { ok: true, doc: None },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::DropCollection { db, name } => {
            match operations.drop_collection(&db, &name).await {
                Ok(()) => OperationResponse::SingleDoc { ok: true, doc: None },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::Distinct { db, collection, field, filter } => {
            match operations.distinct(&db, &collection, &field, filter).await {
                Ok(values) => {
                    let doc = bson::doc! { "values": values };
                    let raw = bson::to_vec(&doc).unwrap_or_default();
                    OperationResponse::SingleDoc { ok: true, doc: Some(raw) }
                }
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::FindOneAndUpdate { db, collection, filter, update } => {
            match operations.find_and_modify(&db, &collection, filter, update, None).await {
                Ok(Some(doc)) => {
                    let raw = bson::to_vec(&doc).unwrap_or_default();
                    OperationResponse::SingleDoc { ok: true, doc: Some(raw) }
                }
                Ok(None) => OperationResponse::SingleDoc { ok: true, doc: None },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::FindOneAndDelete { db, collection, filter } => {
            match operations.find_one_and_delete(&db, &collection, filter).await {
                Ok(Some(doc)) => {
                    let raw = bson::to_vec(&doc).unwrap_or_default();
                    OperationResponse::SingleDoc { ok: true, doc: Some(raw) }
                }
                Ok(None) => OperationResponse::SingleDoc { ok: true, doc: None },
                Err(e) => to_error_response(e),
            }
        }
        OperationRequest::BulkWrite { db, collection, ordered, .. } => {
            match parse_bson_docs(raw_docs) {
                Ok(docs) => match operations.insert_many(&db, &collection, docs).await {
                    Ok(result) => OperationResponse::InsertManyResult {
                        inserted_count: result.inserted_ids.len() as u32,
                    },
                    Err(e) => to_error_response(e),
                },
                Err(e) => OperationResponse::Error { code: 2, message: e },
            }
        }
    }
}

fn to_error_response(err: crate::error::MongoCoreError) -> OperationResponse {
    OperationResponse::Error {
        code: 1,
        message: err.to_string(),
    }
}

fn parse_single_bson_doc(raw: &[u8]) -> Result<Document, String> {
    if raw.len() < 5 {
        return Err("raw doc bytes too short".into());
    }
    bson::from_slice(raw).map_err(|e| format!("invalid BSON document: {e}"))
}

fn parse_bson_docs(raw: &[u8]) -> Result<Vec<Document>, String> {
    let mut docs = Vec::new();
    let mut offset = 0;
    while offset < raw.len() {
        if offset + 4 > raw.len() {
            return Err("truncated BSON document".into());
        }
        let doc_len = i32::from_le_bytes([
            raw[offset], raw[offset + 1], raw[offset + 2], raw[offset + 3],
        ]) as usize;
        if offset + doc_len > raw.len() || doc_len < 5 {
            return Err(format!("invalid BSON doc length {doc_len} at offset {offset}"));
        }
        let doc: Document = bson::from_slice(&raw[offset..offset + doc_len])
            .map_err(|e| format!("invalid BSON at offset {offset}: {e}"))?;
        docs.push(doc);
        offset += doc_len;
    }
    Ok(docs)
}
```

- [ ] **Step 4: Update mod.rs**

```rust
// src/transport/mod.rs
pub mod buffer_pool;
pub mod codec;
pub mod dispatch;
pub mod frame;
```

- [ ] **Step 5: Run tests and build**

Run: `cargo build 2>&1 | grep "warning:"` — fix any warnings
Run: `cargo test --lib transport::dispatch`
Expected: Test passes

- [ ] **Step 6: Commit**

```bash
git add src/transport/dispatch.rs src/transport/mod.rs
git commit -m "feat(transport): add opcode dispatch routing to operations layer"
```

---

## Task 7: Connection Handler (`src/transport/connection.rs`)

**Files:**
- Create: `src/transport/connection.rs`
- Modify: `src/transport/mod.rs`

- [ ] **Step 1: Implement connection handler**

```rust
// src/transport/connection.rs
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tracing::{debug, error, warn, info};

use crate::connection::pool::ConnectionPool;
use crate::operations::Operations;
use crate::transport::buffer_pool::BufferPool;
use crate::transport::codec::{self, OperationRequest, OperationResponse};
use crate::transport::dispatch;
use crate::transport::frame::{self, Frame, FrameHeader, Flags, Opcode, Priority, read_frame, write_frame, HEADER_SIZE, FrameError};

pub struct ConnectionConfig {
    pub max_frame_size: u32,
    pub max_concurrent: u32,
}

pub async fn handle_connection(
    mut stream: UnixStream,
    operations: Operations,
    pool: ConnectionPool,
    config: ConnectionConfig,
    buffer_pool: BufferPool,
) {
    // Step 1: Handshake
    let handshake_frame = match read_frame(&mut stream, config.max_frame_size).await {
        Ok(f) => f,
        Err(e) => {
            warn!("handshake read failed: {e}");
            return;
        }
    };

    if handshake_frame.header.flags.opcode != Opcode::Handshake {
        warn!("first frame was not handshake, closing");
        let _ = send_error(&mut stream, 0, 1, "expected handshake as first frame").await;
        return;
    }

    let req = match codec::parse_envelope(&handshake_frame.envelope, Opcode::Handshake) {
        Ok(r) => r,
        Err(e) => {
            let _ = send_error(&mut stream, handshake_frame.header.req_id, 1, &e.to_string()).await;
            return;
        }
    };

    let response = dispatch::handle_handshake(&req, config.max_frame_size, config.max_concurrent);
    let (env_bytes, doc_bytes) = codec::encode_response(&response).unwrap();
    let resp_frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: handshake_frame.header.req_id,
        },
        envelope: env_bytes,
        raw_docs: doc_bytes,
    };
    if let Err(e) = write_frame(&mut stream, &resp_frame).await {
        warn!("handshake response write failed: {e}");
        return;
    }

    debug!("handshake complete, entering request loop");

    // Step 2: Request/response loop
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent as usize));

    loop {
        let frame = match read_frame(&mut stream, config.max_frame_size).await {
            Ok(f) => f,
            Err(FrameError::Io(_)) => {
                debug!("client disconnected");
                break;
            }
            Err(e) => {
                warn!("frame read error: {e}");
                break;
            }
        };

        let opcode = frame.header.flags.opcode;
        let req_id = frame.header.req_id;
        let no_reply = frame.header.flags.no_reply;

        // Enforce critical priority restriction
        let priority = if frame.header.flags.priority == Priority::Critical && opcode != Opcode::Handshake {
            Priority::High
        } else {
            frame.header.flags.priority
        };

        // Parse envelope
        let request = match codec::parse_envelope(&frame.envelope, opcode) {
            Ok(r) => r,
            Err(e) => {
                if !no_reply {
                    let _ = send_error(&mut stream, req_id, 2, &e.to_string()).await;
                }
                continue;
            }
        };

        // Dispatch (respecting concurrency limit)
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let ops = operations.clone();
        let p = pool.clone();
        let raw_docs = frame.raw_docs;

        let response = dispatch::dispatch(&ops, &p, request, &raw_docs).await;
        drop(permit);

        if no_reply {
            continue;
        }

        // Encode and send response
        let (env_bytes, doc_bytes) = match codec::encode_response(&response) {
            Ok(r) => r,
            Err(e) => {
                error!("response encode failed: {e}");
                break;
            }
        };

        let resp_opcode = if matches!(response, OperationResponse::Error { .. }) {
            Opcode::Error
        } else {
            opcode
        };

        let resp_frame = Frame {
            header: FrameHeader {
                msg_len: 0,
                flags: Flags {
                    opcode: resp_opcode,
                    end_of_stream: true,
                    no_reply: false,
                    batch_follows: false,
                    priority: Priority::Normal,
                },
                req_id,
            },
            envelope: env_bytes,
            raw_docs: doc_bytes,
        };

        if let Err(e) = write_frame(&mut stream, &resp_frame).await {
            debug!("write failed, client likely disconnected: {e}");
            break;
        }
    }
}

async fn send_error(stream: &mut UnixStream, req_id: u32, code: i32, message: &str) -> Result<(), FrameError> {
    let response = OperationResponse::Error { code, message: message.to_string() };
    let (env_bytes, doc_bytes) = codec::encode_response(&response).unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::Error,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: env_bytes,
        raw_docs: doc_bytes,
    };
    write_frame(stream, &frame).await
}
```

- [ ] **Step 2: Update mod.rs**

```rust
// src/transport/mod.rs
pub mod buffer_pool;
pub mod codec;
pub mod connection;
pub mod dispatch;
pub mod frame;
```

- [ ] **Step 3: Build and fix warnings**

Run: `cargo build 2>&1 | grep "warning:"`
Expected: No output (fix any warnings)

- [ ] **Step 4: Commit**

```bash
git add src/transport/connection.rs src/transport/mod.rs
git commit -m "feat(transport): add per-connection handler with handshake and dispatch"
```

---

## Task 8: Transport Server — Accept Loop and Startup (`src/transport/mod.rs`)

**Files:**
- Modify: `src/transport/mod.rs` (add TransportServer and start function)
- Modify: `src/main.rs` (start binary transport alongside gRPC)

- [ ] **Step 1: Implement TransportServer in mod.rs**

```rust
// src/transport/mod.rs
pub mod buffer_pool;
pub mod codec;
pub mod connection;
pub mod dispatch;
pub mod frame;

use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn, error, debug};

use crate::config::Config;
use crate::connection::pool::ConnectionPool;
use crate::operations::Operations;
use buffer_pool::{BufferPool, BufferPoolConfig};
use connection::ConnectionConfig;

pub fn start_binary_transport(
    config: &Config,
    pool: ConnectionPool,
    operations: Operations,
) -> JoinHandle<()> {
    let socket_path = config.binary_socket_path.clone();
    let permissions = config.binary_socket_permissions;
    let max_frame_size = config.binary_max_frame_size as u32;
    let max_concurrent = config.binary_max_concurrent as u32;

    tokio::spawn(async move {
        // Remove stale socket file
        let path = Path::new(&socket_path);
        if path.exists() {
            if let Err(e) = std::fs::remove_file(path) {
                error!("failed to remove stale socket {socket_path}: {e}");
                return;
            }
        }

        let listener = match UnixListener::bind(&socket_path) {
            Ok(l) => l,
            Err(e) => {
                error!("failed to bind binary transport to {socket_path}: {e}");
                return;
            }
        };

        // Set socket permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(permissions);
            if let Err(e) = std::fs::set_permissions(&socket_path, perms) {
                warn!("failed to set socket permissions: {e}");
            }
        }

        let buffer_pool = BufferPool::new(BufferPoolConfig::default());

        info!("Binary transport listening on {socket_path}");
        pool.append_interface_metadata("binary");

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let ops = operations.clone();
                    let p = pool.clone();
                    let bp = buffer_pool.clone();
                    let conn_config = ConnectionConfig {
                        max_frame_size,
                        max_concurrent,
                    };
                    tokio::spawn(async move {
                        connection::handle_connection(stream, ops, p, conn_config, bp).await;
                    });
                }
                Err(e) => {
                    error!("binary transport accept error: {e}");
                }
            }
        }
    })
}
```

- [ ] **Step 2: Wire into main.rs**

In `src/main.rs`, after the gRPC server start and before the `tokio::select!` block, add:

```rust
// Start binary transport (if enabled)
let binary_handle = if config.binary_transport_enabled {
    Some(crate::transport::start_binary_transport(&config, pool.clone(), operations.clone()))
} else {
    None
};
```

Add to the `tokio::select!` block:

```rust
_ = async { if let Some(h) = binary_handle { h.await.ok(); } } => {
    error!("Binary transport exited unexpectedly");
}
```

- [ ] **Step 3: Add cleanup on shutdown**

After the existing UDS socket cleanup in main.rs (around lines 270-275), add:

```rust
// Clean up binary transport socket
if config.binary_transport_enabled {
    let _ = std::fs::remove_file(&config.binary_socket_path);
}
```

- [ ] **Step 4: Build and verify**

Run: `cargo build 2>&1 | grep "warning:"`
Expected: No output

Run: `cargo test --lib`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/transport/mod.rs src/main.rs
git commit -m "feat(transport): add binary transport server startup and accept loop"
```

---

## Task 9: Integration Test — End-to-End Over UDS

**Files:**
- Create: `tests/integration/binary_transport_test.rs`
- Modify: `tests/integration/mod.rs` (if it exists, register new test module)

- [ ] **Step 1: Write integration test**

```rust
// tests/integration/binary_transport_test.rs
use bson::doc;
use tokio::net::UnixStream;

// Re-use frame types from the main crate
use mongocore::transport::frame::*;
use mongocore::transport::codec;

const TEST_SOCKET: &str = "/tmp/mongocore.bin.sock";

async fn connect_and_handshake() -> UnixStream {
    let mut stream = UnixStream::connect(TEST_SOCKET).await
        .expect("failed to connect to binary transport socket - is mongocore running?");

    // Send handshake
    let handshake_env = bson::to_vec(&doc! { "client_language": "rust-test", "doc_bytes_len": 0_i32 }).unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 0,
        },
        envelope: handshake_env,
        raw_docs: Vec::new(),
    };
    write_frame(&mut stream, &frame).await.unwrap();

    // Read handshake response
    let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(resp.header.flags.opcode, Opcode::Handshake);
    assert!(resp.header.flags.end_of_stream);

    stream
}

#[tokio::test]
async fn test_binary_transport_handshake() {
    let _stream = connect_and_handshake().await;
}

#[tokio::test]
async fn test_binary_transport_insert_and_find_one() {
    let mut stream = connect_and_handshake().await;

    // InsertOne
    let insert_env = bson::to_vec(&doc! {
        "db": "test_binary",
        "coll": "integration",
        "doc_bytes_len": 0_i32  // we'll compute real value
    }).unwrap();

    let test_doc = doc! { "_id": "binary_test_1", "name": "Alice", "age": 30 };
    let raw_doc = bson::to_vec(&test_doc).unwrap();

    // Fix envelope with correct doc_bytes_len
    let insert_env = bson::to_vec(&doc! {
        "db": "test_binary",
        "coll": "integration",
        "doc_bytes_len": raw_doc.len() as i32
    }).unwrap();

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::InsertOne,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 1,
        },
        envelope: insert_env,
        raw_docs: raw_doc,
    };
    write_frame(&mut stream, &frame).await.unwrap();

    let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(resp.header.req_id, 1);
    assert!(resp.header.flags.end_of_stream);

    // FindOne
    let find_env = bson::to_vec(&doc! {
        "db": "test_binary",
        "coll": "integration",
        "filter": { "_id": "binary_test_1" },
        "doc_bytes_len": 0_i32
    }).unwrap();

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::FindOne,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 2,
        },
        envelope: find_env,
        raw_docs: Vec::new(),
    };
    write_frame(&mut stream, &frame).await.unwrap();

    let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(resp.header.req_id, 2);
    assert!(resp.header.flags.end_of_stream);
    assert!(!resp.raw_docs.is_empty());

    let found_doc: bson::Document = bson::from_slice(&resp.raw_docs).unwrap();
    assert_eq!(found_doc.get_str("name").unwrap(), "Alice");
}

#[tokio::test]
async fn test_binary_transport_fire_and_forget() {
    let mut stream = connect_and_handshake().await;

    let test_doc = doc! { "_id": "fire_forget_1", "data": "ephemeral" };
    let raw_doc = bson::to_vec(&test_doc).unwrap();
    let insert_env = bson::to_vec(&doc! {
        "db": "test_binary",
        "coll": "fire_forget",
        "doc_bytes_len": raw_doc.len() as i32
    }).unwrap();

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::InsertOne,
                end_of_stream: false,
                no_reply: true, // fire-and-forget
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 100,
        },
        envelope: insert_env,
        raw_docs: raw_doc,
    };
    write_frame(&mut stream, &frame).await.unwrap();

    // No response expected — send another request to confirm connection still works
    let ping_env = bson::to_vec(&doc! { "client_language": "rust-test", "doc_bytes_len": 0_i32 }).unwrap();
    let ping_frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Critical,
            },
            req_id: 101,
        },
        envelope: ping_env,
        raw_docs: Vec::new(),
    };
    write_frame(&mut stream, &ping_frame).await.unwrap();

    // Should get response for the ping, not the fire-and-forget insert
    let resp = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    assert_eq!(resp.header.req_id, 101);
}
```

- [ ] **Step 2: Run integration test (requires running MongoCore + MongoDB)**

Run: `just docker-up` (if not already running)
Start MongoCore: `cargo run -- --connection-uri mongodb://localhost:27017`
Run: `cargo test --test integration binary_transport`
Expected: All 3 tests PASS

- [ ] **Step 3: Commit**

```bash
git add tests/integration/binary_transport_test.rs
git commit -m "test(transport): add binary transport integration tests"
```

---

## Task 10: Benchmark Harness (`benches/transport_bench.rs`)

**Files:**
- Create: `benches/transport_bench.rs`
- Modify: `Cargo.toml` (add `[[bench]]` section)
- Modify: `justfile` (add `bench-transport` command)

- [ ] **Step 1: Add bench configuration to Cargo.toml**

```toml
[[bench]]
name = "transport_bench"
harness = false
```

Add to `[dev-dependencies]`:
```toml
criterion = { version = "0.5", features = ["async_tokio"] }
```

- [ ] **Step 2: Create benchmark binary**

```rust
// benches/transport_bench.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use bson::doc;
use tokio::net::UnixStream;
use tokio::runtime::Runtime;

use mongocore::transport::frame::*;

const BINARY_SOCKET: &str = "/tmp/mongocore.bin.sock";
const GRPC_ADDR: &str = "http://localhost:50051";

async fn binary_handshake() -> UnixStream {
    let mut stream = UnixStream::connect(BINARY_SOCKET).await.unwrap();
    let env = bson::to_vec(&doc! { "client_language": "bench", "doc_bytes_len": 0_i32 }).unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags { opcode: Opcode::Handshake, end_of_stream: false, no_reply: false, batch_follows: false, priority: Priority::Normal },
            req_id: 0,
        },
        envelope: env,
        raw_docs: Vec::new(),
    };
    write_frame(&mut stream, &frame).await.unwrap();
    let _ = read_frame(&mut stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
    stream
}

async fn binary_find_one(stream: &mut UnixStream, req_id: u32) {
    let env = bson::to_vec(&doc! {
        "db": "bench_db",
        "coll": "bench_coll",
        "filter": { "_id": "bench_doc" },
        "doc_bytes_len": 0_i32
    }).unwrap();
    let frame = Frame {
        header: FrameHeader {
            msg_len: 0,
            flags: Flags { opcode: Opcode::FindOne, end_of_stream: false, no_reply: false, batch_follows: false, priority: Priority::Normal },
            req_id,
        },
        envelope: env,
        raw_docs: Vec::new(),
    };
    write_frame(stream, &frame).await.unwrap();
    let _ = read_frame(stream, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
}

fn bench_ping_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("ping_latency");

    group.bench_function("binary", |b| {
        b.to_async(&rt).iter(|| async {
            let mut stream = binary_handshake().await;
            // Ping is just a handshake-style frame after initial handshake
            binary_find_one(&mut stream, 1).await;
        });
    });

    group.finish();
}

fn bench_find_one(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_one");

    group.bench_function("binary", |b| {
        let stream = rt.block_on(binary_handshake());
        let mut stream = stream;
        let mut req_id = 1u32;
        b.to_async(&rt).iter(|| {
            req_id += 1;
            binary_find_one(&mut stream, req_id)
        });
    });

    group.finish();
}

criterion_group!(benches, bench_ping_latency, bench_find_one);
criterion_main!(benches);
```

- [ ] **Step 3: Add justfile command**

Add to `justfile`:
```makefile
bench-transport *args:
    cargo bench --bench transport_bench {{args}}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo bench --bench transport_bench --no-run`
Expected: Compiles successfully

- [ ] **Step 5: Commit**

```bash
git add benches/transport_bench.rs Cargo.toml justfile
git commit -m "feat(bench): add binary transport benchmark harness"
```

---

## Task 11: Update AGENTS.md

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: Update architecture diagram**

Add to the architecture diagram:
```
App (any lang) ──Binary UDS──▶ MongoCore Sidecar (Rust) ──Wire Protocol──▶ MongoDB
App (any lang) ──gRPC─────────▶        │                  ──REST API──────▶ Voyage AI
AI Agent ───────MCP───────────▶        └──Pluggable LLM──▶ Claude / OpenAI
```

- [ ] **Step 2: Add `src/transport/` to Project Layout**

Add after the `mcp/` entry:
```
├── transport/           # Binary UDS transport (frame, buffer pool, dispatch, codec)
```

- [ ] **Step 3: Update "Adding a New RPC" workflow**

Add after step 8:
```
9. Add opcode to `src/transport/frame.rs` Opcode enum
10. Add envelope parsing case in `src/transport/codec.rs` parse_envelope()
11. Add dispatch case in `src/transport/dispatch.rs` dispatch()
```

And renumber subsequent steps.

- [ ] **Step 4: Add binary transport testing commands**

Add to the Testing table:
```
| `cargo test --lib transport` | Binary transport unit tests | None |
```

Add to Test Gates:
```
- **After adding new opcodes:** verify binary transport dispatch compiles and unit tests pass
```

- [ ] **Step 5: Add configuration section**

Add new config fields to documentation or add a note referencing the binary transport config fields.

- [ ] **Step 6: Build and test**

Run: `cargo build 2>&1 | grep "warning:"`
Run: `cargo test --lib`
Expected: Pass

- [ ] **Step 7: Commit**

```bash
git add AGENTS.md
git commit -m "docs: update AGENTS.md with binary transport architecture and workflow"
```

---

## Task 12: Python Client Binary Transport (`clients/python/`)

**Files:**
- Create: `clients/python/src/mongocore/binary_transport.py`
- Modify: `clients/python/src/mongocore/client.py` (add transport selection)

- [ ] **Step 1: Implement binary transport module**

```python
# clients/python/src/mongocore/binary_transport.py
"""Binary UDS transport for MongoCore."""
import os
import struct
import socket
from typing import Optional

import bson

HEADER_SIZE = 10
DEFAULT_SOCKET_PATH = "/tmp/mongocore.bin.sock"
DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024

# Opcodes
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
OP_CREATE_INDEX = 0x0B
OP_LIST_COLLECTIONS = 0x0C
OP_RUN_COMMAND = 0x0D
OP_ERROR = 0x3E
OP_HANDSHAKE = 0x3F

# Flag bits
FLAG_EOS = 1 << 6
FLAG_NO_REPLY = 1 << 7
FLAG_BATCH_FOLLOWS = 1 << 8


class BinaryTransport:
    def __init__(self, socket_path: Optional[str] = None):
        self._socket_path = socket_path or os.environ.get(
            "MONGOCORE_BINARY_SOCKET_PATH", DEFAULT_SOCKET_PATH
        )
        self._sock: Optional[socket.socket] = None
        self._req_id = 0
        self._max_frame_size = DEFAULT_MAX_FRAME_SIZE

    def connect(self):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self._socket_path)
        self._handshake()

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def _handshake(self):
        envelope = bson.encode({"client_language": "python", "doc_bytes_len": 0})
        self._send_frame(OP_HANDSHAKE, envelope, b"")
        resp_envelope, _ = self._read_frame()
        resp = bson.decode(resp_envelope)
        self._max_frame_size = resp.get("max_frame_size", DEFAULT_MAX_FRAME_SIZE)

    def _next_req_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _encode_flags(self, opcode: int, eos: bool = False, no_reply: bool = False, batch_follows: bool = False) -> int:
        flags = opcode & 0x3F
        if eos:
            flags |= FLAG_EOS
        if no_reply:
            flags |= FLAG_NO_REPLY
        if batch_follows:
            flags |= FLAG_BATCH_FOLLOWS
        return flags

    def _send_frame(self, opcode: int, envelope: bytes, raw_docs: bytes, no_reply: bool = False):
        req_id = self._next_req_id()
        flags = self._encode_flags(opcode, no_reply=no_reply)
        payload_len = len(envelope) + len(raw_docs)
        msg_len = 2 + 4 + payload_len  # flags + req_id + payload

        header = struct.pack(">IHI", msg_len, flags, req_id)
        self._sock.sendall(header + envelope + raw_docs)
        return req_id

    def _read_frame(self) -> tuple[bytes, bytes]:
        header = self._recv_exact(HEADER_SIZE)
        msg_len, flags, req_id = struct.unpack(">IHI", header)

        payload_len = msg_len - 6
        payload = self._recv_exact(payload_len)

        # Split using BSON self-delimiting
        if len(payload) >= 4:
            bson_len = struct.unpack("<i", payload[:4])[0]
            envelope = payload[:bson_len]
            raw_docs = payload[bson_len:]
        else:
            envelope = payload
            raw_docs = b""

        return envelope, raw_docs

    def _recv_exact(self, n: int) -> bytes:
        data = b""
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("connection closed")
            data += chunk
        return data

    # Operation methods
    def find_one(self, db: str, collection: str, filter: dict, projection: dict = None) -> Optional[dict]:
        envelope = {"db": db, "coll": collection, "filter": filter, "doc_bytes_len": 0}
        if projection:
            envelope["projection"] = projection
        self._send_frame(OP_FIND_ONE, bson.encode(envelope), b"")
        resp_env, raw_docs = self._read_frame()
        resp = bson.decode(resp_env)
        if resp.get("doc_bytes_len", 0) > 0:
            return bson.decode(raw_docs)
        return None

    def insert_one(self, db: str, collection: str, document: dict) -> str:
        raw_doc = bson.encode(document)
        envelope = {"db": db, "coll": collection, "doc_bytes_len": len(raw_doc)}
        self._send_frame(OP_INSERT_ONE, bson.encode(envelope), raw_doc)
        resp_env, _ = self._read_frame()
        resp = bson.decode(resp_env)
        return resp.get("inserted_id", "")

    def insert_many(self, db: str, collection: str, documents: list[dict], ordered: bool = True) -> int:
        raw_docs = b"".join(bson.encode(d) for d in documents)
        envelope = {"db": db, "coll": collection, "ordered": ordered, "doc_bytes_len": len(raw_docs)}
        self._send_frame(OP_INSERT_MANY, bson.encode(envelope), raw_docs)
        resp_env, _ = self._read_frame()
        resp = bson.decode(resp_env)
        return resp.get("inserted_count", 0)

    def delete_one(self, db: str, collection: str, filter: dict) -> int:
        envelope = {"db": db, "coll": collection, "filter": filter, "doc_bytes_len": 0}
        self._send_frame(OP_DELETE_ONE, bson.encode(envelope), b"")
        resp_env, _ = self._read_frame()
        resp = bson.decode(resp_env)
        return resp.get("deleted_count", 0)

    @staticmethod
    def is_available(socket_path: Optional[str] = None) -> bool:
        path = socket_path or os.environ.get("MONGOCORE_BINARY_SOCKET_PATH", DEFAULT_SOCKET_PATH)
        return os.path.exists(path)
```

- [ ] **Step 2: Add transport selection to client**

Modify `clients/python/src/mongocore/client.py` to accept a `transport` parameter and instantiate `BinaryTransport` when `transport="binary"` or `transport="auto"` and the socket exists.

- [ ] **Step 3: Verify imports work**

Run: `cd clients/python && python -c "from mongocore.binary_transport import BinaryTransport; print('OK')"`
Expected: "OK"

- [ ] **Step 4: Commit**

```bash
git add clients/python/
git commit -m "feat(clients): add Python binary transport client"
```

---

## Task 13: Go Client Binary Transport (`clients/go/`)

**Files:**
- Create: `clients/go/mongocore/binary_transport.go`

- [ ] **Step 1: Implement Go binary transport**

```go
// clients/go/mongocore/binary_transport.go
package mongocore

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	headerSize          = 10
	defaultBinarySocket = "/tmp/mongocore.bin.sock"
	defaultMaxFrameSize = 64 * 1024 * 1024

	opFindOne    = 0x02
	opInsertOne  = 0x03
	opInsertMany = 0x04
	opDeleteOne  = 0x07
	opHandshake  = 0x3F

	flagEOS     = 1 << 6
	flagNoReply = 1 << 7
)

type BinaryTransport struct {
	conn         net.Conn
	reqID        atomic.Uint32
	maxFrameSize uint32
}

func NewBinaryTransport(socketPath string) (*BinaryTransport, error) {
	if socketPath == "" {
		socketPath = os.Getenv("MONGOCORE_BINARY_SOCKET_PATH")
		if socketPath == "" {
			socketPath = defaultBinarySocket
		}
	}

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("binary transport connect: %w", err)
	}

	bt := &BinaryTransport{conn: conn, maxFrameSize: defaultMaxFrameSize}
	if err := bt.handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	return bt, nil
}

func (bt *BinaryTransport) Close() error {
	return bt.conn.Close()
}

func (bt *BinaryTransport) handshake() error {
	env, _ := bson.Marshal(bson.M{"client_language": "go", "doc_bytes_len": int32(0)})
	bt.sendFrame(opHandshake, env, nil, false)
	respEnv, _, err := bt.readFrame()
	if err != nil {
		return err
	}
	var resp bson.M
	bson.Unmarshal(respEnv, &resp)
	if mfs, ok := resp["max_frame_size"]; ok {
		bt.maxFrameSize = uint32(mfs.(int32))
	}
	return nil
}

func (bt *BinaryTransport) nextReqID() uint32 {
	return bt.reqID.Add(1)
}

func (bt *BinaryTransport) sendFrame(opcode uint8, envelope, rawDocs []byte, noReply bool) error {
	reqID := bt.nextReqID()
	flags := uint16(opcode) & 0x3F
	if noReply {
		flags |= flagNoReply
	}

	payloadLen := len(envelope) + len(rawDocs)
	msgLen := uint32(2 + 4 + payloadLen)

	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], msgLen)
	binary.BigEndian.PutUint16(header[4:6], flags)
	binary.BigEndian.PutUint32(header[6:10], reqID)

	if _, err := bt.conn.Write(header); err != nil {
		return err
	}
	if _, err := bt.conn.Write(envelope); err != nil {
		return err
	}
	if len(rawDocs) > 0 {
		if _, err := bt.conn.Write(rawDocs); err != nil {
			return err
		}
	}
	return nil
}

func (bt *BinaryTransport) readFrame() (envelope, rawDocs []byte, err error) {
	header := make([]byte, headerSize)
	if _, err := bt.readExact(header); err != nil {
		return nil, nil, err
	}

	msgLen := binary.BigEndian.Uint32(header[0:4])
	payloadLen := msgLen - 6
	payload := make([]byte, payloadLen)
	if _, err := bt.readExact(payload); err != nil {
		return nil, nil, err
	}

	if len(payload) >= 4 {
		bsonLen := int32(binary.LittleEndian.Uint32(payload[0:4]))
		envelope = payload[:bsonLen]
		rawDocs = payload[bsonLen:]
	} else {
		envelope = payload
	}
	return envelope, rawDocs, nil
}

func (bt *BinaryTransport) readExact(buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := bt.conn.Read(buf[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func BinaryTransportAvailable(socketPath string) bool {
	if socketPath == "" {
		socketPath = os.Getenv("MONGOCORE_BINARY_SOCKET_PATH")
		if socketPath == "" {
			socketPath = defaultBinarySocket
		}
	}
	_, err := os.Stat(socketPath)
	return err == nil
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd clients/go && go build ./...`
Expected: Compiles successfully

- [ ] **Step 3: Commit**

```bash
git add clients/go/
git commit -m "feat(clients): add Go binary transport client"
```

---

## Task 14: Final Verification and Cleanup

**Files:**
- All modified files

- [ ] **Step 1: Full build with zero warnings**

Run: `cargo build 2>&1 | grep "warning:"`
Expected: No output

- [ ] **Step 2: All unit tests pass**

Run: `cargo test --lib`
Expected: All pass

- [ ] **Step 3: Integration tests compile**

Run: `cargo test --test integration --no-run`
Expected: Compiles

- [ ] **Step 4: Run integration tests (if MongoDB available)**

Run: `just docker-up && cargo run --release &` (start server in background)
Run: `cargo test --test integration binary_transport`
Expected: All pass

- [ ] **Step 5: Run benchmarks**

Run: `just bench-transport`
Expected: Produces comparison table

- [ ] **Step 6: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "chore(transport): final cleanup and verification"
```

---

## Summary of Deliverables

| Task | Component | Tests |
|------|-----------|-------|
| 1 | Frame types + header encode/decode | 4 unit tests |
| 2 | Async frame I/O over UDS | 2 unit tests |
| 3 | Buffer pool (slab allocator) | 3 unit tests |
| 4 | Config fields (CLI, env, TOML) | Existing config tests updated |
| 5 | BSON codec (all opcodes) | 4 unit tests |
| 6 | Dispatch (opcode → operation routing) | 1 unit test + compile check |
| 7 | Connection handler (handshake + loop) | Compile check |
| 8 | Transport server (accept loop, startup) | Integration via Task 9 |
| 9 | Integration tests | 3 end-to-end tests |
| 10 | Benchmark harness | Criterion benchmarks |
| 11 | AGENTS.md documentation | — |
| 12 | Python client | Import check |
| 13 | Go client | Compile check |
| 14 | Final verification | Full test suite |
