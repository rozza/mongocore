//! Binary frame encoding/decoding for the UDS transport protocol.
//!
//! Frame format:
//! ```text
//! ┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────────────┐
//! │ 4B msg_len   │ 2B flags     │ 4B req_id    │ N bytes BSON │ M bytes raw docs     │
//! └──────────────┴──────────────┴──────────────┴──────────────┴──────────────────────┘
//!   big-endian u32   see below    big-endian u32   envelope       passthrough bytes
//! ```

use std::fmt;
use std::io::IoSlice;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::transport::buffer_pool::BufferPool;

/// Size of the frame header in bytes (4 + 2 + 4 = 10).
pub const HEADER_SIZE: usize = 10;

/// Default maximum frame size: 64 MiB.
pub const MAX_FRAME_SIZE_DEFAULT: u32 = 64 * 1024 * 1024;

/// Errors that can occur during frame encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Opcode 0x00 is reserved/invalid, or the value exceeds the valid range.
    InvalidOpcode(u8),
    /// The frame exceeds the maximum allowed size.
    FrameTooLarge,
    /// The raw docs length doesn't match expectations.
    DocBytesLenMismatch,
    /// The BSON envelope is malformed.
    MalformedEnvelope(String),
    /// An I/O error occurred.
    Io(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::InvalidOpcode(op) => write!(f, "invalid opcode: 0x{:02X}", op),
            FrameError::FrameTooLarge => write!(f, "frame exceeds maximum allowed size"),
            FrameError::DocBytesLenMismatch => {
                write!(f, "raw docs length does not match expected size")
            }
            FrameError::MalformedEnvelope(msg) => write!(f, "malformed envelope: {}", msg),
            FrameError::Io(msg) => write!(f, "I/O error: {}", msg),
        }
    }
}

impl std::error::Error for FrameError {}

/// Operation codes for the binary protocol.
///
/// bits[0:5] of the flags field (6 bits, supporting up to 64 opcodes).
/// Opcode 0x00 is reserved/invalid.
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
    /// Decode an opcode from a raw u8 value.
    /// Returns an error if the value is 0x00 or not a recognized opcode.
    pub fn from_u8(value: u8) -> Result<Self, FrameError> {
        match value {
            0x00 => Err(FrameError::InvalidOpcode(0x00)),
            0x01 => Ok(Opcode::Find),
            0x02 => Ok(Opcode::FindOne),
            0x03 => Ok(Opcode::InsertOne),
            0x04 => Ok(Opcode::InsertMany),
            0x05 => Ok(Opcode::UpdateOne),
            0x06 => Ok(Opcode::UpdateMany),
            0x07 => Ok(Opcode::DeleteOne),
            0x08 => Ok(Opcode::DeleteMany),
            0x09 => Ok(Opcode::Aggregate),
            0x0A => Ok(Opcode::CountDocuments),
            0x0B => Ok(Opcode::CreateIndex),
            0x0C => Ok(Opcode::ListCollections),
            0x0D => Ok(Opcode::RunCommand),
            0x0E => Ok(Opcode::FindOneAndUpdate),
            0x0F => Ok(Opcode::FindOneAndDelete),
            0x10 => Ok(Opcode::BulkWrite),
            0x11 => Ok(Opcode::Distinct),
            0x12 => Ok(Opcode::CreateCollection),
            0x13 => Ok(Opcode::DropCollection),
            0x3E => Ok(Opcode::Error),
            0x3F => Ok(Opcode::Handshake),
            other => Err(FrameError::InvalidOpcode(other)),
        }
    }
}

/// Priority levels for frame processing.
///
/// Encoded in bits[9:10] of the flags field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Priority {
    Normal = 0,
    Low = 1,
    High = 2,
    Critical = 3,
}

impl Priority {
    /// Decode a priority from a raw 2-bit value.
    pub fn from_u8(value: u8) -> Result<Self, FrameError> {
        match value {
            0 => Ok(Priority::Normal),
            1 => Ok(Priority::Low),
            2 => Ok(Priority::High),
            3 => Ok(Priority::Critical),
            _ => Err(FrameError::InvalidOpcode(value)), // reuse error for simplicity
        }
    }
}

/// Decoded flags from the 2-byte flags field.
///
/// Layout (big-endian u16, bit 0 = LSB):
/// - bits[0:5] = opcode (6 bits)
/// - bit 6 = end-of-stream
/// - bit 7 = no-reply (fire-and-forget)
/// - bit 8 = batch-follows
/// - bits[9:10] = priority (2 bits)
/// - bits[11:15] = unused/reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags {
    pub opcode: Opcode,
    pub end_of_stream: bool,
    pub no_reply: bool,
    pub batch_follows: bool,
    pub priority: Priority,
}

impl Flags {
    /// Encode flags into a big-endian u16 value.
    pub fn encode(&self) -> u16 {
        let mut bits: u16 = 0;
        bits |= (self.opcode as u8 as u16) & 0x3F; // bits[0:5]
        if self.end_of_stream {
            bits |= 1 << 6;
        }
        if self.no_reply {
            bits |= 1 << 7;
        }
        if self.batch_follows {
            bits |= 1 << 8;
        }
        bits |= ((self.priority as u8 as u16) & 0x03) << 9; // bits[9:10]
        bits
    }

    /// Decode flags from a big-endian u16 value.
    pub fn decode(bits: u16) -> Result<Self, FrameError> {
        let opcode_raw = (bits & 0x3F) as u8;
        let opcode = Opcode::from_u8(opcode_raw)?;
        let end_of_stream = (bits >> 6) & 1 == 1;
        let no_reply = (bits >> 7) & 1 == 1;
        let batch_follows = (bits >> 8) & 1 == 1;
        let priority_raw = ((bits >> 9) & 0x03) as u8;
        let priority = Priority::from_u8(priority_raw)?;

        Ok(Flags {
            opcode,
            end_of_stream,
            no_reply,
            batch_follows,
            priority,
        })
    }
}

/// The 10-byte frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Total message length (header + envelope + raw_docs).
    pub msg_len: u32,
    /// Decoded flags.
    pub flags: Flags,
    /// Request identifier for correlating requests and responses.
    pub req_id: u32,
}

impl FrameHeader {
    /// Encode the header into a 10-byte array (big-endian).
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.msg_len.to_be_bytes());
        buf[4..6].copy_from_slice(&self.flags.encode().to_be_bytes());
        buf[6..10].copy_from_slice(&self.req_id.to_be_bytes());
        buf
    }

    /// Decode a header from a 10-byte array (big-endian).
    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self, FrameError> {
        let msg_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let flags_raw = u16::from_be_bytes([buf[4], buf[5]]);
        let flags = Flags::decode(flags_raw)?;
        let req_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

        Ok(FrameHeader {
            msg_len,
            flags,
            req_id,
        })
    }
}

/// A complete frame consisting of header, BSON envelope, and optional raw document bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    /// BSON envelope (metadata, filter, pipeline, etc.).
    pub envelope: Bytes,
    /// Raw document bytes (passthrough, not parsed by transport layer).
    pub raw_docs: Bytes,
}

/// Write a frame to a UnixStream using vectored I/O.
///
/// Computes msg_len from actual content (ignores the header's msg_len field),
/// then writes header + envelope + raw_docs in a single `write_vectored` syscall.
pub async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), FrameError> {
    let payload_len = frame.envelope.len() + frame.raw_docs.len();
    let msg_len = (6 + payload_len) as u32; // 2 (flags) + 4 (req_id) + payload

    let computed_header = FrameHeader {
        msg_len,
        flags: frame.header.flags,
        req_id: frame.header.req_id,
    };
    let header_bytes = computed_header.encode();

    let bufs = &[
        IoSlice::new(&header_bytes),
        IoSlice::new(&frame.envelope),
        IoSlice::new(&frame.raw_docs),
    ];

    let total_len = HEADER_SIZE + frame.envelope.len() + frame.raw_docs.len();
    let written = stream
        .write_vectored(bufs)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;

    if written < total_len {
        // Partial write — write remaining bytes with write_all.
        // This is rare on UDS but must be handled correctly.
        let mut all_bytes = Vec::with_capacity(total_len);
        all_bytes.extend_from_slice(&header_bytes);
        all_bytes.extend_from_slice(&frame.envelope);
        all_bytes.extend_from_slice(&frame.raw_docs);
        stream
            .write_all(&all_bytes[written..])
            .await
            .map_err(|e| FrameError::Io(e.to_string()))?;
    }

    Ok(())
}

/// Read a frame from a UnixStream.
///
/// Reads the 10-byte header, validates message size, then reads the payload
/// and splits it into envelope and raw_docs using BSON self-delimiting format.
pub async fn read_frame(stream: &mut UnixStream, max_frame_size: u32) -> Result<Frame, FrameError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    stream
        .read_exact(&mut header_buf)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;

    let header = FrameHeader::decode(&header_buf)?;

    if header.msg_len > max_frame_size {
        return Err(FrameError::FrameTooLarge);
    }

    // msg_len encodes flags(2) + req_id(4) + payload, so payload_len = msg_len - 6
    let payload_len = header.msg_len.saturating_sub(6) as usize;

    if payload_len == 0 {
        return Ok(Frame {
            header,
            envelope: Bytes::new(),
            raw_docs: Bytes::new(),
        });
    }

    let mut payload = BytesMut::zeroed(payload_len);
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;

    split_payload(header, payload)
}

/// Read a frame from a UnixStream using a buffer pool for allocation.
///
/// Like `read_frame` but checks out a buffer from the pool instead of allocating,
/// enabling zero-copy slicing via `Bytes` (the frozen `BytesMut` shares the
/// underlying allocation between envelope and raw_docs).
pub async fn read_frame_pooled(
    stream: &mut UnixStream,
    max_frame_size: u32,
    pool: &BufferPool,
) -> Result<Frame, FrameError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    stream
        .read_exact(&mut header_buf)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;

    let header = FrameHeader::decode(&header_buf)?;

    if header.msg_len > max_frame_size {
        return Err(FrameError::FrameTooLarge);
    }

    let payload_len = header.msg_len.saturating_sub(6) as usize;

    if payload_len == 0 {
        return Ok(Frame {
            header,
            envelope: Bytes::new(),
            raw_docs: Bytes::new(),
        });
    }

    let mut payload = pool.checkout(payload_len);
    payload.resize(payload_len, 0);
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;

    split_payload(header, payload)
}

/// Split a payload BytesMut into envelope and raw_docs Bytes using BSON self-delimiting size.
fn split_payload(header: FrameHeader, mut payload: BytesMut) -> Result<Frame, FrameError> {
    if payload.len() < 4 {
        return Err(FrameError::MalformedEnvelope(
            "payload too short to contain BSON size".to_string(),
        ));
    }

    let bson_size =
        i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;

    if bson_size > payload.len() {
        return Err(FrameError::MalformedEnvelope(
            "BSON size exceeds payload length".to_string(),
        ));
    }

    // Split without copying: freeze the BytesMut into Bytes slices
    let raw_docs_buf = payload.split_off(bson_size);
    let envelope = payload.freeze();
    let raw_docs = raw_docs_buf.freeze();

    Ok(Frame {
        header,
        envelope,
        raw_docs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode_decode_roundtrip() {
        let header = FrameHeader {
            msg_len: 1024,
            flags: Flags {
                opcode: Opcode::Find,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 42,
        };

        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_header_encode_decode_roundtrip_all_opcodes() {
        let opcodes = [
            Opcode::InsertOne,
            Opcode::InsertMany,
            Opcode::FindOne,
            Opcode::Find,
            Opcode::UpdateOne,
            Opcode::UpdateMany,
            Opcode::DeleteOne,
            Opcode::DeleteMany,
            Opcode::Aggregate,
            Opcode::CountDocuments,
            Opcode::CreateIndex,
            Opcode::ListCollections,
            Opcode::RunCommand,
            Opcode::FindOneAndUpdate,
            Opcode::FindOneAndDelete,
            Opcode::BulkWrite,
            Opcode::Distinct,
            Opcode::CreateCollection,
            Opcode::DropCollection,
            Opcode::Error,
            Opcode::Handshake,
        ];

        for opcode in opcodes {
            let header = FrameHeader {
                msg_len: 256,
                flags: Flags {
                    opcode,
                    end_of_stream: false,
                    no_reply: false,
                    batch_follows: false,
                    priority: Priority::Normal,
                },
                req_id: 1,
            };
            let encoded = header.encode();
            let decoded = FrameHeader::decode(&encoded).unwrap();
            assert_eq!(header, decoded, "roundtrip failed for opcode {:?}", opcode);
        }
    }

    #[test]
    fn test_flags_roundtrip_all_bits_set() {
        let flags = Flags {
            opcode: Opcode::Handshake, // 0x3F = all 6 opcode bits set
            end_of_stream: true,
            no_reply: true,
            batch_follows: true,
            priority: Priority::Critical, // 0b11 = both priority bits set
        };

        let encoded = flags.encode();
        let decoded = Flags::decode(encoded).unwrap();
        assert_eq!(flags, decoded);
    }

    #[test]
    fn test_opcode_zero_rejected() {
        let result = Opcode::from_u8(0x00);
        assert_eq!(result, Err(FrameError::InvalidOpcode(0x00)));
    }

    #[test]
    fn test_flags_with_opcode_zero_rejected() {
        // Flags with opcode bits = 0 should fail to decode
        let bits: u16 = 0b0000_0111_1100_0000; // opcode = 0, other bits set
        let result = Flags::decode(bits);
        assert!(result.is_err());
    }

    #[test]
    fn test_all_individual_opcodes_decode() {
        let expected: &[(u8, Opcode)] = &[
            (0x01, Opcode::Find),
            (0x02, Opcode::FindOne),
            (0x03, Opcode::InsertOne),
            (0x04, Opcode::InsertMany),
            (0x05, Opcode::UpdateOne),
            (0x06, Opcode::UpdateMany),
            (0x07, Opcode::DeleteOne),
            (0x08, Opcode::DeleteMany),
            (0x09, Opcode::Aggregate),
            (0x0A, Opcode::CountDocuments),
            (0x0B, Opcode::CreateIndex),
            (0x0C, Opcode::ListCollections),
            (0x0D, Opcode::RunCommand),
            (0x0E, Opcode::FindOneAndUpdate),
            (0x0F, Opcode::FindOneAndDelete),
            (0x10, Opcode::BulkWrite),
            (0x11, Opcode::Distinct),
            (0x12, Opcode::CreateCollection),
            (0x13, Opcode::DropCollection),
            (0x3E, Opcode::Error),
            (0x3F, Opcode::Handshake),
        ];

        for &(raw, expected_opcode) in expected {
            let decoded = Opcode::from_u8(raw).unwrap();
            assert_eq!(
                decoded, expected_opcode,
                "opcode 0x{:02X} decoded incorrectly",
                raw
            );
        }
    }

    #[test]
    fn test_invalid_opcodes_rejected() {
        // Test some values that are not valid opcodes
        for val in [0x14, 0x20, 0x30, 0x3D] {
            assert!(
                Opcode::from_u8(val).is_err(),
                "opcode 0x{:02X} should be rejected",
                val
            );
        }
    }

    #[test]
    fn test_priority_roundtrip() {
        for val in 0..=3u8 {
            let priority = Priority::from_u8(val).unwrap();
            assert_eq!(priority as u8, val);
        }
    }

    #[test]
    fn test_header_with_large_msg_len() {
        let header = FrameHeader {
            msg_len: MAX_FRAME_SIZE_DEFAULT,
            flags: Flags {
                opcode: Opcode::Aggregate,
                end_of_stream: true,
                no_reply: false,
                batch_follows: true,
                priority: Priority::High,
            },
            req_id: 0xDEAD_BEEF,
        };

        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[tokio::test]
    async fn test_frame_write_read_roundtrip() {
        let (mut client, mut server) = UnixStream::pair().unwrap();

        // Build a minimal BSON envelope: a 5-byte empty BSON document {}: [05 00 00 00 00]
        let envelope = Bytes::from_static(&[5, 0, 0, 0, 0]);
        let raw_docs = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]);

        let msg_len = 2 + 4 + envelope.len() as u32 + raw_docs.len() as u32;
        let frame = Frame {
            header: FrameHeader {
                msg_len,
                flags: Flags {
                    opcode: Opcode::Find,
                    end_of_stream: false,
                    no_reply: false,
                    batch_follows: false,
                    priority: Priority::Normal,
                },
                req_id: 123,
            },
            envelope: envelope.clone(),
            raw_docs: raw_docs.clone(),
        };

        write_frame(&mut client, &frame).await.unwrap();

        let read_back = read_frame(&mut server, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
        assert_eq!(read_back.header.flags, frame.header.flags);
        assert_eq!(read_back.header.req_id, frame.header.req_id);
        assert_eq!(read_back.envelope, envelope);
        assert_eq!(read_back.raw_docs, raw_docs);
    }

    #[tokio::test]
    async fn test_frame_read_rejects_oversized() {
        let (mut client, mut server) = UnixStream::pair().unwrap();

        // Write a header claiming a huge msg_len
        let header = FrameHeader {
            msg_len: 100_000_000, // way over any reasonable max
            flags: Flags {
                opcode: Opcode::Find,
                end_of_stream: false,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 1,
        };

        let header_bytes = header.encode();
        client.write_all(&header_bytes).await.unwrap();

        let result = read_frame(&mut server, 1024).await;
        assert_eq!(result.unwrap_err(), FrameError::FrameTooLarge);
    }

    #[tokio::test]
    async fn test_frame_empty_payload() {
        let (mut client, mut server) = UnixStream::pair().unwrap();

        // msg_len = 6 means payload_len = 0 (no envelope, no raw_docs)
        let frame = Frame {
            header: FrameHeader {
                msg_len: 6,
                flags: Flags {
                    opcode: Opcode::Handshake,
                    end_of_stream: true,
                    no_reply: false,
                    batch_follows: false,
                    priority: Priority::Normal,
                },
                req_id: 42,
            },
            envelope: Bytes::new(),
            raw_docs: Bytes::new(),
        };

        write_frame(&mut client, &frame).await.unwrap();

        let read_back = read_frame(&mut server, MAX_FRAME_SIZE_DEFAULT).await.unwrap();
        assert_eq!(read_back.header.flags, frame.header.flags);
        assert_eq!(read_back.header.req_id, 42);
        assert!(read_back.envelope.is_empty());
        assert!(read_back.raw_docs.is_empty());
    }
}
