//! Per-connection handler for the binary UDS transport protocol.
//!
//! Manages the lifecycle of a single client connection: handshake negotiation,
//! request/response loop with concurrency limiting, and graceful disconnect handling.
//!
//! Requests are dispatched in priority order: Critical (immediate, bypasses concurrency
//! limit), High (before Normal/Low), Normal (FIFO), Low (after all others).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::{watch, Semaphore};
use tracing::{debug, error, info, warn};

use crate::connection::pool::ConnectionPool;
use crate::operations::Operations;
use crate::transport::buffer_pool::BufferPool;
use crate::transport::codec::{self, OperationResponse};
use crate::transport::dispatch;
use crate::transport::frame::{
    read_frame_pooled, write_frame, Flags, Frame, FrameError, FrameHeader, Opcode, Priority,
};

/// Configuration for a per-connection handler.
pub struct ConnectionConfig {
    pub max_frame_size: u32,
    pub max_concurrent: u32,
}

/// A parsed request ready for dispatch, with its priority resolved.
struct PendingRequest {
    frame: Frame,
    priority: Priority,
}

/// Priority queue for pending requests.
///
/// Maintains separate queues per priority level. Frames are dequeued in order:
/// High > Normal > Low. Critical frames bypass this queue entirely.
struct PriorityQueue {
    high: VecDeque<PendingRequest>,
    normal: VecDeque<PendingRequest>,
    low: VecDeque<PendingRequest>,
}

impl PriorityQueue {
    fn new() -> Self {
        Self {
            high: VecDeque::new(),
            normal: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    /// Enqueue a request at the appropriate priority level.
    fn push(&mut self, request: PendingRequest) {
        match request.priority {
            Priority::High | Priority::Critical => self.high.push_back(request),
            Priority::Normal => self.normal.push_back(request),
            Priority::Low => self.low.push_back(request),
        }
    }

    /// Dequeue the highest-priority pending request.
    fn pop(&mut self) -> Option<PendingRequest> {
        self.high
            .pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }

}

/// Send an error frame to the client.
async fn send_error(
    stream: &mut UnixStream,
    req_id: u32,
    code: i32,
    message: &str,
) -> Result<(), FrameError> {
    let response = OperationResponse::Error {
        code,
        message: message.to_string(),
    };
    let (envelope, raw_docs) = codec::encode_response(&response)
        .map_err(|e| FrameError::MalformedEnvelope(e.to_string()))?;

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0, // write_frame computes this
            flags: Flags {
                opcode: Opcode::Error,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id,
        },
        envelope: Bytes::from(envelope),
        raw_docs: Bytes::from(raw_docs),
    };

    write_frame(stream, &frame).await
}

/// Default drain period in milliseconds for graceful shutdown.
const SHUTDOWN_DRAIN_MS: u64 = 5000;

/// Send a shutdown notification frame to the client.
///
/// Uses opcode 0x3F (Handshake) with end-of-stream set and a JSON envelope
/// indicating shutdown with a drain period. Clients should stop sending new
/// requests and disconnect within the drain period.
async fn send_shutdown_frame(stream: &mut UnixStream) {
    let envelope = format!(
        r#"{{"shutdown":true,"drain_ms":{},"doc_bytes_len":0}}"#,
        SHUTDOWN_DRAIN_MS
    );

    let frame = Frame {
        header: FrameHeader {
            msg_len: 0, // write_frame computes this
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: 0,
        },
        envelope: Bytes::from(envelope),
        raw_docs: Bytes::new(),
    };

    if let Err(e) = write_frame(stream, &frame).await {
        debug!("failed to send shutdown frame: {}", e);
    }
    if let Err(e) = stream.flush().await {
        debug!("failed to flush shutdown frame: {}", e);
    }
}

/// Handle a single client connection over the binary UDS protocol.
///
/// Performs the handshake, then enters a request/response loop until the client
/// disconnects, an unrecoverable error occurs, or a shutdown signal is received.
///
/// On shutdown, sends a shutdown frame to the client and waits up to
/// `SHUTDOWN_DRAIN_MS` for the client to disconnect gracefully.
pub async fn handle_connection(
    mut stream: UnixStream,
    operations: Operations,
    pool: ConnectionPool,
    config: ConnectionConfig,
    buffer_pool: BufferPool,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Step 1: Read the first frame and enforce it's a Handshake
    let first_frame = match read_frame_pooled(&mut stream, config.max_frame_size, &buffer_pool).await {
        Ok(f) => f,
        Err(FrameError::Io(msg)) => {
            debug!("client disconnected before handshake: {}", msg);
            return;
        }
        Err(e) => {
            error!("failed to read handshake frame: {}", e);
            return;
        }
    };

    if first_frame.header.flags.opcode != Opcode::Handshake {
        warn!(
            "expected Handshake opcode, got {:?}",
            first_frame.header.flags.opcode
        );
        let _ = send_error(
            &mut stream,
            first_frame.header.req_id,
            1,
            "first frame must be Handshake",
        )
        .await;
        return;
    }

    // Step 2: Parse handshake and respond with server capabilities
    let handshake_request =
        match codec::parse_envelope(&first_frame.envelope, Opcode::Handshake) {
            Ok(req) => req,
            Err(e) => {
                error!("failed to parse handshake envelope: {}", e);
                let _ = send_error(
                    &mut stream,
                    first_frame.header.req_id,
                    1,
                    &format!("invalid handshake: {}", e),
                )
                .await;
                return;
            }
        };

    let handshake_response = dispatch::handle_handshake(
        &handshake_request,
        config.max_frame_size,
        config.max_concurrent,
    );

    let (envelope, raw_docs) = match codec::encode_response(&handshake_response) {
        Ok(pair) => pair,
        Err(e) => {
            error!("failed to encode handshake response: {}", e);
            return;
        }
    };

    let handshake_frame = Frame {
        header: FrameHeader {
            msg_len: 0, // write_frame computes this
            flags: Flags {
                opcode: Opcode::Handshake,
                end_of_stream: true,
                no_reply: false,
                batch_follows: false,
                priority: Priority::Normal,
            },
            req_id: first_frame.header.req_id,
        },
        envelope: Bytes::from(envelope),
        raw_docs: Bytes::from(raw_docs),
    };

    if let Err(e) = write_frame(&mut stream, &handshake_frame).await {
        error!("failed to write handshake response: {}", e);
        return;
    }

    debug!("handshake complete");

    // Step 3: Enter request/response loop with priority-based dispatch
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent as usize));
    // Buffer for coalescing responses when client uses batch_follows
    let mut pending_responses: Vec<Frame> = Vec::new();
    // Priority queue for reordering requests under load
    let mut priority_queue = PriorityQueue::new();

    loop {
        // Get the next frame to process: either from the priority queue or by reading
        let frame = if let Some(pending) = priority_queue.pop() {
            pending.frame
        } else {
            // No queued frames — block waiting for the next frame or shutdown signal
            tokio::select! {
                result = read_frame_pooled(&mut stream, config.max_frame_size, &buffer_pool) => {
                    match result {
                        Ok(f) => f,
                        Err(FrameError::Io(_)) => {
                            debug!("client disconnected");
                            break;
                        }
                        Err(e) => {
                            warn!("frame read error: {}", e);
                            break;
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        send_shutdown_frame(&mut stream).await;
                        // Wait for client to disconnect within drain period
                        let drain = Duration::from_millis(SHUTDOWN_DRAIN_MS);
                        let _ = tokio::time::timeout(drain, async {
                            // Keep reading frames until client disconnects
                            loop {
                                match read_frame_pooled(&mut stream, config.max_frame_size, &buffer_pool).await {
                                    Ok(_) => continue,
                                    Err(_) => break,
                                }
                            }
                        }).await;
                        info!("connection drained, closing");
                        break;
                    }
                    continue;
                }
            }
        };

        // Try to coalesce: read any additional frames that are immediately available
        // on the socket and enqueue them by priority. This ensures that under load,
        // high-priority requests jump ahead of queued normal/low requests.
        // We use a zero-duration timeout to avoid blocking if no data is ready.
        loop {
            match tokio::time::timeout(
                std::time::Duration::ZERO,
                read_frame_pooled(&mut stream, config.max_frame_size, &buffer_pool),
            )
            .await
            {
                Ok(Ok(extra_frame)) => {
                    let extra_priority = resolve_priority(&extra_frame);
                    priority_queue.push(PendingRequest {
                        frame: extra_frame,
                        priority: extra_priority,
                    });
                }
                _ => break,
            }
        }

        let opcode = frame.header.flags.opcode;
        let req_id = frame.header.req_id;
        let no_reply = frame.header.flags.no_reply;
        let batch_follows = frame.header.flags.batch_follows;
        debug!("received frame: opcode={:?}, req_id={}, envelope_len={}, raw_docs_len={}", opcode, req_id, frame.envelope.len(), frame.raw_docs.len());

        // Resolve priority (Critical is downgraded to High for non-Handshake opcodes)
        let priority = resolve_priority(&frame);
        let is_critical = priority == Priority::Critical;

        // Parse the envelope
        let request = match codec::parse_envelope(&frame.envelope, opcode) {
            Ok(req) => req,
            Err(e) => {
                warn!("failed to parse envelope for {:?}: {}", opcode, e);
                if !no_reply {
                    // Build error response frame and handle batching
                    let error_resp = OperationResponse::Error {
                        code: 1,
                        message: format!("invalid envelope: {}", e),
                    };
                    if let Ok((env, docs)) = codec::encode_response(&error_resp) {
                        let err_frame = Frame {
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
                            envelope: Bytes::from(env),
                            raw_docs: Bytes::from(docs),
                        };
                        if batch_follows {
                            pending_responses.push(err_frame);
                        } else if let Err(e) = flush_batch(&mut stream, &mut pending_responses, err_frame).await {
                            debug!("failed to write error response batch: {}", e);
                            break;
                        }
                    }
                }
                continue;
            }
        };

        // Acquire semaphore permit for concurrency limiting.
        // Critical priority bypasses the semaphore to ensure immediate processing.
        let permit = if is_critical {
            None
        } else {
            match semaphore.clone().acquire_owned().await {
                Ok(p) => Some(p),
                Err(_) => {
                    error!("semaphore closed unexpectedly");
                    break;
                }
            }
        };

        // Dispatch the operation
        let response =
            dispatch::dispatch(&operations, &pool, request, &frame.raw_docs).await;

        // Release permit
        drop(permit);

        // If no_reply, skip sending response but respect batch boundary
        if no_reply {
            if !batch_follows {
                // End of batch but this request has no reply — flush any pending
                if !pending_responses.is_empty() {
                    for buffered in pending_responses.drain(..) {
                        if let Err(e) = write_frame(&mut stream, &buffered).await {
                            debug!("failed to write buffered response: {}", e);
                            break;
                        }
                    }
                    if let Err(e) = stream.flush().await {
                        debug!("failed to flush stream: {}", e);
                        break;
                    }
                }
            }
            continue;
        }

        // Determine response opcode
        let response_opcode = match &response {
            OperationResponse::Error { .. } => Opcode::Error,
            _ => opcode,
        };

        // Encode response
        let (resp_envelope, resp_raw_docs) = match codec::encode_response(&response) {
            Ok(pair) => pair,
            Err(e) => {
                error!("failed to encode response for {:?}: {}", opcode, e);
                // Build error frame for encoding failure
                let err_resp = OperationResponse::Error {
                    code: 1,
                    message: "internal encoding error".to_string(),
                };
                if let Ok((env, docs)) = codec::encode_response(&err_resp) {
                    let err_frame = Frame {
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
                        envelope: Bytes::from(env),
                        raw_docs: Bytes::from(docs),
                    };
                    if batch_follows {
                        pending_responses.push(err_frame);
                    } else if let Err(e) = flush_batch(&mut stream, &mut pending_responses, err_frame).await {
                        debug!("failed to write error batch: {}", e);
                        break;
                    }
                }
                continue;
            }
        };

        // Build response frame
        let response_frame = Frame {
            header: FrameHeader {
                msg_len: 0, // write_frame computes this
                flags: Flags {
                    opcode: response_opcode,
                    end_of_stream: true,
                    no_reply: false,
                    batch_follows: false,
                    priority: Priority::Normal,
                },
                req_id,
            },
            envelope: Bytes::from(resp_envelope),
            raw_docs: Bytes::from(resp_raw_docs),
        };

        if batch_follows {
            // Client indicated more requests follow — buffer this response
            pending_responses.push(response_frame);
        } else {
            // Final frame in batch (or standalone request) — flush all
            if let Err(e) = flush_batch(&mut stream, &mut pending_responses, response_frame).await {
                debug!("failed to write response, client likely disconnected: {}", e);
                break;
            }
        }
    }
}

/// Resolve the effective priority for a frame.
///
/// Critical priority is only allowed for Handshake (opcode 0x3F). All other opcodes
/// using Critical are downgraded to High.
fn resolve_priority(frame: &Frame) -> Priority {
    if frame.header.flags.priority == Priority::Critical
        && frame.header.flags.opcode != Opcode::Handshake
    {
        debug!(
            "downgrading Critical priority to High for opcode {:?}",
            frame.header.flags.opcode
        );
        Priority::High
    } else {
        frame.header.flags.priority
    }
}

/// Flush all pending response frames plus a final frame, then flush the stream.
///
/// Writes all buffered frames followed by the final frame in a single burst,
/// then calls flush to ensure all bytes are sent to the client.
async fn flush_batch(
    stream: &mut UnixStream,
    pending: &mut Vec<Frame>,
    final_frame: Frame,
) -> Result<(), FrameError> {
    for buffered in pending.drain(..) {
        write_frame(stream, &buffered).await?;
    }
    write_frame(stream, &final_frame).await?;
    stream
        .flush()
        .await
        .map_err(|e| FrameError::Io(e.to_string()))?;
    Ok(())
}
