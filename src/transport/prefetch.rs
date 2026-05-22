//! Eager cursor prefetch for streaming responses.
//!
//! Provides a [`PrefetchedCursor`] that reads ahead from a MongoDB cursor on a background
//! task, buffering batches in a bounded channel. This allows the transport layer to send
//! response frames to the client while simultaneously fetching the next batch from MongoDB,
//! reducing end-to-end latency for large result sets.

use bson::RawDocumentBuf;
use mongodb::Cursor;
use tokio::sync::mpsc;
use tracing::warn;

/// Default number of batches to prefetch ahead of the consumer.
pub const DEFAULT_PREFETCH_AHEAD: usize = 2;

/// Default number of documents per batch.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// A cursor wrapper that eagerly prefetches batches from MongoDB in the background.
///
/// When created, it spawns a tokio task that reads from the underlying MongoDB cursor
/// and fills a bounded channel with serialized document batches. The channel capacity
/// determines how far ahead the prefetcher reads (controlled by `prefetch_ahead`).
///
/// The consumer calls [`next_batch`](PrefetchedCursor::next_batch) to retrieve batches.
/// When the cursor is exhausted, `next_batch` returns `None`.
///
/// If the consumer is dropped, the background task detects the closed channel and stops.
pub struct PrefetchedCursor {
    receiver: mpsc::Receiver<PrefetchBatch>,
    _handle: tokio::task::JoinHandle<()>,
}

/// A single batch of prefetched documents, ready for transport.
pub struct PrefetchBatch {
    /// Raw BSON bytes for each document in this batch.
    pub documents: Vec<Vec<u8>>,
    /// Total byte size of all documents in this batch.
    pub total_bytes: usize,
}

impl PrefetchedCursor {
    /// Create a new prefetched cursor that reads from a MongoDB `RawDocumentBuf` cursor.
    ///
    /// # Arguments
    ///
    /// * `cursor` — The MongoDB cursor to read from.
    /// * `batch_size` — Number of documents per batch. Use [`DEFAULT_BATCH_SIZE`] if unsure.
    /// * `prefetch_ahead` — Channel capacity (number of batches buffered ahead).
    ///   Use [`DEFAULT_PREFETCH_AHEAD`] if unsure.
    ///
    /// # Behavior
    ///
    /// The background task reads documents from the cursor, accumulating them into
    /// batches of `batch_size`. Each completed batch is sent through the channel.
    /// When the cursor is exhausted, any remaining partial batch is sent and the
    /// task exits.
    pub fn new(cursor: Cursor<RawDocumentBuf>, batch_size: usize, prefetch_ahead: usize) -> Self {
        let (tx, rx) = mpsc::channel(prefetch_ahead);

        let handle = tokio::spawn(async move {
            Self::prefetch_loop(cursor, batch_size, tx).await;
        });

        Self {
            receiver: rx,
            _handle: handle,
        }
    }

    /// Create a prefetched cursor with default settings.
    pub fn with_defaults(cursor: Cursor<RawDocumentBuf>) -> Self {
        Self::new(cursor, DEFAULT_BATCH_SIZE, DEFAULT_PREFETCH_AHEAD)
    }

    /// Get the next batch of documents. Returns `None` when the cursor is exhausted.
    pub async fn next_batch(&mut self) -> Option<PrefetchBatch> {
        self.receiver.recv().await
    }

    /// Internal prefetch loop that runs on the background task.
    async fn prefetch_loop(
        mut cursor: Cursor<RawDocumentBuf>,
        batch_size: usize,
        tx: mpsc::Sender<PrefetchBatch>,
    ) {
        let mut docs: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
        let mut total_bytes: usize = 0;

        loop {
            let advanced = match cursor.advance().await {
                Ok(true) => true,
                Ok(false) => false,
                Err(e) => {
                    warn!("Prefetch cursor error: {}", e);
                    false
                }
            };

            if !advanced {
                // Cursor exhausted or errored — send remaining docs
                if !docs.is_empty() {
                    let batch = PrefetchBatch {
                        documents: std::mem::take(&mut docs),
                        total_bytes,
                    };
                    let _ = tx.send(batch).await;
                }
                break;
            }

            match cursor.deserialize_current() {
                Ok(raw_doc) => {
                    let bytes = raw_doc.as_bytes().to_vec();
                    total_bytes += bytes.len();
                    docs.push(bytes);
                }
                Err(e) => {
                    warn!("Prefetch cursor deserialization error, skipping document: {}", e);
                    continue;
                }
            }

            if docs.len() >= batch_size {
                let batch = PrefetchBatch {
                    documents: std::mem::take(&mut docs),
                    total_bytes,
                };
                if tx.send(batch).await.is_err() {
                    // Consumer dropped — stop prefetching
                    break;
                }
                docs = Vec::with_capacity(batch_size);
                total_bytes = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_batch_creation() {
        let batch = PrefetchBatch {
            documents: vec![vec![1, 2, 3], vec![4, 5, 6]],
            total_bytes: 6,
        };
        assert_eq!(batch.documents.len(), 2);
        assert_eq!(batch.total_bytes, 6);
    }

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_PREFETCH_AHEAD, 2);
        assert_eq!(DEFAULT_BATCH_SIZE, 100);
    }
}
