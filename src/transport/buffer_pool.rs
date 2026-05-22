use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

const SMALL_SIZE: usize = 4 * 1024; // 4 KiB
const MEDIUM_SIZE: usize = 64 * 1024; // 64 KiB
const LARGE_SIZE: usize = 1024 * 1024; // 1 MiB

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

        Self {
            small,
            medium,
            large,
        }
    }

    pub fn checkout(&self, needed: usize) -> BytesMut {
        if needed <= SMALL_SIZE {
            if let Some(mut buf) = self.small.pop() {
                buf.clear();
                return buf;
            }
            BytesMut::with_capacity(SMALL_SIZE)
        } else if needed <= MEDIUM_SIZE {
            if let Some(mut buf) = self.medium.pop() {
                buf.clear();
                return buf;
            }
            BytesMut::with_capacity(MEDIUM_SIZE)
        } else if needed <= LARGE_SIZE {
            if let Some(mut buf) = self.large.pop() {
                buf.clear();
                return buf;
            }
            BytesMut::with_capacity(LARGE_SIZE)
        } else {
            BytesMut::with_capacity(needed)
        }
    }

    pub fn checkin(&self, mut buf: BytesMut) {
        buf.clear();
        let capacity = buf.capacity();
        if capacity <= SMALL_SIZE {
            let _ = self.small.push(buf);
        } else if capacity <= MEDIUM_SIZE {
            let _ = self.medium.push(buf);
        } else if capacity <= LARGE_SIZE {
            let _ = self.large.push(buf);
        }
        // Oversized buffers are dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkout_returns_buffer_of_correct_tier() {
        let pool = BufferPool::new(BufferPoolConfig::default());
        let buf = pool.checkout(100);
        assert!(buf.capacity() >= SMALL_SIZE);

        let buf = pool.checkout(5000);
        assert!(buf.capacity() >= MEDIUM_SIZE);

        let buf = pool.checkout(100_000);
        assert!(buf.capacity() >= LARGE_SIZE);
    }

    #[test]
    fn test_checkin_returns_buffer_to_pool() {
        let pool = BufferPool::new(BufferPoolConfig {
            small_count: 1,
            medium_count: 1,
            large_count: 1,
        });

        // Checkout the single small buffer
        let buf = pool.checkout(100);
        let ptr = buf.as_ptr();
        // Return it
        pool.checkin(buf);
        // Checkout again — should get the same allocation back
        let buf2 = pool.checkout(100);
        assert_eq!(ptr, buf2.as_ptr());
    }

    #[test]
    fn test_exhausted_pool_falls_back_to_heap() {
        let pool = BufferPool::new(BufferPoolConfig {
            small_count: 1,
            medium_count: 0,
            large_count: 0,
        });

        let _buf1 = pool.checkout(100);
        // Pool is exhausted, this should still work (heap allocation)
        let buf2 = pool.checkout(100);
        assert!(buf2.capacity() >= SMALL_SIZE);
    }
}
