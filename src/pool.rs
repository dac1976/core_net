// This file is part of core-net Rust crate containing useful reusable
// networking utilities.
//
// Copyright (C) 2026 to present, Duncan Crutchley
// Contact <15799155+dac1976@users.noreply.github.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License and GNU Lesser General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License
// and GNU Lesser General Public License along with this program. If
// not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, Mutex};

/// Simple fixed-capacity buffer pool.
///
/// The pool owns a number of reusable `Vec<u8>` allocations. Callers can
/// acquire a buffer, fill it, and when the buffer is dropped it is returned
/// to the pool automatically.
///
/// This is useful for network code because it avoids repeatedly allocating
/// and freeing message buffers on hot paths.
///
/// Design:
///
/// ```text
/// BufferPool
///   -> Arc<BufferPoolInner>
///      -> Mutex<Vec<Vec<u8>>>
/// ```
///
/// The pool is cloneable and cheap to pass around because clones share the
/// same underlying pool state.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

/// Shared internal pool state.
struct BufferPoolInner {
    /// Nominal capacity of each pooled buffer.
    block_size: usize,

    /// Stack of currently-free buffers.
    ///
    /// Protected by a mutex because buffers may be acquired/returned from
    /// multiple async tasks or threads.
    free: Mutex<Vec<Vec<u8>>>,
}

impl BufferPool {
    /// Creates a new buffer pool.
    ///
    /// `count` buffers are preallocated, each with capacity `block_size`.
    ///
    /// Note:
    ///
    /// The buffers are created with capacity but length zero.
    pub fn new(block_size: usize, count: usize) -> Self {
        let mut free = Vec::with_capacity(count);

        for _ in 0..count {
            free.push(Vec::with_capacity(block_size));
        }

        Self {
            inner: Arc::new(BufferPoolInner {
                block_size,
                free: Mutex::new(free),
            }),
        }
    }

    /// Returns the configured nominal block size for this pool.
    pub fn block_size(&self) -> usize {
        self.inner.block_size
    }

    /// Attempts to acquire a buffer from the pool.
    ///
    /// Returns:
    ///
    /// - `Some(PooledBuffer)` if a free buffer is available
    /// - `None` if the pool is temporarily exhausted
    ///
    /// The returned buffer has length zero but retains its allocation.
    pub fn try_acquire(&self) -> Option<PooledBuffer> {
        let mut guard = self.inner.free.lock().unwrap();

        let mut buf = guard.pop()?;

        // Ensure callers always receive a logically empty buffer.
        buf.clear();

        Some(PooledBuffer {
            buf,
            pool: Some(self.clone()),
        })
    }
}

/// RAII wrapper around a pooled `Vec<u8>`.
///
/// When this object is dropped, the underlying Vec is automatically returned
/// to the originating pool.
///
/// This makes buffer lifetime management explicit and safe:
///
/// ```text
/// acquire buffer
///   -> use it
///   -> drop wrapper
///   -> buffer returns to pool
/// ```
pub struct PooledBuffer {
    /// Actual reusable byte buffer.
    buf: Vec<u8>,

    /// Pool to return this buffer to on drop.
    ///
    /// Stored as Option so `Drop` can `take()` it safely and ensure return
    /// happens at most once.
    pool: Option<BufferPool>,
}

impl PooledBuffer {
    /// Clears the logical contents of the buffer.
    ///
    /// Capacity is retained.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Resizes the logical length of the buffer.
    ///
    /// New bytes are filled with `value`.
    pub fn resize(&mut self, len: usize, value: u8) {
        self.buf.resize(len, value);
    }

    /// Appends bytes to the buffer.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Returns the buffer contents as an immutable byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Returns the buffer contents as a mutable byte slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    /// Returns the current logical length.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Returns the current allocated capacity.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

impl Drop for PooledBuffer {
    /// Returns the buffer to its pool when dropped.
    ///
    /// This is the core RAII behaviour of the pool.
    ///
    /// The Vec allocation is preserved for reuse, but the logical contents
    /// are cleared.
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let mut buf = std::mem::take(&mut self.buf);

            buf.clear();

            let mut guard = pool.inner.free.lock().unwrap();

            guard.push(buf);
        }
    }
}

/// Shared message payload buffer.
///
/// Message payloads can either be:
///
/// - backed by a pooled buffer
/// - backed by a dynamically allocated Vec
///
/// Both variants are wrapped in `Arc` so `MessageBuf` is cheap to clone and
/// can be moved across tasks without copying payload bytes.
///
/// This is important for pipelines such as:
///
/// ```text
/// receive datagram
///   -> wrap payload in MessageBuf
///   -> send MessageBuf to another task
///   -> parse/demux while original storage remains alive
/// ```
#[derive(Clone)]
pub enum MessageBuf {
    /// Payload backed by a buffer from `BufferPool`.
    Pooled(Arc<PooledBuffer>),

    /// Payload backed by a dynamically allocated Vec.
    ///
    /// Used when:
    ///
    /// - no pool was provided
    /// - pool was exhausted
    /// - data exceeded the pool block size
    Dynamic(Arc<Vec<u8>>),
}

impl std::fmt::Debug for MessageBuf {
    /// Custom debug output that avoids dumping payload bytes.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageBuf")
            .field("len", &self.len())
            .finish()
    }
}

impl MessageBuf {
    /// Wraps a pooled buffer in a shared `MessageBuf`.
    pub fn from_pooled(buf: PooledBuffer) -> Self {
        Self::Pooled(Arc::new(buf))
    }

    /// Creates a MessageBuf from a byte slice, using a pool when possible.
    ///
    /// Selection logic:
    ///
    /// ```text
    /// if pool exists
    ///   and data fits in pool block
    ///   and pool has a free buffer
    ///       -> copy into pooled buffer
    /// else
    ///       -> allocate dynamic Vec
    /// ```
    ///
    /// Note:
    ///
    /// This function still copies `data` into owned storage. It is intended
    /// for cases where the source slice does not already have an owned buffer
    /// lifetime suitable for message passing.
    pub fn from_slice_with_pool(pool: Option<&BufferPool>, data: &[u8]) -> Self {
        if let Some(pool) = pool {
            if data.len() <= pool.block_size() {
                if let Some(mut buf) = pool.try_acquire() {
                    buf.extend_from_slice(data);

                    return Self::from_pooled(buf);
                }
            }
        }

        Self::Dynamic(Arc::new(data.to_vec()))
    }

    /// Returns the payload as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(p) => p.as_slice(),

            Self::Dynamic(v) => v.as_slice(),
        }
    }

    /// Returns payload length in bytes.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Returns true if the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
