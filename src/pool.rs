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

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

struct BufferPoolInner {
    block_size: usize,
    free: Mutex<Vec<Vec<u8>>>,
}

impl BufferPool {
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

    pub fn block_size(&self) -> usize {
        self.inner.block_size
    }

    pub fn try_acquire(&self) -> Option<PooledBuffer> {
        let mut guard = self.inner.free.lock().unwrap();
        let mut buf = guard.pop()?;
        buf.clear();

        Some(PooledBuffer {
            buf,
            pool: Some(self.clone()),
        })
    }
}

pub struct PooledBuffer {
    buf: Vec<u8>,
    pool: Option<BufferPool>,
}

impl PooledBuffer {
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn resize(&mut self, len: usize, value: u8) {
        self.buf.resize(len, value);
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf.as_mut_slice()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let mut buf = std::mem::take(&mut self.buf);
            buf.clear();

            let mut guard = pool.inner.free.lock().unwrap();
            guard.push(buf);
        }
    }
}

#[derive(Clone)]
pub enum MessageBuf {
    Pooled(Arc<PooledBuffer>),
    Dynamic(Arc<Vec<u8>>),
}

impl std::fmt::Debug for MessageBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageBuf")
            .field("len", &self.len())
            .finish()
    }
}

impl MessageBuf {
    pub fn from_pooled(buf: PooledBuffer) -> Self {
        Self::Pooled(Arc::new(buf))
    }

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

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(p) => p.as_slice(),
            Self::Dynamic(v) => v.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
