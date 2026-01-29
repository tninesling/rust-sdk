//! Byte-level middleware for transport layer
//!
//! This module provides tower middleware support at the byte level, before any
//! JSON-RPC parsing occurs. This is useful for:
//! - Byte counting and bandwidth monitoring
//! - Compression
//! - Encryption
//! - Protocol-level logging
//!
//! # Example
//!
//! ```rust,no_run
//! # use rmcp::transport::*;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use tokio::io::{AsyncRead, AsyncWrite};
//!
//! // Wrap your streams with byte-level middleware
//! let (read, write) = tokio::io::split(stream);
//! let counting_read = ByteCountingReader::new(read);
//! let counting_write = ByteCountingWriter::new(write);
//!
//! // Use with transport
//! let transport = (counting_read, counting_write).into_transport();
//! # Ok(())
//! # }
//! ```

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pin_project! {
    /// A reader that counts bytes as they pass through
    ///
    /// This wraps an `AsyncRead` and tracks the number of bytes read.
    pub struct ByteCountingReader<R> {
        #[pin]
        inner: R,
        bytes_read: Arc<AtomicU64>,
    }
}

impl<R> ByteCountingReader<R> {
    /// Create a new byte counting reader
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a new byte counting reader with a shared counter
    pub fn with_counter(inner: R, counter: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            bytes_read: counter,
        }
    }

    /// Get the number of bytes read so far
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get a reference to the shared counter
    pub fn counter(&self) -> &Arc<AtomicU64> {
        &self.bytes_read
    }

    /// Get the inner reader
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead> AsyncRead for ByteCountingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        let before = buf.filled().len();

        match this.inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = (buf.filled().len() - before) as u64;
                this.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

pin_project! {
    /// A writer that counts bytes as they pass through
    ///
    /// This wraps an `AsyncWrite` and tracks the number of bytes written.
    pub struct ByteCountingWriter<W> {
        #[pin]
        inner: W,
        bytes_written: Arc<AtomicU64>,
    }
}

impl<W> ByteCountingWriter<W> {
    /// Create a new byte counting writer
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a new byte counting writer with a shared counter
    pub fn with_counter(inner: W, counter: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            bytes_written: counter,
        }
    }

    /// Get the number of bytes written so far
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get a reference to the shared counter
    pub fn counter(&self) -> &Arc<AtomicU64> {
        &self.bytes_written
    }

    /// Get the inner writer
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite> AsyncWrite for ByteCountingWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();

        match this.inner.poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.bytes_written.fetch_add(n as u64, Ordering::Relaxed);
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

/// Statistics for byte-level transport
#[derive(Debug, Clone, Default)]
pub struct ByteStats {
    /// Number of bytes read
    pub bytes_read: u64,
    /// Number of bytes written
    pub bytes_written: u64,
}

impl ByteStats {
    /// Create a new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total bytes transferred (read + written)
    pub fn total_bytes(&self) -> u64 {
        self.bytes_read.saturating_add(self.bytes_written)
    }
}

/// A pair of byte counting reader and writer with shared statistics
pub struct ByteCountingTransport<R, W> {
    reader: ByteCountingReader<R>,
    writer: ByteCountingWriter<W>,
}

impl<R, W> ByteCountingTransport<R, W> {
    /// Create a new byte counting transport
    pub fn new(read: R, write: W) -> Self {
        Self {
            reader: ByteCountingReader::new(read),
            writer: ByteCountingWriter::new(write),
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> ByteStats {
        ByteStats {
            bytes_read: self.reader.bytes_read(),
            bytes_written: self.writer.bytes_written(),
        }
    }

    /// Get a reference to the read counter
    pub fn read_counter(&self) -> &Arc<AtomicU64> {
        self.reader.counter()
    }

    /// Get a reference to the write counter
    pub fn write_counter(&self) -> &Arc<AtomicU64> {
        self.writer.counter()
    }

    /// Split into reader and writer
    pub fn split(self) -> (ByteCountingReader<R>, ByteCountingWriter<W>) {
        (self.reader, self.writer)
    }
}
