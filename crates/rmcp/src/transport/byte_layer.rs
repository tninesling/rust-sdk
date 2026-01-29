//! Tower layer support for byte-level transport middleware
//!
//! This module provides a way to apply tower `Layer`s to `AsyncRead` and `AsyncWrite`
//! streams, enabling composable middleware at the byte level.
//!
//! # Example
//!
//! ```rust,no_run
//! # use rmcp::transport::*;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use tower::ServiceBuilder;
//!
//! // Create your custom layer
//! let my_layer = ByteCountingLayer::new();
//!
//! // Apply it to streams
//! let (read, write) = tokio::io::split(stream);
//! let layered_read = my_layer.layer_read(read);
//! let layered_write = my_layer.layer_write(write);
//!
//! // Use with transport
//! let transport = (layered_read, layered_write).into_transport();
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

/// A trait for layers that can be applied to byte streams
///
/// This is similar to tower's `Layer` trait, but works with `AsyncRead`/`AsyncWrite`
/// instead of `Service`.
pub trait ByteLayer: Clone {
    /// The type of reader produced by this layer
    type Reader<R: AsyncRead>: AsyncRead;

    /// The type of writer produced by this layer
    type Writer<W: AsyncWrite>: AsyncWrite;

    /// Wrap a reader with this layer
    fn layer_read<R: AsyncRead>(&self, reader: R) -> Self::Reader<R>;

    /// Wrap a writer with this layer
    fn layer_write<W: AsyncWrite>(&self, writer: W) -> Self::Writer<W>;
}

/// A layer that counts bytes as they pass through
///
/// # Example
///
/// ```rust,no_run
/// # use rmcp::transport::*;
/// # use std::sync::{Arc, atomic::AtomicU64};
/// # async fn example<R: tokio::io::AsyncRead, W: tokio::io::AsyncWrite>(read: R, write: W) {
/// let counter = Arc::new(AtomicU64::new(0));
/// let layer = ByteCountingLayer::new(counter.clone());
///
/// let counting_read = layer.layer_read(read);
/// let counting_write = layer.layer_write(write);
///
/// // Use with transport
/// let transport = (counting_read, counting_write).into_transport();
///
/// // Monitor bytes
/// println!("Bytes: {}", counter.load(std::sync::atomic::Ordering::Relaxed));
/// # }
/// ```
#[derive(Clone)]
pub struct ByteCountingLayer {
    counter: Arc<AtomicU64>,
}

impl ByteCountingLayer {
    /// Create a new byte counting layer with a shared counter
    pub fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }

    /// Create a new byte counting layer with a new counter
    pub fn with_new_counter() -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        (Self::new(counter.clone()), counter)
    }
}

impl ByteLayer for ByteCountingLayer {
    type Reader<R: AsyncRead> = ByteCountingReader<R>;
    type Writer<W: AsyncWrite> = ByteCountingWriter<W>;

    fn layer_read<R: AsyncRead>(&self, reader: R) -> Self::Reader<R> {
        ByteCountingReader {
            inner: reader,
            counter: self.counter.clone(),
        }
    }

    fn layer_write<W: AsyncWrite>(&self, writer: W) -> Self::Writer<W> {
        ByteCountingWriter {
            inner: writer,
            counter: self.counter.clone(),
        }
    }
}

pin_project! {
    /// A reader that counts bytes
    pub struct ByteCountingReader<R> {
        #[pin]
        inner: R,
        counter: Arc<AtomicU64>,
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
                this.counter.fetch_add(bytes_read, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

pin_project! {
    /// A writer that counts bytes
    pub struct ByteCountingWriter<W> {
        #[pin]
        inner: W,
        counter: Arc<AtomicU64>,
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
                this.counter.fetch_add(n as u64, Ordering::Relaxed);
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

/// Extension trait to easily apply byte layers to streams
pub trait ByteLayerExt: Sized {
    /// Apply a byte layer to this stream
    fn with_byte_layer<L: ByteLayer>(self, layer: L) -> L::Reader<Self>
    where
        Self: AsyncRead;

    /// Apply a byte layer to this stream (for writers)
    fn with_byte_layer_write<L: ByteLayer>(self, layer: L) -> L::Writer<Self>
    where
        Self: AsyncWrite;
}

impl<T> ByteLayerExt for T {
    fn with_byte_layer<L: ByteLayer>(self, layer: L) -> L::Reader<Self>
    where
        Self: AsyncRead,
    {
        layer.layer_read(self)
    }

    fn with_byte_layer_write<L: ByteLayer>(self, layer: L) -> L::Writer<Self>
    where
        Self: AsyncWrite,
    {
        layer.layer_write(self)
    }
}
