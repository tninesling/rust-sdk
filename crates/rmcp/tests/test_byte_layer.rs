//! Integration tests for byte-level layer middleware

use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use rmcp::transport::{ByteCountingLayer, ByteLayer, ByteLayerExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_byte_counting_layer_read() {
    let data = b"Hello, World!";
    let cursor = Cursor::new(data.to_vec());

    let counter = Arc::new(AtomicU64::new(0));
    let layer = ByteCountingLayer::new(counter.clone());

    let mut reader = layer.layer_read(cursor);

    // Initially, no bytes read
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    // Read some bytes
    let mut buf = vec![0u8; 5];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Hello");
    assert_eq!(counter.load(Ordering::Relaxed), 5);

    // Read more bytes
    let mut buf = vec![0u8; 8];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b", World!");
    assert_eq!(counter.load(Ordering::Relaxed), 13);
}

#[tokio::test]
async fn test_byte_counting_layer_write() {
    let cursor = Cursor::new(Vec::new());
    let counter = Arc::new(AtomicU64::new(0));
    let layer = ByteCountingLayer::new(counter.clone());

    let mut writer = layer.layer_write(cursor);

    // Initially, no bytes written
    assert_eq!(counter.load(Ordering::Relaxed), 0);

    // Write some bytes
    writer.write_all(b"Hello").await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 5);

    // Write more bytes
    writer.write_all(b", World!").await.unwrap();
    assert_eq!(counter.load(Ordering::Relaxed), 13);
}

#[tokio::test]
async fn test_byte_layer_ext() {
    let data = b"Test";
    let cursor = Cursor::new(data.to_vec());

    let counter = Arc::new(AtomicU64::new(0));
    let layer = ByteCountingLayer::new(counter.clone());

    // Use extension trait
    let mut reader = cursor.with_byte_layer(layer);

    let mut buf = vec![0u8; 4];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Test");
    assert_eq!(counter.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn test_composable_layers() {
    // Demonstrate that layers can be composed
    let cursor = Cursor::new(Vec::new());

    let counter1 = Arc::new(AtomicU64::new(0));
    let counter2 = Arc::new(AtomicU64::new(0));

    let layer1 = ByteCountingLayer::new(counter1.clone());
    let layer2 = ByteCountingLayer::new(counter2.clone());

    // Apply multiple layers
    let writer = layer1.layer_write(cursor);
    let mut writer = layer2.layer_write(writer);

    writer.write_all(b"Composable!").await.unwrap();
    writer.flush().await.unwrap();

    // Both counters should have counted
    assert_eq!(counter1.load(Ordering::Relaxed), 11);
    assert_eq!(counter2.load(Ordering::Relaxed), 11);
}

#[tokio::test]
async fn test_with_new_counter() {
    let data = b"Test";
    let cursor = Cursor::new(data.to_vec());

    let (layer, counter) = ByteCountingLayer::with_new_counter();
    let mut reader = layer.layer_read(cursor);

    let mut buf = vec![0u8; 4];
    reader.read_exact(&mut buf).await.unwrap();

    assert_eq!(counter.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn test_custom_layer_trait() {
    // This test demonstrates that users can implement their own ByteLayer

    #[derive(Clone)]
    struct NoOpLayer;

    impl ByteLayer for NoOpLayer {
        type Reader<R: tokio::io::AsyncRead> = R;
        type Writer<W: tokio::io::AsyncWrite> = W;

        fn layer_read<R: tokio::io::AsyncRead>(&self, reader: R) -> Self::Reader<R> {
            reader // Pass through unchanged
        }

        fn layer_write<W: tokio::io::AsyncWrite>(&self, writer: W) -> Self::Writer<W> {
            writer // Pass through unchanged
        }
    }

    let data = b"Test";
    let cursor = Cursor::new(data.to_vec());
    let layer = NoOpLayer;

    let mut reader = layer.layer_read(cursor);
    let mut buf = vec![0u8; 4];
    reader.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"Test");
}
