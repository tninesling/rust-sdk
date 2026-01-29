//! Example demonstrating byte-level layer middleware
//!
//! This example shows how to use tower-style layers with transport streams
//! to monitor bandwidth usage without any serialization/deserialization overhead.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use common::calculator::Calculator;
use rmcp::{
    ServiceExt,
    transport::{ByteCountingLayer, ByteLayer},
};

mod common;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    println!("Byte Layer Middleware Example");
    println!("==============================\n");
    println!("This example uses tower-style layers to count bytes at the transport layer.");
    println!("The layer approach is composable - you can stack multiple layers!\n");

    // Get stdio streams
    let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());

    // Create a byte counting layer
    let counter = Arc::new(AtomicU64::new(0));
    let layer = ByteCountingLayer::new(counter.clone());

    // Apply the layer to both streams
    let counting_stdin = layer.layer_read(stdin);
    let counting_stdout = layer.layer_write(stdout);

    // Spawn a background task to periodically log byte counts
    let counter_clone = counter.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let total_bytes = counter_clone.load(Ordering::Relaxed);
            tracing::info!(
                "📊 Transport stats: {} total bytes (read + written)",
                total_bytes
            );
        }
    });

    // Create the calculator service with byte-counting transport
    let service = Calculator::new()
        .serve((counting_stdin, counting_stdout))
        .await?;

    tracing::info!("✅ Server started with byte layer middleware");
    tracing::info!("   This demonstrates the tower-style layer pattern at the byte level");
    tracing::info!("   Byte counts will be logged every 5 seconds");

    service.waiting().await?;
    Ok(())
}
