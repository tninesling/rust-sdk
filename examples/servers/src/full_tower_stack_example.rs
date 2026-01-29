//! Fluent Tower Middleware Example
//!
//! This example demonstrates the fluent API for building MCP servers
//! with Tower middleware:
//!
//! ```text
//! McpServer::new(server_info)
//!     .layer(LoggingLayer)
//!     .layer(TimeoutLayer::new(...))
//!     .serve(StdioTransport, handler)
//!     .await?;
//! ```
//!
//! The `serve()` method accepts a `ServerHandler` directly - no need for
//! explicit conversion to Tower services.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Instant,
};

use anyhow::Result;
use common::calculator::Calculator;
use futures::future::BoxFuture;
use rmcp::{
    McpServer, RoleServer, ServerHandler,
    service::{McpMessage, McpOutput},
    transport::StdioTransport,
};
use tower::Layer;
use tower_service::Service as TowerService;

mod common;

// =============================================================================
// Custom Logging Middleware
// =============================================================================

/// A Tower layer that adds logging to MCP message handling
#[derive(Clone)]
pub struct LoggingLayer {
    request_counter: Arc<AtomicU64>,
}

impl LoggingLayer {
    pub fn new() -> Self {
        Self {
            request_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for LoggingLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for LoggingLayer {
    type Service = LoggingService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LoggingService {
            inner: service,
            request_counter: self.request_counter.clone(),
        }
    }
}

/// The middleware service created by LoggingLayer
#[derive(Clone)]
pub struct LoggingService<S> {
    inner: S,
    request_counter: Arc<AtomicU64>,
}

impl<S> TowerService<McpMessage<RoleServer>> for LoggingService<S>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + 'static,
    S::Error: std::fmt::Debug + Send,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, msg: McpMessage<RoleServer>) -> Self::Future {
        let count = self.request_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let start = Instant::now();

        let msg_desc = match &msg {
            McpMessage::Request { request, .. } => {
                format!("Request({:?})", std::mem::discriminant(request))
            }
            McpMessage::Notification { notification, .. } => {
                format!("Notification({:?})", std::mem::discriminant(notification))
            }
        };

        tracing::info!("→ #{} {}", count, msg_desc);

        let mut inner = self.inner.clone();
        Box::pin(async move {
            let result = inner.call(msg).await;
            let elapsed = start.elapsed();

            match &result {
                Ok(_) => tracing::info!("← #{} OK ({:?})", count, elapsed),
                Err(e) => tracing::warn!("← #{} ERR: {:?} ({:?})", count, e, elapsed),
            }

            result
        })
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Create the handler
    let handler = Calculator::new();
    let server_info = ServerHandler::get_info(&handler);

    println!("🚀 MCP Server with Fluent Tower API");
    println!("====================================");
    println!();
    println!("API usage:");
    println!("  McpServer::new(server_info)");
    println!("      .layer(LoggingLayer::new())");
    println!("      .serve(StdioTransport, handler)");
    println!("      .await?;");
    println!();

    // The fluent API - clean and simple!
    let server = McpServer::new(server_info)
        .layer(LoggingLayer::new())
        .serve(StdioTransport, handler)
        .await?;

    println!("✅ Server running with logging middleware");
    println!();
    println!("Connect with:");
    println!("  npx @modelcontextprotocol/inspector cargo run -p mcp-server-examples --example full_tower_stack_example");
    println!();

    server.wait().await?;
    Ok(())
}
