//! Streamable HTTP Server Example with Fluent Builder API
//!
//! This example demonstrates the new fluent API for building streamable HTTP
//! MCP servers with Tower middleware:
//!
//! ```text
//! StreamableHttpServiceBuilder::new(|| Ok(MyHandler))
//!     .layer(LoggingLayer::new())
//!     .sse_keep_alive(Duration::from_secs(15))
//!     .build()
//! ```

use std::{
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    task::{Context, Poll},
    time::Instant,
};

use futures::future::BoxFuture;
use rmcp::{
    RoleServer,
    service::{McpMessage, McpOutput},
    transport::streamable_http_server::StreamableHttpServiceBuilder,
};
use tower::Layer;
use tower_service::Service as TowerService;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod common;
use common::counter::Counter;

const BIND_ADDRESS: &str = "127.0.0.1:8000";

// =============================================================================
// Logging Middleware (same as stdio example)
// =============================================================================

#[derive(Clone)]
pub struct LoggingLayer {
    counter: Arc<AtomicU64>,
}

impl LoggingLayer {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
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
            counter: self.counter.clone(),
        }
    }
}

#[derive(Clone)]
pub struct LoggingService<S> {
    inner: S,
    counter: Arc<AtomicU64>,
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
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        let start = Instant::now();

        let desc = match &msg {
            McpMessage::Request { request, .. } => format!("Request({:?})", std::mem::discriminant(request)),
            McpMessage::Notification { notification, .. } => format!("Notification({:?})", std::mem::discriminant(notification)),
        };

        tracing::info!("→ #{} {}", n, desc);

        let mut inner = self.inner.clone();
        Box::pin(async move {
            let result = inner.call(msg).await;
            tracing::info!("← #{} {:?} ({:?})", n, result.as_ref().map(|_| "OK"), start.elapsed());
            result
        })
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".to_string().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let ct = tokio_util::sync::CancellationToken::new();

    println!("🚀 Streamable HTTP Server with Fluent Builder API");
    println!("==================================================");
    println!();
    println!("API usage:");
    println!("  StreamableHttpServiceBuilder::new(|| Ok(Counter::new()))");
    println!("      .layer(LoggingLayer::new())");
    println!("      .cancellation_token(ct)");
    println!("      .build()");
    println!();

    // New fluent builder API
    let service = StreamableHttpServiceBuilder::new(|| Ok(Counter::new()))
        .layer(LoggingLayer::new())
        .cancellation_token(ct.child_token())
        .build();

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind(BIND_ADDRESS).await?;

    println!("✅ Server listening on http://{}/mcp", BIND_ADDRESS);
    println!();
    println!("Connect with:");
    println!("  npx @anthropic/mcp-inspector http://{}:8000/mcp", BIND_ADDRESS.split(':').next().unwrap());
    println!();

    axum::serve(tcp_listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.unwrap();
            ct.cancel();
        })
        .await?;

    Ok(())
}
