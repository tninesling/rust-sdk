//! Complete Tower Middleware Stack Example
//!
//! This example demonstrates middleware at multiple layers:
//! - Layer 1: Byte-level (counting bytes)
//! - Layer 2: Pre-parse (JSON-RPC telemetry and logging)
//! - Layer 4: Peer service (rate limiting outbound requests)
//!
//! Layer 3 (post-parse typed requests) is demonstrated in post_parse_middleware_example.rs

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use common::calculator::Calculator;
use futures::future::BoxFuture;
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    model::*,
    service::*,
    transport::{ByteCountingLayer, ByteLayer},
};
use tower::ServiceBuilder as TowerServiceBuilder;
use tower_service::Service as TowerService;

mod common;

// =============================================================================
// Layer 1: Byte-Level Middleware (Transport)
// =============================================================================

/// Already implemented: ByteCountingLayer
/// This operates on raw bytes before any parsing

// =============================================================================
// Layer 2: Pre-Parse Middleware (Raw JSON-RPC Messages)
// =============================================================================

/// Telemetry middleware that logs JSON-RPC message metadata
/// This operates on the raw JSON-RPC level before MCP-specific parsing
struct JsonRpcTelemetry {
    message_counter: Arc<AtomicU64>,
}

impl JsonRpcTelemetry {
    fn new(counter: Arc<AtomicU64>) -> Self {
        Self {
            message_counter: counter,
        }
    }
}

impl<R: ServiceRole> RawMessageService<R> for JsonRpcTelemetry {
    fn handle_message(
        &self,
        message: RxJsonRpcMessage<R>,
        _context: RawMessageContext<R>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<R>, ErrorData>> {
        let count = self.message_counter.fetch_add(1, Ordering::Relaxed) + 1;

        Box::pin(async move {
            // Log telemetry about the JSON-RPC message
            // We use a simple count-based log since the message structure is generic
            let message_type = match &message {
                JsonRpcMessage::Request(req) => {
                    format!("Request (id: {:?})", req.id)
                }
                JsonRpcMessage::Notification(_) => "Notification".to_string(),
                JsonRpcMessage::Response(resp) => {
                    format!("Response (id: {:?})", resp.id)
                }
                JsonRpcMessage::Error(err) => {
                    format!(
                        "Error (code: {:?}, msg: {})",
                        err.error.code, err.error.message
                    )
                }
            };

            tracing::info!("📊 Layer 2: JSON-RPC {} #{}", message_type, count);

            // Continue processing
            Ok(RawMessageResponse::Continue)
        })
    }
}

// =============================================================================
// Layer 3: Post-Parse Middleware (Typed Requests)
// =============================================================================

/// Simple logging middleware for demonstration
/// In a real application, this could be authentication, authorization, etc.
#[derive(Clone)]
struct RequestCountingMiddleware<S> {
    inner: S,
    counter: Arc<AtomicU64>,
}

impl<S> RequestCountingMiddleware<S> {
    fn new(inner: S, counter: Arc<AtomicU64>) -> Self {
        Self { inner, counter }
    }
}

impl<S> TowerService<McpRequest<RoleServer>> for RequestCountingMiddleware<S>
where
    S: TowerService<McpRequest<RoleServer>, Response = ServerResult>,
    S::Error: Into<ErrorData>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: McpRequest<RoleServer>) -> Self::Future {
        let count = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!("📝 Layer 3: Processing request #{}", count);

        let future = self.inner.call(req);
        Box::pin(async move {
            let result = future.await;
            match &result {
                Ok(_) => tracing::debug!("✅ Layer 3: Request #{} succeeded", count),
                Err(_) => tracing::warn!("❌ Layer 3: Request #{} failed", count),
            }
            result
        })
    }
}

// =============================================================================
// Layer 4: Peer Service Middleware (Outbound Requests)
// =============================================================================

/// Simple rate limiter for outbound requests
#[derive(Clone)]
struct SimpleRateLimiter<S> {
    inner: S,
    last_request: Arc<tokio::sync::Mutex<Option<std::time::Instant>>>,
    min_interval: Duration,
}

impl<S> SimpleRateLimiter<S> {
    fn new(inner: S, min_interval: Duration) -> Self {
        Self {
            inner,
            last_request: Arc::new(tokio::sync::Mutex::new(None)),
            min_interval,
        }
    }
}

impl<S, R> TowerService<PeerRequest<R>> for SimpleRateLimiter<S>
where
    S: TowerService<PeerRequest<R>>,
    S::Future: Send + 'static,
    R: ServiceRole,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: PeerRequest<R>) -> Self::Future {
        let last_request = self.last_request.clone();
        let min_interval = self.min_interval;
        let future = self.inner.call(req);

        Box::pin(async move {
            // Check rate limit
            let mut last = last_request.lock().await;
            if let Some(last_time) = *last {
                let elapsed = last_time.elapsed();
                if elapsed < min_interval {
                    let wait_time = min_interval - elapsed;
                    tracing::debug!("⏱️  Layer 4: Rate limiting - waiting {:?}", wait_time);
                    tokio::time::sleep(wait_time).await;
                }
            }
            *last = Some(std::time::Instant::now());
            drop(last);

            tracing::debug!("🚀 Layer 4: Sending outbound request");
            future.await
        })
    }
}

// =============================================================================
// Main: Assembling the Complete Stack
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("🏗️  Complete Tower Middleware Stack Example");
    println!("==========================================\n");
    println!("This example demonstrates middleware at 3 key layers:\n");
    println!("  Layer 1: Byte counting (raw bytes)");
    println!("  Layer 2: JSON-RPC telemetry (pre-parse)");
    println!("  Layer 4: Rate limiting (outbound requests)");
    println!("\n  (Layer 3 post-parse is shown in post_parse_middleware_example.rs)\n");

    // =========================================================================
    // Layer 1: Byte-Level Middleware
    // =========================================================================
    println!("📊 Setting up Layer 1: Byte counting...");

    let byte_counter = Arc::new(AtomicU64::new(0));
    let byte_layer = ByteCountingLayer::new(byte_counter.clone());

    let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
    let counting_stdin = byte_layer.layer_read(stdin);
    let counting_stdout = byte_layer.layer_write(stdout);

    // Spawn byte counter monitor
    let counter_clone = byte_counter.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let total = counter_clone.load(Ordering::Relaxed);
            tracing::info!("📊 Layer 1: Total bytes transferred: {}", total);
        }
    });

    // =========================================================================
    // Layer 2: Pre-Parse Middleware (Raw Message)
    // =========================================================================
    println!("📊 Setting up Layer 2: JSON-RPC telemetry...");

    let jsonrpc_counter = Arc::new(AtomicU64::new(0));
    let base_service = Calculator::new();
    let server_info = ServerHandler::get_info(&base_service);

    // =========================================================================
    // Layer 3: Post-Parse Middleware (Typed Requests)
    // =========================================================================
    println!("📝 Layer 3 is demonstrated in post_parse_middleware_example.rs");
    println!("   For this example, we focus on Layers 1, 2, and 4");

    // Build the service with layer 2
    let service = ServiceBuilder::new(server_info)
        // Layer 2: JSON-RPC telemetry middleware
        .with_raw_message_middleware(JsonRpcTelemetry::new(jsonrpc_counter.clone()))
        .build(base_service);

    // Start the service
    let running = service.serve((counting_stdin, counting_stdout)).await?;

    // =========================================================================
    // Layer 4: Peer Service Middleware (Outbound Requests)
    // =========================================================================
    println!("⏱️  Setting up Layer 4: Rate limiting for outbound requests...");

    let _rate_limited_peer = running.peer_builder().with_layer(|peer_service| {
        SimpleRateLimiter::new(peer_service, Duration::from_millis(100))
    });

    println!("\n✅ All layers configured!");
    println!("🚀 Server is running with complete middleware stack\n");
    println!("Try connecting with the MCP inspector:");
    println!(
        "  npx @modelcontextprotocol/inspector cargo run --example full_tower_stack_example\n"
    );

    tracing::info!("✅ Server started with multi-layer middleware stack");
    tracing::info!("   Layer 1: Byte counting");
    tracing::info!("   Layer 2: JSON-RPC telemetry");
    tracing::info!("   Layer 4: Rate limiting (100ms between requests)");

    running.waiting().await?;
    Ok(())
}
