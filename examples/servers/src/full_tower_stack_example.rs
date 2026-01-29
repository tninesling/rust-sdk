//! Complete Tower Middleware Stack Example
//!
//! This example demonstrates the ergonomic fluent API for building a complete
//! middleware stack with layers at different levels:
//!
//! - Layer 0: Byte-level (counting bytes via `.byte_layer()`)
//! - Layer 1: JSON-RPC (telemetry via `.jsonrpc_layer()`)
//! - Layer 2: Peer (rate limiting via `.peer_builder().with_layer()`)
//!
//! The key improvement is that byte layers are now integrated into the fluent
//! builder API and applied automatically when `.serve()` is called!

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
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::*,
    service::{
        PeerRequest, RawMessageContext, RawMessageResponse, RawMessageService, RxJsonRpcMessage,
        ServiceRole, StackBuilder,
    },
    transport::{ByteCountingLayer, ByteLayer},
};
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
    println!("This example demonstrates the ergonomic fluent API:");
    println!("  StackBuilder::new(info)");
    println!("    .byte_layer(...)        // Layer 0: Byte-level");
    println!("    .jsonrpc_layer(...)     // Layer 1: JSON-RPC");
    println!("    .service(...)           // Core service");
    println!("    .serve(transport)       // Applies layers automatically!\n");

    // =========================================================================
    // Setup
    // =========================================================================
    let byte_counter = Arc::new(AtomicU64::new(0));
    let jsonrpc_counter = Arc::new(AtomicU64::new(0));
    let base_service = Calculator::new();
    let server_info = ServerHandler::get_info(&base_service);

    // Spawn byte counter monitor
    let counter_clone = byte_counter.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let total = counter_clone.load(Ordering::Relaxed);
            tracing::info!("📊 Layer 0: Total bytes transferred: {}", total);
        }
    });

    // =========================================================================
    // Build the complete stack with fluent API!
    // =========================================================================
    // This demonstrates the ergonomic builder pattern:
    //   StackBuilder::new(info)
    //     .byte_layer(layer)          // Layer 0: Byte-level
    //     .jsonrpc_layer(middleware)  // Layer 1: JSON-RPC
    //     .service(my_service)        // Core service
    //     .serve(transport)           // Start (applies byte layers automatically!)
    
    println!("🏗️  Building complete middleware stack...");
    println!("   Layer 0: Byte counting");
    println!("   Layer 1: JSON-RPC telemetry");
    println!("   Layer 2: Peer rate limiting\n");

    let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());

    let running = StackBuilder::<RoleServer>::new(server_info)
        .byte_layer(ByteCountingLayer::new(byte_counter.clone()))
        .jsonrpc_layer(JsonRpcTelemetry::new(jsonrpc_counter.clone()))
        .service(base_service)
        .serve((stdin, stdout))
        .await?;

    // =========================================================================
    // Layer 4: Peer Service Middleware (Outbound Requests)
    // =========================================================================
    println!("⏱️  Setting up Layer 4: Rate limiting for outbound requests...");

    // Peer middleware is added after the service starts
    let _rate_limited_peer = running.peer_builder().with_layer(|peer_service| {
        SimpleRateLimiter::new(peer_service, Duration::from_millis(100))
    });

    println!("✅ All layers configured!");
    println!("🚀 Server is running with complete middleware stack\n");
    println!("Try connecting with the MCP inspector:");
    println!(
        "  npx @modelcontextprotocol/inspector cargo run --example full_tower_stack_example\n"
    );

    tracing::info!("✅ Server started with complete middleware stack");
    tracing::info!("   Layer 0: Byte counting");
    tracing::info!("   Layer 1: JSON-RPC telemetry");
    tracing::info!("   Layer 2: Peer rate limiting (100ms between requests)");

    running.waiting().await?;
    Ok(())
}
