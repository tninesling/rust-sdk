//! Tests for streamable HTTP with Tower middleware
//!
//! This verifies that the `StreamableHttpServiceBuilder` fluent API works correctly
//! with Tower middleware layers.

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::future::BoxFuture;
use rmcp::{
    RoleServer,
    service::{McpMessage, McpOutput},
    transport::streamable_http_server::StreamableHttpServiceBuilder,
};
use tokio_util::sync::CancellationToken;
use tower::Layer;
use tower_service::Service as TowerService;

mod common;
use common::calculator::Calculator;

// =============================================================================
// Test middleware that counts requests
// =============================================================================

#[derive(Clone)]
struct CountingLayer {
    counter: Arc<AtomicU32>,
}

impl CountingLayer {
    fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl<S> Layer<S> for CountingLayer {
    type Service = CountingService<S>;

    fn layer(&self, service: S) -> Self::Service {
        CountingService {
            inner: service,
            counter: self.counter.clone(),
        }
    }
}

#[derive(Clone)]
struct CountingService<S> {
    inner: S,
    counter: Arc<AtomicU32>,
}

impl<S> TowerService<McpMessage<RoleServer>> for CountingService<S>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>>
        + Clone
        + Send
        + 'static,
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
        self.counter.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(msg).await })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_streamable_http_with_tower_layer() -> anyhow::Result<()> {
    let ct = CancellationToken::new();
    let counting_layer = CountingLayer::new();
    let counter = counting_layer.counter.clone();

    // Build the service using the fluent builder API with a layer
    let service = StreamableHttpServiceBuilder::new(|| Ok(Calculator::new()))
        .layer(counting_layer)
        .sse_keep_alive(Duration::from_secs(15))
        .cancellation_token(ct.child_token())
        .build();

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;

    let handle = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(tcp_listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let client = reqwest::Client::new();

    // Send initialize request
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
        )
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    // Get session ID from response
    let session_id = response.headers()["mcp-session-id"]
        .to_str()?
        .to_string();

    let body = response.text().await?;
    assert!(body.contains("serverInfo"));

    // Counter should have been incremented by the middleware
    // (once for the initialize request)
    assert!(counter.load(Ordering::Relaxed) >= 1, "CountingLayer should have been invoked");

    // Send initialized notification
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .send()
        .await?;

    assert_eq!(response.status(), 202); // Accepted

    // Counter should be incremented again for the notification
    assert!(
        counter.load(Ordering::Relaxed) >= 2,
        "CountingLayer should count notifications too"
    );

    // Send list_tools request
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let body = response.text().await?;
    assert!(body.contains("tools"), "Expected body to contain 'tools': {}", body);
    assert!(body.contains("sum"), "Expected body to contain 'sum' tool: {}", body);

    // Counter should be incremented for the list_tools request
    assert!(
        counter.load(Ordering::Relaxed) >= 3,
        "CountingLayer should count all requests"
    );

    // Send call_tool request for the "sum" tool
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session_id)
        .body(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"sum","arguments":{"a":2,"b":3}}}"#,
        )
        .send()
        .await?;

    assert_eq!(response.status(), 200);
    let body = response.text().await?;
    assert!(body.contains("5") || body.contains("result")); // 2 + 3 = 5

    let final_count = counter.load(Ordering::Relaxed);
    assert!(
        final_count >= 4,
        "CountingLayer should have counted all messages, got {}",
        final_count
    );

    ct.cancel();
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn test_streamable_http_builder_stateless_mode() -> anyhow::Result<()> {
    let ct = CancellationToken::new();

    // Build the service in stateless mode (no sessions)
    let service = StreamableHttpServiceBuilder::new(|| Ok(Calculator::new()))
        .stateful(false)
        .cancellation_token(ct.child_token())
        .build();

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;

    let handle = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(tcp_listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let client = reqwest::Client::new();

    // In stateless mode, each request is independent
    // Send an initialize request
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
        )
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    // In stateless mode, there should be no session ID header
    assert!(
        response.headers().get("mcp-session-id").is_none(),
        "Stateless mode should not return session ID"
    );

    let body = response.text().await?;
    assert!(body.contains("serverInfo"));

    ct.cancel();
    handle.await?;

    Ok(())
}

#[tokio::test]
async fn test_streamable_http_builder_multiple_layers() -> anyhow::Result<()> {
    let ct = CancellationToken::new();

    // Create two counting layers to verify layer composition
    let layer1 = CountingLayer::new();
    let layer2 = CountingLayer::new();
    let counter1 = layer1.counter.clone();
    let counter2 = layer2.counter.clone();

    // Build the service with multiple layers
    let service = StreamableHttpServiceBuilder::new(|| Ok(Calculator::new()))
        .layer(layer1)
        .layer(layer2)
        .cancellation_token(ct.child_token())
        .build();

    let router = axum::Router::new().nest_service("/mcp", service);
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;

    let handle = tokio::spawn({
        let ct = ct.clone();
        async move {
            let _ = axum::serve(tcp_listener, router)
                .with_graceful_shutdown(async move { ct.cancelled_owned().await })
                .await;
        }
    });

    let client = reqwest::Client::new();

    // Send initialize request
    let response = client
        .post(format!("http://{addr}/mcp"))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}"#,
        )
        .send()
        .await?;

    assert_eq!(response.status(), 200);

    // Both layers should have been invoked
    assert!(
        counter1.load(Ordering::Relaxed) >= 1,
        "First layer should be invoked"
    );
    assert!(
        counter2.load(Ordering::Relaxed) >= 1,
        "Second layer should be invoked"
    );

    ct.cancel();
    handle.await?;

    Ok(())
}
