use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{Router, http::StatusCode, response::IntoResponse, routing::post};
use rmcp::transport::streamable_http_client::{StreamableHttpClient, StreamableHttpError};

#[tokio::test]
async fn post_message_returns_error_on_429() -> anyhow::Result<()> {
    async fn rate_limit_handler() -> impl IntoResponse {
        (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded")
    }

    let app = Router::new().route("/mcp", post(rate_limit_handler));
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let port = listener.local_addr()?.port();
    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let uri: Arc<str> = format!("http://127.0.0.1:{port}/mcp").into();

    let message: rmcp::model::ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "1.0" }
        }
    }))?;

    let result = client
        .post_message(uri, message, None, None, HashMap::new())
        .await;

    assert!(
        result.is_err(),
        "expected an error for 429, got: {result:?}"
    );

    let err = result.unwrap_err();

    // Previously returned UnexpectedContentType because post_message did not
    // call error_for_status() before inspecting the content type.
    assert!(
        matches!(err, StreamableHttpError::Client(_)),
        "expected StreamableHttpError::Client for 429, got: {err:?}"
    );

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn post_message_returns_error_on_500() -> anyhow::Result<()> {
    async fn error_handler() -> impl IntoResponse {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }

    let app = Router::new().route("/mcp", post(error_handler));
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let port = listener.local_addr()?.port();
    let server_handle = tokio::spawn(async move { axum::serve(listener, app).await });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let uri: Arc<str> = format!("http://127.0.0.1:{port}/mcp").into();

    let message: rmcp::model::ClientJsonRpcMessage = serde_json::from_value(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "1.0" }
        }
    }))?;

    let result = client
        .post_message(uri, message, None, None, HashMap::new())
        .await;

    assert!(
        result.is_err(),
        "expected an error for 500, got: {result:?}"
    );

    let err = result.unwrap_err();

    // Previously returned UnexpectedContentType because post_message did not
    // call error_for_status() before inspecting the content type.
    assert!(
        matches!(err, StreamableHttpError::Client(_)),
        "expected StreamableHttpError::Client for 500, got: {err:?}"
    );

    server_handle.abort();
    Ok(())
}
