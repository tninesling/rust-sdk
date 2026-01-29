//! MCP Server Builder
//!
//! This module provides the `McpServer` type which is the main entry point
//! for building and serving MCP servers with Tower-compatible middleware.
//!
//! # Example
//!
//! ```rust,no_run
//! use rmcp::{McpServer, ServerHandler};
//! use rmcp::transport::StdioTransport;
//!
//! #[derive(Clone)]
//! struct MyHandler;
//! impl ServerHandler for MyHandler {}
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Simple case - no middleware
//! McpServer::new(MyHandler::get_info(&MyHandler))
//!     .serve(StdioTransport, MyHandler)
//!     .await?
//!     .wait()
//!     .await?;
//!
//! // With middleware
//! // McpServer::new(server_info)
//! //     .layer(LoggingLayer)
//! //     .layer(TimeoutLayer::new(Duration::from_secs(30)))
//! //     .serve(StdioTransport, MyHandler)
//! //     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use tower::Layer;
use tower_service::Service as TowerService;

use crate::{
    ServerHandler,
    model::{ClientInfo, ServerInfo},
    service::{
        McpMessage, McpOutput, Peer, RoleServer, RunningService, ServerInitializeError,
        TowerServiceAdapter,
    },
    transport::TransportProvider,
};

/// Builder for MCP servers with fluent layer API
///
/// This is the main entry point for creating MCP servers. Use `layer()` to add
/// Tower middleware, then `serve()` to start the server.
///
/// # Type Parameters
///
/// - `L`: The accumulated layer stack (defaults to `Identity` for no layers)
///
/// # Example
///
/// ```rust,no_run
/// use rmcp::{McpServer, ServerHandler};
/// use rmcp::transport::StdioTransport;
///
/// #[derive(Clone)]
/// struct MyHandler;
/// impl ServerHandler for MyHandler {}
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let server = McpServer::new(MyHandler::get_info(&MyHandler))
///     // .layer(MyLoggingLayer)  // Add middleware
///     .serve(StdioTransport, MyHandler)
///     .await?;
///
/// server.wait().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct McpServer<L = tower::layer::util::Identity> {
    info: ServerInfo,
    layer: L,
}

impl McpServer {
    /// Create a new MCP server builder with the given server info
    pub fn new(info: impl Into<ServerInfo>) -> McpServer<tower::layer::util::Identity> {
        McpServer {
            info: info.into(),
            layer: tower::layer::util::Identity::new(),
        }
    }
}

impl<L> McpServer<L> {
    /// Add a Tower middleware layer to the server
    ///
    /// Layers are applied in order - the first layer added is the outermost
    /// (processes requests first, responses last).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rmcp::McpServer;
    ///
    /// # fn example(server_info: rmcp::model::ServerInfo) {
    /// let server = McpServer::new(server_info)
    ///     .layer(tower::timeout::TimeoutLayer::new(std::time::Duration::from_secs(30)));
    /// # }
    /// ```
    pub fn layer<NewLayer>(self, layer: NewLayer) -> McpServer<tower::layer::util::Stack<NewLayer, L>> {
        McpServer {
            info: self.info,
            layer: tower::layer::util::Stack::new(layer, self.layer),
        }
    }

    /// Serve a `ServerHandler` on the given transport
    ///
    /// This performs the MCP handshake and starts the service loop.
    /// Any layers added via `layer()` will be applied to the handler.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rmcp::{McpServer, ServerHandler};
    /// use rmcp::transport::StdioTransport;
    ///
    /// #[derive(Clone)]
    /// struct MyHandler;
    /// impl ServerHandler for MyHandler {}
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// McpServer::new(MyHandler::get_info(&MyHandler))
    ///     .serve(StdioTransport, MyHandler)
    ///     .await?
    ///     .wait()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn serve<T, H>(
        self,
        transport: T,
        handler: H,
    ) -> Result<RunningMcpServer<L::Service>, ServerInitializeError>
    where
        T: TransportProvider<RoleServer>,
        H: ServerHandler + Clone + Send + Sync + 'static,
        L: Layer<TowerServiceAdapter<H>>,
        L::Service: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleServer>>>::Error: std::error::Error + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleServer>>>::Future: Send,
    {
        // Wrap the handler and apply layers
        let base_service = TowerServiceAdapter::new(handler);
        let service = self.layer.layer(base_service);

        self.serve_tower_service(transport, service).await
    }

    /// Serve a pre-built Tower service on the given transport
    ///
    /// Use this when you need full control over the service construction,
    /// or when using a service that doesn't implement `ServerHandler`.
    ///
    /// Note: Layers added via `layer()` will still be applied on top of
    /// the provided service.
    pub async fn serve_with_tower_service<T, S>(
        self,
        transport: T,
        service: S,
    ) -> Result<RunningMcpServer<L::Service>, ServerInitializeError>
    where
        T: TransportProvider<RoleServer>,
        L: Layer<S>,
        L::Service: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleServer>>>::Error: std::error::Error + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleServer>>>::Future: Send,
    {
        let service = self.layer.layer(service);
        self.serve_tower_service(transport, service).await
    }

    /// Internal method to serve a tower service
    async fn serve_tower_service<T, S>(
        &self,
        transport: T,
        service: S,
    ) -> Result<RunningMcpServer<S>, ServerInitializeError>
    where
        T: TransportProvider<RoleServer>,
        S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
        S::Future: Send,
    {
        // 1. Create transport connection
        let mut transport = transport.connect().await.map_err(|e| {
            use crate::transport::DynamicTransportError;
            use std::borrow::Cow;
            ServerInitializeError::TransportError {
                error: DynamicTransportError {
                    transport_name: Cow::Borrowed("TransportProvider"),
                    transport_type_id: std::any::TypeId::of::<()>(),
                    error: Box::new(e),
                },
                context: "connect transport".into(),
            }
        })?;

        // 2. Perform MCP handshake
        let peer_info = self.handshake(&mut transport).await?;

        // 3. Start the service loop
        let (_peer, running) = self.serve_loop(transport, service, peer_info).await;

        Ok(RunningMcpServer { running })
    }

    async fn handshake<T>(
        &self,
        transport: &mut T,
    ) -> Result<Option<ClientInfo>, ServerInitializeError>
    where
        T: crate::transport::Transport<RoleServer>,
    {
        use crate::model::*;

        async fn expect_next_message<T>(
            transport: &mut T,
            context: &str,
        ) -> Result<ClientJsonRpcMessage, ServerInitializeError>
        where
            T: crate::transport::Transport<RoleServer>,
        {
            transport
                .receive()
                .await
                .ok_or_else(|| ServerInitializeError::ConnectionClosed(context.to_string()))
        }

        async fn expect_request<T>(
            transport: &mut T,
            context: &str,
        ) -> Result<(ClientRequest, RequestId), ServerInitializeError>
        where
            T: crate::transport::Transport<RoleServer>,
        {
            let msg = expect_next_message(transport, context).await?;
            let msg_clone = msg.clone();
            msg.into_request()
                .ok_or(ServerInitializeError::ExpectedInitializeRequest(Some(
                    msg_clone,
                )))
        }

        async fn expect_notification<T>(
            transport: &mut T,
            context: &str,
        ) -> Result<ClientNotification, ServerInitializeError>
        where
            T: crate::transport::Transport<RoleServer>,
        {
            let msg = expect_next_message(transport, context).await?;
            let msg_clone = msg.clone();
            msg.into_notification()
                .ok_or(ServerInitializeError::ExpectedInitializedNotification(
                    Some(msg_clone),
                ))
        }

        // Wait for initialize request
        let (request, id) = expect_request(transport, "initialize request").await?;

        let ClientRequest::InitializeRequest(init_request) = request else {
            return Err(ServerInitializeError::ExpectedInitializeRequest(Some(
                ClientJsonRpcMessage::request(request, id),
            )));
        };

        let peer_info = init_request.params.clone();

        // Send initialize response
        let init_result = InitializeResult {
            protocol_version: ProtocolVersion::default(),
            capabilities: ServerCapabilities::default(),
            server_info: self.info.server_info.clone(),
            instructions: self.info.instructions.clone(),
        };

        transport
            .send(ServerJsonRpcMessage::response(
                ServerResult::InitializeResult(init_result),
                id,
            ))
            .await
            .map_err(|e| {
                use crate::transport::DynamicTransportError;
                use std::borrow::Cow;
                ServerInitializeError::TransportError {
                    error: DynamicTransportError {
                        transport_name: Cow::Borrowed("Transport"),
                        transport_type_id: std::any::TypeId::of::<()>(),
                        error: Box::new(e),
                    },
                    context: "send initialize response".into(),
                }
            })?;

        // Wait for initialized notification
        let notification = expect_notification(transport, "initialized notification").await?;
        let ClientNotification::InitializedNotification(_) = notification else {
            return Err(ServerInitializeError::ExpectedInitializedNotification(Some(
                ClientJsonRpcMessage::notification(notification),
            )));
        };

        Ok(Some(peer_info))
    }

    async fn serve_loop<T, S>(
        &self,
        transport: T,
        service: S,
        peer_info: Option<ClientInfo>,
    ) -> (Peer<RoleServer>, RunningService<RoleServer, crate::service::TowerServiceWrapper<S>>)
    where
        T: crate::transport::Transport<RoleServer> + 'static,
        S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
        S::Future: Send,
    {
        use crate::service::serve_tower_inner;
        use crate::service::{Peer as ServicePeer, AtomicU32RequestIdProvider};
        use tokio_util::sync::CancellationToken;

        let id_provider = Arc::new(AtomicU32RequestIdProvider::default());
        let (peer, peer_rx) = ServicePeer::new(id_provider, peer_info);

        let running = serve_tower_inner(
            service,
            transport,
            peer.clone(),
            peer_rx,
            CancellationToken::default(),
        );

        (peer, running)
    }
}

/// A running MCP server
///
/// This is returned after calling `McpServer::serve()`. It provides access
/// to the peer for making outbound requests and methods to wait for the
/// server to finish.
pub struct RunningMcpServer<S>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
{
    running: RunningService<RoleServer, crate::service::TowerServiceWrapper<S>>,
}

impl<S> RunningMcpServer<S>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
{
    /// Get a reference to the peer for making outbound requests
    pub fn peer(&self) -> &Peer<RoleServer> {
        self.running.peer()
    }

    /// Wait for the server to finish
    ///
    /// This will block until the server loop terminates (due to cancellation,
    /// transport closure, or an error).
    pub async fn wait(self) -> Result<crate::service::QuitReason, tokio::task::JoinError> {
        self.running.waiting().await
    }

    /// Gracefully close the server and wait for cleanup to complete
    pub async fn close(mut self) -> Result<crate::service::QuitReason, tokio::task::JoinError> {
        self.running.close().await
    }
}
