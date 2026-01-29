//! Unified Stack Builder - Ergonomic fluent API for building the middleware stack
//!
//! This module provides a single fluent builder that makes it easy to compose
//! middleware at the service layers of the RMCP stack:
//!
//! 1. **JSON-RPC Layer** (`.jsonrpc_layer()`): Pre-parse message middleware (telemetry, validation, etc.)
//! 2. **MCP Layer** (`.mcp_layer()` or `.service()`): Post-parse typed request middleware (auth, caching, etc.)
//! 3. **Peer Layer** (`.with_layer()` on peer): Outbound request middleware (rate limiting, retries, etc.)
//!
//! Note: Byte-level middleware (Layer 0) is applied separately via `ByteLayer` before
//! creating the transport, as it requires compile-time type information.
//!
//! # Example
//!
//! ```rust,no_run
//! # use rmcp::service::*;
//! # use rmcp::RoleServer;
//! # async fn example<S: Service<RoleServer>>(
//! #     server_info: ServerInfo,
//! #     my_service: S,
//! #     my_telemetry: impl RawMessageService<RoleServer>,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
//!
//! // Build a complete stack with fluent API
//! let running = StackBuilder::<RoleServer>::new(server_info)
//!     // Layer 1: JSON-RPC layer
//!     .jsonrpc_layer(my_telemetry)
//!     // Layer 2: MCP service
//!     .service(my_service)
//!     // Start serving
//!     .serve((stdin, stdout))
//!     .await?;
//!
//! // Layer 3: Peer layer (for outbound requests)
//! let _peer = running
//!     .peer_builder()
//!     .with_layer(|peer_svc| my_rate_limiter(peer_svc));
//!
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    service::{LayeredService, RawMessageService, RunningService, Service, ServiceBuilder, ServiceExt, ServiceRole},
    transport::ByteLayer,
    Error,
};

/// Unified stack builder for composing middleware at service layers
///
/// This builder provides a fluent API that makes the layering structure explicit
/// and easy to understand. Each method corresponds to a specific layer in the stack.
///
/// # Layers
///
/// 1. **JSON-RPC Layer** (`.jsonrpc_layer()`): Operates on JSON-RPC messages before MCP parsing
/// 2. **MCP Layer** (`.service()` or `.mcp_layer()`): Operates on typed MCP requests
/// 3. **Peer Layer** (`.with_layer()` on peer_builder): Operates on outbound requests (after `.serve()`)
///
/// # Example
///
/// ```rust,no_run
/// # use rmcp::service::*;
/// # use rmcp::RoleServer;
/// # async fn example<S: Service<RoleServer>>(
/// #     server_info: ServerInfo,
/// #     my_service: S,
/// #     my_telemetry: impl RawMessageService<RoleServer>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
///
/// let running = StackBuilder::<RoleServer>::new(server_info)
///     .jsonrpc_layer(my_telemetry)
///     .service(my_service)
///     .serve((stdin, stdout))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct StackBuilder<R: ServiceRole> {
    inner: ServiceBuilder<R>,
    byte_layers: Vec<Box<dyn ErasedByteLayer>>,
}

impl<R: ServiceRole> StackBuilder<R> {
    /// Create a new stack builder with the given service info
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::RoleServer;
    /// # fn example(server_info: ServerInfo) {
    /// let builder = StackBuilder::<RoleServer>::new(server_info);
    /// # }
    /// ```
    pub fn new(info: R::Info) -> Self {
        Self {
            inner: ServiceBuilder::new(info),
            byte_layers: Vec::new(),
        }
    }

    /// Add byte-level layer middleware (Layer 0)
    ///
    /// This middleware operates on raw bytes before any parsing. Use this for:
    /// - Byte counting and metrics
    /// - Compression
    /// - Encryption
    /// - Protocol-level transformations
    ///
    /// The byte layer will be automatically applied when you call `.serve()`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::transport::*;
    /// # use rmcp::RoleServer;
    /// # async fn example<S: Service<RoleServer>>(
    /// #     server_info: ServerInfo,
    /// #     my_service: S,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
    ///
    /// let running = StackBuilder::<RoleServer>::new(server_info)
    ///     .byte_layer(ByteCountingLayer::with_new_counter().0)
    ///     .service(my_service)
    ///     .serve((stdin, stdout))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn byte_layer<L>(mut self, layer: L) -> Self
    where
        L: ByteLayer + 'static,
        L::Reader<Box<dyn AsyncRead + Unpin + Send>>: Unpin + Send + 'static,
        L::Writer<Box<dyn AsyncWrite + Unpin + Send>>: Unpin + Send + 'static,
    {
        self.byte_layers.push(Box::new(layer));
        self
    }

    /// Add JSON-RPC layer middleware (Layer 1)
    ///
    /// This middleware operates on JSON-RPC messages before MCP-specific parsing.
    /// Use this for:
    /// - JSON-RPC telemetry and logging
    /// - Message validation
    /// - Protocol-level filtering
    /// - Generic message transformations
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::RoleServer;
    /// # fn example<M: RawMessageService<RoleServer>>(server_info: ServerInfo, middleware: M) {
    /// let builder = StackBuilder::<RoleServer>::new(server_info)
    ///     .jsonrpc_layer(middleware);
    /// # }
    /// ```
    pub fn jsonrpc_layer<M>(mut self, middleware: M) -> Self
    where
        M: RawMessageService<R>,
    {
        self.inner = self.inner.with_raw_message_middleware(middleware);
        self
    }

    /// Build the stack with the given service and return a `BuiltStack`
    ///
    /// This prepares the stack for serving but doesn't start it yet.
    /// Call `.serve()` on the returned `BuiltStack` to start serving.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::RoleServer;
    /// # async fn example<S: Service<RoleServer>>(
    /// #     server_info: ServerInfo,
    /// #     my_service: S,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
    ///
    /// let running = StackBuilder::<RoleServer>::new(server_info)
    ///     .service(my_service)
    ///     .serve((stdin, stdout))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn service<S>(self, service: S) -> BuiltStack<S, R>
    where
        S: Service<R>,
    {
        BuiltStack {
            service: self.inner.build(service),
            byte_layers: self.byte_layers,
        }
    }

    /// Add MCP layer middleware using a tower service (Layer 2)
    ///
    /// This middleware operates on typed MCP requests after parsing. Use this for:
    /// - Authentication and authorization
    /// - Request/response caching
    /// - Request transformation
    /// - Business logic middleware
    ///
    /// The provided function receives the current builder and should return a
    /// `LayeredService` with your tower middleware applied.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::RoleServer;
    /// # use tower::ServiceBuilder as TowerServiceBuilder;
    /// # fn example<S: Service<RoleServer>>(
    /// #     server_info: ServerInfo,
    /// #     my_service: S,
    /// # ) -> LayeredService<TowerServiceHandler<S, RoleServer>, RoleServer> {
    /// StackBuilder::<RoleServer>::new(server_info.clone())
    ///     .mcp_layer(|builder| {
    ///         builder.with_tower_service(
    ///             TowerServiceBuilder::new()
    ///                 .service(TowerServiceHandler::new(my_service, server_info))
    ///         )
    ///     })
    /// # }
    /// ```
    #[cfg(feature = "tower")]
    pub fn mcp_layer<F, S>(self, f: F) -> BuiltStack<S, R>
    where
        F: FnOnce(ServiceBuilder<R>) -> LayeredService<S, R>,
        S: Service<R>,
    {
        BuiltStack {
            service: f(self.inner),
            byte_layers: self.byte_layers,
        }
    }
}

/// A built stack ready to serve
///
/// This represents a complete middleware stack that's ready to start serving.
/// Call `.serve()` to apply byte layers and start the service.
pub struct BuiltStack<S, R: ServiceRole> {
    service: LayeredService<S, R>,
    byte_layers: Vec<Box<dyn ErasedByteLayer>>,
}

impl<S, R> BuiltStack<S, R>
where
    S: Service<R>,
    R: ServiceRole,
{
    /// Start serving on the given transport
    ///
    /// This applies all configured byte layers to the transport and starts the service loop.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rmcp::service::*;
    /// # use rmcp::RoleServer;
    /// # async fn example<S: Service<RoleServer>>(
    /// #     server_info: ServerInfo,
    /// #     my_service: S,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
    ///
    /// let running = StackBuilder::<RoleServer>::new(server_info)
    ///     .service(my_service)
    ///     .serve((stdin, stdout))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn serve<T, E, A>(self, transport: T) -> Result<RunningService<R, LayeredService<S, R>>, R::InitializeError>
    where
        T: crate::transport::IntoTransport<R, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        use crate::transport::Transport;
        
        // Apply byte layers if any
        if self.byte_layers.is_empty() {
            // No byte layers, use transport directly
            ServiceExt::serve(self.service, transport).await
        } else {
            // Apply byte layers
            let transport = transport.into_transport();
            let (read, write) = Transport::split(transport);
            let mut boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(read);
            let mut boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(write);

            for layer in &self.byte_layers {
                boxed_read = layer.layer_read_erased(boxed_read);
                boxed_write = layer.layer_write_erased(boxed_write);
            }

            ServiceExt::serve(self.service, (boxed_read, boxed_write)).await
        }
    }
}

/// Type-erased byte layer for dynamic composition
trait ErasedByteLayer {
    fn layer_read_erased(
        &self,
        reader: Box<dyn AsyncRead + Unpin + Send>,
    ) -> Box<dyn AsyncRead + Unpin + Send>;
    fn layer_write_erased(
        &self,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
    ) -> Box<dyn AsyncWrite + Unpin + Send>;
}

impl<L: ByteLayer + 'static> ErasedByteLayer for L
where
    L::Reader<Box<dyn AsyncRead + Unpin + Send>>: Unpin + Send + 'static,
    L::Writer<Box<dyn AsyncWrite + Unpin + Send>>: Unpin + Send + 'static,
{
    fn layer_read_erased(
        &self,
        reader: Box<dyn AsyncRead + Unpin + Send>,
    ) -> Box<dyn AsyncRead + Unpin + Send> {
        Box::new(self.layer_read(reader))
    }

    fn layer_write_erased(
        &self,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
    ) -> Box<dyn AsyncWrite + Unpin + Send> {
        Box::new(self.layer_write(writer))
    }
}
