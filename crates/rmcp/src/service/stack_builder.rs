//! Unified Stack Builder - Ergonomic fluent API for building the middleware stack
//!
//! This module provides a fluent builder that makes it easy to compose
//! middleware at the service layers of the RMCP stack.
//!
//! **Note:** This is an interim API. See `docs/TOWER_REFACTOR_PLAN.md` for the planned
//! Tower-native refactoring that will provide a more idiomatic experience with
//! `tower::ServiceBuilder`.
//!
//! # Current Layers
//!
//! 1. **JSON-RPC Layer** (`.jsonrpc_layer()`): Pre-parse message middleware
//! 2. **MCP Layer** (`.service()`): The core MCP service handler
//! 3. **Peer Layer** (on `RunningService`): Outbound request middleware
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
//! let running = StackBuilder::<RoleServer>::new(server_info)
//!     .jsonrpc_layer(my_telemetry)
//!     .service(my_service)
//!     .serve((stdin, stdout))
//!     .await?;
//!
//! // Peer middleware for outbound requests
//! let _peer = running.peer_builder()
//!     .with_layer(|peer_svc| my_rate_limiter(peer_svc));
//!
//! # Ok(())
//! # }
//! ```

use crate::service::{LayeredService, RawMessageService, Service, ServiceBuilder, ServiceRole};

/// Fluent builder for composing MCP middleware
///
/// # Example
///
/// ```rust,no_run
/// # use rmcp::service::*;
/// # use rmcp::RoleServer;
/// # async fn example<S: Service<RoleServer>>(
/// #     server_info: ServerInfo,
/// #     my_service: S,
/// #     telemetry: impl RawMessageService<RoleServer>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
///
/// let running = StackBuilder::<RoleServer>::new(server_info)
///     .jsonrpc_layer(telemetry)
///     .service(my_service)
///     .serve((stdin, stdout))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct StackBuilder<R: ServiceRole> {
    inner: ServiceBuilder<R>,
}

impl<R: ServiceRole> StackBuilder<R> {
    /// Create a new stack builder with the given service info
    pub fn new(info: R::Info) -> Self {
        Self {
            inner: ServiceBuilder::new(info),
        }
    }

    /// Add JSON-RPC layer middleware
    ///
    /// This middleware operates on JSON-RPC messages before MCP-specific parsing.
    /// Use this for telemetry, validation, or protocol-level filtering.
    pub fn jsonrpc_layer<M>(mut self, middleware: M) -> Self
    where
        M: RawMessageService<R>,
    {
        self.inner = self.inner.with_raw_message_middleware(middleware);
        self
    }

    /// Build with the given service
    ///
    /// Returns a `LayeredService` that can be served on a transport.
    pub fn service<S>(self, service: S) -> LayeredService<S, R>
    where
        S: Service<R>,
    {
        self.inner.build(service)
    }

    /// Build with MCP-level tower middleware
    ///
    /// The provided function receives the inner builder and should return
    /// a `LayeredService` with tower middleware applied.
    #[cfg(feature = "tower")]
    pub fn mcp_layer<F, S>(self, f: F) -> LayeredService<S, R>
    where
        F: FnOnce(ServiceBuilder<R>) -> LayeredService<S, R>,
        S: Service<R>,
    {
        f(self.inner)
    }
}
