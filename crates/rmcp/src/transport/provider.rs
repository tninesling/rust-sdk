//! Transport Provider Abstraction
//!
//! This module provides a trait for different transport types (stdio, HTTP, etc.)
//! that allows them to be used interchangeably with the `McpServer` API.

use std::future::Future;

use crate::service::ServiceRole;
use crate::transport::Transport;

/// Trait for transport providers
///
/// This abstracts over different transport types (stdio, HTTP, etc.)
/// allowing them to be used interchangeably with `McpServer`.
pub trait TransportProvider<R: ServiceRole> {
    /// The transport type this provider creates
    type Transport: Transport<R> + 'static;
    /// The error type for transport creation
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create a transport connection
    fn connect(self) -> impl Future<Output = Result<Self::Transport, Self::Error>> + Send;
}

/// Stdio transport provider
///
/// Creates a transport from stdin/stdout.
#[cfg(feature = "transport-io")]
#[derive(Debug, Clone, Copy, Default)]
pub struct StdioTransport;

#[cfg(feature = "transport-io")]
impl<R: ServiceRole> TransportProvider<R> for StdioTransport {
    type Transport = crate::transport::async_rw::AsyncRwTransport<R, tokio::io::Stdin, tokio::io::Stdout>;
    type Error = std::io::Error;

    async fn connect(self) -> Result<Self::Transport, Self::Error> {
        let (stdin, stdout) = (tokio::io::stdin(), tokio::io::stdout());
        Ok(crate::transport::async_rw::AsyncRwTransport::new(stdin, stdout))
    }
}

/// Helper to create a stdio transport provider
#[cfg(feature = "transport-io")]
pub fn stdio() -> StdioTransport {
    StdioTransport
}

// GenericTransportProvider removed - IntoTransport doesn't expose a concrete Transport type.
// Users should implement TransportProvider directly for their transport types, or use
// the concrete providers like StdioTransport.
