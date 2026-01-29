//! MCP Message Types for Tower Services
//!
//! This module defines the request and response types used when implementing
//! MCP handlers as `tower::Service`s.

use crate::model::*;
use crate::service::{NotificationContext, RequestContext, ServiceRole};

#[cfg(feature = "server")]
use crate::service::RoleServer;
#[cfg(feature = "client")]
use crate::service::RoleClient;

/// The request type for MCP services
///
/// This is the input to a `tower::Service` that handles MCP messages.
/// It can be either a request (expecting a response) or a notification
/// (no response expected).
#[derive(Debug, Clone)]
pub enum McpMessage<R: ServiceRole> {
    /// A request expecting a response
    Request {
        /// The request ID
        id: RequestId,
        /// The typed request (e.g., ListTools, CallTool, etc.)
        request: R::PeerReq,
        /// Request execution context
        context: RequestContext<R>,
    },
    /// A notification (no response expected)
    Notification {
        /// The typed notification
        notification: R::PeerNot,
        /// Notification context
        context: NotificationContext<R>,
    },
}

/// The response type for MCP services
///
/// This is the output from a `tower::Service` that handles MCP messages.
#[derive(Debug)]
pub enum McpOutput<R: ServiceRole> {
    /// Response to a request
    Response {
        /// The request ID this response corresponds to
        id: RequestId,
        /// The result (success or error)
        result: Result<R::Resp, ErrorData>,
    },
    /// Acknowledgment of notification (no actual response sent)
    Ack,
}

/// Type alias for server-side MCP messages
#[cfg(feature = "server")]
pub type ServerMessage = McpMessage<RoleServer>;
#[cfg(feature = "server")]
pub type ServerOutput = McpOutput<RoleServer>;

/// Type alias for client-side MCP messages
#[cfg(feature = "client")]
pub type ClientMessage = McpMessage<RoleClient>;
#[cfg(feature = "client")]
pub type ClientOutput = McpOutput<RoleClient>;
