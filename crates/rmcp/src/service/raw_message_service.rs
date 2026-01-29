//! Raw Message Service - Tower middleware support for pre-parse message processing
//!
//! This module provides middleware support for raw JSON-RPC messages before they are
//! parsed into typed requests. This enables protocol-level concerns to be handled
//! before deserialization occurs.
//!
//! # Example
//!
//! ```rust,no_run
//! # fn example<R: ServiceRole, M: RawMessageService<R>, S: Service<R>>(
//! #     server_info: R::Info,
//! #     my_middleware: M,
//! #     my_service: S,
//! # ) {
//! use rmcp::service::*;
//!
//! let service = ServiceBuilder::new(server_info)
//!     .with_raw_message_middleware(my_middleware)
//!     .build(my_service);
//! # }
//! ```

use futures::future::BoxFuture;

use crate::{
    ErrorData as McpError,
    model::{Extensions, Meta},
    service::{Peer, RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
};

/// Context for raw message handling
///
/// This provides access to the peer and metadata for raw message middleware.
#[derive(Debug, Clone)]
pub struct RawMessageContext<R: ServiceRole> {
    /// The peer handle for sending responses or notifications
    pub peer: Peer<R>,
    /// Metadata extracted from the message
    pub meta: Meta,
    /// Extensions for custom data
    pub extensions: Extensions,
}

/// Response from raw message middleware
///
/// Middleware can either:
/// - Return `None` to continue processing (pass through)
/// - Return `Some(response)` to short-circuit and send a response immediately
#[derive(Debug)]
pub enum RawMessageResponse<R: ServiceRole> {
    /// Continue processing the message through the normal pipeline
    Continue,
    /// Send this response immediately and skip further processing
    Respond(TxJsonRpcMessage<R>),
}

/// Service for processing raw JSON-RPC messages before parsing
///
/// This trait allows middleware to intercept messages at the protocol level,
/// before they are deserialized into typed requests.
pub trait RawMessageService<R: ServiceRole>: Send + Sync + 'static {
    /// Handle a raw message before it's parsed
    ///
    /// # Returns
    ///
    /// - `Ok(RawMessageResponse::Continue)` - Continue processing
    /// - `Ok(RawMessageResponse::Respond(msg))` - Send response and stop
    /// - `Err(error)` - Return error to client
    fn handle_message(
        &self,
        message: RxJsonRpcMessage<R>,
        context: RawMessageContext<R>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<R>, McpError>>;
}

/// Tower service adapter for raw messages
///
/// This wraps a tower service that processes `(RxJsonRpcMessage, RawMessageContext)`
/// and implements the `RawMessageService` trait.
#[cfg(feature = "tower")]
pub struct RawMessageTowerService<S, R: ServiceRole> {
    service: S,
    _phantom: std::marker::PhantomData<R>,
}

#[cfg(feature = "tower")]
impl<S, R> RawMessageTowerService<S, R>
where
    R: ServiceRole,
{
    /// Create a new tower service adapter
    pub fn new(service: S) -> Self {
        Self {
            service,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(feature = "tower")]
impl<S, R> RawMessageService<R> for RawMessageTowerService<S, R>
where
    S: tower_service::Service<
            (RxJsonRpcMessage<R>, RawMessageContext<R>),
            Response = RawMessageResponse<R>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Error: Into<McpError>,
    S::Future: Send,
    R: ServiceRole,
{
    fn handle_message(
        &self,
        message: RxJsonRpcMessage<R>,
        context: RawMessageContext<R>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<R>, McpError>> {
        use std::future::poll_fn;

        let mut service = self.service.clone();
        Box::pin(async move {
            poll_fn(|cx| service.poll_ready(cx))
                .await
                .map_err(Into::into)?;
            service.call((message, context)).await.map_err(Into::into)
        })
    }
}

/// Passthrough service that does nothing
///
/// This is useful as a base service when building middleware stacks.
#[derive(Clone, Copy, Debug)]
pub struct PassthroughService;

#[cfg(feature = "tower")]
impl<R: ServiceRole> tower_service::Service<(RxJsonRpcMessage<R>, RawMessageContext<R>)>
    for PassthroughService
{
    type Response = RawMessageResponse<R>;
    type Error = McpError;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: (RxJsonRpcMessage<R>, RawMessageContext<R>)) -> Self::Future {
        std::future::ready(Ok(RawMessageResponse::Continue))
    }
}

/// Helper function to extract metadata from a message
pub fn extract_message_meta<R: ServiceRole>(message: &RxJsonRpcMessage<R>) -> Meta {
    use crate::model::GetMeta;

    match message {
        crate::model::JsonRpcMessage::Request(req) => req.request.get_meta().clone(),
        crate::model::JsonRpcMessage::Notification(not) => not.notification.get_meta().clone(),
        crate::model::JsonRpcMessage::Response(_) => Meta::default(),
        crate::model::JsonRpcMessage::Error(_) => Meta::default(),
    }
}

/// Helper function to extract extensions from a message
pub fn extract_message_extensions<R: ServiceRole>(message: &RxJsonRpcMessage<R>) -> Extensions {
    use crate::model::GetExtensions;

    match message {
        crate::model::JsonRpcMessage::Request(req) => req.request.extensions().clone(),
        crate::model::JsonRpcMessage::Notification(not) => not.notification.extensions().clone(),
        crate::model::JsonRpcMessage::Response(_) => Extensions::default(),
        crate::model::JsonRpcMessage::Error(_) => Extensions::default(),
    }
}

/// Helper function to get request ID from a message
pub fn get_message_request_id<R: ServiceRole>(
    message: &RxJsonRpcMessage<R>,
) -> Option<crate::model::RequestId> {
    match message {
        crate::model::JsonRpcMessage::Request(req) => Some(req.id.clone()),
        crate::model::JsonRpcMessage::Response(res) => Some(res.id.clone()),
        crate::model::JsonRpcMessage::Error(err) => Some(err.id.clone()),
        crate::model::JsonRpcMessage::Notification(_) => None,
    }
}
