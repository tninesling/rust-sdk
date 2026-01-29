//! Tower Service Adapter for RMCP Services
//!
//! This module provides adapters that allow existing `rmcp::Service` implementations
//! to work as `tower::Service`s, enabling backward compatibility while transitioning
//! to a Tower-native API.
//!
//! ## Converting between service types
//!
//! - **`ServiceAdapter`**: Wraps a `tower::Service<McpMessage>` to implement `rmcp::Service`
//! - **`TowerServiceAdapter`**: Wraps an `rmcp::Service` to implement `tower::Service<McpMessage>`
//!
//! The `TowerServiceAdapter` is particularly useful for using existing `ServerHandler`
//! implementations with the new `McpServer` API and `tower::ServiceBuilder`.

use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tower_service::Service as TowerService;

use crate::service::{McpMessage, McpOutput, NotificationContext, RequestContext, Service, ServiceRole};

/// Adapter that wraps a `tower::Service` to make it work with the existing `rmcp::Service` loop
///
/// This bridges Tower services with the existing service infrastructure, allowing
/// Tower services to be used with `serve_directly` and related functions.
///
/// Note: Tower services require `&mut self` for `call()`, so we use `Arc<tokio::sync::Mutex<S>>`
/// to provide interior mutability in async contexts.
#[derive(Clone)]
pub struct ServiceAdapter<S, R: ServiceRole> {
    inner: Arc<tokio::sync::Mutex<S>>,
    info: R::Info,
}

impl<S, R: ServiceRole> ServiceAdapter<S, R> {
    /// Create a new adapter wrapping the given tower service
    pub fn new(service: S, info: R::Info) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(service)),
            info,
        }
    }

    /// Consume the adapter and return the inner service
    pub async fn into_inner(self) -> S {
        Arc::try_unwrap(self.inner)
            .unwrap_or_else(|_arc| {
                // If there are multiple references, we can't unwrap
                // This shouldn't happen in normal usage
                panic!("Cannot unwrap ServiceAdapter: multiple references exist")
            })
            .into_inner()
    }
}

/// Helper function to adapt a `tower::Service` to work with `rmcp::Service` infrastructure
pub fn adapt_tower_service<S, R>(service: S, info: R::Info) -> ServiceAdapter<S, R>
where
    S: TowerService<McpMessage<R>, Response = McpOutput<R>>,
    R: ServiceRole,
{
    ServiceAdapter::new(service, info)
}

// Explicit implementation for RoleServer to help with trait resolution
#[cfg(feature = "server")]
impl<S> crate::service::Service<crate::service::RoleServer> for ServiceAdapter<S, crate::service::RoleServer>
where
    S: TowerService<McpMessage<crate::service::RoleServer>, Response = McpOutput<crate::service::RoleServer>> + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
{
    async fn handle_request(
        &self,
        request: <crate::service::RoleServer as ServiceRole>::PeerReq,
        context: RequestContext<crate::service::RoleServer>,
    ) -> Result<<crate::service::RoleServer as ServiceRole>::Resp, crate::error::ErrorData> {
        let id = context.id.clone();
        
        let msg = McpMessage::Request {
            id: id.clone(),
            request,
            context,
        };

        let future = {
            let mut svc = self.inner.lock().await;
            svc.call(msg)
        };
        
        let output = future
            .await
            .map_err(|e| crate::error::ErrorData::internal_error(e.to_string(), None))?;

        match output {
            McpOutput::Response { id: response_id, result } => {
                if id != response_id {
                    return Err(crate::error::ErrorData::internal_error(
                        format!("Request ID mismatch: {} != {}", id, response_id),
                        None,
                    ));
                }
                result
            }
            McpOutput::Ack => {
                Err(crate::error::ErrorData::internal_error(
                    "Received Ack for a request".to_string(),
                    None,
                ))
            }
        }
    }

    async fn handle_notification(
        &self,
        notification: <crate::service::RoleServer as ServiceRole>::PeerNot,
        context: NotificationContext<crate::service::RoleServer>,
    ) -> Result<(), crate::error::ErrorData> {
        let msg = McpMessage::Notification {
            notification,
            context,
        };

        let future = {
            let mut svc = self.inner.lock().await;
            svc.call(msg)
        };
        
        let output = future
            .await
            .map_err(|e| crate::error::ErrorData::internal_error(e.to_string(), None))?;

        match output {
            McpOutput::Ack => Ok(()),
            McpOutput::Response { .. } => {
                Err(crate::error::ErrorData::internal_error(
                    "Received Response for a notification".to_string(),
                    None,
                ))
            }
        }
    }

    fn get_info(&self) -> <crate::service::RoleServer as ServiceRole>::Info {
        self.info.clone()
    }
}

// Explicit implementation for RoleClient
#[cfg(feature = "client")]
impl<S> crate::service::Service<crate::service::RoleClient> for ServiceAdapter<S, crate::service::RoleClient>
where
    S: TowerService<McpMessage<crate::service::RoleClient>, Response = McpOutput<crate::service::RoleClient>> + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
{
    async fn handle_request(
        &self,
        request: <crate::service::RoleClient as ServiceRole>::PeerReq,
        context: RequestContext<crate::service::RoleClient>,
    ) -> Result<<crate::service::RoleClient as ServiceRole>::Resp, crate::error::ErrorData> {
        let id = context.id.clone();
        
        let msg = McpMessage::Request {
            id: id.clone(),
            request,
            context,
        };

        let future = {
            let mut svc = self.inner.lock().await;
            svc.call(msg)
        };
        
        let output = future
            .await
            .map_err(|e| crate::error::ErrorData::internal_error(e.to_string(), None))?;

        match output {
            McpOutput::Response { id: response_id, result } => {
                if id != response_id {
                    return Err(crate::error::ErrorData::internal_error(
                        format!("Request ID mismatch: {} != {}", id, response_id),
                        None,
                    ));
                }
                result
            }
            McpOutput::Ack => {
                Err(crate::error::ErrorData::internal_error(
                    "Received Ack for a request".to_string(),
                    None,
                ))
            }
        }
    }

    async fn handle_notification(
        &self,
        notification: <crate::service::RoleClient as ServiceRole>::PeerNot,
        context: NotificationContext<crate::service::RoleClient>,
    ) -> Result<(), crate::error::ErrorData> {
        let msg = McpMessage::Notification {
            notification,
            context,
        };

        let future = {
            let mut svc = self.inner.lock().await;
            svc.call(msg)
        };
        
        let output = future
            .await
            .map_err(|e| crate::error::ErrorData::internal_error(e.to_string(), None))?;

        match output {
            McpOutput::Ack => Ok(()),
            McpOutput::Response { .. } => {
                Err(crate::error::ErrorData::internal_error(
                    "Received Response for a notification".to_string(),
                    None,
                ))
            }
        }
    }

    fn get_info(&self) -> <crate::service::RoleClient as ServiceRole>::Info {
        self.info.clone()
    }
}

/// Helper trait to ensure ServiceAdapter implements Service
///
/// This is a workaround to help the compiler verify trait implementations
/// in complex generic contexts.
pub trait EnsureServiceAdapter<R: ServiceRole> {
    fn ensure_service_impl();
}

// =============================================================================
// TowerServiceAdapter: Convert rmcp::Service to tower::Service
// =============================================================================

/// Adapter that wraps an `rmcp::Service` to implement `tower::Service<McpMessage>`
///
/// This enables using existing `ServerHandler` implementations with the new
/// Tower-based `McpServer` API and `tower::ServiceBuilder` middleware.
///
/// # Example
///
/// ```rust,no_run
/// use rmcp::{ServerHandler, McpServer};
/// use rmcp::service::TowerServiceAdapter;
/// use rmcp::transport::StdioTransport;
/// use tower::ServiceBuilder;
///
/// #[derive(Clone)]
/// struct MyHandler;
///
/// impl ServerHandler for MyHandler {
///     // ... implement handler methods
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let handler = MyHandler;
///
/// // Wrap the handler to use with Tower middleware
/// let service = ServiceBuilder::new()
///     // .layer(MyMiddleware::new())
///     .service(TowerServiceAdapter::new(handler));
///
/// // Use with McpServer
/// let server = McpServer::new(rmcp::model::ServerInfo::default())
///     .serve(StdioTransport, service)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TowerServiceAdapter<S> {
    inner: S,
}

impl<S> TowerServiceAdapter<S> {
    /// Create a new adapter wrapping the given rmcp service
    pub fn new(service: S) -> Self {
        Self { inner: service }
    }

    /// Get a reference to the inner service
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner service
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consume the adapter and return the inner service
    pub fn into_inner(self) -> S {
        self.inner
    }
}

// Implement tower::Service for TowerServiceAdapter wrapping Service<RoleServer>
#[cfg(feature = "server")]
impl<S> TowerService<McpMessage<crate::service::RoleServer>> for TowerServiceAdapter<S>
where
    S: Service<crate::service::RoleServer> + Clone + Send + Sync + 'static,
{
    type Response = McpOutput<crate::service::RoleServer>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, msg: McpMessage<crate::service::RoleServer>) -> Self::Future {
        let service = self.inner.clone();
        Box::pin(async move {
            match msg {
                McpMessage::Request { id, request, context } => {
                    let result = service.handle_request(request, context).await;
                    Ok(McpOutput::Response { id, result })
                }
                McpMessage::Notification { notification, context } => {
                    let _ = service.handle_notification(notification, context).await;
                    Ok(McpOutput::Ack)
                }
            }
        })
    }
}

// Implement tower::Service for TowerServiceAdapter wrapping Service<RoleClient>
#[cfg(feature = "client")]
impl<S> TowerService<McpMessage<crate::service::RoleClient>> for TowerServiceAdapter<S>
where
    S: Service<crate::service::RoleClient> + Clone + Send + Sync + 'static,
{
    type Response = McpOutput<crate::service::RoleClient>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, msg: McpMessage<crate::service::RoleClient>) -> Self::Future {
        let service = self.inner.clone();
        Box::pin(async move {
            match msg {
                McpMessage::Request { id, request, context } => {
                    let result = service.handle_request(request, context).await;
                    Ok(McpOutput::Response { id, result })
                }
                McpMessage::Notification { notification, context } => {
                    let _ = service.handle_notification(notification, context).await;
                    Ok(McpOutput::Ack)
                }
            }
        })
    }
}

/// Extension trait for converting `rmcp::Service` implementations to Tower services
///
/// This provides a convenient method for wrapping services.
///
/// Due to Rust's orphan rules, we cannot provide a blanket impl of
/// `tower::Service<McpMessage<R>>` for all `S: Service<R>`. The orphan rule
/// requires that uncovered type parameters not appear before the first local
/// type. In `impl<S> ForeignTrait<LocalType> for S`, `S` appears "before"
/// the local type in the trait's perspective, violating the rule.
///
/// The wrapper type `TowerServiceAdapter<S>` "covers" `S` with a local type,
/// making the impl valid.
///
/// # Example
///
/// ```rust,no_run
/// use rmcp::ServerHandler;
/// use rmcp::service::IntoTowerService;
///
/// #[derive(Clone)]
/// struct MyHandler;
/// impl ServerHandler for MyHandler {}
///
/// let tower_service = MyHandler.into_tower_service();
/// ```
pub trait IntoTowerService<R: ServiceRole>: Service<R> + Sized {
    /// Convert this service into a Tower-compatible service
    fn into_tower_service(self) -> TowerServiceAdapter<Self> {
        TowerServiceAdapter::new(self)
    }
}

// Blanket implementation for all Service<R>
impl<S, R> IntoTowerService<R> for S
where
    S: Service<R> + Sized,
    R: ServiceRole,
{
}
