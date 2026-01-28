use std::{future::poll_fn, marker::PhantomData};

use futures::future::BoxFuture;
use tower_service::Service as TowerService;

use super::NotificationContext;
use crate::service::{RequestContext, Service, ServiceRole};

/// Request with full context for tower middleware
///
/// This wraps a request along with its execution context, providing access
/// to metadata, extensions, the peer handle, and cancellation token.
#[derive(Debug, Clone)]
pub struct McpRequest<R: ServiceRole> {
    /// The actual request
    pub request: R::PeerReq,
    /// The request context with metadata, peer, etc.
    pub context: RequestContext<R>,
}

/// Notification with full context for tower middleware
///
/// This wraps a notification along with its execution context.
#[derive(Debug, Clone)]
pub struct McpNotification<R: ServiceRole> {
    /// The actual notification
    pub notification: R::PeerNot,
    /// The notification context with metadata, peer, etc.
    pub context: NotificationContext<R>,
}

/// Legacy tower handler (deprecated, use TowerServiceHandler instead)
///
/// This handler only processes requests and ignores the context.
/// For new code, use `TowerServiceHandler` which provides full context access.
#[deprecated(
    since = "0.15.0",
    note = "Use TowerServiceHandler for full context access"
)]
pub struct TowerHandler<S, R: ServiceRole> {
    pub service: S,
    pub info: R::Info,
    role: PhantomData<R>,
}

#[allow(deprecated)]
impl<S, R: ServiceRole> TowerHandler<S, R> {
    pub fn new(service: S, info: R::Info) -> Self {
        Self {
            service,
            role: PhantomData,
            info,
        }
    }
}

#[allow(deprecated)]
impl<S, R: ServiceRole> Service<R> for TowerHandler<S, R>
where
    S: TowerService<R::PeerReq, Response = R::Resp> + Sync + Send + Clone + 'static,
    S::Error: Into<crate::ErrorData>,
    S::Future: Send,
{
    async fn handle_request(
        &self,
        request: R::PeerReq,
        _context: RequestContext<R>,
    ) -> Result<R::Resp, crate::ErrorData> {
        let mut service = self.service.clone();
        poll_fn(|cx| service.poll_ready(cx))
            .await
            .map_err(Into::into)?;
        let resp = service.call(request).await.map_err(Into::into)?;
        Ok(resp)
    }

    fn handle_notification(
        &self,
        _notification: R::PeerNot,
        _context: NotificationContext<R>,
    ) -> impl Future<Output = Result<(), crate::ErrorData>> + Send + '_ {
        std::future::ready(Ok(()))
    }

    fn get_info(&self) -> R::Info {
        self.info.clone()
    }
}

/// Enhanced tower service handler with full context access
///
/// This handler provides access to the full request context through the
/// `McpRequest` wrapper, enabling middleware to access metadata, extensions,
/// the peer handle, and cancellation token.
///
/// # Example
///
/// ```rust,no_run
/// # async fn example<R: ServiceRole, S>(
/// #     my_tower_service: S,
/// #     server_info: R::Info,
/// #     transport: impl crate::transport::Transport<R>,
/// # ) -> Result<(), Box<dyn std::error::Error>>
/// # where
/// #     S: tower_service::Service<McpRequest<R>, Response = R::Resp> + Sync + Send + Clone + 'static,
/// #     S::Error: Into<crate::ErrorData>,
/// #     S::Future: Send,
/// # {
/// use rmcp::service::*;
///
/// let service = TowerServiceHandler::new(my_tower_service, server_info);
/// let running = service.serve(transport).await?;
/// # Ok(())
/// # }
/// ```
pub struct TowerServiceHandler<S, R: ServiceRole> {
    service: S,
    info: R::Info,
    _phantom: PhantomData<R>,
}

impl<S, R: ServiceRole> TowerServiceHandler<S, R> {
    /// Create a new tower service handler
    pub fn new(service: S, info: R::Info) -> Self {
        Self {
            service,
            info,
            _phantom: PhantomData,
        }
    }
}

impl<S, R: ServiceRole> Service<R> for TowerServiceHandler<S, R>
where
    S: TowerService<McpRequest<R>, Response = R::Resp> + Sync + Send + Clone + 'static,
    S::Error: Into<crate::ErrorData>,
    S::Future: Send,
{
    async fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> Result<R::Resp, crate::ErrorData> {
        let mut service = self.service.clone();
        poll_fn(|cx| service.poll_ready(cx))
            .await
            .map_err(Into::into)?;
        
        let mcp_request = McpRequest { request, context };
        let resp = service.call(mcp_request).await.map_err(Into::into)?;
        Ok(resp)
    }

    fn handle_notification(
        &self,
        _notification: R::PeerNot,
        _context: NotificationContext<R>,
    ) -> impl Future<Output = Result<(), crate::ErrorData>> + Send + '_ {
        std::future::ready(Ok(()))
    }

    fn get_info(&self) -> R::Info {
        self.info.clone()
    }
}

/// Notification handler trait for tower middleware
///
/// This allows middleware to process notifications with full context access.
pub trait NotificationHandler<R: ServiceRole>: Send + Sync + 'static {
    /// Handle a notification
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> BoxFuture<'static, Result<(), crate::ErrorData>>;
}

/// Tower service adapter for notifications
pub struct NotificationTowerService<S, R: ServiceRole> {
    service: S,
    _phantom: PhantomData<R>,
}

impl<S, R> NotificationTowerService<S, R>
where
    R: ServiceRole,
{
    pub fn new(service: S) -> Self {
        Self {
            service,
            _phantom: PhantomData,
        }
    }
}

impl<S, R> NotificationHandler<R> for NotificationTowerService<S, R>
where
    S: TowerService<McpNotification<R>, Response = ()> + Clone + Send + Sync + 'static,
    S::Error: Into<crate::ErrorData>,
    S::Future: Send,
    R: ServiceRole,
{
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> BoxFuture<'static, Result<(), crate::ErrorData>> {
        let mut service = self.service.clone();
        Box::pin(async move {
            poll_fn(|cx| service.poll_ready(cx))
                .await
                .map_err(Into::into)?;
            
            let mcp_notification = McpNotification {
                notification,
                context,
            };
            service.call(mcp_notification).await.map_err(Into::into)?;
            Ok(())
        })
    }
}
