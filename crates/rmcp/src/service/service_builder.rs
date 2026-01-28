//! Service Builder - Composable service construction with middleware
//!
//! This module provides a builder pattern for constructing services with
//! middleware at multiple layers (raw message, post-parse, etc.).

use std::marker::PhantomData;
use std::sync::Arc;

use crate::service::{RawMessageService, Service, ServiceRole};

/// Builder for constructing services with middleware
///
/// This builder allows you to compose services with middleware at different layers:
/// - Raw message layer (before JSON-RPC parsing)
/// - Post-parse layer (after JSON-RPC parsing)
///
/// # Example
///
/// ```rust,no_run
/// # fn example<R: ServiceRole, M: RawMessageService<R>, S: Service<R>>(
/// #     server_info: R::Info,
/// #     my_middleware: M,
/// #     my_service: S,
/// # ) {
/// use rmcp::service::*;
///
/// let service = ServiceBuilder::new(server_info)
///     .with_raw_message_middleware(my_middleware)
///     .build(my_service);
/// # }
/// ```
pub struct ServiceBuilder<R: ServiceRole> {
    info: R::Info,
    raw_message_middleware: Vec<Arc<dyn RawMessageService<R>>>,
    _phantom: PhantomData<R>,
}

impl<R: ServiceRole> ServiceBuilder<R> {
    /// Create a new service builder with the given info
    pub fn new(info: R::Info) -> Self {
        Self {
            info,
            raw_message_middleware: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Add raw message middleware
    ///
    /// This middleware will be applied before JSON-RPC parsing, allowing
    /// protocol-level validation and filtering.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # fn example<R: ServiceRole, M: RawMessageService<R>>(
    /// #     server_info: R::Info,
    /// #     my_middleware: M,
    /// # ) {
    /// use rmcp::service::*;
    ///
    /// let builder = ServiceBuilder::new(server_info)
    ///     .with_raw_message_middleware(my_middleware);
    /// # }
    /// ```
    pub fn with_raw_message_middleware<M>(mut self, middleware: M) -> Self
    where
        M: RawMessageService<R>,
    {
        self.raw_message_middleware.push(Arc::new(middleware));
        self
    }

    /// Build the service with the configured middleware
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # fn example<R: ServiceRole, M: RawMessageService<R>, S: Service<R>>(
    /// #     server_info: R::Info,
    /// #     my_middleware: M,
    /// #     my_service: S,
    /// # ) {
    /// use rmcp::service::*;
    ///
    /// let service = ServiceBuilder::new(server_info)
    ///     .with_raw_message_middleware(my_middleware)
    ///     .build(my_service);
    /// # }
    /// ```
    pub fn build<S>(self, service: S) -> LayeredService<S, R>
    where
        S: Service<R>,
    {
        LayeredService {
            service,
            raw_message_middleware: self.raw_message_middleware,
            info: self.info,
        }
    }

    /// Convenience method to build with a service (same as `build`)
    pub fn with_service<S>(self, service: S) -> LayeredService<S, R>
    where
        S: Service<R>,
    {
        self.build(service)
    }

    /// Build with a tower service that has full context access
    ///
    /// This creates a `TowerServiceHandler` that wraps the tower service
    /// and provides access to the full request context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # fn example<R: ServiceRole, M: RawMessageService<R>, S>(
    /// #     server_info: R::Info,
    /// #     my_middleware: M,
    /// #     my_tower_service: S,
    /// # )
    /// # where
    /// #     S: tower_service::Service<McpRequest<R>, Response = R::Resp> + Sync + Send + Clone + 'static,
    /// #     S::Error: Into<crate::ErrorData>,
    /// #     S::Future: Send,
    /// # {
    /// use rmcp::service::*;
    ///
    /// let service = ServiceBuilder::new(server_info)
    ///     .with_raw_message_middleware(my_middleware)
    ///     .with_tower_service(my_tower_service);
    /// # }
    /// ```
    #[cfg(feature = "tower")]
    pub fn with_tower_service<S>(
        self,
        service: S,
    ) -> LayeredService<crate::service::TowerServiceHandler<S, R>, R>
    where
        S: tower_service::Service<crate::service::McpRequest<R>, Response = R::Resp>
            + Sync
            + Send
            + Clone
            + 'static,
        S::Error: Into<crate::ErrorData>,
        S::Future: Send,
    {
        let tower_handler = crate::service::TowerServiceHandler::new(service, self.info.clone());
        LayeredService {
            service: tower_handler,
            raw_message_middleware: self.raw_message_middleware,
            info: self.info,
        }
    }
}

/// Service with raw message middleware applied
///
/// This wraps a service and applies raw message middleware before
/// the service's request handler is called.
pub struct LayeredService<S, R: ServiceRole> {
    service: S,
    raw_message_middleware: Vec<Arc<dyn RawMessageService<R>>>,
    info: R::Info,
}

impl<S, R> Service<R> for LayeredService<S, R>
where
    S: Service<R>,
    R: ServiceRole,
{
    async fn handle_request(
        &self,
        request: R::PeerReq,
        context: crate::service::RequestContext<R>,
    ) -> Result<R::Resp, crate::ErrorData> {
        self.service.handle_request(request, context).await
    }

    async fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: crate::service::NotificationContext<R>,
    ) -> Result<(), crate::ErrorData> {
        self.service
            .handle_notification(notification, context)
            .await
    }

    fn get_info(&self) -> R::Info {
        self.info.clone()
    }
}

impl<S, R> LayeredService<S, R>
where
    S: Service<R>,
    R: ServiceRole,
{
    /// Get the raw message middleware stack
    ///
    /// This is used internally by the service loop to apply middleware
    /// before parsing messages.
    pub fn raw_message_middleware(&self) -> &[Arc<dyn RawMessageService<R>>] {
        &self.raw_message_middleware
    }

    /// Get a reference to the underlying service
    pub fn inner(&self) -> &S {
        &self.service
    }
}

/// Helper trait to check if a service has raw message middleware
pub trait HasRawMessageMiddleware<R: ServiceRole> {
    fn get_raw_message_middleware(&self) -> Option<&[Arc<dyn RawMessageService<R>>]>;
}

impl<S, R> HasRawMessageMiddleware<R> for LayeredService<S, R>
where
    S: Service<R>,
    R: ServiceRole,
{
    fn get_raw_message_middleware(&self) -> Option<&[Arc<dyn RawMessageService<R>>]> {
        if self.raw_message_middleware.is_empty() {
            None
        } else {
            Some(&self.raw_message_middleware)
        }
    }
}

impl<R: ServiceRole> HasRawMessageMiddleware<R> for Box<dyn crate::service::DynService<R>> {
    fn get_raw_message_middleware(&self) -> Option<&[Arc<dyn RawMessageService<R>>]> {
        None
    }
}

