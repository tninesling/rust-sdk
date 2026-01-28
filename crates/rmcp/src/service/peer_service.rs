//! Peer Service - Tower middleware support for outbound peer requests
//!
//! This module provides tower service wrappers for the `Peer` handle, enabling
//! middleware for outbound peer requests.
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let service = todo!();
//! # let transport = todo!();
//! # let my_middleware = todo!();
//! # let request = todo!();
//! use rmcp::*;
//! use tower::ServiceBuilder;
//!
//! // Create service and get running instance
//! let running = service.serve(transport).await?;
//!
//! // Wrap peer with middleware
//! let layered_peer = running.peer_builder()
//!     .with_layer(|peer_service| {
//!         ServiceBuilder::new()
//!             .layer(my_middleware)
//!             .service(peer_service)
//!     });
//!
//! // Use layered peer
//! let result = layered_peer.send_request(request).await?;
//! # Ok(())
//! # }
//! ```

use std::future::poll_fn;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use tower_service::Service as TowerService;

use crate::service::{Peer, PeerRequestOptions, ServiceError, ServiceRole};

/// A request wrapper for peer service middleware
///
/// This wraps the actual request along with options like timeout and metadata.
#[derive(Debug, Clone)]
pub struct PeerRequest<R: ServiceRole> {
    /// The actual request to send to the peer
    pub request: R::Req,
    /// Options for the request (timeout, metadata, etc.)
    pub options: PeerRequestOptions,
}

impl<R: ServiceRole> PeerRequest<R> {
    /// Create a new peer request with default options
    pub fn new(request: R::Req) -> Self {
        Self {
            request,
            options: PeerRequestOptions::default(),
        }
    }

    /// Create a new peer request with custom options
    pub fn with_options(request: R::Req, options: PeerRequestOptions) -> Self {
        Self { request, options }
    }
}

/// Tower service wrapper for the Peer handle
///
/// This implements `tower_service::Service` for the `Peer` type, enabling
/// middleware for outbound peer requests.
///
/// # Example
///
/// ```rust,no_run
/// # fn example<R: ServiceRole>(peer: Peer<R>) {
/// # let my_middleware = tower::layer::util::Identity::new();
/// use rmcp::service::*;
/// use tower::ServiceBuilder;
///
/// let peer_service = PeerService::new(peer);
/// let layered = ServiceBuilder::new()
///     .layer(my_middleware)
///     .service(peer_service);
/// # }
/// ```
#[derive(Clone)]
pub struct PeerService<R: ServiceRole> {
    peer: Peer<R>,
}

impl<R: ServiceRole> PeerService<R> {
    /// Create a new peer service from a peer handle
    pub fn new(peer: Peer<R>) -> Self {
        Self { peer }
    }

    /// Get a reference to the underlying peer
    pub fn peer(&self) -> &Peer<R> {
        &self.peer
    }
}

impl<R: ServiceRole> TowerService<PeerRequest<R>> for PeerService<R> {
    type Response = R::PeerResp;
    type Error = ServiceError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.peer.is_transport_closed() {
            Poll::Ready(Err(ServiceError::TransportClosed))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: PeerRequest<R>) -> Self::Future {
        let peer = self.peer.clone();
        Box::pin(async move {
            peer.send_request_with_option(req.request, req.options)
                .await?
                .await_response()
                .await
        })
    }
}

/// Builder for creating a peer with middleware
///
/// This builder allows you to wrap a `Peer` handle with tower middleware,
/// enabling cross-cutting concerns to be applied to outbound peer requests.
///
/// # Example
///
/// ```rust,no_run
/// # fn example<R: ServiceRole>(peer: Peer<R>) {
/// # let my_middleware = tower::layer::util::Identity::new();
/// use rmcp::service::*;
/// use tower::ServiceBuilder;
///
/// let peer_builder = PeerBuilder::new(peer);
/// let layered_peer = peer_builder
///     .with_layer(|peer_service| {
///         ServiceBuilder::new()
///             .layer(my_middleware)
///             .service(peer_service)
///     });
/// # }
/// ```
pub struct PeerBuilder<R: ServiceRole> {
    peer: Peer<R>,
}

impl<R: ServiceRole> PeerBuilder<R> {
    /// Create a new peer builder
    pub fn new(peer: Peer<R>) -> Self {
        Self { peer }
    }

    /// Wrap the peer with middleware using a tower service
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tower::ServiceBuilder;
    ///
    /// let peer_service = PeerService::new(peer.clone());
    /// let rate_limited = ServiceBuilder::new()
    ///     .rate_limit(10, Duration::from_secs(1))
    ///     .service(peer_service);
    ///     
    /// let layered_peer = peer_builder.with_middleware(rate_limited);
    /// ```
    pub fn with_middleware<S>(self, service: S) -> LayeredPeer<S, R>
    where
        S: TowerService<PeerRequest<R>, Response = R::PeerResp, Error = ServiceError>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send,
    {
        LayeredPeer {
            service,
            peer: self.peer,
        }
    }

    /// Wrap the peer with middleware using a function that transforms the PeerService
    ///
    /// This is a convenience method that creates a PeerService and applies a transformation
    /// function to it, which is useful for applying tower layers.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tower::ServiceBuilder;
    ///
    /// let layered_peer = peer_builder.with_layer(|peer_service| {
    ///     ServiceBuilder::new()
    ///         .rate_limit(10, Duration::from_secs(1))
    ///         .service(peer_service)
    /// });
    /// ```
    pub fn with_layer<F, S>(self, f: F) -> LayeredPeer<S, R>
    where
        F: FnOnce(PeerService<R>) -> S,
        S: TowerService<PeerRequest<R>, Response = R::PeerResp, Error = ServiceError>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send,
    {
        let peer_service = PeerService::new(self.peer.clone());
        let service = f(peer_service);
        LayeredPeer {
            service,
            peer: self.peer,
        }
    }
}

/// A peer with middleware applied
///
/// This wraps a tower service that processes peer requests, while also
/// maintaining access to the underlying peer for operations that should
/// bypass middleware (such as notifications).
///
/// # Example
///
/// ```rust,no_run
/// # async fn example<S, R>(layered_peer: LayeredPeer<S, R>) -> Result<(), Box<dyn std::error::Error>>
/// # where
/// #     S: tower_service::Service<PeerRequest<R>, Response = R::Resp, Error = ServiceError> + Clone,
/// #     R: ServiceRole,
/// # {
/// # let request = todo!();
/// # let notification = todo!();
/// use rmcp::service::*;
///
/// // Send request through middleware
/// let result = layered_peer.send_request(request).await?;
///
/// // Send notification directly (bypasses middleware)
/// layered_peer.peer().send_notification(notification).await?;
/// # Ok(())
/// # }
/// ```
pub struct LayeredPeer<S, R: ServiceRole> {
    service: S,
    peer: Peer<R>,
}

impl<S, R> LayeredPeer<S, R>
where
    S: TowerService<PeerRequest<R>, Response = R::PeerResp, Error = ServiceError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    R: ServiceRole,
{
    /// Send a request through the middleware stack
    ///
    /// This will apply all configured middleware (rate limiting, circuit breaking,
    /// retries, etc.) before sending the request to the peer.
    pub async fn send_request(&self, request: R::Req) -> Result<R::PeerResp, ServiceError> {
        self.send_request_with_options(request, PeerRequestOptions::default())
            .await
    }

    /// Send a request with custom options through the middleware stack
    pub async fn send_request_with_options(
        &self,
        request: R::Req,
        options: PeerRequestOptions,
    ) -> Result<R::PeerResp, ServiceError> {
        let mut service = self.service.clone();
        poll_fn(|cx| service.poll_ready(cx)).await?;
        service.call(PeerRequest { request, options }).await
    }

    /// Get a reference to the underlying peer
    ///
    /// Use this for operations that should bypass middleware.
    pub fn peer(&self) -> &Peer<R> {
        &self.peer
    }

    /// Get a mutable reference to the underlying service
    ///
    /// This is useful for advanced use cases where you need to interact
    /// with the middleware directly.
    pub fn service_mut(&mut self) -> &mut S {
        &mut self.service
    }
}

impl<S, R> Clone for LayeredPeer<S, R>
where
    S: Clone,
    R: ServiceRole,
{
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            peer: self.peer.clone(),
        }
    }
}

/// Extension trait for `Peer` to easily create a builder
pub trait PeerExt<R: ServiceRole>: Sized {
    /// Create a builder for adding middleware to this peer
    fn builder(self) -> PeerBuilder<R>;
}

impl<R: ServiceRole> PeerExt<R> for Peer<R> {
    fn builder(self) -> PeerBuilder<R> {
        PeerBuilder::new(self)
    }
}
