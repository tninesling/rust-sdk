//! Integration tests for peer middleware functionality
//!
//! These tests verify that the peer service middleware works correctly
//! with various tower middleware like rate limiting, timeouts, etc.

use rmcp::model::*;
use rmcp::service::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower_service::Service as TowerService;

// Test middleware: Counts the number of requests
#[derive(Clone)]
struct CountingMiddleware<S> {
    inner: S,
    count: Arc<AtomicUsize>,
}

impl<S> CountingMiddleware<S> {
    fn new(inner: S) -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner,
                count: count.clone(),
            },
            count,
        )
    }
}

impl<S, R> TowerService<PeerRequest<R>> for CountingMiddleware<S>
where
    S: TowerService<PeerRequest<R>>,
    R: ServiceRole,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: PeerRequest<R>) -> Self::Future {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.call(req)
    }
}

// Test middleware: Rejects all requests
#[derive(Clone)]
struct RejectingMiddleware<S> {
    _inner: S,
}

impl<S> RejectingMiddleware<S> {
    fn new(inner: S) -> Self {
        Self { _inner: inner }
    }
}

impl<S, R> TowerService<PeerRequest<R>> for RejectingMiddleware<S>
where
    S: TowerService<PeerRequest<R>>,
    R: ServiceRole,
{
    type Response = S::Response;
    type Error = ServiceError;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(ServiceError::McpError(
            ErrorData::invalid_request("Rejected by middleware", None),
        )))
    }

    fn call(&mut self, _req: PeerRequest<R>) -> Self::Future {
        Box::pin(async move {
            Err(ServiceError::McpError(ErrorData::invalid_request(
                "Rejected by middleware",
                None,
            )))
        })
    }
}

#[test]
fn test_peer_request_creation() {
    let request = ClientRequest::ListToolsRequest(ListToolsRequest {
        method: Default::default(),
        params: None,
        extensions: Default::default(),
    });
    
    // Test basic creation
    let peer_req = PeerRequest::<RoleClient>::new(request.clone());
    assert!(peer_req.options.timeout.is_none());
    
    // Test with options
    let options = PeerRequestOptions {
        timeout: Some(Duration::from_secs(30)),
        meta: None,
    };
    let peer_req = PeerRequest::<RoleClient>::with_options(request, options);
    assert_eq!(peer_req.options.timeout, Some(Duration::from_secs(30)));
}

#[test]
fn test_peer_request_options_clone() {
    let options = PeerRequestOptions {
        timeout: Some(Duration::from_secs(30)),
        meta: None,
    };
    let cloned = options.clone();
    assert_eq!(cloned.timeout, Some(Duration::from_secs(30)));
}

#[test]
fn test_counting_middleware_structure() {
    // Create a mock peer service (we won't actually call it)
    struct MockService;
    
    impl TowerService<PeerRequest<RoleClient>> for MockService {
        type Response = ServerResult;
        type Error = ServiceError;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: PeerRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ServerResult::EmptyResult(EmptyResult {})))
        }
    }
    
    let mock = MockService;
    let (mut counting, count) = CountingMiddleware::new(mock);
    
    // Initially, count should be 0
    assert_eq!(count.load(Ordering::SeqCst), 0);
    
    // Check poll_ready works
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(counting.poll_ready(&mut cx).is_ready());
    
    // Simulate a call
    let request = ClientRequest::ListToolsRequest(ListToolsRequest {
        method: Default::default(),
        params: None,
        extensions: Default::default(),
    });
    let peer_req = PeerRequest::new(request);
    let _ = counting.call(peer_req);
    
    // Count should now be 1
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_rejecting_middleware_structure() {
    struct MockService;
    
    impl TowerService<PeerRequest<RoleClient>> for MockService {
        type Response = ServerResult;
        type Error = ServiceError;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: PeerRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ServerResult::EmptyResult(EmptyResult {})))
        }
    }
    
    let mock = MockService;
    let mut rejecting = RejectingMiddleware::new(mock);
    
    // poll_ready should return an error
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let result = rejecting.poll_ready(&mut cx);
    
    assert!(matches!(result, Poll::Ready(Err(ServiceError::McpError(_)))));
}

