//! Integration tests for post-parse (typed request) middleware functionality

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use rmcp::{model::*, service::*};
use tower_service::Service as TowerService;

// Test middleware: Request counter with context access
#[derive(Clone)]
struct RequestCounterMiddleware<S> {
    inner: S,
    count: Arc<AtomicUsize>,
}

impl<S> RequestCounterMiddleware<S> {
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

impl<S> TowerService<McpRequest<RoleClient>> for RequestCounterMiddleware<S>
where
    S: TowerService<McpRequest<RoleClient>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: McpRequest<RoleClient>) -> Self::Future {
        self.count.fetch_add(1, Ordering::SeqCst);
        // Can access context here: req.context.peer, req.context.meta, etc.
        self.inner.call(req)
    }
}

// Test middleware: Context validator
#[derive(Clone)]
struct ContextValidator<S> {
    inner: S,
}

impl<S> ContextValidator<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> TowerService<McpRequest<RoleClient>> for ContextValidator<S>
where
    S: TowerService<McpRequest<RoleClient>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: McpRequest<RoleClient>) -> Self::Future {
        // Can validate context here
        // e.g., check req.context.extensions for auth tokens
        self.inner.call(req)
    }
}

#[test]
fn test_request_counter_middleware_structure() {
    // Mock service
    struct MockService;

    impl TowerService<McpRequest<RoleClient>> for MockService {
        type Response = ClientResult;
        type Error = ErrorData;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: McpRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ClientResult::EmptyResult(EmptyResult {})))
        }
    }

    let mock = MockService;
    let (mut counter, count) = RequestCounterMiddleware::new(mock);

    // Initially count should be 0
    assert_eq!(count.load(Ordering::SeqCst), 0);

    // Check poll_ready works
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(counter.poll_ready(&mut cx).is_ready());
}

#[test]
fn test_context_validator_structure() {
    struct MockService;

    impl TowerService<McpRequest<RoleClient>> for MockService {
        type Response = ClientResult;
        type Error = ErrorData;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: McpRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ClientResult::EmptyResult(EmptyResult {})))
        }
    }

    let mock = MockService;
    let mut validator = ContextValidator::new(mock);

    // Check poll_ready works
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(validator.poll_ready(&mut cx).is_ready());
}

#[test]
fn test_service_builder_with_tower_service() {
    // Mock tower service
    #[derive(Clone)]
    struct MockTowerService;

    impl TowerService<McpRequest<RoleClient>> for MockTowerService {
        type Response = ClientResult;
        type Error = ErrorData;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: McpRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ClientResult::EmptyResult(EmptyResult {})))
        }
    }

    let info = ClientInfo {
        protocol_version: ProtocolVersion::default(),
        capabilities: ClientCapabilities::default(),
        client_info: Implementation::default(),
        meta: None,
    };

    let _service = ServiceBuilder::<RoleClient>::new(info).with_tower_service(MockTowerService);
}

#[test]
fn test_combined_middleware() {
    // Mock tower service
    #[derive(Clone)]
    struct MockTowerService;

    impl TowerService<McpRequest<RoleClient>> for MockTowerService {
        type Response = ClientResult;
        type Error = ErrorData;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: McpRequest<RoleClient>) -> Self::Future {
            futures::future::ready(Ok(ClientResult::EmptyResult(EmptyResult {})))
        }
    }

    // Raw message middleware
    struct DummyRawMiddleware;

    impl RawMessageService<RoleClient> for DummyRawMiddleware {
        fn handle_message(
            &self,
            _message: RxJsonRpcMessage<RoleClient>,
            _context: RawMessageContext<RoleClient>,
        ) -> futures::future::BoxFuture<'static, Result<RawMessageResponse<RoleClient>, ErrorData>>
        {
            Box::pin(async move { Ok(RawMessageResponse::Continue) })
        }
    }

    let info = ClientInfo {
        protocol_version: ProtocolVersion::default(),
        capabilities: ClientCapabilities::default(),
        client_info: Implementation::default(),
        meta: None,
    };

    // Combine raw message middleware (Layer 2) with tower service (Layer 3)
    let _service = ServiceBuilder::<RoleClient>::new(info)
        .with_raw_message_middleware(DummyRawMiddleware)
        .with_tower_service(MockTowerService);
}
