//! Integration tests for raw message middleware functionality

use futures::future::BoxFuture;
use rmcp::model::*;
use rmcp::service::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Test middleware: Message size validator
struct MessageSizeValidator {
    max_size: usize,
}

impl RawMessageService<RoleClient> for MessageSizeValidator {
    fn handle_message(
        &self,
        message: RxJsonRpcMessage<RoleClient>,
        _context: RawMessageContext<RoleClient>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<RoleClient>, ErrorData>> {
        let max_size = self.max_size;
        Box::pin(async move {
            let size = serde_json::to_string(&message)
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?
                .len();
            
            if size > max_size {
                return Err(ErrorData::invalid_params(
                    format!("Message size {} exceeds maximum {}", size, max_size),
                    None,
                ));
            }
            
            Ok(RawMessageResponse::Continue)
        })
    }
}

// Test middleware: Counting middleware
struct CountingMiddleware {
    count: Arc<AtomicUsize>,
}

impl CountingMiddleware {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

impl RawMessageService<RoleClient> for CountingMiddleware {
    fn handle_message(
        &self,
        _message: RxJsonRpcMessage<RoleClient>,
        _context: RawMessageContext<RoleClient>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<RoleClient>, ErrorData>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(RawMessageResponse::Continue) })
    }
}

// Test middleware: Rejecting middleware
struct RejectingMiddleware;

impl RawMessageService<RoleClient> for RejectingMiddleware {
    fn handle_message(
        &self,
        _message: RxJsonRpcMessage<RoleClient>,
        _context: RawMessageContext<RoleClient>,
    ) -> BoxFuture<'static, Result<RawMessageResponse<RoleClient>, ErrorData>> {
        Box::pin(async move {
            Err(ErrorData::invalid_request(
                "Rejected by middleware",
                None,
            ))
        })
    }
}

#[test]
fn test_message_size_validator_structure() {
    let validator = MessageSizeValidator { max_size: 1000 };
    assert_eq!(validator.max_size, 1000);
}

#[test]
fn test_counting_middleware_structure() {
    let (middleware, count) = CountingMiddleware::new();
    
    // Initially count should be 0
    assert_eq!(count.load(Ordering::SeqCst), 0);
    
    // Verify middleware exists
    drop(middleware);
}

#[test]
fn test_rejecting_middleware_structure() {
    let _middleware = RejectingMiddleware;
    // Just verify it can be constructed
}

#[test]
fn test_service_builder_with_middleware() {
    let info = ClientInfo {
        protocol_version: ProtocolVersion::default(),
        capabilities: ClientCapabilities::default(),
        client_info: Implementation::default(),
        meta: None,
    };
    
    let validator = MessageSizeValidator { max_size: 1_000_000 };
    
    let builder = ServiceBuilder::<RoleClient>::new(info)
        .with_raw_message_middleware(validator);
    
    // Builder should compile and be usable
    drop(builder);
}

#[test]
fn test_passthrough_service() {
    use std::task::Poll;
    use tower_service::Service as TowerService;
    
    let mut service: PassthroughService = PassthroughService;
    
    // Test poll_ready - need to specify type for the service
    let waker = futures::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    
    // Create a dummy message and context for type checking
    type TestInput = (RxJsonRpcMessage<RoleClient>, RawMessageContext<RoleClient>);
    let result: Poll<Result<(), ErrorData>> = <PassthroughService as TowerService<TestInput>>::poll_ready(&mut service, &mut cx);
    assert!(matches!(result, Poll::Ready(Ok(()))));
}
