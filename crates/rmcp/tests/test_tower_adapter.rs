//! Test Tower adapters for converting between rmcp::Service and tower::Service

#[cfg(all(feature = "tower", feature = "server"))]
mod tests {
    use rmcp::service::{
        DynService, IntoTowerService, McpOutput, RoleServer, ServerMessage, ServerOutput,
        ServiceAdapter, TowerServiceAdapter,
    };
    use rmcp::model::ServerInfo;
    use rmcp::ServerHandler;
    use futures::future::BoxFuture;
    use tower_service::Service as TowerService;

    // A tower::Service for testing ServiceAdapter (tower -> rmcp)
    struct TestTowerService;

    impl TowerService<ServerMessage> for TestTowerService {
        type Response = ServerOutput;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ServerMessage) -> Self::Future {
            Box::pin(async { Ok(McpOutput::Ack) })
        }
    }

    // A ServerHandler for testing TowerServiceAdapter (rmcp -> tower)
    #[derive(Clone)]
    struct TestHandler;
    
    impl ServerHandler for TestHandler {
        fn get_info(&self) -> rmcp::model::ServerInfo {
            ServerInfo::default()
        }
    }

    #[test]
    fn test_service_adapter_tower_to_rmcp() {
        // ServiceAdapter: tower::Service -> rmcp::Service
        let info = ServerInfo::default();
        let adapter: ServiceAdapter<TestTowerService, RoleServer> =
            ServiceAdapter::new(TestTowerService, info.clone());

        // This should compile if ServiceAdapter implements Service<RoleServer>
        let _: &dyn DynService<RoleServer> = &adapter;
    }

    #[test]
    fn test_tower_service_adapter_rmcp_to_tower() {
        // TowerServiceAdapter: rmcp::Service -> tower::Service
        let handler = TestHandler;
        let tower_service: TowerServiceAdapter<TestHandler> = TowerServiceAdapter::new(handler);
        
        // Verify it implements tower::Service
        fn assert_tower_service<S: TowerService<ServerMessage>>(_s: &S) {}
        assert_tower_service(&tower_service);
    }

    #[test]
    fn test_into_tower_service_trait() {
        // Test the IntoTowerService extension trait
        let handler = TestHandler;
        let tower_service = handler.into_tower_service();
        
        // Verify it implements tower::Service
        fn assert_tower_service<S: TowerService<ServerMessage>>(_s: &S) {}
        assert_tower_service(&tower_service);
    }

    #[test]
    fn test_tower_adapter_is_clone() {
        // TowerServiceAdapter should be Clone if the inner service is Clone
        let handler = TestHandler;
        let tower_service = handler.into_tower_service();
        let _cloned = tower_service.clone();
    }
}
