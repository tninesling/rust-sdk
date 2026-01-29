//! MCP Client Builder
//!
//! This module provides the `McpClient` type for connecting to MCP servers
//! as a client, with support for Tower middleware on outbound requests.
//!
//! # Example
//!
//! ```rust,no_run
//! use rmcp::McpClient;
//! use rmcp::model::ClientInfo;
//! # use rmcp::transport::TokioChildProcess;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Simple connection with default handler
//! let client = McpClient::new(ClientInfo::default())
//!     .connect(TokioChildProcess::new(tokio::process::Command::new("mcp-server"))?)
//!     .await?;
//!
//! // List tools from the server
//! let tools = client.list_tools(Default::default()).await?;
//! # Ok(())
//! # }
//! ```

use tower::Layer;
use tower_service::Service as TowerService;

use crate::{
    ClientHandler,
    model::ClientInfo,
    service::{
        ClientInitializeError, McpMessage, McpOutput, Peer, RoleClient, RunningService,
        ServiceAdapter, TowerServiceAdapter, serve_client_with_ct,
    },
    transport::IntoTransport,
};

/// Builder for MCP clients with fluent layer API
///
/// This is the main entry point for creating MCP client connections.
/// Use `layer()` to add Tower middleware.
///
/// # Example
///
/// ```rust,no_run
/// use rmcp::McpClient;
/// use rmcp::model::ClientInfo;
/// # use rmcp::transport::TokioChildProcess;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = McpClient::new(ClientInfo::default())
///     // .layer(TimeoutLayer::new(Duration::from_secs(30)))
///     .connect(TokioChildProcess::new(tokio::process::Command::new("server"))?)
///     .await?;
///
/// let tools = client.list_tools(Default::default()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct McpClient<L = tower::layer::util::Identity> {
    info: ClientInfo,
    layer: L,
}

impl McpClient {
    /// Create a new MCP client builder with the given client info
    pub fn new(info: impl Into<ClientInfo>) -> McpClient<tower::layer::util::Identity> {
        McpClient {
            info: info.into(),
            layer: tower::layer::util::Identity::new(),
        }
    }
}

impl<L> McpClient<L> {
    /// Add a Tower middleware layer
    ///
    /// Layers are applied to the client handler (for handling server-initiated requests).
    pub fn layer<NewLayer>(self, layer: NewLayer) -> McpClient<tower::layer::util::Stack<NewLayer, L>> {
        McpClient {
            info: self.info,
            layer: tower::layer::util::Stack::new(layer, self.layer),
        }
    }

    /// Connect to an MCP server using a default (no-op) client handler
    ///
    /// Use this when you don't need to handle server-initiated requests.
    pub async fn connect<T, E, A>(
        self,
        transport: T,
    ) -> Result<RunningMcpClient<ServiceAdapter<L::Service, RoleClient>>, ClientInitializeError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
        L: Layer<TowerServiceAdapter<ClientInfo>>,
        L::Service: TowerService<McpMessage<RoleClient>, Response = McpOutput<RoleClient>> + Clone + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleClient>>>::Error: std::error::Error + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleClient>>>::Future: Send,
    {
        let info = self.info.clone();
        self.connect_with_handler(transport, info).await
    }

    /// Connect to an MCP server with a custom client handler
    ///
    /// Use this when you need to handle server-initiated requests (e.g., sampling).
    pub async fn connect_with_handler<T, H, E, A>(
        self,
        transport: T,
        handler: H,
    ) -> Result<RunningMcpClient<ServiceAdapter<L::Service, RoleClient>>, ClientInitializeError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
        H: ClientHandler + Clone + Send + Sync + 'static,
        L: Layer<TowerServiceAdapter<H>>,
        L::Service: TowerService<McpMessage<RoleClient>, Response = McpOutput<RoleClient>> + Clone + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleClient>>>::Error: std::error::Error + Send + Sync + 'static,
        <L::Service as TowerService<McpMessage<RoleClient>>>::Future: Send,
    {
        // Wrap the handler and apply layers
        let base_service = TowerServiceAdapter::new(handler);
        let tower_service = self.layer.layer(base_service);
        
        // Convert back to rmcp::Service via ServiceAdapter
        let service = ServiceAdapter::new(tower_service, self.info.clone());

        // Use the existing serve_client infrastructure
        let running = serve_client_with_ct(
            service,
            transport,
            Default::default(),
        ).await?;

        Ok(RunningMcpClient { running })
    }
}

/// A running MCP client connection
///
/// Provides access to make requests to the connected MCP server.
pub struct RunningMcpClient<S>
where
    S: crate::service::Service<RoleClient>,
{
    running: RunningService<RoleClient, S>,
}

impl<S> RunningMcpClient<S>
where
    S: crate::service::Service<RoleClient>,
{
    /// Get the server info received during handshake
    pub fn server_info(&self) -> Option<&crate::model::ServerInfo> {
        self.running.peer().peer_info()
    }

    /// Get a reference to the peer for making raw requests
    pub fn peer(&self) -> &Peer<RoleClient> {
        self.running.peer()
    }

    /// List tools available on the server
    pub async fn list_tools(
        &self,
        params: crate::model::PaginatedRequestParams,
    ) -> Result<crate::model::ListToolsResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, ListToolsRequest, ListToolsRequestMethod, ServerResult};

        let request = ClientRequest::ListToolsRequest(ListToolsRequest {
            method: ListToolsRequestMethod,
            params: if params == Default::default() { None } else { Some(params) },
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::ListToolsResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// Call a tool on the server
    pub async fn call_tool(
        &self,
        params: crate::model::CallToolRequestParams,
    ) -> Result<crate::model::CallToolResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, CallToolRequest, CallToolRequestMethod, ServerResult};

        let request = ClientRequest::CallToolRequest(CallToolRequest {
            method: CallToolRequestMethod,
            params,
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::CallToolResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// List prompts available on the server
    pub async fn list_prompts(
        &self,
        params: crate::model::PaginatedRequestParams,
    ) -> Result<crate::model::ListPromptsResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, ListPromptsRequest, ListPromptsRequestMethod, ServerResult};

        let request = ClientRequest::ListPromptsRequest(ListPromptsRequest {
            method: ListPromptsRequestMethod,
            params: if params == Default::default() { None } else { Some(params) },
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::ListPromptsResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// Get a prompt from the server
    pub async fn get_prompt(
        &self,
        params: crate::model::GetPromptRequestParams,
    ) -> Result<crate::model::GetPromptResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, GetPromptRequest, GetPromptRequestMethod, ServerResult};

        let request = ClientRequest::GetPromptRequest(GetPromptRequest {
            method: GetPromptRequestMethod,
            params,
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::GetPromptResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// List resources available on the server
    pub async fn list_resources(
        &self,
        params: crate::model::PaginatedRequestParams,
    ) -> Result<crate::model::ListResourcesResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, ListResourcesRequest, ListResourcesRequestMethod, ServerResult};

        let request = ClientRequest::ListResourcesRequest(ListResourcesRequest {
            method: ListResourcesRequestMethod,
            params: if params == Default::default() { None } else { Some(params) },
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::ListResourcesResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// Read a resource from the server
    pub async fn read_resource(
        &self,
        params: crate::model::ReadResourceRequestParams,
    ) -> Result<crate::model::ReadResourceResult, crate::service::ServiceError> {
        use crate::model::{ClientRequest, ReadResourceRequest, ReadResourceRequestMethod, ServerResult};

        let request = ClientRequest::ReadResourceRequest(ReadResourceRequest {
            method: ReadResourceRequestMethod,
            params,
            extensions: Default::default(),
        });

        let response = self.running.peer().send_request(request).await?;
        match response {
            ServerResult::ReadResourceResult(result) => Ok(result),
            _ => Err(crate::service::ServiceError::UnexpectedResponse),
        }
    }

    /// Wait for the client connection to close
    pub async fn wait(self) -> Result<crate::service::QuitReason, tokio::task::JoinError> {
        self.running.waiting().await
    }

    /// Gracefully close the client connection
    pub async fn close(mut self) -> Result<crate::service::QuitReason, tokio::task::JoinError> {
        self.running.close().await
    }
}
