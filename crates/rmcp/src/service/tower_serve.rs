//! Tower-native service loop
//!
//! This module provides a service loop that works directly with Tower services,
//! avoiding the need for adapters and making the integration more natural.

use std::{
    collections::HashMap,
    sync::Arc,
};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tower_service::Service as TowerService;
use tracing::{instrument, Instrument};

use crate::{
    model::{
        CancelledNotification, CancelledNotificationParam, ClientNotification, ClientRequest,
        Extensions, GetExtensions, GetMeta, JsonRpcError, JsonRpcMessage,
        JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, Meta, RequestId, ServerResult,
    },
    service::{
        McpMessage, McpOutput, NotificationContext, Peer, PeerSinkMessage, QuitReason,
        RequestContext, Responder, RoleServer, RunningService, ServiceError,
    },
    transport::{DynamicTransportError, Transport},
};

/// Serve a Tower service with the given transport
///
/// This is a Tower-native version of the service loop that works directly
/// with `tower::Service` implementations, converting between JSON-RPC messages
/// and `McpMessage`/`McpOutput` types.
///
/// This is the core function that powers `McpServer::serve()` for Tower services.
///
/// Note: This is an internal function. Use `McpServer::serve()` for the public API.
#[instrument(skip_all)]
pub(crate) fn serve_tower_inner<S, T>(
    service: S,
    transport: T,
    peer: Peer<RoleServer>,
    mut peer_rx: tokio::sync::mpsc::Receiver<PeerSinkMessage<RoleServer>>,
    ct: CancellationToken,
) -> RunningService<RoleServer, TowerServiceWrapper<S>>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Clone + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
    T: Transport<RoleServer> + 'static,
{
    const SINK_PROXY_BUFFER_SIZE: usize = 64;
    let (sink_proxy_tx, mut sink_proxy_rx) =
        tokio::sync::mpsc::channel::<crate::service::TxJsonRpcMessage<RoleServer>>(SINK_PROXY_BUFFER_SIZE);
    let peer_info = peer.peer_info();
    tracing::info!(?peer_info, "Tower service initialized as server");

    let mut local_responder_pool =
        HashMap::<RequestId, Responder<Result<crate::model::ClientResult, ServiceError>>>::new();
    let mut local_ct_pool = HashMap::<RequestId, CancellationToken>::new();
    let shared_service = Arc::new(TowerServiceWrapper::new(service));
    let service_for_return = shared_service.clone();

    let serve_loop_ct = ct.child_token();
    let peer_return: Peer<RoleServer> = peer.clone();
    let current_span = tracing::Span::current();
    let handle = tokio::spawn(async move {
        let mut transport = transport;
        let mut batch_messages = std::collections::VecDeque::<crate::service::RxJsonRpcMessage<RoleServer>>::new();
        let mut send_task_set = tokio::task::JoinSet::<SendTaskResult>::new();
        
        #[derive(Debug)]
        enum SendTaskResult {
            Request {
                id: RequestId,
                result: Result<(), DynamicTransportError>,
            },
            Notification {
                responder: Responder<Result<(), ServiceError>>,
                cancellation_param: Option<CancelledNotificationParam>,
                result: Result<(), DynamicTransportError>,
            },
        }
        
        #[derive(Debug)]
        enum Event {
            ProxyMessage(PeerSinkMessage<RoleServer>),
            PeerMessage(crate::service::RxJsonRpcMessage<RoleServer>),
            ToSink(crate::service::TxJsonRpcMessage<RoleServer>),
            SendTaskResult(SendTaskResult),
        }

        let quit_reason = loop {
            let evt = if let Some(m) = batch_messages.pop_front() {
                Event::PeerMessage(m)
            } else {
                tokio::select! {
                    m = sink_proxy_rx.recv(), if !sink_proxy_rx.is_closed() => {
                        if let Some(m) = m {
                            Event::ToSink(m)
                        } else {
                            continue
                        }
                    }
                    m = transport.receive() => {
                        if let Some(m) = m {
                            Event::PeerMessage(m)
                        } else {
                            tracing::info!("input stream terminated");
                            break QuitReason::Closed
                        }
                    }
                    m = peer_rx.recv(), if !peer_rx.is_closed() => {
                        if let Some(m) = m {
                            Event::ProxyMessage(m)
                        } else {
                            continue
                        }
                    }
                    m = send_task_set.join_next(), if !send_task_set.is_empty() => {
                        let Some(result) = m else {
                            continue
                        };
                        match result {
                            Err(e) => {
                                tracing::error!(%e, "send request task encounter a tokio join error");
                                break QuitReason::JoinError(e)
                            }
                            Ok(result) => {
                                Event::SendTaskResult(result)
                            }
                        }
                    }
                    _ = serve_loop_ct.cancelled() => {
                        tracing::info!("task cancelled");
                        break QuitReason::Cancelled
                    }
                }
            };

            tracing::trace!(?evt, "new event");
            match evt {
                Event::SendTaskResult(SendTaskResult::Request { id, result }) => {
                    if let Err(e) = result {
                        if let Some(responder) = local_responder_pool.remove(&id) {
                            let _ = responder.send(Err(ServiceError::TransportSend(e)));
                        }
                    }
                }
                Event::SendTaskResult(SendTaskResult::Notification {
                    responder,
                    result,
                    cancellation_param,
                }) => {
                    let response = if let Err(e) = result {
                        Err(ServiceError::TransportSend(e))
                    } else {
                        Ok(())
                    };
                    let _ = responder.send(response);
                    if let Some(param) = cancellation_param {
                        if let Some(responder) = local_responder_pool.remove(&param.request_id) {
                            tracing::info!(id = %param.request_id, reason = param.reason, "cancelled");
                            let _response_result = responder.send(Err(ServiceError::Cancelled {
                                reason: param.reason.clone(),
                            }));
                        }
                    }
                }
                Event::ToSink(m) => {
                    if let Some(id) = match &m {
                        JsonRpcMessage::Response(response) => Some(&response.id),
                        JsonRpcMessage::Error(error) => Some(&error.id),
                        _ => None,
                    } {
                        if let Some(ct) = local_ct_pool.remove(id) {
                            ct.cancel();
                        }
                        let send = transport.send(m);
                        let current_span = tracing::Span::current();
                        tokio::spawn(async move {
                            let send_result = send.await;
                            if let Err(error) = send_result {
                                tracing::error!(%error, "fail to response message");
                            }
                        }.instrument(current_span));
                    }
                }
                Event::ProxyMessage(PeerSinkMessage::Request {
                    request,
                    id,
                    responder,
                }) => {
                    local_responder_pool.insert(id.clone(), responder);
                    let send = transport.send(JsonRpcMessage::request(request, id.clone()));
                    {
                        let id = id.clone();
                        let current_span = tracing::Span::current();
                        send_task_set.spawn(send.map(move |r| SendTaskResult::Request {
                            id,
                            result: r.map_err(DynamicTransportError::new::<T, RoleServer>),
                        }).instrument(current_span));
                    }
                }
                Event::ProxyMessage(PeerSinkMessage::Notification {
                    notification,
                    responder,
                }) => {
                    let mut cancellation_param = None;
                    let notification = match notification.try_into() {
                        Ok::<CancelledNotification, _>(cancelled) => {
                            cancellation_param.replace(cancelled.params.clone());
                            cancelled.into()
                        }
                        Err(notification) => notification,
                    };
                    let send = transport.send(JsonRpcMessage::notification(notification));
                    let current_span = tracing::Span::current();
                    send_task_set.spawn(send.map(move |result| SendTaskResult::Notification {
                        responder,
                        cancellation_param,
                        result: result.map_err(DynamicTransportError::new::<T, RoleServer>),
                    }).instrument(current_span));
                }
                Event::PeerMessage(JsonRpcMessage::Request(JsonRpcRequest {
                    id,
                    mut request,
                    ..
                })) => {
                    tracing::debug!(%id, ?request, "received request");
                    {
                        let service = shared_service.clone();
                        let sink = sink_proxy_tx.clone();
                        let request_ct = serve_loop_ct.child_token();
                        let context_ct = request_ct.child_token();
                        local_ct_pool.insert(id.clone(), request_ct);
                        let mut extensions = Extensions::new();
                        let mut meta = Meta::new();
                        std::mem::swap(&mut meta, request.get_meta_mut());
                        std::mem::swap(&mut extensions, request.extensions_mut());
                        let context = RequestContext {
                            ct: context_ct,
                            id: id.clone(),
                            peer: peer.clone(),
                            meta,
                            extensions,
                        };
                        let current_span = tracing::Span::current();
                        tokio::spawn(async move {
                            // Convert JSON-RPC request to McpMessage
                            let msg = McpMessage::Request {
                                id: id.clone(),
                                request,
                                context,
                            };
                            
                            // Call the Tower service
                            let mut svc = service.inner.lock().await;
                            let future = svc.call(msg);
                            drop(svc); // Release lock before await
                            
                            let output = match future.await {
                                Ok(output) => output,
                                Err(e) => {
                                    tracing::error!(%e, "Tower service error");
                                    let error = crate::error::ErrorData::internal_error(e.to_string(), None);
                                    let response = JsonRpcMessage::error(error, id);
                                    let _ = sink.send(response).await;
                                    return;
                                }
                            };
                            
                            // Convert McpOutput back to JSON-RPC response
                            let response = match output {
                                McpOutput::Response { id: response_id, result } => {
                                    if id != response_id {
                                        tracing::warn!(request_id = %id, response_id = %response_id, "Request ID mismatch");
                                    }
                                    match result {
                                        Ok(result) => {
                                            tracing::debug!(%id, ?result, "response message");
                                            JsonRpcMessage::response(result, id)
                                        }
                                        Err(error) => {
                                            tracing::warn!(%id, ?error, "response error");
                                            JsonRpcMessage::error(error, id)
                                        }
                                    }
                                }
                                McpOutput::Ack => {
                                    tracing::warn!(%id, "Received Ack for a request");
                                    let error = crate::error::ErrorData::internal_error(
                                        "Received Ack for a request".to_string(),
                                        None,
                                    );
                                    JsonRpcMessage::error(error, id)
                                }
                            };
                            let _send_result = sink.send(response).await;
                        }.instrument(current_span));
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Notification(JsonRpcNotification {
                    notification,
                    ..
                })) => {
                    tracing::info!(?notification, "received notification");
                    let mut notification = match notification.try_into() {
                        Ok::<CancelledNotification, _>(cancelled) => {
                            if let Some(ct) = local_ct_pool.remove(&cancelled.params.request_id) {
                                tracing::info!(id = %cancelled.params.request_id, reason = cancelled.params.reason, "cancelled");
                                ct.cancel();
                            }
                            cancelled.into()
                        }
                        Err(notification) => notification,
                    };
                    {
                        let service = shared_service.clone();
                        let mut extensions = Extensions::new();
                        let mut meta = Meta::new();
                        std::mem::swap(&mut extensions, notification.extensions_mut());
                        std::mem::swap(&mut meta, notification.get_meta_mut());
                        let context = NotificationContext {
                            peer: peer.clone(),
                            meta,
                            extensions,
                        };
                        let current_span = tracing::Span::current();
                        tokio::spawn(async move {
                            // Convert JSON-RPC notification to McpMessage
                            let msg = McpMessage::Notification {
                                notification,
                                context,
                            };
                            
                            // Call the Tower service
                            let mut svc = service.inner.lock().await;
                            let future = svc.call(msg);
                            drop(svc); // Release lock before await
                            
                            match future.await {
                                Ok(McpOutput::Ack) => {
                                    // Success - notification handled
                                }
                                Ok(McpOutput::Response { .. }) => {
                                    tracing::warn!("Received Response for a notification");
                                }
                                Err(e) => {
                                    tracing::warn!(%e, "Error handling notification");
                                }
                            }
                        }.instrument(current_span));
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Response(JsonRpcResponse {
                    result,
                    id,
                    ..
                })) => {
                    if let Some(responder) = local_responder_pool.remove(&id) {
                        let response_result = responder.send(Ok(result));
                        if let Err(_error) = response_result {
                            tracing::warn!(%id, "Error sending response");
                        }
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Error(JsonRpcError { error, id, .. })) => {
                    if let Some(responder) = local_responder_pool.remove(&id) {
                        let _response_result = responder.send(Err(ServiceError::McpError(error)));
                        if let Err(_error) = _response_result {
                            tracing::warn!(%id, "Error sending response");
                        }
                    }
                }
            }
        };
        let sink_close_result = transport.close().await;
        if let Err(e) = sink_close_result {
            tracing::error!(%e, "fail to close sink");
        }
        tracing::info!(?quit_reason, "serve finished");
        quit_reason
    }.instrument(current_span));
    
    RunningService {
        service: service_for_return,
        peer: peer_return,
        handle: Some(handle),
        cancellation_token: ct.clone(),
        dg: ct.drop_guard(),
    }
}

/// Wrapper that holds a Tower service with interior mutability
///
/// Tower services require `&mut self` for `call()`, so we use `Arc<tokio::sync::Mutex<S>>`
/// to provide interior mutability in async contexts.
#[derive(Debug)]
pub struct TowerServiceWrapper<S> {
    inner: Arc<tokio::sync::Mutex<S>>,
}

impl<S> TowerServiceWrapper<S> {
    pub fn new(service: S) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(service)),
        }
    }
}

impl<S> Clone for TowerServiceWrapper<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// Implement Service<RoleServer> for TowerServiceWrapper
// This is a minimal implementation since the actual handling is done
// in the Tower-native service loop. These methods should never be called
// in normal operation, but they're required by RunningService.
impl<S> crate::service::Service<RoleServer> for TowerServiceWrapper<S>
where
    S: TowerService<McpMessage<RoleServer>, Response = McpOutput<RoleServer>> + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send,
{
    async fn handle_request(
        &self,
        _request: ClientRequest,
        _context: RequestContext<RoleServer>,
    ) -> Result<ServerResult, crate::error::ErrorData> {
        // This should never be called - requests are handled in the Tower-native loop
        Err(crate::error::ErrorData::internal_error(
            "TowerServiceWrapper::handle_request should not be called".to_string(),
            None,
        ))
    }

    async fn handle_notification(
        &self,
        _notification: ClientNotification,
        _context: NotificationContext<RoleServer>,
    ) -> Result<(), crate::error::ErrorData> {
        // This should never be called - notifications are handled in the Tower-native loop
        Err(crate::error::ErrorData::internal_error(
            "TowerServiceWrapper::handle_notification should not be called".to_string(),
            None,
        ))
    }

    fn get_info(&self) -> crate::model::ServerInfo {
        // Return a default - this shouldn't be used since we handle info in McpServer
        crate::model::ServerInfo::default()
    }
}
