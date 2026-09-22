/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::sync::Arc;
use std::time::SystemTime;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        oneshot, Notify,
    },
    task::JoinHandle,
};

use crate::{
    adapters::{DynTransport, DynUriProvider},
    helpers,
    notification_manager::{self, NotificationEvent},
    subscription_manager::{self, SubscriptionEvent},
    USubscriptionConfiguration,
};
use tracing::{error, info};
use up_rust::{
    communication::{
        InMemoryRpcClient, RequestHandler, ServiceInvocationError, SubscriptionStatus, UPayload,
    },
    core::usubscription::{
        extract_usubscription_request, SubscribeResponse, USubscriptionRequest,
        USubscriptionResponse,
    },
    UAttributes, UCode, UStatus, UTransport, UUri,
};

/// Whether to include 'up:' uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub(crate) const UP_REMOTE_TTL: u32 = 300000;

// Alias definitions to provide more clarity, and make it easier to accomodate potential changes to expiry type in up-spec
pub(crate) type SubscriberUUri = UUri;
pub(crate) type TopicUUri = UUri;
pub(crate) type ExpirationTimestamp = SystemTime;

/// This trait primarily serves to provide a hook-point for using the mockall crate, for mocking USubscriptionService objects
/// where we also need/want to inject custom/mock UTransport implementations that subsequently get used in test cases.
pub trait UTransportHolder {
    fn get_transport(&self) -> Arc<dyn UTransport>;
}
impl<S> UTransportHolder for USubscriptionService<S> {
    fn get_transport(&self) -> Arc<dyn UTransport> {
        self.transport.clone()
    }
}

/// This object holds all mutable content associated with a running `USubscriptionService`, and is populated and returned when
/// calling `USubscriptionService::run()`. It exists for two reasons: a) allow `USubscriptionService` to remain useable as an immutable
/// object that can be put into `Arc`s and passed around freely, while b) offering a well-defined way to stop a running `USubscriptionService`
/// by simply calling `USubscriptionStopper::stop()`.
pub struct USubscriptionStopper {
    shutdown_notification: Arc<Notify>,
    subscription_joiner: JoinHandle<()>,
    notification_joiner: JoinHandle<()>,
}

impl USubscriptionStopper {
    pub async fn stop(self) {
        info!("Stopping uSubscription service");
        self.shutdown_notification.notify_waiters();
        self.subscription_joiner
            .await
            .expect("Error shutting down subscription manager");
        self.notification_joiner
            .await
            .expect("Error shutting down notification manager");
    }
}

/// USubscriptionService, compile-time enforced type-state pattern
pub struct Idle;
pub struct Running {
    subscription_sender: Sender<SubscriptionEvent>,
    notification_sender: Sender<NotificationEvent>,
}
pub struct USubscriptionService<S = Idle> {
    config: Arc<USubscriptionConfiguration>,
    transport: Arc<dyn UTransport>,
    state: S,
}

impl USubscriptionService<Idle> {
    pub fn new(config: Arc<USubscriptionConfiguration>, transport: Arc<dyn UTransport>) -> Self {
        Self {
            config,
            transport,
            state: Idle,
        }
    }

    pub async fn run(
        self,
    ) -> Result<(USubscriptionService<Running>, USubscriptionStopper), UStatus> {
        let shutdown_notification = Arc::new(Notify::new());
        let (notification_sender, notification_receiver) =
            mpsc::channel::<NotificationEvent>(self.config.notification_command_buffer.into());
        let (subscription_sender, subscription_receiver) =
            mpsc::channel::<SubscriptionEvent>(self.config.subscription_command_buffer.into());

        // RpcClient for handling remote subscriptions
        let rpc_client = Arc::new(
            InMemoryRpcClient::new(
                Arc::new(DynTransport::new(self.transport.clone())),
                Arc::new(DynUriProvider::new(self.config.clone())),
            )
            .await
            .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?,
        );

        // Set up notification manager actor
        let config_cloned = self.config.clone();
        let transport_cloned = self.transport.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let notification_joiner = helpers::spawn_and_log_error(async move {
            notification_manager::notification_engine(
                config_cloned,
                transport_cloned,
                notification_receiver,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        // Set up subscription manager actor
        let config_cloned = self.config.clone();
        let shutdown_notification_cloned = shutdown_notification.clone();
        let notification_sender_cloned = notification_sender.clone();
        let subscription_joiner = helpers::spawn_and_log_error(async move {
            subscription_manager::handle_message(
                config_cloned,
                rpc_client,
                subscription_receiver,
                notification_sender_cloned,
                shutdown_notification_cloned,
            )
            .await;
            Ok(())
        });

        Ok((
            USubscriptionService {
                config: self.config,
                transport: self.transport,
                state: Running {
                    subscription_sender,
                    notification_sender,
                },
            },
            USubscriptionStopper {
                subscription_joiner,
                notification_joiner,
                shutdown_notification,
            },
        ))
    }
}

#[async_trait::async_trait]
impl RequestHandler for USubscriptionService<Running> {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Decode the payload of uSubscription operations into a typed request. Malformed or
        // unsupported requests result in a `ServiceInvocationError`.
        let request = extract_usubscription_request(resource_id, request_payload)?;

        #[allow(clippy::wildcard_enum_match_arm)]
        match request {
            USubscriptionRequest::Subscribe(req) => {
                // Interact with subscription manager backend
                let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
                let se = SubscriptionEvent::AddSubscription {
                    subscriber: message_attributes.source().clone(),
                    topic: req.topic.clone(),
                    expiration: req.expiration,
                    sample_period: req.sample_period,
                    respond_to,
                };
                if let Err(e) = self.state.subscription_sender.send(se).await {
                    error!("Error communicating with subscription manager: {e}");
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                }
                let Ok(status) = receive_from.await else {
                    return Err(ServiceInvocationError::Internal(
                        "Error processing request".to_string(),
                    ));
                };

                USubscriptionResponse::Subscribe(SubscribeResponse {
                    topic: req.topic,
                    status,
                })
            }
            USubscriptionRequest::Unsubscribe(req) => {
                println!(
                    "UNSUBSCRIBE subscriber={}, topic={}",
                    message_attributes.source(),
                    req.topic
                );
                USubscriptionResponse::Unsubscribe(())
            }
            // `USubscriptionRequest` is `#[non_exhaustive]`, so new operations can be
            // added in future releases without breaking this code.
            other => {
                return Err(ServiceInvocationError::Unimplemented(format!(
                    "operation not supported by this service: {other:?}"
                )))
            }
        };

        Ok(None)
    }
}
