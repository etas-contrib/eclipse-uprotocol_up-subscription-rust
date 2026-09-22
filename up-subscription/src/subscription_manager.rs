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

#[cfg(test)]
use std::collections::HashMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::{
    sync::{mpsc, mpsc::Receiver, mpsc::Sender, oneshot, Notify},
    time::sleep,
};
use tracing::{debug, error, warn};
use up_rust::{
    communication::{CallOptions, RpcClient, SubscriptionStatus},
    core::usubscription::{
        SubscribeRequest, SubscribeResponse, UnsubscribeRequest, RESOURCE_ID_SUBSCRIBE,
        RESOURCE_ID_UNSUBSCRIBE, USUBSCRIPTION_TYPE_ID, USUBSCRIPTION_VERSION_MAJOR,
    },
    LocalUriProvider, UCode, UPriority, UStatus, UUri,
};

use crate::{
    helpers, notification_manager,
    notification_manager::NotificationEvent,
    persistency,
    usubscription::{SubscriberUUri, TopicUUri, UP_REMOTE_TTL},
    ExpirationTimestamp, USubscriptionConfiguration,
};

// This is the core business logic for handling and tracking subscriptions. It is currently implemented as a single event-consuming
// function `handle_message()`, which is supposed to be spawned into a task and process the various `Events` that it can receive
// via tokio mpsc channel. This design allows to forgo the use of any synhronization primitives on the subscription-tracking container
// data types, as any access is coordinated/serialized via the Event selection loop.

// Queue size of message channel for internal commands - like subscription status change messages or subscription expiration commands.
const INTERNAL_COMMAND_BUFFER_SIZE: usize = 128;

// Timeout to use when sending subscription removal command after a subscription has expired; if exceeded, subscription won't be removed
// directly but will be cleaned up at next startup.
const SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT_SECONDS: u64 = 5;

#[derive(Debug)]
pub(crate) struct SubscriptionEntry {
    pub(crate) topic: TopicUUri,
    pub(crate) subscriber: SubscriberUUri,
    pub(crate) status: SubscriptionStatus,
    pub(crate) sample_period: Option<Duration>,
}

// This is the 'outside API' of subscription manager, it includes some events that are only to be used in (and only enabled for) testing.
#[derive(Debug)]
pub(crate) enum SubscriptionEvent {
    AddSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        expiration: Option<ExpirationTimestamp>,
        sample_period: Option<Duration>,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    RemoveSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
        respond_to: oneshot::Sender<SubscriptionStatus>,
    },
    FetchSubscriptions {
        subscriber_filter: UUri,
        topic_filter: UUri,
        respond_to: oneshot::Sender<Vec<SubscriptionEntry>>,
    },
    Reset {
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetTopicSubscribers {
        respond_to: oneshot::Sender<persistency::SubscriptionSet>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetTopicSubscribers {
        topic_subscribers_replacement: persistency::SubscriptionSet,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current topic-subscriper ledger
    #[cfg(test)]
    GetRemoteTopics {
        respond_to: oneshot::Sender<HashMap<TopicUUri, SubscriptionStatus>>,
    },
    // Purely for use during testing: force-set new topic-subscriber ledger
    #[cfg(test)]
    SetRemoteTopics {
        topic_subscribers_replacement: HashMap<TopicUUri, SubscriptionStatus>,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get internal remote-subscription-change command sender
    #[cfg(test)]
    GetRemoteSubscriptionChangeSender {
        respond_to: oneshot::Sender<Sender<InternalSubscriptionEvent>>,
    },
}

// Internal subscription manager API - used to update on remote subscriptions (deal with _PENDING states)
#[derive(Debug)]
pub(crate) enum InternalSubscriptionEvent {
    TopicStateUpdate {
        topic: TopicUUri,
        state: SubscriptionStatus,
    },
    RemoveExpiredSubscription {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
    },
}

// Wrapper type, include all kinds of actions subscription manager knows
enum Event {
    LocalSubscription(SubscriptionEvent),
    RemoteSubscription(InternalSubscriptionEvent),
}

// Core business logic of subscription management - includes container data types for tracking subscriptions and remote subscriptions.
// Interfacing with this purely works via channels, so we do not have to deal with mutexes and similar concepts.
// [impl->req~usubscription-unsubscribe-notifications~1]
// [impl->dsn~usubscription-state-machine~1]
pub(crate) async fn handle_message(
    configuration: Arc<USubscriptionConfiguration>,
    rpc_client: Arc<dyn RpcClient>,
    mut command_receiver: Receiver<SubscriptionEvent>,
    notification_sender: Sender<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    // track subscribers for topics - if you're in this list, you have SUBSCRIBED, otherwise you're considered UNSUBSCRIBED
    // [impl->req~usubscription-subscribe-persistency~1]
    let mut subscriptions = persistency::SubscriptionsStore::new(&configuration);

    // for remote topics, we need to additionally deal with _PENDING states, this tracks states of these topics
    let mut remote_topics = persistency::RemoteTopicsStore::new(&configuration);

    let (internal_cmd_sender, mut internal_cmd_receiver) =
        mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

    // At startup, set up timed unsubscribe for any persisted subscriptions that define an expiration timestamp
    // [impl->req~usubscription-subscribe-expiration~1]
    // [impl->req~usubscription-subscribe-no-expiration~1]
    match subscriptions.get_and_prune_expiring_subscriptions() {
        Ok(list) => {
            for (subscriber, topic, expiration_millis) in list {
                schedule_unsubscribe(
                    expiration_millis,
                    subscriber.clone(),
                    topic.clone(),
                    internal_cmd_sender.clone(),
                );
            }
        }
        Err(e) => {
            panic!("Persistency failure {e}")
        }
    };

    loop {
        let event: Event = tokio::select! {
            // "Outside" events - actions that need to be performed
            event = command_receiver.recv() => match event {
                None => {
                    error!("Problem with subscription command channel, received None-event");
                    break
                },
                Some(event) => Event::LocalSubscription(event),
            },
            // "Inside" events - updates around remote subscription states
            event = internal_cmd_receiver.recv() => match event {
                None => {
                    error!("Problem with subscription command channel, received None-event");
                    break
                },
                Some(event) => Event::RemoteSubscription(event),
            },
            _ = shutdown.notified() => break,
        };
        match event {
            // These all deal with client-driven interactions (the core usubscription interface functionality)
            Event::LocalSubscription(event) => match event {
                SubscriptionEvent::AddSubscription {
                    subscriber,
                    topic,
                    expiration,
                    sample_period,
                    respond_to,
                } => {
                    // [impl->req~usubscription-subscribe~1]
                    match add_subscription(
                        configuration.clone(),
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                        expiration,
                    ) {
                        // [impl->req~usubscription-subscribe-notifications~1]
                        // [impl->dsn~usubscription-change-notification-update~1]
                        Ok(result) => {
                            // Send topic state change notification
                            notification_manager::notify_state_change(
                                notification_sender.clone(),
                                Some(subscriber.clone()),
                                topic.clone(),
                                result.clone(),
                            )
                            .await;

                            if respond_to.send(result).is_err() {
                                error!("Problem with internal communication");
                            }
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    };
                }
                SubscriptionEvent::RemoveSubscription {
                    subscriber,
                    topic,
                    respond_to,
                } => {
                    // [impl->req~usubscription-unsubscribe~1]
                    match remove_subscription(
                        configuration.clone(),
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            // [impl->dsn~usubscription-change-notification-update~1]
                            notification_manager::notify_state_change(
                                notification_sender.clone(),
                                Some(subscriber),
                                topic,
                                result.clone(),
                            )
                            .await;

                            if respond_to.send(result).is_err() {
                                error!("Problem with internal communication");
                            }
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                SubscriptionEvent::FetchSubscriptions {
                    subscriber_filter,
                    topic_filter,
                    respond_to,
                } => match fetch_subscriptions(
                    &subscriptions,
                    &remote_topics,
                    &subscriber_filter,
                    &topic_filter,
                ) {
                    Ok(result) => {
                        if respond_to.send(result).is_err() {
                            error!("Problem with internal communication");
                        };
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                SubscriptionEvent::Reset { respond_to } => {
                    reset(
                        &mut subscriptions,
                        &mut remote_topics,
                        notification_sender.clone(),
                    )
                    .await;
                    if respond_to.send(()).is_err() {
                        error!("Problem with internal communication");
                    };
                }
                #[cfg(test)]
                SubscriptionEvent::GetTopicSubscribers { respond_to } => {
                    match subscriptions.get_data() {
                        Ok(result) => {
                            let _r = respond_to.send(result);
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                #[cfg(test)]
                SubscriptionEvent::SetTopicSubscribers {
                    topic_subscribers_replacement,
                    respond_to,
                } => match subscriptions.set_data(topic_subscribers_replacement) {
                    Ok(_) => {
                        let _r = respond_to.send(());
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                #[cfg(test)]
                SubscriptionEvent::GetRemoteTopics { respond_to } => {
                    match remote_topics.get_data() {
                        Ok(result) => {
                            let _r = respond_to.send(result);
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
                #[cfg(test)]
                SubscriptionEvent::SetRemoteTopics {
                    topic_subscribers_replacement: remote_topics_replacement,
                    respond_to,
                } => match remote_topics.set_data(remote_topics_replacement) {
                    Ok(_) => {
                        let _r = respond_to.send(());
                    }
                    Err(e) => {
                        panic!("Persistency failure {e}")
                    }
                },
                #[cfg(test)]
                SubscriptionEvent::GetRemoteSubscriptionChangeSender { respond_to } => {
                    let _ = respond_to.send(internal_cmd_sender.clone());
                }
            },
            // deal with feedback/state updates from the remote subscription handlers
            Event::RemoteSubscription(event) => match event {
                InternalSubscriptionEvent::TopicStateUpdate { topic, state } => {
                    match remote_topics.set_topic_state(&topic, state.clone()) {
                        Ok(_) => {
                            // We're supposed to send topic change notifications to any subscribers of topic
                            // [impl->dsn~usubscription-change-notification-update~1]
                            if let Ok(subscribers) = subscriptions.get_topic_subscribers(&topic) {
                                let topic_clone = topic.clone();
                                let notification_sender_clone = notification_sender.clone();
                                // Want to do this out off the main control flow
                                helpers::spawn_and_log_error(async move {
                                    for subscriber in subscribers {
                                        notification_manager::notify_state_change(
                                            notification_sender_clone.clone(),
                                            Some(subscriber),
                                            topic_clone.clone(),
                                            state.clone(),
                                        )
                                        .await;
                                    }
                                    Ok(())
                                });
                            } else {
                                warn!("Failed to send topic state change update notification to topic subscribers");
                            }

                            // Send topic state change notification - in the case of remote subscriptions,
                            // the subscriber is usubscription service itself, so leave that field empty.
                            // TODO: Let's see if this is actually covered by a requirement - otherwise it should go
                            // notification_manager::notify(
                            //     notification_sender.clone(),
                            //     None,
                            //     topic,
                            //     SubscriptionStatus {
                            //         state: state.into(),
                            //         ..Default::default()
                            //     },
                            // )
                            // .await;
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}");
                        }
                    }
                }
                // Remote expired subscriptions, via internal command channel
                InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic } => {
                    match remove_subscription(
                        configuration.clone(),
                        rpc_client.clone(),
                        internal_cmd_sender.clone(),
                        &mut subscriptions,
                        &mut remote_topics,
                        subscriber.clone(),
                        topic.clone(),
                    ) {
                        Ok(result) => {
                            // Send topic state change notification
                            // [impl->dsn~usubscription-change-notification-update~1]
                            notification_manager::notify_state_change(
                                notification_sender.clone(),
                                Some(subscriber),
                                topic,
                                result.clone(),
                            )
                            .await;
                        }
                        Err(e) => {
                            panic!("Persistency failure {e}")
                        }
                    }
                }
            },
        }
    }
}

// Add a subscription relationship to bookkeeping, initiate remote subscribe request if neccessary
#[allow(clippy::too_many_arguments)]
fn add_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    expiration: Option<ExpirationTimestamp>,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    let _ = topic_subscribers.add_subscription(&subscriber, &topic, expiration)?;

    // For REMOTE topics, we explicitly track state due to _PENDING scenarios
    // [impl->req~usubscription-subscribe-multiple~1]
    let state = if topic.is_remote_authority(&uri_provider.get_authority()) {
        let state = remote_topics.add_topic_or_get_state(&topic)?;

        // if this remote topic is not yet SUBSCRIBED, perform remote subscription
        // [impl->req~usubscription-subscribe-remote~1]
        // [impl->req~usubscription-subscribe-unsubscribe-pending~1]
        if state != SubscriptionStatus::Subscribed {
            let topic_clone = topic.clone();
            let internal_cmd_sender_clone = internal_cmd_sender.clone();
            helpers::spawn_and_log_error(async move {
                remote_subscribe(topic_clone, rpc_client, internal_cmd_sender_clone).await?;
                Ok(())
            });
        }
        state
    } else {
        // Otherwise, LOCAL topics are considered to have status SUBSCRIBED as soon as they are registered here
        SubscriptionStatus::Subscribed
    };

    // Set up timed unsubscribe in case expiration timestamp is set
    // [impl->req~usubscription-subscribe-expiration~1]
    // [impl->req~usubscription-subscribe-no-expiration~1]
    if let Some(expiry_millis) = expiration {
        schedule_unsubscribe(
            expiry_millis,
            subscriber.clone(),
            topic,
            internal_cmd_sender,
        );
    };

    Ok(state)
}

// Remove a subscription relationship to bookkeeping, initiate remote unsubscribe request if neccessary
fn remove_subscription(
    uri_provider: Arc<dyn LocalUriProvider>,
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
) -> Result<SubscriptionStatus, persistency::PersistencyError> {
    // if this was the last subscriber to topic and topic is remote
    // [impl->req~usubscription-unsubscribe-last-remote~1]
    // [impl->req~usubscription-unsubscribe-subscribe-pending~1]
    if topic_subscribers.remove_subscription(&subscriber, &topic)?
        && topic.is_remote_authority(&uri_provider.get_authority())
    {
        // set remote topic state tracker to UNSUBSCRIBE_PENDING (until remote ubsubscribe confirmed)
        let _r = remote_topics.set_topic_state(&topic, SubscriptionStatus::UnsubscribePending)?;

        // perform remote unsubscription
        helpers::spawn_and_log_error(async move {
            remote_unsubscribe(topic, rpc_client, internal_cmd_sender).await?;
            Ok(())
        });
    }

    // [impl->req~usubscription-unsubscribe-multiple~1]
    // [impl->req~usubscription-unsubscribe-remote-unsubscribed~1]
    Ok(
        // Whatever happens with the remote topic state - as far as the local client is concerned, it has now UNSUBSCRIBED from this topic
        SubscriptionStatus::Unsubscribed,
    )
}

// Fetch all subscriptions of a topic or subscribers
fn fetch_subscriptions(
    topic_subscribers: &persistency::SubscriptionsStore,
    remote_topics: &persistency::RemoteTopicsStore,
    subscriber_filter: &UUri,
    topic_filter: &UUri,
) -> Result<Vec<SubscriptionEntry>, persistency::PersistencyError> {
    // let results: Vec<SubscriptionEntry> = match request {
    //     // [impl->req~usubscription-fetch-subscriptions-by-subscriber~1]
    //     RequestKind::Subscriber(subscriber) => topic_subscribers
    //         .get_subscriber_topics(&subscriber)?
    //         .iter()
    //         .map(|topic| SubscriptionEntry {
    //             topic: topic.clone(),
    //             subscriber: subscriber.clone(),
    //             status: SubscriptionStatus {
    //                 state: remote_topics
    //                     .get_topic_state(topic)
    //                     .unwrap_or(Some(TopicState::SUBSCRIBED))
    //                     .unwrap_or(TopicState::SUBSCRIBED)
    //                     .into(),
    //                 ..Default::default()
    //             },
    //         })
    //         .collect(),

    //     // [impl->req~usubscription-fetch-subscriptions-by-topic~1]
    //     RequestKind::Topic(topic) => topic_subscribers
    //         .get_topic_subscribers(&topic)?
    //         .iter()
    //         .map(|subscriber| SubscriptionEntry {
    //             topic: topic.clone(),
    //             subscriber: subscriber.clone(),
    //             status: SubscriptionStatus {
    //                 state: remote_topics
    //                     .get_topic_state(&topic)
    //                     .unwrap_or(Some(TopicState::SUBSCRIBED))
    //                     .unwrap_or(TopicState::SUBSCRIBED)
    //                     .into(),
    //                 ..Default::default()
    //             },
    //         })
    //         .collect(),
    // };
    // Ok(results)
    todo!()
}

// [impl->req~usubscription-reset~1]
async fn reset(
    topic_subscribers: &mut persistency::SubscriptionsStore,
    remote_topics: &mut persistency::RemoteTopicsStore,
    notification_sender: Sender<NotificationEvent>,
) {
    // 1. Retrieve list of all current subscriber-topic combinations
    let flattened_subscriptions = topic_subscribers.get_flattened_subscriptions();

    // 2. Reset/clear all stored subscriptions, remote subscriptions and notification-registrations
    // We plow through errors for now - re-subscribing to existing things should do no harm, in case a reset did now work
    if let Err(e) = topic_subscribers.reset() {
        error!("Error resetting subscriptions list: {e}");
    }
    if let Err(e) = remote_topics.reset() {
        error!("Error resetting remote subscriptions list: {e}");
    }
    #[allow(clippy::mutable_key_type)]
    let registered_notifactions = notification_manager::reset(notification_sender.clone())
        .await
        .unwrap_or_default();

    // 3. Notify every topic subscriber about the reset (all former subscriptions are UNSUBSCRIBED now)
    if let Ok(flattened_subscriptions) = flattened_subscriptions {
        helpers::spawn_and_log_error(async move {
            // Notify all topic subscribers
            for (subscriber, topic, _) in flattened_subscriptions {
                notification_manager::notify_state_change(
                    notification_sender.clone(),
                    Some(subscriber),
                    topic,
                    SubscriptionStatus::Unsubscribed,
                )
                .await;
            }

            // Notify all registered-for-notification clients
            for (subscriber, topic) in registered_notifactions {
                notification_manager::notify_state_change(
                    notification_sender.clone(),
                    Some(subscriber),
                    topic,
                    SubscriptionStatus::Unsubscribed,
                )
                .await;
            }
            Ok(())
        });
    }
}

// Perform remote topic subscription
async fn remote_subscribe(
    topic: TopicUUri,
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    // [impl->dsn~usubscription-subscribe-remote-subscriber-change~1]

    // build request
    let subscribe_request = SubscribeRequest {
        topic: topic.clone(),
        expiration: None,
        sample_period: None,
    };

    // send request
    // [impl->req~usubscription-remote-max-timeout~1]
    let subscription_response: SubscribeResponse = rpc_client
        .invoke_proto_method(
            make_remote_subscribe_uuri(&subscribe_request.topic)?,
            CallOptions::for_rpc_request(UP_REMOTE_TTL, None, None, Some(UPriority::CS4)),
            subscribe_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("Error invoking remote subscription request: {e}"),
            )
        })?;

    // deal with response
    // [impl->req~usubscription-subscribe-remote-response~1]
    if subscription_response
        .status
        .eq(&SubscriptionStatus::Subscribed)
    {
        debug!("Got remote subscription response, state SUBSCRIBED");

        let _ = internal_cmd_sender
            .send(InternalSubscriptionEvent::TopicStateUpdate {
                topic,
                state: SubscriptionStatus::Subscribed,
            })
            .await;
    } else {
        debug!("Got remote subscription response, some other state");
    }

    Ok(())
}

// Perform remote topic unsubscription
async fn remote_unsubscribe(
    topic: TopicUUri,
    rpc_client: Arc<dyn RpcClient>,
    internal_cmd_sender: Sender<InternalSubscriptionEvent>,
) -> Result<(), UStatus> {
    // [impl->dsn~usubscription-unsubscribe-remote-subscriber-change~1]

    // build request
    let unsubscribe_request = UnsubscribeRequest {
        topic: topic.clone(),
    };

    // send request
    let unsubscribe_response: UStatus = rpc_client
        .invoke_proto_method(
            make_remote_unsubscribe_uuri(&unsubscribe_request.topic)?,
            CallOptions::for_rpc_request(UP_REMOTE_TTL, None, None, Some(UPriority::CS4)),
            unsubscribe_request,
        )
        .await
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("Error invoking remote unsubscribe request: {e}"),
            )
        })?;

    // deal with response
    match unsubscribe_response.get_code() {
        UCode::Ok => {
            debug!("Got OK remote unsubscribe response");
            let _ = internal_cmd_sender
                .send(InternalSubscriptionEvent::TopicStateUpdate {
                    topic,
                    state: SubscriptionStatus::Unsubscribed,
                })
                .await;
        }
        code => {
            debug!("Got {code:?} remote unsubscribe response");
            return Err(UStatus::fail_with_code(
                code,
                "Error during remote unsubscribe",
            ));
        }
    };

    Ok(())
}

// Internal helper that will spawn a task to unsubscribe a subscriber-topic relationship at timestamp `expiry`.
// In case the expiry timestamp is already in the past, unsubscribe will be initiated immediately.
// This is hopefully good enough for now - in case we get very many expiring subscriptions, might have
// to look for an approach that scales better.
fn schedule_unsubscribe(
    expiration: ExpirationTimestamp,
    subscriber: SubscriberUUri,
    topic: TopicUUri,
    sender: Sender<InternalSubscriptionEvent>,
) {
    helpers::spawn_and_log_error(async move {
        if let Ok(delay) = expiration.duration_since(SystemTime::now()) {
            sleep(delay).await;
        }

        sender
            .send_timeout(
                InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic },
                Duration::from_secs(SUBSCRIPTION_EXPIRY_REMOVAL_TIMEOUT_SECONDS),
            )
            .await?;

        Ok(())
    });
}

// Create a remote Subscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Subscribe-endpoint properties
pub(crate) fn make_remote_subscribe_uuri(uri: &UUri) -> Result<UUri, UStatus> {
    UUri::try_from_parts(
        uri.authority_name(),
        USUBSCRIPTION_TYPE_ID as u32,
        USUBSCRIPTION_VERSION_MAJOR,
        RESOURCE_ID_SUBSCRIBE,
    )
    .map_err(|e| UStatus::fail_with_code(UCode::InvalidArgument, e.to_string()))
}

// Create a remote Unsubscribe UUri from a (topic) uri; copies the UUri authority and
// replaces id, version and resource IDs with Unsubscribe-endpoint properties
pub(crate) fn make_remote_unsubscribe_uuri(uri: &UUri) -> Result<UUri, UStatus> {
    UUri::try_from_parts(
        uri.authority_name(),
        USUBSCRIPTION_TYPE_ID as u32,
        USUBSCRIPTION_VERSION_MAJOR,
        RESOURCE_ID_UNSUBSCRIBE,
    )
    .map_err(|e| UStatus::fail_with_code(UCode::InvalidArgument, e.to_string()))
}

#[cfg(test)]
mod tests {
    // These are tests just for the locally used helper functions of subscription manager. More complex and complete
    // tests of the susbcription manager business logic are located in tests/subscription_manager_tests.rs
    use super::*;
    use crate::test_lib::mocks::MockRpcClientMock;
    use crate::test_lib::{self};
    use tokio::time::{Duration, Instant};

    // [utest->req~usubscription-subscribe-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_schedule_future_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start: Instant = Instant::now();
        schedule_unsubscribe(
            SystemTime::now() + Duration::from_secs(1),
            test_lib::helpers::subscriber_uri1(),
            test_lib::helpers::local_topic1_uri(),
            internal_cmd_sender,
        );

        let message = internal_cmd_receiver.recv().await;
        if let Some(InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic }) =
            message
        {
            // Dunno how flaky this might turn out to be - but let's test if there is at least a second elapsed between scheduling the
            // unsubscribe task and it's reaction to the expiration timer 1s in the future...
            let elapsed = start.elapsed();
            assert!(elapsed >= Duration::from_millis(1000));

            assert_eq!(topic, test_lib::helpers::local_topic1_uri());
            assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
        } else {
            panic!("Expected RemoveExpiredSubscription event from scheduled task");
        }
    }

    // [utest->req~usubscription-subscribe-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_schedule_past_unsubscribe() {
        let (internal_cmd_sender, mut internal_cmd_receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);

        let start = Instant::now();
        schedule_unsubscribe(
            SystemTime::now() + Duration::from_secs(1),
            test_lib::helpers::subscriber_uri1(),
            test_lib::helpers::local_topic1_uri(),
            internal_cmd_sender,
        );

        let message = internal_cmd_receiver.recv().await;
        if let Some(InternalSubscriptionEvent::RemoveExpiredSubscription { subscriber, topic }) =
            message
        {
            // Dunno how flaky this might turn out to be - but let's test if there is at most half a second elapsed between scheduling the
            // unsubscribe task and it's reaction to the expiration timer 1s in the past...
            let elapsed = start.elapsed();
            assert!(elapsed < Duration::from_millis(500));

            assert_eq!(topic, test_lib::helpers::local_topic1_uri());
            assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
        } else {
            panic!("Expected RemoveExpiredSubscription event from scheduled task");
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_remote_subscribe() {
        let expected_topic = test_lib::helpers::remote_topic1_uri();

        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);
        let mock_client = Arc::new(MockRpcClientMock::default());

        // perform operation to test
        let result = remote_subscribe(expected_topic.clone(), mock_client, sender).await;

        // validate response
        assert!(result.is_ok());
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, SubscriptionStatus::Subscribed);
        };
    }

    #[test_log::test(tokio::test)]
    async fn test_remote_unsubscribe() {
        let expected_topic = test_lib::helpers::remote_topic1_uri();

        let (sender, mut receiver) =
            mpsc::channel::<InternalSubscriptionEvent>(INTERNAL_COMMAND_BUFFER_SIZE);
        let mock_client = Arc::new(MockRpcClientMock::default());

        // perform operation to test
        let result: Result<(), UStatus> =
            remote_unsubscribe(expected_topic.clone(), mock_client, sender).await;

        // validate response
        assert!(result.is_ok());
        let response = receiver.recv().await;
        assert!(response.is_some());
        if let InternalSubscriptionEvent::TopicStateUpdate { topic, state } = response.unwrap() {
            assert_eq!(topic, expected_topic);
            assert_eq!(state, SubscriptionStatus::Unsubscribed);
        };
    }

    #[test]
    fn test_make_remote_subscribe_uuri() {
        let expected_uri = UUri::try_from_parts(
            test_lib::helpers::remote_topic1_uri().authority_name(),
            USUBSCRIPTION_TYPE_ID as u32,
            USUBSCRIPTION_VERSION_MAJOR,
            RESOURCE_ID_SUBSCRIBE,
        )
        .expect("test UUri creation not expected to fail");

        let remote_method = make_remote_subscribe_uuri(&test_lib::helpers::remote_topic1_uri())
            .expect("validation UUri creation not expected to fail");

        assert_eq!(expected_uri, remote_method);
    }

    #[test]
    fn test_make_remote_unsubscribe_uuri() {
        let expected_uri = UUri::try_from_parts(
            test_lib::helpers::remote_topic1_uri().authority_name(),
            USUBSCRIPTION_TYPE_ID as u32,
            USUBSCRIPTION_VERSION_MAJOR,
            RESOURCE_ID_UNSUBSCRIBE,
        )
        .expect("test UUri creation not expected to fail");

        let remote_method = make_remote_unsubscribe_uuri(&test_lib::helpers::remote_topic1_uri())
            .expect("validation UUri creation not expected to fail");

        assert_eq!(expected_uri, remote_method);
    }
}
