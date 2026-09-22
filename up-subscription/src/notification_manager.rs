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

use tokio::sync::{mpsc::Receiver, mpsc::Sender, oneshot, Notify};
use tracing::{debug, error, warn};
use up_rust::{
    communication::SubscriptionStatus,
    core::usubscription::{usubscription_uri, SubscriptionInfo, RESOURCE_ID_SUBSCRIPTION_CHANGE},
    LocalUriProvider, UMessageBuilder, UTransport, UUID,
};

use crate::{
    persistency::{self, PersistencyError},
    usubscription::{SubscriberUUri, TopicUUri, INCLUDE_SCHEMA},
    USubscriptionConfiguration,
};

// From usubscription.proto, the uprotocol.notification_topic resource ID
pub(crate) const SOURCE_URI_RESOURCE_ID: u16 = 0x8000;

// This is the core business logic for tracking and sending subscription update notifications. It is currently implemented as a single
// event-consuming function `notification_engine()`, which is supposed to be spawned into a task, and process the various notification
// `Events` that it can receive via tokio mpsc channel.

// This is the 'outside API' of notification handler
#[derive(Debug)]
pub(crate) enum NotificationEvent {
    AddNotifyee {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
    },
    RemoveNotifyee {
        subscriber: SubscriberUUri,
        topic: TopicUUri,
    },
    StateChange {
        subscriber: Option<SubscriberUUri>,
        topic: TopicUUri,
        status: SubscriptionStatus,
        respond_to: oneshot::Sender<()>,
    },
    Reset {
        respond_to: oneshot::Sender<Result<(), PersistencyError>>,
    },
    GetNotificationTopics {
        respond_to: oneshot::Sender<Vec<(SubscriberUUri, TopicUUri)>>,
    },
    // Purely for use during testing: force-set new notifyees ledger
    #[cfg(test)]
    SetNotificationTopics {
        notification_topics_replacement: Vec<(SubscriberUUri, TopicUUri)>,
        respond_to: oneshot::Sender<()>,
    },
}

impl PartialEq for NotificationEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                NotificationEvent::StateChange {
                    subscriber: s1,
                    topic: t1,
                    status: st1,
                    ..
                },
                NotificationEvent::StateChange {
                    subscriber: s2,
                    topic: t2,
                    status: st2,
                    ..
                },
            ) => s1 == s2 && t1 == t2 && st1 == st2,
            (
                NotificationEvent::AddNotifyee {
                    subscriber: s1,
                    topic: t1,
                },
                NotificationEvent::AddNotifyee {
                    subscriber: s2,
                    topic: t2,
                },
            ) => s1 == s2 && t1 == t2,
            (
                NotificationEvent::RemoveNotifyee {
                    subscriber: s1,
                    topic: t1,
                },
                NotificationEvent::RemoveNotifyee {
                    subscriber: s2,
                    topic: t2,
                },
            ) => s1 == s2 && t1 == t2,
            // Don't care about the test-only variants
            _ => false,
        }
    }
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
// Interfacing with this purely works via channels.
pub(crate) async fn notification_engine(
    configuration: Arc<USubscriptionConfiguration>,
    up_transport: Arc<dyn UTransport>,
    mut events: Receiver<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    // keep track of which subscriber wants to be notified on which topic
    let mut notifications = persistency::NotificationStore::new(&configuration);

    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                None => {
                    error!("Problem with notification command channel, received None-event");
                    break
                },
                Some(event) => event,
            },
            _ = shutdown.notified() => {
                break
            },
        };
        match event {
            // [impl->req~usubscription-register-notifications~1]
            NotificationEvent::AddNotifyee { subscriber, topic } => {
                if !topic.is_event() {
                    error!("Topic UUri is not a valid event target");
                    break;
                }

                match notifications.add_notifyee(&subscriber, &topic) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            // [impl->req~usubscription-unregister-notifications~1]
            NotificationEvent::RemoveNotifyee { subscriber, topic } => {
                match notifications.remove_notifyee(&subscriber, &topic) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            NotificationEvent::StateChange {
                subscriber,
                topic,
                status,
                respond_to,
            } => {
                // [impl->dsn~usubscription-change-notification-type~1]
                let update = SubscriptionInfo::new(
                    topic.clone(),
                    subscriber.expect("msg"),
                    status,
                    None,
                    None,
                );

                // Send Update message to general notification channel
                // as per usubscription.proto RegisterForNotifications(NotificationsRequest)
                // [impl->dsn~usubscription-change-notification-topic~1]
                match UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                    .with_message_id(UUID::build())
                    .build_with_protobuf_payload(&update)
                {
                    Err(e) => {
                        error!("Error building global update notification message: {e}");
                    }
                    Ok(update_msg) => {
                        // [impl->dsn~usubscription-change-notification-topic~1]
                        if let Err(e) = up_transport.send(update_msg).await {
                            error!(
                                "Error sending global subscription-change update notification: {e}"
                            );
                        }
                    }
                }

                // Send Update notification message to any dedicated registered notification-subscribers
                // [impl->req~usubscription-register-notifications~1]
                if let Ok(subscribers) = notifications.get_subscribers_registered_for_topic(&topic)
                {
                    for subscribers_entry in subscribers {
                        debug!(
                            "Sending notification to ({}), about topic {} changing state to {}",
                            subscribers_entry.to_uri(INCLUDE_SCHEMA),
                            update.topic(),
                            update.status()
                        );

                        match UMessageBuilder::notification(
                            configuration.get_resource_uri(SOURCE_URI_RESOURCE_ID),
                            subscribers_entry.clone(),
                        )
                        .build_with_protobuf_payload(&update)
                        {
                            Ok(update_msg) => {
                                let _r = up_transport.send(update_msg).await.inspect_err(|e|
                                    warn!("Error sending susbcriber-specific subscription-change update notification: {e}")
                                );
                            }
                            Err(e) => {
                                error!("Error building susbcriber-specific update notification message: {e}");
                            }
                        }
                    }
                }

                let _ = respond_to.send(());
            }
            NotificationEvent::Reset { respond_to } => {
                if respond_to.send(notifications.reset()).is_err() {
                    error!("Problem with internal communication");
                }
            }
            NotificationEvent::GetNotificationTopics { respond_to } => {
                let _r = respond_to.send(
                    notifications
                        .get_data()
                        .expect("Error getting notification store contents"),
                );
            }
            #[cfg(test)]
            NotificationEvent::SetNotificationTopics {
                notification_topics_replacement,
                respond_to,
            } => {
                notifications
                    .set_data(notification_topics_replacement)
                    .expect("Error setting notification store contents");
                let _r = respond_to.send(());
            }
        }
    }
}

// Convenience wrapper for sending state change notification messages
// `susbcriber` is an Option, because in the case ob remote subscription state changes, there is no subscriber (other than local usubscription service)
pub(crate) async fn notify_state_change(
    notification_sender: Sender<NotificationEvent>,
    subscriber: Option<SubscriberUUri>,
    topic: TopicUUri,
    status: SubscriptionStatus,
) {
    let (respond_to, receive_from) = oneshot::channel::<()>();
    if let Err(e) = notification_sender
        .send(NotificationEvent::StateChange {
            subscriber,
            topic,
            status,
            respond_to,
        })
        .await
    {
        error!("Error initiating subscription-change update notification: {e}");
    }
    if let Err(e) = receive_from.await {
        // Not returning an error here, as update notification is not a core concern wrt the actual subscription management
        warn!("Error sending subscription-change update notification: {e}");
    };
}

// Convenience wrapper for resetting notification manager - returns all subscriber-topic notification registrations that were registered before the reset.
// Might return an empty list if data retrieval fails for some reason, but will perform the reset in any case.
pub(crate) async fn reset(
    notification_sender: Sender<NotificationEvent>,
) -> Result<Vec<(SubscriberUUri, TopicUUri)>, Box<dyn std::error::Error>> {
    // Get current notification registrations
    let (respond_to, receive_from) = oneshot::channel::<Vec<(SubscriberUUri, TopicUUri)>>();
    notification_sender
        .send(NotificationEvent::GetNotificationTopics { respond_to })
        .await?;
    #[allow(clippy::mutable_key_type)]
    let notification_registrations = receive_from.await.unwrap_or_default();

    // Do the reset
    let (respond_to, receive_from) = oneshot::channel::<Result<(), PersistencyError>>();
    notification_sender
        .send(NotificationEvent::Reset { respond_to })
        .await?;
    let _ = receive_from.await?;

    Ok(notification_registrations)
}
