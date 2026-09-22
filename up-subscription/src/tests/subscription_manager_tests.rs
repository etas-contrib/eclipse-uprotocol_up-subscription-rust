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

// [utest->dsn~usubscription-state-machine~1]
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use std::vec;

    use test_case::test_case;
    use tokio::sync::{mpsc, mpsc::Sender, oneshot, Notify};
    use tracing::debug;

    use up_rust::{
        communication::SubscriptionStatus,
        core::usubscription::{SubscribeRequest, SubscribeResponse, UnsubscribeRequest},
        ProtobufMappable, UStatus, UUri,
    };

    use crate::subscription_manager::SubscriptionEntry;
    use crate::test_lib::{helpers::*, mocks::MockRpcClientMock};
    use crate::{
        configuration::DEFAULT_COMMAND_BUFFER_SIZE,
        helpers,
        notification_manager::NotificationEvent,
        persistency,
        subscription_manager::{self, InternalSubscriptionEvent, SubscriptionEvent},
        test_lib,
        usubscription::{ExpirationTimestamp, SubscriberUUri, TopicUUri},
        USubscriptionConfiguration,
    };

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<SubscriptionEvent>,
        notification_sender: Option<Sender<NotificationEvent>>,
        shutdowner: Arc<Notify>,
    }

    impl CommandSender {
        fn new() -> Self {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let rpc_client = Arc::new(MockRpcClientMock::default());
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());
            let (notification_sender, mut notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            // Spawn notification receiver task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                loop {
                    tokio::select! {
                        Some(event) = notification_receiver.recv() => {
                            if let NotificationEvent::StateChange { subscriber, topic, status, respond_to } = event {
                               debug!(
                                    "Change Notification received: {} - {} - {}",
                                    subscriber.unwrap().to_uri(true),
                                    topic.to_uri(true),
                                    status
                                );

                                let _ = respond_to.send(());
                            }
                            else {
                                panic!("Expected a NotificationEvent::StateChange message, got something else")
                            }
                        },
                        _ = shutdown_notification_cloned.notified() => break,
                    };
                }
                Ok(())
            });

            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                subscription_manager::handle_message(
                    config.clone(),
                    rpc_client,
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });
            CommandSender {
                command_sender,
                notification_sender: None,
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected notifications during test
        async fn new_with_expected_notifications(
            mut expected_notifications: Vec<NotificationEvent>,
        ) -> Self {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let rpc_client = Arc::new(MockRpcClientMock::default());
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());
            let (notification_sender, mut notification_receiver) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            // Spawn notification receiver task
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                #[allow(clippy::mutable_key_type)]
                let mut notification_topics: Vec<(SubscriberUUri, TopicUUri)> = Vec::new();
                loop {
                    tokio::select! {
                        Some(event) = notification_receiver.recv() => {
                            match event {
                                NotificationEvent::StateChange { .. } => {
                                    if let Some(pos) = expected_notifications.iter().position(|e| e == &event) {
                                        if let NotificationEvent::StateChange { subscriber, status, topic, respond_to } = event {
                                            debug!(
                                                "Change Notification received: {} - {} - {}",
                                                subscriber.expect("subscriber Uri for subscription change event must not be None").to_uri(true),
                                                topic.to_uri(true),
                                                status
                                            );
                                            // This is the ack response to the entity that initiated the notification to be send (e.g. subscription manager)
                                            let _ = respond_to.send(());
                                        }
                                        // Send ack back to test case that was providing the expected_notifications return channel
                                        let matched = expected_notifications.remove(pos);
                                        if let NotificationEvent::StateChange { respond_to, .. } = matched {
                                            let _ = respond_to.send(());
                                        }
                                    }
                                },
                                NotificationEvent::SetNotificationTopics { notification_topics_replacement, respond_to } => {
                                    debug!("Notification Manager SetNotificationTopic command received");
                                    notification_topics = notification_topics_replacement;
                                    let _ = respond_to.send(());
                                },
                                NotificationEvent::GetNotificationTopics { respond_to } =>  {
                                    debug!("Notification Manager GetNotificationTopic command received");
                                    let _ = respond_to.send(notification_topics.clone());
                                },
                                NotificationEvent::Reset { respond_to } => {
                                    debug!("Notification Manager Reset command received");
                                    let _ = respond_to.send(Ok(()));
                                }
                                _ => panic!("Received unexpected notification event: {event:?}")
                            }
                        },
                        _ = shutdown_notification_cloned.notified() => {
                            debug!("Shutting down notification reception loop");
                            break;
                        },
                    };
                }
                Ok(())
            });

            // Spawn off subscription manager task
            let notification_sender_cloned = notification_sender.clone();
            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                subscription_manager::handle_message(
                    config.clone(),
                    rpc_client,
                    command_receiver,
                    notification_sender_cloned,
                    shutdown_notification_cloned,
                )
                .await;

                Ok(())
            });

            CommandSender {
                command_sender,
                notification_sender: Some(notification_sender),
                shutdowner: shutdown_notification,
            }
        }

        // Allows configuration of expected invoke_method() calls from subscription manager (provide expected request and response for utransport mock)
        // Useful e.g. for testing remote subscription operations, where subscription manager is expected to invoke methods on other uEntities
        async fn new_with_client_options<R, S>(expected_request: R, expected_response: S) -> Self
        where
            R: ProtobufMappable + Clone + Send + Sync + 'static,
            S: ProtobufMappable + Clone + Send + Sync + 'static,
        {
            let config = Arc::new(
                USubscriptionConfiguration::create(
                    test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                    None,
                    None,
                    false,
                    None,
                )
                .unwrap(),
            );
            let shutdown_notification = Arc::new(Notify::new());

            let (command_sender, command_receiver) =
                mpsc::channel::<SubscriptionEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());

            let rpc_client = Arc::new(MockRpcClientMock::default());
            // TODO map in expected requests and responses
            // let mock_transport = Arc::new(
            //     test_lib::mocks::utransport_mock_for_rpc(vec![(
            //         expected_request,
            //         expected_response,
            //     )])
            //     .await,
            // );
            let (notification_sender, _) =
                mpsc::channel::<NotificationEvent>(config.notification_command_buffer.into());

            let shutdown_notification_cloned = shutdown_notification.clone();
            helpers::spawn_and_log_error(async move {
                subscription_manager::handle_message(
                    config,
                    rpc_client,
                    command_receiver,
                    notification_sender,
                    shutdown_notification_cloned,
                )
                .await;
                Ok(())
            });

            CommandSender {
                command_sender,
                notification_sender: None,
                shutdowner: shutdown_notification,
            }
        }

        async fn shutdown(&self) {
            self.shutdowner.notify_waiters();
        }

        async fn subscribe(
            &self,
            topic: TopicUUri,
            subscriber: SubscriberUUri,
            expiration: Option<ExpirationTimestamp>,
            sample_period: Option<Duration>,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::AddSubscription {
                subscriber,
                topic,
                expiration,
                sample_period,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn unsubscribe(
            &self,
            topic: TopicUUri,
            subscriber: SubscriberUUri,
        ) -> Result<SubscriptionStatus, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<SubscriptionStatus>();
            let command = SubscriptionEvent::RemoveSubscription {
                subscriber,
                topic,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn fetch_subscribers(
            &self,
            subscriber_filter: UUri,
            topic_filter: UUri,
        ) -> Result<Vec<SubscriptionEntry>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<Vec<SubscriptionEntry>>();
            let command = SubscriptionEvent::FetchSubscriptions {
                subscriber_filter,
                topic_filter,
                respond_to,
            };
            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_topic_subscribers(
            &self,
        ) -> Result<persistency::SubscriptionSet, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<persistency::SubscriptionSet>();
            let command = SubscriptionEvent::GetTopicSubscribers { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_topic_subscribers(
            &self,
            topic_subscribers_replacement: persistency::SubscriptionSet,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetTopicSubscribers {
                topic_subscribers_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_topics(
            &self,
        ) -> Result<HashMap<TopicUUri, SubscriptionStatus>, Box<dyn Error>> {
            let (respond_to, receive_from) =
                oneshot::channel::<HashMap<TopicUUri, SubscriptionStatus>>();
            let command = SubscriptionEvent::GetRemoteTopics { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_remote_topics(
            &self,
            remote_topics_replacement: HashMap<TopicUUri, SubscriptionStatus>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::SetRemoteTopics {
                topic_subscribers_replacement: remote_topics_replacement,
                respond_to,
            };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_notification_topics(
            &self,
            notification_topics_replacement: Vec<(SubscriberUUri, TopicUUri)>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = NotificationEvent::SetNotificationTopics {
                notification_topics_replacement,
                respond_to,
            };
            self.notification_sender
                .as_ref()
                .unwrap()
                .send(command)
                .await?;
            Ok(receive_from.await?)
        }

        async fn reset(&self) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            let command = SubscriptionEvent::Reset { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }

        async fn get_remote_subcription_change_sender(
            &self,
        ) -> Result<Sender<InternalSubscriptionEvent>, Box<dyn Error>> {
            let (respond_to, receive_from) =
                oneshot::channel::<Sender<InternalSubscriptionEvent>>();
            let command = SubscriptionEvent::GetRemoteSubscriptionChangeSender { respond_to };

            self.command_sender.send(command).await?;
            Ok(receive_from.await?)
        }
    }

    // [utest->req~usubscription-subscribe~1]
    // [utest->req~usubscription-subscribe-multiple~1]
    #[test_case(vec![(local_topic1_uri(), subscriber_uri1())]; "Default subscriber-topic")]
    #[test_case(vec![(local_topic1_uri(), subscriber_uri1()), (local_topic1_uri(), subscriber_uri1())]; "Multiple identical subscriber-topic combinations")]
    #[test_case(vec![
         (local_topic1_uri(), subscriber_uri1()), (local_topic1_uri(), subscriber_uri2()),
         (local_topic2_uri(), subscriber_uri1()), (local_topic2_uri(), subscriber_uri2())
         ]; "Multiple susbcriber-topic combinations")]
    #[test_log::test(tokio::test)]
    async fn test_subscribe(topic_subscribers: Vec<(TopicUUri, SubscriberUUri)>) {
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        for (topic, subscriber) in topic_subscribers {
            desired_state
                .entry(topic.clone())
                .or_default()
                .insert(subscriber.clone(), None);

            // Operation to test
            let result = command_sender
                .subscribe(topic, subscriber, None, None)
                .await;
            assert!(result.is_ok());

            // Verify operation result content
            assert_eq!(result.unwrap(), SubscriptionStatus::Subscribed);
        }

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), desired_state.len());
        assert_eq!(topic_subscribers, desired_state);
    }

    // [utest->req~usubscription-subscribe-expiration~1]
    // [utest->req~usubscription-subscribe-no-expiration~1]
    #[test_log::test(tokio::test)]
    async fn test_subscribe_with_expiry() {
        let command_sender = CommandSender::new();

        // Prepare things
        let mut desired_state: Vec<(SubscriberUUri, TopicUUri, Option<ExpirationTimestamp>)> = vec![
            (
                test_lib::helpers::subscriber_uri1(),
                test_lib::helpers::local_topic1_uri(),
                // subscription with no expiration property
                None,
            ),
            (
                test_lib::helpers::subscriber_uri2(),
                test_lib::helpers::local_topic2_uri(),
                // expired subscription
                Some(SystemTime::now() - Duration::from_secs(1)),
            ),
            (
                test_lib::helpers::subscriber_uri3(),
                test_lib::helpers::local_topic2_uri(),
                // yet to expire subscription
                Some(SystemTime::now() + Duration::from_secs(1)),
            ),
        ];

        for (subscriber, topic, expiry) in desired_state.iter() {
            // Operation to test
            let result = command_sender
                .subscribe(topic.clone(), subscriber.clone(), *expiry, None)
                .await;
            assert!(result.is_ok());

            // Verify operation result content
            assert_eq!(result.unwrap(), SubscriptionStatus::Subscribed);
        }

        // Verify iternal bookeeping
        let actual_subscribers = command_sender.get_topic_subscribers().await;
        assert!(actual_subscribers.is_ok());

        let flattened_subscribers: Vec<(SubscriberUUri, TopicUUri, Option<ExpirationTimestamp>)> =
            actual_subscribers
                .unwrap()
                .iter()
                .flat_map(|(outer_key, inner_map)| {
                    inner_map.iter().map(move |(inner_key, value)| {
                        (outer_key.clone(), inner_key.clone(), *value)
                    })
                })
                .collect();

        desired_state.remove(1); // Remote item that has expiry timestamp in the past, so hasn't been added by subscription manager
        assert_eq!(flattened_subscribers.len(), desired_state.len());

        for (topic, subscriber, expiry) in flattened_subscribers {
            assert!(desired_state.contains(&(subscriber, topic, expiry)));
        }
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-subscribe-remote-pending~1]
    // [utest->req~usubscription-subscribe-remote-response~1]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriptionStatus::SubscribePending; "Remote topic, remote state SubscribePending")]
    #[test_case(test_lib::helpers::remote_topic1_uri(), SubscriptionStatus::Subscribed; "Remote topic, remote state Subscribed")]
    #[test_log::test(tokio::test)]
    async fn test_remote_subscribe(remote_topic: TopicUUri, remote_state: SubscriptionStatus) {
        // Prepare things
        let remote_subscribe_request = SubscribeRequest {
            topic: remote_topic.clone(),
            expiration: None,
            sample_period: None,
        };
        let remote_subscribe_response = SubscribeResponse {
            topic: remote_topic.clone(),
            status: remote_state,
        };
        let command_sender = CommandSender::new_with_client_options::<
            SubscribeRequest,
            SubscribeResponse,
        >(remote_subscribe_request, remote_subscribe_response)
        .await;

        // Operation to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            subscription_status == SubscriptionStatus::SubscribePending
                || subscription_status == SubscriptionStatus::Subscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be SUBSCRIBE_PENDING or SUBSCRIBED
        assert!(
            *remote_topics.get(&remote_topic.clone()).unwrap()
                == SubscriptionStatus::SubscribePending
                || *remote_topics.get(&remote_topic.clone()).unwrap()
                    == SubscriptionStatus::Subscribed
        );
    }

    // [utest->req~usubscription-subscribe-remote~1]
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[test_log::test(tokio::test)]
    async fn test_repeated_remote_subscribe() {
        // Prepare things
        let remote_topic = test_lib::helpers::remote_topic1_uri();
        let remote_subscribe_request = SubscribeRequest {
            topic: remote_topic.clone(),
            expiration: None,
            sample_period: None,
        };
        let remote_subscribe_response = SubscribeResponse {
            topic: remote_topic.clone(),
            status: SubscriptionStatus::Subscribed,
        };
        let command_sender = CommandSender::new_with_client_options::<
            SubscribeRequest,
            SubscribeResponse,
        >(remote_subscribe_request, remote_subscribe_response)
        .await;

        // Operation to test
        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri1(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        let result = command_sender
            .subscribe(
                remote_topic.clone(),
                test_lib::helpers::subscriber_uri2(),
                None,
                None,
            )
            .await;
        assert!(result.is_ok());

        // Assert we have two local topic-subscriber entries...
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        let entry = topic_subscribers.get(&remote_topic);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().len(), 2);

        // ... and one remote topic entry
        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        assert_eq!(remote_topics.len(), 1);
    }

    // All subscribers for a topic unsubscribe
    // [utest->req~usubscription-unsubscribe~1]
    #[test_log::test(tokio::test)]
    async fn test_final_unsubscribe() {
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status, SubscriptionStatus::Unsubscribed);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 0);
    }

    // Only some subscribers of a topic unsubscribe
    #[test_log::test(tokio::test)]
    async fn test_partial_unsubscribe() {
        let command_sender = CommandSender::new();

        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(
                test_lib::helpers::local_topic1_uri(),
                test_lib::helpers::subscriber_uri1(),
            )
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(subscription_status, SubscriptionStatus::Unsubscribed);

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        assert_eq!(topic_subscribers.len(), 1);
        assert_eq!(
            topic_subscribers
                .get(&test_lib::helpers::local_topic1_uri())
                .unwrap()
                .len(),
            1
        );
        assert!(topic_subscribers
            .get(&test_lib::helpers::local_topic1_uri())
            .unwrap()
            .contains_key(&test_lib::helpers::subscriber_uri2()));
    }

    // All subscribers for a remote topic unsubscribe
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    #[test_log::test(tokio::test)]
    async fn test_final_remote_unsubscribe() {
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things
        let remote_unsubscribe_request = UnsubscribeRequest {
            topic: remote_topic.clone(),
        };
        let remote_unsubscribe_response = UStatus::ok();
        let command_sender = CommandSender::new_with_client_options::<UnsubscribeRequest, UStatus>(
            remote_unsubscribe_request,
            remote_unsubscribe_response,
        )
        .await;

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<TopicUUri, SubscriptionStatus> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), SubscriptionStatus::Subscribed);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(
            subscription_status,
            // No matter what happens to the remove topic state, as far as the local client is concerned this is now an UNSUBSCRIBED topic
            SubscriptionStatus::Unsubscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        // We're expecting our local topic-subscriber tracker to be empty at this point
        assert_eq!(topic_subscribers.len(), 0);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let entry = remote_topics.get(&remote_topic);
        assert!(entry.is_some());
        let state = entry.unwrap();
        // Depending on timing of the various async operations involved in remote subscriptions and bookkeeping updates,
        // this might be UNSUBSCRIBE_PENDING or UNSUBSCRIBED
        assert!(
            *state == SubscriptionStatus::Unsubscribed
                || *state == SubscriptionStatus::UnsubscribePending
        );
    }

    // Some subscribers for a remote topic unsubscribe, but at least one subscriber is left
    // [utest->req~usubscription-unsubscribe-last-remote~1]
    // [utest->req~usubscription-unsubscribe-remote-unsubscribed~1]
    #[test_log::test(tokio::test)]
    async fn test_partial_remote_unsubscribe() {
        let remote_topic = test_lib::helpers::remote_topic1_uri();

        // Prepare things - we're not expecting any remote-unsubscribe action in this case
        let command_sender = CommandSender::new();

        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(remote_topic.clone()).or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);
        entry.insert(test_lib::helpers::subscriber_uri2(), None);

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        #[allow(clippy::mutable_key_type)]
        let mut desired_remote_state: HashMap<TopicUUri, SubscriptionStatus> = HashMap::new();
        desired_remote_state.insert(remote_topic.clone(), SubscriptionStatus::Subscribed);
        command_sender
            .set_remote_topics(desired_remote_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender
            .unsubscribe(remote_topic.clone(), test_lib::helpers::subscriber_uri1())
            .await;
        assert!(result.is_ok());

        // Verify operation result content
        let subscription_status = result.unwrap();
        assert_eq!(
            subscription_status,
            // this client immediately is getting UNSUBSCRIBED, no _PENDING, as for it the op is done
            SubscriptionStatus::Unsubscribed
        );

        // Verify iternal bookeeping
        let topic_subscribers = command_sender.get_topic_subscribers().await;
        assert!(topic_subscribers.is_ok());
        #[allow(clippy::mutable_key_type)]
        let topic_subscribers = topic_subscribers.unwrap();
        // We're expecting one of the two original subscribers to still be tracked at this point
        assert_eq!(topic_subscribers.len(), 1);

        let remote_topics = command_sender.get_remote_topics().await;
        assert!(remote_topics.is_ok());
        #[allow(clippy::mutable_key_type)]
        let remote_topics = remote_topics.unwrap();
        // our remote topic status tracker should still track this topic, and...
        assert_eq!(remote_topics.len(), 1);

        let entry = remote_topics.get(&remote_topic);
        assert!(entry.is_some());
        let state = entry.unwrap();
        // ... it should still be in state SUBSCRIBED, as there is still another subscriber left
        assert_eq!(*state, SubscriptionStatus::Subscribed);
    }

    // [utest->req~usubscription-subscribe-notifications~1]
    // [utest->dsn~usubscription-change-notification-update~1]
    #[test_log::test(tokio::test)]
    async fn test_local_subscribe_notification() {
        // Prepare things
        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus::Subscribed,
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        // Operation to test
        let result = command_sender
            .subscribe(topic, subscriber, None, None)
            .await;
        assert!(result.is_ok());

        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->dsn~usubscription-change-notification-update~1]
    #[test_log::test(tokio::test)]
    async fn test_local_unsubscribe_notification() {
        // Prepare things
        // Prepare things
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state
            .entry(test_lib::helpers::local_topic1_uri())
            .or_default();
        entry.insert(test_lib::helpers::subscriber_uri1(), None);

        let topic = test_lib::helpers::local_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus::Unsubscribed,
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        command_sender
            .set_topic_subscribers(desired_state)
            .await
            .expect("Interaction with subscription handler broken");

        // Operation to test
        let result = command_sender.unsubscribe(topic, subscriber).await;
        assert!(result.is_ok());

        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // TODO: Let's see if this is actually covered by a requirement - otherwise it should go
    // #[test_log::test(tokio::test)]
    // async fn test_remote_subscribe_notification() {
    //     ;

    //     // Prepare things
    //     let topic = test_lib::helpers::remote_topic1_uri();
    //     let (respond_to, state_changed) = oneshot::channel::<()>();

    //     let expected_notification = NotificationEvent::StateChange {
    //         subscriber: None,
    //         topic: topic.clone(),
    //         status: SubscriptionStatus {
    //             state: State::SUBSCRIBE_PENDING.into(),
    //             ..Default::default()
    //         },
    //         respond_to,
    //     };

    //     let command_sender =
    //         CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

    //     let sender = command_sender
    //         .get_remote_subcription_change_sender()
    //         .await
    //         .expect("Error retrieving remote-subscription change event command channel");

    //     // Initiate notification event
    //     let _ = sender
    //         .send(InternalSubscriptionEvent::TopicStateUpdate {
    //             topic: topic.clone(),
    //             state: State::SUBSCRIBE_PENDING,
    //         })
    //         .await;

    //     // ensure that we have run through all the async layers and reached the notification assertion statements
    //     let _ = state_changed.await;
    //     command_sender.shutdown().await;
    // }

    // [utest->dsn~usubscription-change-notification-update~1]
    #[test_log::test(tokio::test)]
    async fn test_state_change_notification() {
        // Prepare things
        let topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to, state_changed) = oneshot::channel::<()>();

        let expected_notification = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus::UnsubscribePending,
            respond_to,
        };

        let command_sender =
            CommandSender::new_with_expected_notifications(vec![expected_notification]).await;

        // We need a subscriber to topic, which we're subsequently expecting a state change notification to be sent to
        // let subscribers = HashMap<TopicUUri, HashMap<SubscriberUUri, Option<ExpiryTimestamp>>>::new();
        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(topic.clone()).or_default();
        entry.insert(subscriber.clone(), None);
        assert!(command_sender
            .set_topic_subscribers(desired_state)
            .await
            .is_ok());

        let sender = command_sender
            .get_remote_subcription_change_sender()
            .await
            .expect("Error retrieving remote-subscription change event command channel");

        // Initiate notification event
        let _ = sender
            .send(InternalSubscriptionEvent::TopicStateUpdate {
                topic: topic.clone(),
                state: SubscriptionStatus::UnsubscribePending,
            })
            .await;

        // ensure that we have run through all the async layers and reached the notification assertion statements
        let _ = state_changed.await;
        command_sender.shutdown().await;
    }

    // [utest->req~usubscription-reset~1]
    #[test_log::test(tokio::test)]
    async fn test_reset_notifications() {
        // Prepare things
        let topic = test_lib::helpers::remote_topic1_uri();
        let subscriber = test_lib::helpers::subscriber_uri1();
        let (respond_to_topic_subscriber, state_changed_topic_subscriber) =
            oneshot::channel::<()>();
        let (respond_to_notification_registrar, state_changed_notification_registrar) =
            oneshot::channel::<()>();

        let expected_notification_topic_subscriber = NotificationEvent::StateChange {
            subscriber: subscriber.clone().into(),
            topic: topic.clone(),
            status: SubscriptionStatus::Unsubscribed,
            respond_to: respond_to_topic_subscriber,
        };
        let expected_notification_notification_registrar = NotificationEvent::StateChange {
            subscriber: test_lib::helpers::subscriber_uri2().into(),
            topic: test_lib::helpers::local_topic2_uri(),
            status: SubscriptionStatus::Unsubscribed,
            respond_to: respond_to_notification_registrar,
        };

        let command_sender = CommandSender::new_with_expected_notifications(vec![
            expected_notification_topic_subscriber,
            expected_notification_notification_registrar,
        ])
        .await;

        // We need a subscriber to topic, which we're subsequently expecting a state change notification to be sent to on reset
        // let subscribers = HashMap<TopicUUri, HashMap<SubscriberUUri, Option<ExpiryTimestamp>>>::new();
        // set starting state
        #[allow(clippy::mutable_key_type)]
        let mut desired_state: persistency::SubscriptionSet = HashMap::new();
        #[allow(clippy::mutable_key_type)]
        let entry = desired_state.entry(topic.clone()).or_default();
        entry.insert(subscriber.clone(), None);
        assert!(command_sender
            .set_topic_subscribers(desired_state)
            .await
            .is_ok());

        // Add a notification-registration to the mock backend, for which we subsequently expect a notifcation on reset
        #[allow(clippy::mutable_key_type)]
        let notification_topics_replacement: Vec<(SubscriberUUri, TopicUUri)> = vec![(
            test_lib::helpers::subscriber_uri2(),
            test_lib::helpers::local_topic2_uri(),
        )];
        assert!(command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .is_ok());

        // Perform reset
        command_sender
            .reset()
            .await
            .expect("Error performing reset command");

        // ensure that we have run through all the async layers and reached the notification assertion statements
        assert!(state_changed_topic_subscriber.await.is_ok());
        assert!(state_changed_notification_registrar.await.is_ok());

        // Assert that topic subscriber lists is empty after reset
        // (We don't do the same thing for notification manager, because that is entirely mocked in the context of this suite of tests)
        #[allow(clippy::mutable_key_type)]
        let subscribers = command_sender
            .get_topic_subscribers()
            .await
            .expect("Error retrieving subscriber list after reset");
        assert!(subscribers.is_empty());

        command_sender.shutdown().await;
    }
}
