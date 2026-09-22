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
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use tokio::sync::{
        mpsc::{self, Sender},
        oneshot, Notify,
    };

    use up_rust::{
        communication::SubscriptionStatus,
        core::usubscription::{
            usubscription_uri, SubscriptionInfo, RESOURCE_ID_SUBSCRIPTION_CHANGE,
        },
        LocalUriProvider, UMessage, UMessageBuilder, UUID,
    };

    use crate::{
        configuration::DEFAULT_COMMAND_BUFFER_SIZE,
        helpers,
        notification_manager::{notification_engine, NotificationEvent, SOURCE_URI_RESOURCE_ID},
        test_lib,
        usubscription::{SubscriberUUri, TopicUUri},
        USubscriptionConfiguration,
    };

    // Simple subscription-manager-actor front-end to use for testing
    struct CommandSender {
        command_sender: Sender<NotificationEvent>,
    }

    impl CommandSender {
        fn get_config() -> USubscriptionConfiguration {
            USubscriptionConfiguration::create(
                test_lib::helpers::LOCAL_AUTHORITY.to_string(),
                None,
                None,
                false,
                None,
            )
            .unwrap()
        }

        fn new(config: Arc<USubscriptionConfiguration>, expected_message: Vec<UMessage>) -> Self {
            let shutdown_notification = Arc::new(Notify::new());
            let (command_sender, command_receiver) =
                mpsc::channel::<NotificationEvent>(DEFAULT_COMMAND_BUFFER_SIZE.into());
            let transport_mock =
                test_lib::mocks::utransport_mock_for_notification_manager(expected_message);

            helpers::spawn_and_log_error(async move {
                notification_engine(
                    config,
                    Arc::new(transport_mock),
                    command_receiver,
                    shutdown_notification,
                )
                .await;
                Ok(())
            });
            CommandSender { command_sender }
        }

        async fn add_notifyee(
            &self,
            subscriber: SubscriberUUri,
            topic: TopicUUri,
        ) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::AddNotifyee { subscriber, topic })
                .await?)
        }

        async fn remove_notifyee(
            &self,
            subscriber: SubscriberUUri,
            topic: TopicUUri,
        ) -> Result<(), Box<dyn Error>> {
            Ok(self
                .command_sender
                .send(NotificationEvent::RemoveNotifyee { subscriber, topic })
                .await?)
        }

        async fn state_change(
            &self,
            subscriber: SubscriberUUri,
            topic: TopicUUri,
            status: SubscriptionStatus,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();

            self.command_sender
                .send(NotificationEvent::StateChange {
                    subscriber: Some(subscriber),
                    topic,
                    status,
                    respond_to,
                })
                .await?;

            Ok(receive_from.await?)
        }

        async fn get_notification_topics(
            &self,
        ) -> Result<Vec<(SubscriberUUri, TopicUUri)>, Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<Vec<(SubscriberUUri, TopicUUri)>>();
            self.command_sender
                .send(NotificationEvent::GetNotificationTopics { respond_to })
                .await?;

            Ok(receive_from.await?)
        }

        #[allow(clippy::mutable_key_type)]
        async fn set_notification_topics(
            &self,
            notification_topics_replacement: Vec<(SubscriberUUri, TopicUUri)>,
        ) -> Result<(), Box<dyn Error>> {
            let (respond_to, receive_from) = oneshot::channel::<()>();
            self.command_sender
                .send(NotificationEvent::SetNotificationTopics {
                    respond_to,
                    notification_topics_replacement,
                })
                .await?;

            Ok(receive_from.await?)
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_add_notifyee() {
        let command_sender = CommandSender::new(Arc::new(CommandSender::get_config()), vec![]);

        let expected_subscriber = test_lib::helpers::subscriber_uri1();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        command_sender
            .add_notifyee(expected_subscriber.clone(), expected_topic.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 1);
        assert!(notification_topics.contains(&(expected_subscriber, expected_topic)));
    }

    #[test_log::test(tokio::test)]
    async fn test_remove_notifyee() {
        let command_sender = CommandSender::new(Arc::new(CommandSender::get_config()), vec![]);

        // prepare things
        let expected_subscriber = test_lib::helpers::subscriber_uri1();
        let expected_topic = test_lib::helpers::local_topic1_uri();

        #[allow(clippy::mutable_key_type)]
        let notification_topics_replacement: Vec<(SubscriberUUri, TopicUUri)> =
            vec![(expected_subscriber.clone(), expected_topic.clone())];

        command_sender
            .set_notification_topics(notification_topics_replacement)
            .await
            .expect("Error communicating with subscription manager");

        // operation to test
        command_sender
            .remove_notifyee(expected_subscriber.clone(), expected_topic.clone())
            .await
            .expect("Error communicating with subscription manager");

        #[allow(clippy::mutable_key_type)]
        let notification_topics = command_sender
            .get_notification_topics()
            .await
            .expect("Error communicating with subscription manager");

        assert_eq!(notification_topics.len(), 0);
    }

    // [utest->dsn~usubscription-change-notification-type~1]
    // [utest->dsn~usubscription-change-notification-topic~1]
    #[test_log::test(tokio::test)]
    async fn test_state_change() {
        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus::Subscribed;
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let changing_subscriber = test_lib::helpers::subscriber_uri1();

        // the update message that we're expecting
        let expected_update = SubscriptionInfo::new(
            changing_topic.clone(),
            changing_subscriber.clone(),
            changing_status.clone(),
            None,
            None,
        );

        // this is the generic update channel notification, that always is sent
        // [utest->dsn~usubscription-change-notification-topic~1]
        let expected_message_general_channel =
            UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        let command_sender = CommandSender::new(
            Arc::new(CommandSender::get_config()),
            vec![expected_message_general_channel],
        );

        // operation to test
        let r = command_sender
            .state_change(changing_subscriber, changing_topic, changing_status)
            .await;
        assert!(r.is_ok())
    }

    // [utest->req~usubscription-register-notifications~1]
    #[test_log::test(tokio::test)]
    async fn test_state_change_direct_notification() {
        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus::Subscribed;
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let interested_subscriber = test_lib::helpers::subscriber_uri1();

        // the update message that we're expecting
        let expected_update = SubscriptionInfo::new(
            changing_topic.clone(),
            interested_subscriber.clone(),
            changing_status.clone(),
            None,
            None,
        );

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        // this is the expected direct notification message
        let config = Arc::new(CommandSender::get_config());
        let expected_notitication_message = UMessageBuilder::notification(
            config.get_resource_uri(SOURCE_URI_RESOURCE_ID),
            interested_subscriber.clone(),
        )
        .with_message_id(UUID::build())
        .build_with_protobuf_payload(&expected_update)
        .unwrap();

        // Set up test rig, to expect both general-channel and direct notification messages
        let command_sender = CommandSender::new(
            config,
            vec![
                expected_message_general_channel,
                expected_notitication_message,
            ],
        );

        command_sender
            .add_notifyee(interested_subscriber.clone(), changing_topic.clone())
            .await
            .expect("Error preparing test case context");

        // operation to test - we now expect two messages, the general-channel notification as well as the direct notification to subscriber1
        let r = command_sender
            .state_change(interested_subscriber, changing_topic, changing_status)
            .await;
        assert!(r.is_ok())
    }

    // [utest->req~usubscription-unregister-notifications~1]
    #[test_log::test(tokio::test)]
    async fn test_unregister_direct_notification() {
        // prepare things
        // this is the status&topic&subscriber that the notification is about
        let changing_status = SubscriptionStatus::Subscribed;
        let changing_topic = test_lib::helpers::local_topic1_uri();
        let interested_subscriber = test_lib::helpers::subscriber_uri1();

        // the update message that we're expecting
        let expected_update = SubscriptionInfo::new(
            changing_topic.clone(),
            interested_subscriber.clone(),
            changing_status.clone(),
            None,
            None,
        );

        // this is the generic update channel notification, that always is sent
        let expected_message_general_channel =
            UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&expected_update)
                .unwrap();

        // Set up test rig, to expect only general-channel message
        let config = Arc::new(CommandSender::get_config());
        let command_sender = CommandSender::new(config, vec![expected_message_general_channel]);

        // pre-set notification manager with direct-notification request for topic1 by subscriber1
        command_sender
            .set_notification_topics(vec![(
                interested_subscriber.clone(),
                changing_topic.clone(),
            )])
            .await
            .expect("Error preparing test case context");

        // check that we're really set up for 1 direct notification of subscriber1 for topic1
        let list = command_sender
            .get_notification_topics()
            .await
            .expect("Error preparing test case context");
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.first().expect("Error preparing test case context"),
            &(interested_subscriber.clone(), changing_topic.clone())
        );

        // Unsubscribe subscriber1 for notifications on topic1
        command_sender
            .remove_notifyee(interested_subscriber.clone(), changing_topic.clone())
            .await
            .expect("Error preparing test case context");

        // operation to test - we're now only expecting the general-channel notification message
        let r = command_sender
            .state_change(interested_subscriber, changing_topic, changing_status)
            .await;
        assert!(r.is_ok())
    }
}
