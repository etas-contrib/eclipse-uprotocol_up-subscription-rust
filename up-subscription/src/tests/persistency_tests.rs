/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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
    use std::time::{Duration, SystemTime};
    use tokio::time::sleep;

    use crate::{persistency, test_lib, USubscriptionConfiguration};

    fn get_configuration() -> USubscriptionConfiguration {
        USubscriptionConfiguration::create(
            test_lib::helpers::LOCAL_AUTHORITY.to_string(),
            None,
            None,
            false,
            None,
        )
        .unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_get_and_prune_expiring_subscriptions() {
        let mut subscriptions = persistency::SubscriptionsStore::new(&get_configuration());

        // Prepare subscription persistency with two subscriptions, one with and one without expiry timestamp
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic1_uri(),
            None,
        );
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri2(),
            &test_lib::helpers::local_topic2_uri(),
            Some(SystemTime::now() - Duration::from_secs(3600)),
        );
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri3(),
            &test_lib::helpers::local_topic2_uri(),
            Some(SystemTime::now() + Duration::from_secs(1)),
        );

        let flattened_subscriptions = subscriptions
            .get_flattened_subscriptions()
            .expect("Error interacting with subscription persistency");
        assert!(
            flattened_subscriptions.len() == 3,
            "Incorrect number of persisted subscription relationships - should be 3"
        );

        // get all subscriptions that have a future expiration timestamp set - should be 1, as one has None and another has a past timestamp
        let list = subscriptions
            .get_and_prune_expiring_subscriptions()
            .expect("Error interacting with subscription persistency");
        assert!(
            list.len() == 1,
            "There should only be 1 subscription with an expiration timer in persistency"
        );

        // wait a moment, this should result in the one subscription expiration timestamp to pass
        sleep(Duration::from_millis(1500)).await;

        // now, the one timed subscription we had should be pruned by get_and_prune_expiring_subscriptions() function call, so list should be empty
        let list = subscriptions
            .get_and_prune_expiring_subscriptions()
            .expect("Error interacting with subscription persistency");
        assert!(
            list.is_empty(),
            "There should be no subscription with an expiration timestamp left at this point"
        );
    }

    // [utest->req~usubscription-reset~1]
    #[test_log::test(tokio::test)]
    async fn test_reset_subscriptions() {
        let mut subscriptions = persistency::SubscriptionsStore::new(&get_configuration());

        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic1_uri(),
            None,
        );
        let _ = subscriptions.add_subscription(
            &test_lib::helpers::subscriber_uri2(),
            &test_lib::helpers::local_topic2_uri(),
            Some(SystemTime::now() - Duration::from_secs(3600)),
        );

        let data = subscriptions.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 2);

        let r = subscriptions.reset();
        assert!(r.is_ok());

        let data = subscriptions.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 0);
    }

    // [utest->req~usubscription-reset~1]
    #[test_log::test(tokio::test)]
    async fn test_reset_remote_subscriptions() {
        let mut remote_topics = persistency::RemoteTopicsStore::new(&get_configuration());

        let _ = remote_topics.add_topic_or_get_state(&test_lib::helpers::local_topic1_uri());
        let _ = remote_topics.add_topic_or_get_state(&test_lib::helpers::local_topic2_uri());

        let data = remote_topics.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 2);

        let r = remote_topics.reset();
        assert!(r.is_ok());

        let data = remote_topics.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 0);
    }

    // [utest->req~usubscription-reset~1]
    #[test_log::test(tokio::test)]
    async fn test_reset_notifications() {
        let mut notifications = persistency::NotificationStore::new(&get_configuration());

        notifications
            .add_notifyee(
                &test_lib::helpers::subscriber_uri1(),
                &test_lib::helpers::local_topic1_uri(),
            )
            .expect("Error adding test data set");

        notifications
            .add_notifyee(
                &test_lib::helpers::subscriber_uri2(),
                &test_lib::helpers::local_topic2_uri(),
            )
            .expect("Error adding test data set");

        let data = notifications.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 2);

        let r = notifications.reset();
        assert!(r.is_ok());

        let data = notifications.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_one_subscriber_multiple_topics() {
        let mut notifications = persistency::NotificationStore::new(&get_configuration());

        let _ = notifications.add_notifyee(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic1_uri(),
        );
        let _ = notifications.add_notifyee(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic2_uri(),
        );

        // Should be two entries
        let data = notifications.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_multiple_subscribers_one_topic() {
        let mut notifications = persistency::NotificationStore::new(&get_configuration());

        let _ = notifications.add_notifyee(
            &test_lib::helpers::subscriber_uri1(),
            &test_lib::helpers::local_topic1_uri(),
        );
        let _ = notifications.add_notifyee(
            &test_lib::helpers::subscriber_uri2(),
            &test_lib::helpers::local_topic1_uri(),
        );

        // Should be two entries
        let data = notifications.get_data();
        assert!(data.is_ok());
        assert_eq!(data.unwrap().len(), 2);
    }
}
