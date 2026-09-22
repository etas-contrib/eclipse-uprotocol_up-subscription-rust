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

use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use std::{collections::HashMap, path::PathBuf, time::SystemTime};

use up_rust::{communication::SubscriptionStatus as TopicState, UUri};

use crate::{
    usubscription::{SubscriberUUri, TopicUUri},
    ExpirationTimestamp, USubscriptionConfiguration,
};

// Whether to include 'up:' in serialized UUris
const PERSIST_UP_SCHEMA: bool = true;

// For better code clarity
type SubscriberAsString = String;
type SerializedTopicState = u8;

pub(crate) type SubscriptionSet =
    HashMap<TopicUUri, HashMap<SubscriberUUri, Option<ExpirationTimestamp>>>;

#[derive(Debug)]
pub(crate) enum PersistencyError {
    InternalError(String),
    SerializationError(String),
}

impl PersistencyError {
    pub(crate) fn serialization_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::SerializationError(message.into())
    }

    pub(crate) fn internal_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::InternalError(message.into())
    }
}

impl std::fmt::Display for PersistencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SerializationError(e) => f.write_fmt(format_args!("Serialization error: {e}")),
            Self::InternalError(e) => f.write_fmt(format_args!("Internal error: {e}")),
        }
    }
}

impl std::error::Error for PersistencyError {}

/// Persistent store for tracking subscriber-topic relationships
pub(crate) struct SubscriptionsStore {
    persistency: PickleDb,
}

// [impl->req~usubscription-subscribe-persistency~1]
impl SubscriptionsStore {
    const SUBSCRIPTION_STORE_NAME: &str = ".subscriptions.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> SubscriptionsStore {
        SubscriptionsStore {
            persistency: get_store(
                SubscriptionsStore::SUBSCRIPTION_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Adds a new topic-subscriber relationship to persistent storage
    /// * any such relationship in this store implies a subscription state of SUBSCRIBED, except for remote topics (refer to `RemoteTopicsStore`)
    ///
    /// # Arguments
    ///
    /// * `subscriber` - UUri of the topic subscriber.
    /// * `topic` - UUri of the topic that is being subscribed.
    /// * `expires` - Optional subscription expiration time - in milliseconds since Unix epoch (1970-01-01)
    ///
    /// # Returns
    ///
    /// * returns `Ok(true)` if this is the first subscription to this topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn add_subscription(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
        expiration: Option<ExpirationTimestamp>,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);

        Ok(
            // [impl->req~usubscription-subscribe-multiple~1]
            if let Some(mut subscriber_list) = self
                .persistency
                .get::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>(topic_string)
            {
                // [impl->req~usubscription-subscribe-expiration-extension~1]
                subscriber_list.insert(subscriber_string.clone(), expiration);
                self.persistency
                    .set(topic_string, &subscriber_list)
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error updating topic-subscriber list {e}"
                        ))
                    })?;
                false
            } else {
                self.persistency
                    .set(
                        topic_string,
                        &HashMap::from([(subscriber_string.clone(), expiration)]),
                    )
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error adding new topic-subscriber {e}"
                        ))
                    })?;
                true
            },
        )
    }

    /// Removes a topic-subscriber combination from persistent storage
    /// * returns `Ok(true)` if this was the last subscriber to the topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn remove_subscription(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);

        if let Some(mut subscriber_list) = self
            .persistency
            .get::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>(topic_string)
        {
            subscriber_list.remove(subscriber_string);

            if subscriber_list.is_empty() {
                let _r = self.persistency.rem(topic_string).map_err(|e| {
                    PersistencyError::internal_error(format!(
                        "Error removing topic-subscriber list {e}"
                    ))
                })?;
                return Ok(true);
            } else {
                self.persistency
                    .set(topic_string, &subscriber_list)
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error storing updated topic-subscriber list {e}"
                        ))
                    })?;
            }
        };
        Ok(false)
    }

    /// Returns a list of all subscribers of given topic
    /// * returns `Vec<SubscriberUUri>` that contains all subscriber UUris registered for the topic
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_topic_subscribers(
        &self,
        topic: &TopicUUri,
    ) -> Result<Vec<SubscriberUUri>, PersistencyError> {
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let mut subscribers = vec![];

        // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
        // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
        if let Some(list) = self
            .persistency
            .get::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>(topic_string)
        {
            for entry in list.keys() {
                subscribers.push(UUri::try_from(entry.clone()).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing subscriber uri {e}"
                    ))
                })?);
            }
        }

        Ok(subscribers)
    }

    /// Returns a list of all topics subscribed to by given subscriber
    /// * returns `Vec<TopicUUri>` that contains all topics subscribed to by subscriber
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_subscriber_topics(
        &self,
        subscriber: &SubscriberUUri,
    ) -> Result<Vec<TopicUUri>, PersistencyError> {
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);
        let mut result_subs: Vec<TopicUUri> = Vec::new();

        for entry in self.persistency.iter() {
            if let Some(subscribers) =
                entry.get_value::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>()
            {
                if subscribers.contains_key(subscriber_string) {
                    result_subs.push(UUri::try_from(entry.get_key()).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing topic uri {e}"
                        ))
                    })?);
                }
            }
        }

        Ok(result_subs)
    }

    /// Returns a flattened list of all subscriptions stored in persistency
    /// * returns `Vec<(SubscriberUUri, TopicUUri, Option<ExpirationTimestamp>)` that contains all subscribers and their associated subscription topics
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_flattened_subscriptions(
        &mut self,
    ) -> Result<Vec<(SubscriberUUri, TopicUUri, Option<ExpirationTimestamp>)>, PersistencyError>
    {
        let mut flattened_subscriptions: Vec<(
            SubscriberUUri,
            TopicUUri,
            Option<ExpirationTimestamp>,
        )> = Vec::new();

        // Extract every subscription entry that carries an expiration timestamp value
        for topic_subs in self.persistency.iter() {
            if let Some(entry) =
                topic_subs.get_value::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>()
            {
                for (subscriber, expiry) in entry.iter() {
                    flattened_subscriptions.push((
                        UUri::try_from(subscriber.clone()).map_err(|e| {
                            PersistencyError::serialization_error(format!(
                                "Error deserializing subscriber uri {e}"
                            ))
                        })?,
                        UUri::try_from(topic_subs.get_key()).map_err(|e| {
                            PersistencyError::serialization_error(format!(
                                "Error deserializing subscriber uri {e}"
                            ))
                        })?,
                        *expiry,
                    ));
                }
            }
        }

        Ok(flattened_subscriptions)
    }

    /// This function does two things
    /// - remove any subscription relationships from persistency that have an expiration timestamp that lies in the past
    /// - return all remaining subscription relationships which have an expiration timestamp that has not yet expired
    // [impl->req~usubscription-subscribe-no-expiration~1]
    pub(crate) fn get_and_prune_expiring_subscriptions(
        &mut self,
    ) -> Result<Vec<(SubscriberUUri, TopicUUri, ExpirationTimestamp)>, PersistencyError> {
        // Extract every subscription entry that carries an expiration timestamp value
        let mut expiring_subscriptions: Vec<(SubscriberUUri, TopicUUri, ExpirationTimestamp)> =
            self.get_flattened_subscriptions()?
                .into_iter()
                .filter_map(|(subscriber, topic, expiration)| {
                    expiration.map(|exp| (subscriber, topic, exp))
                })
                .collect();

        // Remove every expiration-subscription entry that has already expired from persistency
        expiring_subscriptions.retain(|(subscriber, topic, expiration)| {
            if *expiration <= SystemTime::now() {
                let _ = self.remove_subscription(subscriber, topic);
                false
            } else {
                true
            }
        });

        // return remaining subscription entries (all entries with expiration timestamp in the future)
        Ok(expiring_subscriptions)
    }

    /// Clears the subscription database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        let keys = self.persistency.get_all();
        for key in keys {
            self.persistency.rem(&key).map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error removing subscription entries from persistency {e}"
                ))
            })?;
        }
        self.persistency.dump().map_err(|e| {
            PersistencyError::internal_error(format!(
                "Error dumping cleared subscription data to persistency {e}"
            ))
        })?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_data(&self) -> Result<SubscriptionSet, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut map: SubscriptionSet = HashMap::new();

        for entry in self.persistency.iter() {
            #[allow(clippy::mutable_key_type)]
            let mut topic_subscribers = HashMap::new();

            if let Some(list) =
                entry.get_value::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>()
            {
                for (subscriber, expiry) in list {
                    topic_subscribers.insert(
                        UUri::try_from(subscriber).map_err(|e| {
                            PersistencyError::serialization_error(format!(
                                "Error deserializing subscriber uri {e}"
                            ))
                        })?,
                        expiry,
                    );
                }
            }

            map.insert(
                UUri::try_from(entry.get_key()).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic uri {e}"
                    ))
                })?,
                topic_subscribers,
            );
        }
        Ok(map)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        map: SubscriptionSet,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (topic, subscribers) in map {
            self.persistency
                .set(
                    &topic.to_uri(PERSIST_UP_SCHEMA),
                    &subscribers
                        .iter()
                        .map(|(u, e)| (u.to_uri(PERSIST_UP_SCHEMA), *e))
                        .collect::<HashMap<SubscriberAsString, Option<ExpirationTimestamp>>>(),
                )
                .map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error storing topic-subscriber data in persistency {e}"
                    ))
                })?;
        }
        Ok(())
    }
}

/// Persistent store for tracking remote topic status
pub(crate) struct RemoteTopicsStore {
    persistency: PickleDb,
}

impl RemoteTopicsStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const REMOTE_TOPICS_STORE_NAME: &str = ".remote_topics.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> RemoteTopicsStore {
        RemoteTopicsStore {
            persistency: get_store(
                RemoteTopicsStore::REMOTE_TOPICS_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Returns subscription state of topic
    /// * returns `Ok(Some(TopicState))` (with current TopicState value) if topic exists in store, otherwise returns `Ok(None)`
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_topic_state(
        &self,
        topic: &TopicUUri,
    ) -> Result<Option<TopicState>, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);

        Ok(if self.persistency.exists(topic_string) {
            let bytes = self
                .persistency
                .get::<SerializedTopicState>(topic_string)
                .ok_or(PersistencyError::internal_error(
                    "Error retrieving remote topic state from persistency",
                ))?;
            Some(deserialize_topic_state(bytes).map_err(|e| {
                PersistencyError::serialization_error(format!(
                    "Error deserializing topic state {e}"
                ))
            })?)
        } else {
            None
        })
    }

    /// Updates subscription state of topic in remote-topics store
    /// * returns `Ok(TopicState)` (with updated TopicState value) if state update is successful
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn set_topic_state(
        &mut self,
        topic: &TopicUUri,
        state: TopicState,
    ) -> Result<TopicState, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);
        self.persistency
            .set(topic_string, &serialize_topic_state(&state))
            .map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error setting remote topic state in persistency {e}"
                ))
            })?;

        Ok(state)
    }

    /// Returns subscription state of remote topic, or adds new remote-topic with state TopicState::SUBSCRIBE_PENDING if topic is new
    /// * returns `Ok(TopicState)` (where TopicState is the new topic state)
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_topic_or_get_state(
        &mut self,
        topic: &TopicUUri,
    ) -> Result<TopicState, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);

        // if remote topic already has been registered, retrieve state
        Ok(if self.persistency.exists(topic_string) {
            let bytes = self
                .persistency
                .get::<SerializedTopicState>(topic_string)
                .ok_or(PersistencyError::internal_error(
                    "Error retrieving remote topic state from persistency",
                ))?;
            deserialize_topic_state(bytes).map_err(|e| {
                PersistencyError::serialization_error(format!(
                    "Error deserializing topic state {e}"
                ))
            })?
        } else {
            // [impl->req~usubscription-subscribe-remote-pending~1]
            self.set_topic_state(topic, TopicState::SubscribePending)?
        })
    }

    /// Clears the remote subscriptions database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        let keys = self.persistency.get_all();
        for key in keys {
            self.persistency.rem(&key).map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error removing remote subscriptions from persistency {e}"
                ))
            })?;
        }
        self.persistency.dump().map_err(|e| {
            PersistencyError::internal_error(format!(
                "Error dumping cleared remote subscription data to persistency {e}"
            ))
        })?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_data(
        &self,
    ) -> Result<HashMap<TopicUUri, TopicState>, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut map: HashMap<TopicUUri, TopicState> = HashMap::new();

        for kv in self.persistency.iter() {
            if let Some(bytes) = kv.get_value::<SerializedTopicState>() {
                let value = deserialize_topic_state(bytes)?;
                map.insert(UUri::try_from(kv.get_key())?, value);
            }
        }

        Ok(map)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        map: HashMap<TopicUUri, TopicState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (key, value) in map {
            let _r = self.persistency.set(
                &key.to_uri(Self::PERSIST_UP_SCHEMA),
                &serialize_topic_state(&value),
            );
        }
        Ok(())
    }
}

pub(crate) struct NotificationStore {
    persistency: PickleDb,
}

impl NotificationStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const NOTIFICATION_STORE_NAME: &str = ".notification.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> NotificationStore {
        NotificationStore {
            persistency: get_store(
                NotificationStore::NOTIFICATION_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Adds subscriber to custom-notifications store
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_notifyee(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
    ) -> Result<(), PersistencyError> {
        let subscriber_string = subscriber.to_uri(Self::PERSIST_UP_SCHEMA);
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

        if !self.persistency.lexists(&topic_string) {
            self.persistency.lcreate(&topic_string).map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error setting notification configuration in persistency {e}"
                ))
            })?;
        }

        self.persistency
            .ladd(&topic_string, &subscriber_string)
            .map(|_| ())
            .ok_or_else(|| {
                PersistencyError::internal_error(
                    "Error setting notification configuration in persistency",
                )
            })
    }

    /// Removes subscriber from custom-notifications store
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn remove_notifyee(
        &mut self,
        subscriber: &SubscriberUUri,
        topic: &TopicUUri,
    ) -> Result<(), PersistencyError> {
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);
        if !self.persistency.lexists(&topic_string) {
            return Ok(());
        }

        let subscriber_string = subscriber.to_uri(Self::PERSIST_UP_SCHEMA);
        self.persistency
            .lrem_value(&topic_string, &subscriber_string)
            .map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error setting notification configuration in persistency {e}"
                ))
            })?;

        Ok(())
    }

    /// Returns a list of all subscribers that have registered to be notified on change of topic state
    ///
    /// # Arguments
    ///
    /// * `topic` - UUri of the topic that subscribers registered to be notified about.
    ///
    /// # Returns
    ///
    /// * return a `Vec<SubscriberUUri>` list of subscriber UUris that want to be notified about topic state changes
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_subscribers_registered_for_topic(
        &mut self,
        topic: &TopicUUri,
    ) -> Result<Vec<SubscriberUUri>, PersistencyError> {
        let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

        if !self.persistency.lexists(&topic_string) {
            return Ok(vec![]);
        }

        let mut result = vec![];

        for entry in self.persistency.liter(&topic_string) {
            if let Some(subscriber_string) = entry.get_item::<String>() {
                let subscriber = UUri::try_from(subscriber_string)
                    .map_err(|e| PersistencyError::serialization_error(e.to_string()))?;
                result.push(subscriber);
            }
        }
        Ok(result)
    }

    /// Clears the notifications database
    // [impl->req~usubscription-reset~1]
    pub(crate) fn reset(&mut self) -> Result<(), PersistencyError> {
        let keys = self.persistency.get_all();
        for key in keys {
            self.persistency.rem(&key).map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error removing registered-for-notifications from persistency {e}"
                ))
            })?;
        }
        self.persistency.dump().map_err(|e| {
            PersistencyError::internal_error(format!(
                "Error dumping cleared registered-for-notification data to persistency {e}"
            ))
        })?;
        Ok(())
    }

    pub(crate) fn get_data(
        &self,
    ) -> Result<Vec<(SubscriberUUri, TopicUUri)>, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut list: Vec<(SubscriberUUri, TopicUUri)> = Vec::new();

        let topic_strings = self.persistency.get_all();
        for topic_string in topic_strings {
            for entry in self.persistency.liter(&topic_string) {
                if let Some(subscriber_string) = entry.get_item::<String>() {
                    let subscriber = UUri::try_from(subscriber_string)?;
                    let topic = UUri::try_from(topic_string.clone())?;
                    list.push((subscriber, topic));
                }
            }
        }

        Ok(list)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        list: Vec<(SubscriberUUri, TopicUUri)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (subscriber, topic) in list {
            let subscriber_string = subscriber.to_uri(Self::PERSIST_UP_SCHEMA);
            let topic_string = topic.to_uri(Self::PERSIST_UP_SCHEMA);

            if !self.persistency.lexists(&topic_string) {
                self.persistency.lcreate(&topic_string).map_err(|e| {
                    PersistencyError::internal_error(format!(
                        "Error setting notification configuration in persistency {e}"
                    ))
                })?;
            }

            self.persistency.ladd(&topic_string, &subscriber_string);
        }

        Ok(())
    }
}

fn serialize_topic_state(state: &TopicState) -> SerializedTopicState {
    match state {
        TopicState::Unsubscribed => 0,
        TopicState::SubscribePending => 1,
        TopicState::Subscribed => 2,
        TopicState::UnsubscribePending => 3,
    }
}

fn deserialize_topic_state(v: SerializedTopicState) -> Result<TopicState, PersistencyError> {
    match v {
        0 => Ok(TopicState::Unsubscribed),
        1 => Ok(TopicState::SubscribePending),
        2 => Ok(TopicState::Subscribed),
        3 => Ok(TopicState::UnsubscribePending),
        _ => Err(PersistencyError::serialization_error(
            "invalid TopicState value",
        )),
    }
}

// Return a notification store instance, configured according to a USubscriptionConfiguration
fn get_store(name: String, path: PathBuf, persistency_enabled: bool) -> PickleDb {
    // duplicate policy returns, because there is no way to `clone()` this thing - and I need two instances below for load / new calls
    let (path, policy_load, policy_new) = {
        let path = validate_and_append_filename(&path, &name)
            .unwrap_or_else(|e| panic!("Problem with persistency, invalid storage file name: {e}"));

        if persistency_enabled {
            (
                path,
                PickleDbDumpPolicy::AutoDump,
                PickleDbDumpPolicy::AutoDump,
            )
        } else {
            (
                path,
                PickleDbDumpPolicy::NeverDump,
                PickleDbDumpPolicy::NeverDump,
            )
        }
    };

    PickleDb::load(&path, policy_load, SerializationMethod::Bin)
        .unwrap_or_else(|_| PickleDb::new(&path, policy_new, SerializationMethod::Bin))
}

// Check whether a filename contains any relative/path traversal characters, combine with directory if all is well
fn validate_and_append_filename(dir: &PathBuf, filename: &str) -> Result<PathBuf, &'static str> {
    // Check if filename contains any path separators or special directory components
    if filename.contains('/')
        || filename.contains('\\')
        || filename == "."
        || filename == ".."
        || filename.is_empty()
    {
        return Err("filename contains path components");
    }

    let mut full_path = dir.clone();
    full_path.push(filename);

    // Additional safety check - verify the resulting path is actually under the original directory
    if !full_path.starts_with(dir) {
        return Err("path traversal attempt detected");
    }

    Ok(full_path)
}

#[cfg(test)]
mod tests {
    // These are tests just for locally used helper functions. More complex and complete tests of the persistency
    // manager business logic are located in tests/persistency_tests.rs

    use super::*;
    use test_case::test_case;

    #[test_case(TopicState::Unsubscribed; "State UNSUBSCRIBED")]
    #[test_case(TopicState::SubscribePending; "State SUBSCRIBE_PENDING")]
    #[test_case(TopicState::Subscribed; "State SUBSCRIBED")]
    #[test_case(TopicState::UnsubscribePending; "State UNSUBSCRIBE_PENDING")]
    #[test_log::test(tokio::test)]
    async fn test_serialize_deserialize_topic_state(state: TopicState) {
        // One way...
        let serialized_bytes = serialize_topic_state(&state);

        // ... then the other
        let reconstructed_state = deserialize_topic_state(serialized_bytes);
        assert!(reconstructed_state.is_ok());

        let reconstructed_state = reconstructed_state.unwrap();
        assert_eq!(reconstructed_state, state);
    }

    #[test_log::test(tokio::test)]
    async fn test_validate_and_append_filename_success() {
        let mut expected = PathBuf::from(".");
        expected.push("newfile");

        let r = validate_and_append_filename(&PathBuf::from("."), "newfile");
        assert!(r.is_ok());
        assert_eq!(r.unwrap().as_os_str(), expected.as_os_str());
    }

    #[test_case("."; "Relative path 1")]
    #[test_case(".."; "Relative path 2")]
    #[test_case( "\\"; "Relative path 3")]
    #[test_case( ""; "Empty filename")]
    #[test_case( "/dummy/newfile"; "Random path and name")]
    #[test_log::test(tokio::test)]
    async fn test_validate_and_append_filename_relative_paths(file: &str) {
        let r = validate_and_append_filename(&PathBuf::from("."), file);
        assert!(r.is_err());
    }
}
