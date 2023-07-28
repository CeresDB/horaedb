// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, hash::Hash, sync::RwLock};

use tokio::sync::mpsc::Sender;

type Notifier<T> = Sender<T>;

#[derive(Debug)]
struct Notifiers<T> {
    notifiers: RwLock<Vec<Notifier<T>>>,
}

impl<T> Notifiers<T> {
    pub fn new(notifier: Notifier<T>) -> Self {
        let notifiers = vec![notifier];
        Self {
            notifiers: RwLock::new(notifiers),
        }
    }

    pub fn add_notifier(&self, notifier: Notifier<T>) {
        self.notifiers.write().unwrap().push(notifier);
    }
}

#[derive(Debug)]
pub struct RequestNotifiers<K, T>
where
    K: PartialEq + Eq + Hash,
{
    notifiers_by_key: RwLock<HashMap<K, Notifiers<T>>>,
}

impl<K, T> Default for RequestNotifiers<K, T>
where
    K: PartialEq + Eq + Hash,
{
    fn default() -> Self {
        Self {
            notifiers_by_key: RwLock::new(HashMap::new()),
        }
    }
}

impl<K, T> RequestNotifiers<K, T>
where
    K: PartialEq + Eq + Hash,
{
    /// Insert a notifier for the given key.
    pub fn insert_notifier(&self, key: K, notifier: Notifier<T>) -> RequestResult {
        // First try to read the notifiers, if the key exists, add the notifier to the
        // notifiers.
        let notifiers_by_key = self.notifiers_by_key.read().unwrap();
        if let Some(notifiers) = notifiers_by_key.get(&key) {
            notifiers.add_notifier(notifier);
            return RequestResult::Wait;
        }
        drop(notifiers_by_key);

        // If the key does not exist, try to write the notifiers.
        let mut notifiers_by_key = self.notifiers_by_key.write().unwrap();
        // double check, if the key exists, add the notifier to the notifiers.
        if let Some(notifiers) = notifiers_by_key.get(&key) {
            notifiers.add_notifier(notifier);
            return RequestResult::Wait;
        }

        //the key is not existed, insert the key and the notifier.
        notifiers_by_key.insert(key, Notifiers::new(notifier));
        RequestResult::First
    }

    /// Take the notifiers for the given key, and remove the key from the map.
    pub fn take_notifiers(&self, key: &K) -> Option<Vec<Notifier<T>>> {
        self.notifiers_by_key
            .write()
            .unwrap()
            .remove(key)
            .map(|notifiers| notifiers.notifiers.into_inner().unwrap())
    }
}

pub enum RequestResult {
    // The first request for this key, need to handle this request.
    First,
    // There are other requests for this key, just wait for the result.
    Wait,
}
