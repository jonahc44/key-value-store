//! The key space as a copy-on-write map, so a snapshot can be taken under
//! the node lock without copying it (Raft thesis §5.1.1).
//!
//! Keys are spread over a fixed number of shards, each a `HashMap` behind an
//! `Arc`. Cloning the map clones the shard pointers only. A write to a shard
//! that a clone still shares copies that one shard first, so while a snapshot
//! is being saved each shard is copied at most once, on its first write, and
//! a write costs at most one shard's worth of copying.

use serde_json::Value;
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

/// Enough that copying one shard stays small at millions of keys, few
/// enough that cloning the map is negligible.
const SHARDS: usize = 1024;

#[derive(Clone, Debug)]
pub(crate) struct KeySpace {
    /// Keys and values are shared too, so copying a shard copies pointers,
    /// not JSON.
    shards: Vec<Arc<HashMap<Arc<str>, Arc<Value>>>>,
    /// Picks a key's shard; shared by every clone.
    hasher: RandomState,
    len: usize,
}

impl KeySpace {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Arc::default()).collect(),
            hasher: RandomState::new(),
            len: 0,
        }
    }

    fn shard(&self, key: &str) -> usize {
        (self.hasher.hash_one(key) % SHARDS as u64) as usize
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        self.shards[self.shard(key)].get(key).map(|value| &**value)
    }

    pub(crate) fn insert(&mut self, key: String, value: Value) {
        let shard = self.shard(&key);
        if Arc::make_mut(&mut self.shards[shard])
            .insert(key.into(), Arc::new(value))
            .is_none()
        {
            self.len += 1;
        }
    }

    pub(crate) fn remove(&mut self, key: &str) {
        let shard = self.shard(key);
        // Only a shard that holds the key is copied.
        if self.shards[shard].contains_key(key) {
            Arc::make_mut(&mut self.shards[shard]).remove(key);
            self.len -= 1;
        }
    }

    pub(crate) fn clear(&mut self) {
        *self = Self::new();
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.shards
            .iter()
            .flat_map(|shard| shard.iter().map(|(key, value)| (&**key, &**value)))
    }
}

impl Default for KeySpace {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for KeySpace {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().all(|(key, value)| other.get(key) == Some(value))
    }
}

impl FromIterator<(String, Value)> for KeySpace {
    fn from_iter<I: IntoIterator<Item = (String, Value)>>(iter: I) -> Self {
        let mut keys = Self::new();
        for (key, value) in iter {
            keys.insert(key, value);
        }
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn clone_is_unaffected_by_later_writes() {
        let mut keys: KeySpace = (0..5000)
            .map(|n| (format!("k{n}"), json!(n)))
            .collect();
        let frozen = keys.clone();
        keys.insert("k1".into(), json!("changed"));
        keys.insert("new".into(), json!(true));
        keys.remove("k2");
        keys.remove("absent");

        assert_eq!(frozen.len(), 5000);
        assert_eq!(frozen.get("k1"), Some(&json!(1)));
        assert_eq!(frozen.get("k2"), Some(&json!(2)));
        assert_eq!(frozen.get("new"), None);
        assert_eq!(keys.len(), 5000);
        assert_eq!(keys.get("k1"), Some(&json!("changed")));
        assert_eq!(keys.get("k2"), None);
        assert_eq!(keys.iter().count(), keys.len());
        assert_ne!(keys, frozen);
    }

    #[test]
    fn writes_copy_only_the_shards_they_touch() {
        let mut keys: KeySpace = (0..5000)
            .map(|n| (format!("k{n}"), json!(n)))
            .collect();
        let frozen = keys.clone();
        keys.insert("k1".into(), json!("changed"));
        let copied = keys
            .shards
            .iter()
            .zip(&frozen.shards)
            .filter(|(mine, theirs)| !Arc::ptr_eq(mine, theirs))
            .count();
        assert_eq!(copied, 1);
    }
}
