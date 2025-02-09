// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Datelike, TimeZone, Utc};
use tokio::sync::RwLock;
pub struct MetricId(pub u64);
pub struct SeriesId(u64);

pub const METRIC_NAME: &str = "__name__";
pub const DEFAULT_FIELD_NAME: &str = "value";
pub const DEFAULT_FIELD_TYPE: u8 = 0;

pub type MetricName = Vec<u8>;
pub type FieldName = Vec<u8>;
pub type FieldType = u8;
pub type TimeStamp = i64;

pub struct Task {
    pub create_time: Duration,
    pub timestamp: TimeStamp,
    pub data: TaskData,
}

pub enum TaskData {
    Metric(MetricName, FieldName, FieldType),
}

impl Task {
    pub fn metric_task(
        timestamp: TimeStamp,
        metric_name: MetricName,
        field_name: FieldName,
        field_type: FieldType,
    ) -> Self {
        Self {
            create_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
            timestamp,
            data: TaskData::Metric(metric_name, field_name, field_type),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SegmentTimeStamp(i64);

impl SegmentTimeStamp {
    pub fn date(timestamp: i64) -> Self {
        let dt = DateTime::from_timestamp_millis(timestamp).unwrap();
        let start_of_day = Utc
            .with_ymd_and_hms(dt.year(), dt.month(), dt.day(), 0, 0, 0)
            .unwrap();

        Self(start_of_day.timestamp())
    }

    pub fn day_diff(lhs: i64, rhs: i64) -> i32 {
        let lhs = DateTime::from_timestamp_millis(lhs).unwrap();
        let rhs = DateTime::from_timestamp_millis(rhs).unwrap();
        lhs.num_days_from_ce() - rhs.num_days_from_ce()
    }
}

pub struct Label {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

/// This is the main struct used for write, optional values will be filled in
/// different modules.
pub struct Sample {
    pub name: Vec<u8>,
    pub lables: Vec<Label>,
    pub timestamp: i64, // millisecond
    pub value: f64,
    /// hash of name
    pub name_id: Option<MetricId>,
    /// hash of labels(sorted)
    pub series_id: Option<SeriesId>,
}

pub fn hash(buf: &[u8]) -> u64 {
    seahash::hash(buf)
}

pub struct SectionedHashMap<V> {
    sections: Vec<RwLock<HashMap<usize, V>>>,
    section_count: usize,
    min_keys: RwLock<BinaryHeap<Reverse<usize>>>,
}

impl<V> SectionedHashMap<V> {
    pub fn new(section_count: usize) -> Self {
        assert!(section_count > 0, "Section count must be greater than 0");

        let mut sections = Vec::with_capacity(section_count);
        for _ in 0..section_count {
            sections.push(RwLock::new(HashMap::new()));
        }

        SectionedHashMap {
            sections,
            section_count,
            min_keys: RwLock::new(BinaryHeap::new()),
        }
    }

    #[inline]
    fn get_section_index(&self, key: usize) -> usize {
        key % self.section_count
    }

    #[inline]
    pub fn get_section_count(&self) -> usize {
        self.section_count
    }

    pub async fn insert(&self, key: usize, value: V) -> Option<V>
    where
        V: Clone,
    {
        let section_idx = self.get_section_index(key);
        let mut write_guard = self.sections[section_idx].write().await;
        let (inserted, result) =
            if let std::collections::hash_map::Entry::Vacant(e) = write_guard.entry(key) {
                let result = e.insert(value);
                (true, Some(result.clone()))
            } else {
                (false, write_guard.get(&key).cloned())
            };
        drop(write_guard);
        if inserted {
            self.min_keys.write().await.push(Reverse(key));
        }
        result
    }

    pub async fn get(&self, key: usize) -> Option<V>
    where
        V: Clone,
    {
        let section_idx = self.get_section_index(key);
        let section = self.sections[section_idx].read().await;
        section.get(&key).cloned()
    }

    pub async fn evict_oldest(&self, oldest_day: usize) {
        let mut min_keys = self.min_keys.write().await;
        while min_keys.peek().unwrap().0 <= oldest_day {
            let min_key = {
                match min_keys.pop() {
                    Some(Reverse(key)) => key,
                    None => return,
                }
            };

            let section_idx = self.get_section_index(min_key);
            self.sections[section_idx].write().await.remove(&min_key);
        }
    }
}
