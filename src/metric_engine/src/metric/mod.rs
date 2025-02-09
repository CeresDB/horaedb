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
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::Context;
use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Builder, RecordBatch, UInt64Builder, UInt8Builder},
    datatypes::{DataType, Field, Schema, ToByteSlice},
};
use chrono::{Datelike, Timelike, Utc};
use horaedb_storage::storage::{TimeMergeStorageRef, WriteRequest};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time::timeout,
};
use tracing::{error, warn};

use crate::{
    types::{
        hash, FieldName, FieldType, MetricName, Sample, SectionedHashMap, SegmentTimeStamp, Task,
        TaskData, TimeStamp, DEFAULT_FIELD_NAME, DEFAULT_FIELD_TYPE,
    },
    Result,
};

type MetricsData = RwLock<HashMap<MetricName, (FieldName, FieldType)>>;

struct MetricsCache {
    pub inner: Arc<RwLock<MetricsInner>>,
}

struct MetricsInner {
    // global cache data older than X days
    pub global: Arc<SectionedHashMap<Arc<MetricsData>>>,
    // section of cache data for latest X days
    pub local: Arc<SectionedHashMap<Arc<MetricsData>>>,
    // TODO: compute next day cache in advance
    pub next_day: Arc<MetricsData>,
}

impl MetricsCache {
    pub fn new(num_of_days: usize) -> Self {
        let global = Arc::new(SectionedHashMap::new(128));
        let local = Arc::new(SectionedHashMap::new(num_of_days));
        let inner = Arc::new(RwLock::new(MetricsInner {
            global,
            local: local.clone(),
            next_day: Arc::new(RwLock::new(HashMap::new())),
        }));
        let inner_clone = inner.clone();
        let inner_clone2 = inner.clone();
        tokio::spawn(async move { Self::evict_local(inner_clone, num_of_days).await });
        tokio::spawn(async move { Self::evict_global(inner_clone2).await });
        Self { inner }
    }

    async fn evict_local(inner: Arc<RwLock<MetricsInner>>, num_of_days: usize) {
        // evict day cache of the earliest one from local cache
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut last_eviction_day: i32 = 0;
        loop {
            interval.tick().await;
            let now = chrono::Utc::now();
            let hour = now.hour();
            let day = now.num_days_from_ce();

            if hour == 0 && day > last_eviction_day {
                last_eviction_day = day;
                // This write lock is acquired only here once per day.
                let mut inner_guard = inner.write().await;
                // use pre computed next day cache to initialize the coming day cache.
                let next_day = inner_guard.next_day.clone();
                inner_guard.local.insert(day as usize, next_day).await;
                // invalidate the next day cache
                inner_guard.next_day = Arc::new(RwLock::new(HashMap::new()));
                // evict the earlist day cache
                inner_guard
                    .local
                    .evict_oldest(last_eviction_day as usize - num_of_days)
                    .await;
            }
        }
    }

    async fn evict_global(_inner: Arc<RwLock<MetricsInner>>) {
        todo!("to evict the global cache");
    }
}

impl MetricsInner {
    pub async fn _update(
        &self,
        days: i32,
        name: &[u8],
        cache: Arc<SectionedHashMap<Arc<MetricsData>>>,
    ) -> Result<bool> {
        let cache_day = cache.get(days as usize).await;
        // hit the cache
        if let Some(ref cache_day) = cache_day {
            if cache_day.read().await.contains_key(name) {
                return Ok(false);
            }
        }
        // not hit...
        let cache_day = if let Some(cache_day) = cache_day {
            cache_day
        } else {
            cache
                .insert(days as usize, Arc::new(RwLock::new(HashMap::new())))
                .await
                .unwrap()
        };

        // insert metric
        let mut write_guard = cache_day.write().await;
        let entry = write_guard.entry(name.to_vec());
        match entry {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert((DEFAULT_FIELD_NAME.as_bytes().to_vec(), DEFAULT_FIELD_TYPE));
                Ok(true)
            }
            std::collections::hash_map::Entry::Occupied(_) => Ok(false),
        }
    }

    pub async fn update(&self, timestamp: TimeStamp, name: &[u8]) -> Result<bool> {
        // find the cache
        let now_ts = Utc::now().timestamp();
        let days = SegmentTimeStamp::day_diff(now_ts, timestamp);
        // input timestamp is too old to stay in local cache
        if days >= self.local.get_section_count() as i32 {
            self._update(days, name, self.global.clone()).await
        } else {
            self._update(days, name, self.local.clone()).await
        }
    }
}

pub struct MetricManager {
    inner: Arc<Inner>,
    cache_read: MetricsCache,
    cache_write: MetricsCache,
}

impl MetricManager {
    pub fn new(storage: TimeMergeStorageRef) -> Self {
        let inner = Arc::new(Inner::new(storage));
        Self {
            inner,
            cache_read: MetricsCache::new(30),
            cache_write: MetricsCache::new(30),
        }
    }

    // update metric name cache with return value regarding if to write back
    pub async fn update_metric(&self, timestamp: TimeStamp, name: &[u8]) -> Result<bool> {
        let read_guard = self.cache_write.inner.read().await;
        read_guard.update(timestamp, name).await
    }

    /// Populate metric ids from names.
    /// If a name does not exist, it will be created on demand.
    pub async fn populate_metric_ids(&self, samples: &mut [Sample]) -> Result<()> {
        // 1. update cache
        let res = futures::future::join_all(
            samples
                .iter()
                .map(|s| self.update_metric(s.timestamp, s.name.as_slice())),
        )
        .await;

        // 2. TODO: as the eviction may invalid some day cache, shall we verify if the
        //    day-metric exists in storage, or just keep redundant data in the storage

        // 3. write back to storage if not exists
        futures::future::join_all(res.iter().zip(samples.iter()).filter_map(
            |(write_back, sample)| {
                if let Ok(true) = write_back {
                    Some(
                        self.inner
                            .populate_metric_ids(sample.timestamp, sample.name.as_slice()),
                    )
                } else {
                    None
                }
            },
        ))
        .await;

        Ok(())
    }
}

struct Inner {
    sender: Sender<Task>,
}

impl Inner {
    fn new(storage: TimeMergeStorageRef) -> Self {
        // TODO: need channel capacity configuration ?
        let (sender, receiver) = mpsc::channel(1024);
        tokio::spawn(async move { Inner::execute_write(receiver, storage).await });
        Self { sender }
    }

    async fn populate_metric_ids(&self, timestamp: i64, name: &[u8]) -> Result<()> {
        self.sender
            .send(Task::metric_task(
                timestamp,
                name.to_vec(),
                DEFAULT_FIELD_NAME.as_bytes().to_vec(),
                DEFAULT_FIELD_TYPE,
            ))
            .await
            .context("send ")?;
        Ok(())
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Binary, true),
            Field::new("metric_id", DataType::UInt64, true),
            Field::new("field_name", DataType::Binary, true),
            Field::new("field_id", DataType::UInt64, true),
            Field::new("field_type", DataType::UInt8, true),
            Field::new("timestamp", DataType::Int64, true),
        ]))
    }

    async fn batch_write_metrics(batch_tasks: Vec<Task>, storage: TimeMergeStorageRef) {
        let arrays: Vec<ArrayRef> = {
            let mut metric_name_builder = BinaryBuilder::new();
            let mut metric_id_builder = UInt64Builder::new();
            let mut field_name_builder = BinaryBuilder::new();
            let mut field_id_builder = UInt64Builder::new();
            let mut field_type_builder = UInt8Builder::new();
            let mut field_duration_builder = Int64Builder::new();

            let mut start_ts: i64 = 0;
            let mut end_ts: i64 = 0;
            let task_len = batch_tasks.len();

            batch_tasks
                .into_iter()
                .enumerate()
                .for_each(|(index, task)| {
                    let (timestamp, TaskData::Metric(name, field_name, field_type)) =
                        (task.timestamp, task.data);

                    if index == 0 {
                        start_ts = timestamp;
                    } else if index == task_len - 1 {
                        end_ts = timestamp;
                    }

                    metric_id_builder.append_value(hash(&name));
                    metric_name_builder.append_value(name);
                    field_id_builder.append_value(hash(field_name.to_byte_slice()));
                    field_name_builder.append_value(field_name);
                    field_type_builder.append_value(field_type);
                    field_duration_builder.append_value(timestamp);
                });

            vec![
                Arc::new(metric_name_builder.finish()),
                Arc::new(metric_id_builder.finish()),
                Arc::new(field_name_builder.finish()),
                Arc::new(field_id_builder.finish()),
                Arc::new(field_type_builder.finish()),
                Arc::new(field_duration_builder.finish()),
            ]
        };
        let batch = RecordBatch::try_new(Self::schema(), arrays).unwrap();
        storage
            .write(WriteRequest {
                batch,
                time_range: (0..10).into(),
                enable_check: true,
            })
            .await
            .unwrap_or_else(|e| {
                error!("write metrics failed: {:?}", e);
            });
    }

    async fn execute_write(mut receiver: Receiver<Task>, storage: TimeMergeStorageRef) {
        let mut task_queue: Vec<Task> = Vec::new();
        let mut batch_tasks: Vec<Task>;
        // TODO: make it configurable
        let max_wait_time_ms: u64 = 1000;
        let max_queue_length: usize = 16;

        loop {
            loop {
                let wait_time: Duration = {
                    if task_queue.is_empty() {
                        Duration::from_millis(max_wait_time_ms)
                    } else {
                        let current = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap();
                        let wait_in_ms = std::cmp::min(
                            max_wait_time_ms
                                - (current - task_queue[0].create_time).as_millis() as u64,
                            1,
                        );
                        Duration::from_millis(wait_in_ms)
                    }
                };
                match timeout(wait_time, receiver.recv()).await {
                    Ok(Some(task)) => {
                        if task_queue.is_empty()
                            || SegmentTimeStamp::day_diff(task_queue[0].timestamp, task.timestamp)
                                == 0
                        {
                            task_queue.push(task);
                            if task_queue.len() >= max_queue_length {
                                batch_tasks = std::mem::take(&mut task_queue);
                                break;
                            }
                        } else {
                            batch_tasks = std::mem::take(&mut task_queue);
                            task_queue.push(task);
                            break;
                        }
                    }
                    Ok(None) => {
                        warn!("Channel closed");
                        return;
                    }
                    Err(_) => {
                        batch_tasks = std::mem::take(&mut task_queue);
                        break;
                    }
                }
            }

            if batch_tasks.is_empty() {
                continue;
            }

            Self::batch_write_metrics(batch_tasks, storage.clone()).await;
        }
    }
}
