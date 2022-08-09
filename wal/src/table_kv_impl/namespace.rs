// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Wal namespace.

use std::{
    collections::{BTreeMap, HashMap},
    fmt, str,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use common_types::time::Timestamp;
use common_util::{config::ReadableDuration, define_result, runtime::Runtime};
use log::{debug, error, info};
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_kv::{ScanIter, TableError, TableKv, WriteBatch, WriteContext};

use crate::{
    log_batch::LogWriteBatch,
    manager::{self, ReadContext, ReadRequest, RegionId, SequenceNumber},
    table_kv_impl::{
        consts, encoding,
        model::{BucketEntry, NamespaceConfig, NamespaceEntry},
        region::{Region, RegionRef, TableLogIterator},
        timed_task::{TaskHandle, TimedTask},
        WalRuntimes,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create table, err:{}", source,))]
    CreateTable {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to init region meta, namespace:{}, err:{}", namespace, source,))]
    InitRegionMeta {
        namespace: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load buckets, namespace:{}, err:{}", namespace, source,))]
    LoadBuckets {
        namespace: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to open bucket, namespace:{}, err:{}", namespace, source,))]
    OpenBucket {
        namespace: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Bucket timestamp out of range, namespace:{}, timestamp:{:?}.\nBacktrace:\n{}",
        namespace,
        timestamp,
        backtrace
    ))]
    BucketOutOfRange {
        namespace: String,
        timestamp: Timestamp,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to drop bucket shard, namespace:{}, err:{}", namespace, source,))]
    DropShard {
        namespace: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to encode entry, namespace:{}, err:{}", namespace, source,))]
    Encode {
        namespace: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display("Failed to decode entry, key:{}, err:{}", key, source,))]
    Decode {
        key: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display("Failed to persist value, key:{}, err:{}", key, source,))]
    PersistValue {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to purge bucket, namespace:{}, err:{}", namespace, source,))]
    PurgeBucket {
        namespace: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to get value, key:{}, err:{}", key, source,))]
    GetValue {
        key: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Value not found, key:{}.\nBacktrace:\n{}", key, backtrace))]
    ValueNotFound { key: String, backtrace: Backtrace },

    #[snafu(display("Failed to build namespace, namespace:{}, err:{}", namespace, source,))]
    BuildNamepsace {
        namespace: String,
        source: crate::table_kv_impl::model::Error,
    },

    #[snafu(display(
        "Failed to open region, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    OpenRegion {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },

    #[snafu(display(
        "Failed to create region, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    CreateRegion {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },

    #[snafu(display(
        "Failed to write region, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    WriteRegion {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },

    #[snafu(display(
        "Failed to read region, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    ReadRegion {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },

    #[snafu(display(
        "Failed to delete entries, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    DeleteEntries {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },

    #[snafu(display("Failed to stop task, namespace:{}, err:{}", namespace, source))]
    StopTask {
        namespace: String,
        source: common_util::runtime::Error,
    },

    #[snafu(display(
        "Failed to clean deleted logs, namespace:{}, region_id:{}, err:{}",
        namespace,
        region_id,
        source
    ))]
    CleanLog {
        namespace: String,
        region_id: RegionId,
        source: crate::table_kv_impl::region::Error,
    },
}

define_result!(Error);

/// Duration of a bucket (1d).
pub const BUCKET_DURATION_MS: i64 = 1000 * 3600 * 24;
/// Check whether to create a new bucket every `BUCKET_DURATION_PERIOD`.
const BUCKET_MONITOR_PERIOD: Duration = Duration::from_millis(BUCKET_DURATION_MS as u64 / 8);
/// Clean deleted logs period.
const LOG_CLEANER_PERIOD: Duration = Duration::from_millis(BUCKET_DURATION_MS as u64 / 4);

struct NamespaceInner<T> {
    runtimes: WalRuntimes,
    table_kv: T,
    entry: NamespaceEntry,
    bucket_set: RwLock<BucketSet>,
    regions: RwLock<HashMap<RegionId, RegionRef>>,
    meta_table_name: String,
    region_meta_tables: Vec<String>,
    operator: Mutex<TableOperator>,
    // Only one thread can persist and create a new bucket.
    bucket_creator: Mutex<BucketCreator>,
    config: NamespaceConfig,
}

impl<T> NamespaceInner<T> {
    #[inline]
    pub fn name(&self) -> &str {
        &self.entry.name
    }

    /// Names of region meta tables.
    fn region_meta_tables(&self) -> &[String] {
        &self.region_meta_tables
    }

    fn region_meta_table(&self, region_id: RegionId) -> &str {
        let index = region_id as usize % self.region_meta_tables.len();

        &self.region_meta_tables[index]
    }

    fn list_buckets(&self) -> Vec<BucketRef> {
        self.bucket_set.read().unwrap().buckets()
    }

    fn list_regions(&self) -> Vec<RegionRef> {
        self.regions.read().unwrap().values().cloned().collect()
    }

    fn clear_regions(&self) {
        let mut regions = self.regions.write().unwrap();
        regions.clear();
    }
}

// Blocking operations.
impl<T: TableKv> NamespaceInner<T> {
    /// Pre-build all region meta tables.
    fn init_region_meta(&self) -> Result<()> {
        for table_name in self.region_meta_tables() {
            let exists = self
                .table_kv
                .table_exists(table_name)
                .map_err(|e| Box::new(e) as _)
                .context(InitRegionMeta {
                    namespace: self.name(),
                })?;
            if !exists {
                self.table_kv
                    .create_table(table_name)
                    .map_err(|e| Box::new(e) as _)
                    .context(InitRegionMeta {
                        namespace: self.name(),
                    })?;

                info!("Create region meta table, table_name:{}", table_name);
            }
        }

        Ok(())
    }

    /// Load all buckets of this namespace.
    fn load_buckets(&self) -> Result<()> {
        let bucket_scan_ctx = self.config.new_bucket_scan_ctx();

        let key_prefix = encoding::bucket_key_prefix(self.name());
        let scan_req = encoding::scan_request_for_prefix(&key_prefix);
        let mut iter = self
            .table_kv
            .scan(bucket_scan_ctx, &self.meta_table_name, scan_req)
            .map_err(|e| Box::new(e) as _)
            .context(LoadBuckets {
                namespace: self.name(),
            })?;

        while iter.valid() {
            if !iter.key().starts_with(key_prefix.as_bytes()) {
                break;
            }

            let bucket_entry = BucketEntry::decode(iter.value())
                .map_err(|e| Box::new(e) as _)
                .context(LoadBuckets {
                    namespace: self.name(),
                })?;

            info!(
                "Load bucket for namespace, namespace:{}, bucket:{:?}",
                self.entry.name, bucket_entry
            );

            let bucket = Bucket::new(self.name(), bucket_entry);
            self.open_bucket(bucket)?;

            iter.next()
                .map_err(|e| Box::new(e) as _)
                .context(LoadBuckets {
                    namespace: self.name(),
                })?;
        }

        Ok(())
    }

    /// Open bucket, ensure all tables are created, and insert the bucket into
    /// the bucket set in memory.
    fn open_bucket(&self, bucket: Bucket) -> Result<BucketRef> {
        {
            // Create all wal shards of this bucket.
            let mut operator = self.operator.lock().unwrap();
            for wal_shard in &bucket.wal_shard_names {
                operator.create_table_if_needed(&self.table_kv, self.name(), wal_shard)?;
            }
        }

        let bucket = Arc::new(bucket);
        let mut bucket_set = self.bucket_set.write().unwrap();
        bucket_set.insert_bucket(bucket.clone());

        Ok(bucket)
    }

    /// Get bucket by given timestamp, create it if bucket is not exists. The
    /// timestamp will be aligned to bucket duration automatically.
    fn get_or_create_bucket(&self, timestamp: Timestamp) -> Result<BucketRef> {
        let start_ms =
            timestamp
                .checked_floor_by_i64(BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: self.name(),
                    timestamp,
                })?;

        {
            let bucket_set = self.bucket_set.read().unwrap();
            if let Some(bucket) = bucket_set.get_bucket(start_ms) {
                return Ok(bucket.clone());
            }
        }

        // Bucket does not exist, we need to create a new bucket.
        let mut bucket_creator = self.bucket_creator.lock().unwrap();

        bucket_creator.get_or_create_bucket(self, start_ms)
    }

    /// Given timestamp `now` in current time range, create bucket for next time
    /// range.
    fn create_next_bucket(&self, now: Timestamp) -> Result<BucketRef> {
        let now_start = now
            .checked_floor_by_i64(BUCKET_DURATION_MS)
            .context(BucketOutOfRange {
                namespace: self.name(),
                timestamp: now,
            })?;
        let next_start =
            now_start
                .checked_add_i64(BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: self.name(),
                    timestamp: now_start,
                })?;

        let mut bucket_creator = self.bucket_creator.lock().unwrap();

        bucket_creator.get_or_create_bucket(self, next_start)
    }

    /// Purge expired buckets, remove all related wal shard tables and delete
    /// bucket record from meta table.
    fn purge_expired_buckets(&self, now: Timestamp) -> Result<()> {
        if let Some(ttl) = self.entry.wal.ttl {
            let expired_buckets = self.bucket_set.read().unwrap().expired_buckets(now, ttl.0);
            if expired_buckets.is_empty() {
                return Ok(());
            }

            let mut batch = T::WriteBatch::with_capacity(expired_buckets.len());
            let mut keys = Vec::with_capacity(expired_buckets.len());

            for bucket in &expired_buckets {
                // Delete all tables of this bucket.
                for table_name in &bucket.wal_shard_names {
                    self.table_kv
                        .drop_table(table_name)
                        .map_err(|e| Box::new(e) as _)
                        .context(DropShard {
                            namespace: self.name(),
                        })?;
                }

                // All tables of this bucket have been dropped, we can remove the bucket record
                // later.
                let key = bucket.format_bucket_key(self.name());

                batch.delete(key.as_bytes());

                keys.push(key);
            }

            self.table_kv
                .write(WriteContext::default(), &self.meta_table_name, batch)
                .map_err(|e| Box::new(e) as _)
                .context(PurgeBucket {
                    namespace: self.name(),
                })?;

            {
                let mut bucket_set = self.bucket_set.write().unwrap();
                for bucket in expired_buckets {
                    bucket_set.remove_timed_bucket(bucket.entry.gmt_start_ms());
                }
            }

            info!("Purge expired buckets, keys:{:?}", keys);
        }

        Ok(())
    }

    fn get_region_from_memory(&self, region_id: RegionId) -> Option<RegionRef> {
        let regions = self.regions.read().unwrap();
        regions.get(&region_id).cloned()
    }

    fn insert_or_get_region(&self, region: RegionRef) -> RegionRef {
        let mut regions = self.regions.write().unwrap();
        // Region already exists.
        if let Some(v) = regions.get(&region.region_id()) {
            return v.clone();
        }

        regions.insert(region.region_id(), region.clone());

        region
    }

    fn clean_deleted_logs(&self) -> Result<()> {
        let regions = self.list_regions();
        let buckets = self.list_buckets();
        let clean_ctx = self.config.new_clean_ctx();

        for region in regions {
            region
                .clean_deleted_logs(&self.table_kv, &clean_ctx, &buckets)
                .context(CleanLog {
                    namespace: self.name(),
                    region_id: region.region_id(),
                })?;
        }

        Ok(())
    }
}

// Async operations.
impl<T: TableKv> NamespaceInner<T> {
    async fn get_or_open_region(&self, region_id: RegionId) -> Result<Option<RegionRef>> {
        if let Some(region) = self.get_region_from_memory(region_id) {
            return Ok(Some(region));
        }

        self.open_region(region_id).await
    }

    // TODO(yingwen): Provide a close_region() method.
    async fn open_region(&self, region_id: RegionId) -> Result<Option<RegionRef>> {
        let region_meta_table = self.region_meta_table(region_id);
        let buckets = self.bucket_set.read().unwrap().buckets();

        let region_opt = Region::open(
            self.runtimes.clone(),
            &self.table_kv,
            self.config.new_init_scan_ctx(),
            region_meta_table,
            region_id,
            buckets,
        )
        .await
        .context(OpenRegion {
            namespace: self.name(),
            region_id,
        })?;
        let region = match region_opt {
            Some(v) => Arc::new(v),
            None => return Ok(None),
        };

        debug!(
            "Open wal region, namespace:{}, region_id:{}",
            self.name(),
            region_id
        );

        let region = self.insert_or_get_region(region);

        Ok(Some(region))
    }

    async fn get_or_create_region(&self, region_id: RegionId) -> Result<RegionRef> {
        if let Some(region) = self.get_region_from_memory(region_id) {
            return Ok(region);
        }

        self.create_region(region_id).await
    }

    async fn create_region(&self, region_id: RegionId) -> Result<RegionRef> {
        let region_meta_table = self.region_meta_table(region_id);
        let buckets = self.bucket_set.read().unwrap().buckets();

        let region = Region::open_or_create(
            self.runtimes.clone(),
            &self.table_kv,
            self.config.new_init_scan_ctx(),
            region_meta_table,
            region_id,
            buckets,
        )
        .await
        .context(CreateRegion {
            namespace: self.name(),
            region_id,
        })?;

        debug!(
            "Create wal region, namespace:{}, region_id:{}",
            self.name(),
            region_id
        );

        let region = self.insert_or_get_region(Arc::new(region));

        Ok(region)
    }

    /// Write log to this namespace.
    async fn write_log(
        &self,
        ctx: &manager::WriteContext,
        batch: &LogWriteBatch<'_>,
    ) -> Result<SequenceNumber> {
        let region_id = batch.region_id;
        let now = Timestamp::now();
        // Get current bucket to write.
        let bucket = self.get_or_create_bucket(now)?;

        let region = self.get_or_create_region(region_id).await?;

        let sequence = region
            .write_log(&self.table_kv, &bucket, ctx, batch)
            .await
            .context(WriteRegion {
                namespace: self.name(),
                region_id,
            })?;

        Ok(sequence)
    }

    /// Get last sequence number of this region.
    async fn last_sequence(&self, region_id: RegionId) -> Result<SequenceNumber> {
        if let Some(region) = self.get_or_open_region(region_id).await? {
            return Ok(region.last_sequence());
        }

        Ok(common_types::MIN_SEQUENCE_NUMBER)
    }

    /// Read log from this namespace. Note that the iterating the iterator may
    /// still block caller thread now.
    async fn read_log(&self, ctx: &ReadContext, req: &ReadRequest) -> Result<TableLogIterator<T>> {
        // TODO(yingwen): Skip buckets according to sequence range, avoid scan all
        // buckets.
        let buckets = self.list_buckets();

        let region_id = req.region_id;
        if let Some(region) = self.get_or_open_region(req.region_id).await? {
            region
                .read_log(&self.table_kv, buckets, ctx, req)
                .await
                .context(ReadRegion {
                    namespace: self.name(),
                    region_id,
                })
        } else {
            Ok(TableLogIterator::new_empty(self.table_kv.clone()))
        }
    }

    /// Delete entries up to `sequence_num` of region identified by `region_id`.
    async fn delete_entries(
        &self,
        region_id: RegionId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        if let Some(region) = self.get_or_open_region(region_id).await? {
            let region_meta_table = self.region_meta_table(region_id);

            region
                .delete_entries_up_to(&self.table_kv, region_meta_table, sequence_num)
                .await
                .context(DeleteEntries {
                    namespace: self.name(),
                    region_id,
                })?;
        }

        Ok(())
    }
}

/// BucketCreator handles bucket creation and persistence.
struct BucketCreator;

impl BucketCreator {
    /// Get bucket by given timestamp `start_ms`, create it if bucket is not
    /// exists. The caller should ensure the timestamp is aligned to bucket
    /// duration.
    fn get_or_create_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        start_ms: Timestamp,
    ) -> Result<BucketRef> {
        {
            let bucket_set = inner.bucket_set.read().unwrap();
            if let Some(bucket) = bucket_set.get_bucket(start_ms) {
                return Ok(bucket.clone());
            }
        }

        let bucket_entry = if inner.config.ttl.is_some() {
            // Bucket with ttl.
            BucketEntry::new_timed(inner.entry.wal.shard_num, start_ms, BUCKET_DURATION_MS)
                .context(BucketOutOfRange {
                    namespace: inner.name(),
                    timestamp: start_ms,
                })?
        } else {
            // Permanent bucket.
            BucketEntry::new_permanent(inner.entry.wal.shard_num)
        };

        info!(
            "Try to create bucket, namespace:{}, start_ms:{:?}, bucket:{:?}",
            inner.name(),
            start_ms,
            bucket_entry
        );

        assert!(
            bucket_entry.is_permanent() == inner.config.ttl.is_none(),
            "Bucket should be consistent with ttl config, bucket:{:?}, ttl:{:?}",
            bucket_entry,
            inner.config.ttl,
        );

        let bucket = Bucket::new(inner.name(), bucket_entry);

        self.create_bucket(inner, bucket)
    }

    /// Create and open the bucket.
    fn create_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        bucket: Bucket,
    ) -> Result<BucketRef> {
        // Insert bucket record into TableKv.
        let bucket = self.try_persist_bucket(inner, bucket)?;

        inner.open_bucket(bucket)
    }

    /// Try to persist and return the persisted bucket, if bucket already
    /// exists, return the bucket from storage.
    fn try_persist_bucket<T: TableKv>(
        &mut self,
        inner: &NamespaceInner<T>,
        bucket: Bucket,
    ) -> Result<Bucket> {
        // Insert bucket record into TableKv.
        let key = bucket.format_bucket_key(inner.name());
        let value = bucket.entry.encode().context(Encode {
            namespace: inner.name(),
        })?;

        info!(
            "Persist bucket entry, namespace:{}, bucket:{:?}",
            inner.name(),
            bucket.entry
        );

        let mut batch = T::WriteBatch::default();
        batch.insert(key.as_bytes(), &value);

        let res = inner
            .table_kv
            .write(WriteContext::default(), &inner.meta_table_name, batch);
        if let Err(e) = &res {
            if e.is_primary_key_duplicate() {
                info!(
                    "Bucket already persisted, namespace:{}, bucket:{:?}",
                    inner.name(),
                    bucket.entry
                );

                // Load given bucket entry from storage.
                let bucket = self.get_bucket_by_key(inner, &key)?;

                info!(
                    "Load bucket from storage, namespace:{}, bucket:{:?}",
                    inner.name(),
                    bucket.entry
                );

                return Ok(bucket);
            } else {
                error!("Failed to persist bucket, key:{}, err:{}", key, e);

                res.map_err(|e| Box::new(e) as _)
                    .context(PersistValue { key })?;
            }
        }

        Ok(bucket)
    }

    fn get_bucket_by_key<T: TableKv>(
        &self,
        inner: &NamespaceInner<T>,
        key: &str,
    ) -> Result<Bucket> {
        let value = get_value(&inner.table_kv, &inner.meta_table_name, key)?;
        let bucket_entry = BucketEntry::decode(&value).context(Decode { key })?;

        let bucket = Bucket::new(inner.name(), bucket_entry);

        Ok(bucket)
    }
}

fn get_value<T: TableKv>(table_kv: &T, meta_table_name: &str, key: &str) -> Result<Vec<u8>> {
    table_kv
        .get(meta_table_name, key.as_bytes())
        .map_err(|e| Box::new(e) as _)
        .context(GetValue { key })?
        .context(ValueNotFound { key })
}

fn get_namespace_entry_by_key<T: TableKv>(
    table_kv: &T,
    meta_table_name: &str,
    key: &str,
) -> Result<NamespaceEntry> {
    let value = get_value(table_kv, meta_table_name, key)?;
    let namespace_entry = NamespaceEntry::decode(&value).context(Decode { key })?;

    Ok(namespace_entry)
}

pub struct Namespace<T> {
    inner: Arc<NamespaceInner<T>>,
    monitor_handle: Option<TaskHandle>,
    cleaner_handle: Option<TaskHandle>,
}

impl<T> fmt::Debug for Namespace<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let region_num = self.inner.regions.read().unwrap().len();

        f.debug_struct("Namespace")
            .field("entry", &self.inner.entry)
            .field("bucket_set", &self.inner.bucket_set)
            .field("regions", &region_num)
            .field("meta_table_name", &self.inner.meta_table_name)
            .field("region_meta_tables", &self.inner.region_meta_tables)
            .field("config", &self.inner.config)
            .finish()
    }
}

impl<T> Namespace<T> {
    /// Name of the namespace.
    #[inline]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    #[inline]
    pub fn read_runtime(&self) -> &Arc<Runtime> {
        &self.inner.runtimes.read_runtime
    }
}

// Blocking operations
impl<T: TableKv> Namespace<T> {
    pub fn open(
        table_kv: &T,
        runtimes: WalRuntimes,
        name: &str,
        mut config: NamespaceConfig,
    ) -> Result<Self> {
        config.sanitize();

        Self::init_meta_table(table_kv, consts::META_TABLE_NAME)?;

        let namespace =
            match Self::load_namespace_from_meta(table_kv, consts::META_TABLE_NAME, name)? {
                Some(namespace_entry) => Namespace::new(
                    runtimes,
                    table_kv.clone(),
                    consts::META_TABLE_NAME,
                    namespace_entry,
                    config,
                )?,
                None => Namespace::try_persist_namespace(
                    runtimes,
                    table_kv,
                    consts::META_TABLE_NAME,
                    name,
                    config,
                )?,
            };

        Ok(namespace)
    }

    /// Returns true if we ensure the table is already created.
    fn is_table_created(table_kv: &T, table_name: &str) -> bool {
        match table_kv.table_exists(table_name) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to check table existence, err:{}", e);

                false
            }
        }
    }

    fn init_meta_table(table_kv: &T, meta_table_name: &str) -> Result<()> {
        if !Self::is_table_created(table_kv, meta_table_name) {
            info!("Try to create meta table, table_name:{}", meta_table_name);

            table_kv
                .create_table(meta_table_name)
                .map_err(|e| Box::new(e) as _)
                .context(CreateTable)?;

            info!(
                "Create meta table successfully, table_name:{}",
                meta_table_name
            );
        }

        Ok(())
    }

    fn load_namespace_from_meta(
        table_kv: &T,
        meta_table_name: &str,
        namespace_name: &str,
    ) -> Result<Option<NamespaceEntry>> {
        let key = encoding::format_namespace_key(namespace_name);
        let value_opt = table_kv
            .get(meta_table_name, key.as_bytes())
            .map_err(|e| Box::new(e) as _)
            .context(GetValue { key: &key })?;

        match value_opt {
            Some(value) => {
                let namespace_entry = NamespaceEntry::decode(&value).context(Decode { key })?;
                Ok(Some(namespace_entry))
            }
            None => Ok(None),
        }
    }

    fn new(
        runtimes: WalRuntimes,
        table_kv: T,
        meta_table_name: &str,
        entry: NamespaceEntry,
        config: NamespaceConfig,
    ) -> Result<Self> {
        let mut region_meta_tables = Vec::with_capacity(entry.region_meta.shard_num);
        for shard_id in 0..entry.region_meta.shard_num {
            let table_name = encoding::format_region_meta_name(&entry.name, shard_id);
            region_meta_tables.push(table_name);
        }

        let bucket_set = BucketSet::new(config.ttl.is_some());

        let inner = Arc::new(NamespaceInner {
            runtimes: runtimes.clone(),
            table_kv,
            entry,
            bucket_set: RwLock::new(bucket_set),
            regions: RwLock::new(HashMap::new()),
            meta_table_name: meta_table_name.to_string(),
            region_meta_tables,
            operator: Mutex::new(TableOperator),
            bucket_creator: Mutex::new(BucketCreator),
            config,
        });

        inner.init_region_meta()?;

        inner.load_buckets()?;

        let (mut monitor_handle, mut cleaner_handle) = (None, None);
        if inner.entry.wal.ttl.is_some() {
            info!("Start bucket monitor, namespace:{}", inner.name());

            // Has ttl, we need to periodically create/purge buckets.
            monitor_handle = Some(start_bucket_monitor(
                &runtimes.bg_runtime,
                BUCKET_MONITOR_PERIOD,
                inner.clone(),
            ));
        } else {
            info!("Start log cleaner, namespace:{}", inner.name());

            // Start a cleaner if wal has no ttl.
            cleaner_handle = Some(start_log_cleaner(
                &runtimes.bg_runtime,
                LOG_CLEANER_PERIOD,
                inner.clone(),
            ));
        }

        let namespace = Self {
            inner,
            monitor_handle,
            cleaner_handle,
        };

        Ok(namespace)
    }

    /// Try to persist the namespace, if the namespace already exists, returns
    /// the existing namespace.
    fn try_persist_namespace(
        runtimes: WalRuntimes,
        table_kv: &T,
        meta_table_name: &str,
        namespace: &str,
        config: NamespaceConfig,
    ) -> Result<Namespace<T>> {
        let mut namespace_entry = config
            .new_namespace_entry(namespace)
            .context(BuildNamepsace { namespace })?;

        let key = encoding::format_namespace_key(namespace);
        let value = namespace_entry.encode().context(Encode { namespace })?;

        let mut batch = T::WriteBatch::default();
        batch.insert(key.as_bytes(), &value);

        // Try to persist namespace entry.
        let res = table_kv.write(WriteContext::default(), meta_table_name, batch);
        if let Err(e) = &res {
            if e.is_primary_key_duplicate() {
                // Another client has already persisted the namespace.
                info!(
                    "Namespace already persisted, key:{}, config:{:?}",
                    key, config
                );

                // Load given namespace from storage.
                namespace_entry = get_namespace_entry_by_key(table_kv, meta_table_name, &key)?;

                info!(
                    "Load namespace from storage, key:{}, entry:{:?}",
                    key, namespace_entry
                );
            } else {
                error!("Failed to persist namespace, key:{}, err:{}", key, e);

                res.map_err(|e| Box::new(e) as _)
                    .context(PersistValue { key })?;
            }
        }

        Namespace::new(
            runtimes,
            table_kv.clone(),
            meta_table_name,
            namespace_entry,
            config,
        )
    }
}

// Async operations.
impl<T: TableKv> Namespace<T> {
    /// Write log to this namespace.
    pub async fn write_log(
        &self,
        ctx: &manager::WriteContext,
        batch: &LogWriteBatch<'_>,
    ) -> Result<SequenceNumber> {
        self.inner.write_log(ctx, batch).await
    }

    /// Get last sequence number of this region.
    pub async fn last_sequence(&self, region_id: RegionId) -> Result<SequenceNumber> {
        self.inner.last_sequence(region_id).await
    }

    /// Read log from this namespace. Note that the iterating the iterator may
    /// still block caller thread now.
    pub async fn read_log(
        &self,
        ctx: &ReadContext,
        req: &ReadRequest,
    ) -> Result<TableLogIterator<T>> {
        self.inner.read_log(ctx, req).await
    }

    /// Delete entries up to `sequence_num` of region identified by `region_id`.
    pub async fn delete_entries(
        &self,
        region_id: RegionId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        self.inner.delete_entries(region_id, sequence_num).await
    }

    /// Stop background tasks and close this namespace.
    pub async fn close(&self) -> Result<()> {
        info!("Try to close namespace, namespace:{}", self.name());

        self.inner.clear_regions();

        if let Some(monitor_handle) = &self.monitor_handle {
            monitor_handle.stop_task().await.context(StopTask {
                namespace: self.name(),
            })?;
        }

        if let Some(cleaner_handle) = &self.cleaner_handle {
            cleaner_handle.stop_task().await.context(StopTask {
                namespace: self.name(),
            })?;
        }

        info!("Namespace closed, namespace:{}", self.name());

        Ok(())
    }
}

pub type NamespaceRef<T> = Arc<Namespace<T>>;

/// Table operator wraps create/drop table operations.
struct TableOperator;

impl TableOperator {
    fn create_table_if_needed<T: TableKv>(
        &mut self,
        table_kv: &T,
        namespace: &str,
        table_name: &str,
    ) -> Result<()> {
        let table_exists = table_kv
            .table_exists(table_name)
            .map_err(|e| Box::new(e) as _)
            .context(OpenBucket { namespace })?;
        if !table_exists {
            table_kv
                .create_table(table_name)
                .map_err(|e| Box::new(e) as _)
                .context(OpenBucket { namespace })?;
        }

        Ok(())
    }
}

/// Time buckets of a namespace, orderded by start time.
#[derive(Debug)]
pub enum BucketSet {
    Timed(BTreeMap<Timestamp, BucketRef>),
    Permanent(Option<BucketRef>),
}

impl BucketSet {
    fn new(has_ttl: bool) -> Self {
        if has_ttl {
            BucketSet::Timed(BTreeMap::new())
        } else {
            BucketSet::Permanent(None)
        }
    }

    fn insert_bucket(&mut self, bucket: BucketRef) {
        let old_bucket = match self {
            BucketSet::Timed(buckets) => {
                buckets.insert(bucket.entry.gmt_start_ms(), bucket.clone())
            }
            BucketSet::Permanent(old_bucket) => old_bucket.replace(bucket.clone()),
        };

        assert!(
            old_bucket.is_none(),
            "Try to overwrite old bucket, old_bucket:{:?}, new_bucket:{:?}",
            old_bucket,
            bucket,
        );
    }

    /// Get bucket by start time. The caller need to ensure the timestamp is
    /// aligned to the bucket duration.
    fn get_bucket(&self, start_ms: Timestamp) -> Option<&BucketRef> {
        match self {
            BucketSet::Timed(buckets) => buckets.get(&start_ms),
            BucketSet::Permanent(bucket) => bucket.as_ref(),
        }
    }

    fn expired_buckets(&self, now: Timestamp, ttl: Duration) -> Vec<BucketRef> {
        match self {
            BucketSet::Timed(all_buckets) => {
                let mut buckets = Vec::new();
                if let Some(earliest) = now.checked_sub_duration(ttl) {
                    for (_ts, bucket) in all_buckets.range(..=earliest) {
                        if bucket.entry.is_expired(earliest) {
                            buckets.push(bucket.clone());
                        }
                    }
                }

                buckets
            }
            BucketSet::Permanent(_) => Vec::new(),
        }
    }

    fn buckets(&self) -> Vec<BucketRef> {
        match self {
            BucketSet::Timed(buckets) => buckets.values().cloned().collect(),
            BucketSet::Permanent(bucket) => match bucket {
                Some(b) => vec![b.clone()],
                None => Vec::new(),
            },
        }
    }

    /// Remove timed bucket, does nothing if this is a permanent bucket set.
    fn remove_timed_bucket(&mut self, start_ms: Timestamp) {
        if let BucketSet::Timed(buckets) = self {
            buckets.remove(&start_ms);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    entry: BucketEntry,
    wal_shard_names: Vec<String>,
}

impl Bucket {
    fn new(namespace: &str, entry: BucketEntry) -> Self {
        let mut wal_shard_names = Vec::with_capacity(entry.shard_num);

        for shard_id in 0..entry.shard_num {
            let table_name = if entry.is_permanent() {
                encoding::format_permanent_wal_name(namespace, shard_id)
            } else {
                encoding::format_timed_wal_name(namespace, entry.gmt_start_ms(), shard_id)
            };

            wal_shard_names.push(table_name);
        }

        Self {
            entry,
            wal_shard_names,
        }
    }

    #[inline]
    pub fn gmt_start_ms(&self) -> Timestamp {
        self.entry.gmt_start_ms()
    }

    #[inline]
    pub fn wal_shard_table(&self, region_id: RegionId) -> &str {
        let index = region_id as usize % self.wal_shard_names.len();
        &self.wal_shard_names[index]
    }

    fn format_bucket_key(&self, namespace: &str) -> String {
        match self.entry.bucket_duration() {
            Some(bucket_duration) => {
                // Timed bucket.
                encoding::format_timed_bucket_key(
                    namespace,
                    ReadableDuration(bucket_duration),
                    self.entry.gmt_start_ms(),
                )
            }
            None => {
                // This is a permanent bucket.
                encoding::format_permanent_bucket_key(namespace)
            }
        }
    }
}

pub type BucketRef = Arc<Bucket>;

async fn log_cleaner_routine<T: TableKv>(inner: Arc<NamespaceInner<T>>) {
    debug!(
        "Periodical log cleaning process start, namespace:{}",
        inner.name(),
    );

    if let Err(e) = inner.clean_deleted_logs() {
        error!(
            "Failed to clean deleted logs, namespace:{}, err:{}",
            inner.name(),
            e,
        );
    }

    debug!(
        "Periodical log cleaning process end, namespace:{}",
        inner.name()
    );
}

fn start_log_cleaner<T: TableKv>(
    runtime: &Runtime,
    period: Duration,
    namespace: Arc<NamespaceInner<T>>,
) -> TaskHandle {
    let name = format!("LogCleaner-{}", namespace.name());
    let builder = move || {
        let inner = namespace.clone();

        log_cleaner_routine(inner)
    };

    TimedTask::start_timed_task(name, runtime, period, builder)
}

async fn bucket_monitor_routine<T: TableKv>(inner: Arc<NamespaceInner<T>>, now: Timestamp) {
    debug!(
        "Periodical bucket monitor process start, namespace:{}, now:{:?}.",
        inner.name(),
        now
    );

    // Now failure of one namespace won't abort the whole manage procedure.
    if let Err(e) = inner.create_next_bucket(now) {
        error!(
            "Failed to create next bucket, namespace:{}, now:{:?}, err:{}",
            inner.name(),
            now,
            e,
        );
    }

    if let Err(e) = inner.purge_expired_buckets(now) {
        error!(
            "Failed to purge expired buckets, namespace:{}, now:{:?}, err:{}",
            inner.name(),
            now,
            e,
        );
    }

    debug!(
        "Periodical bucket monitor process end, namespace:{}",
        inner.name()
    );
}

fn start_bucket_monitor<T: TableKv>(
    runtime: &Runtime,
    period: Duration,
    namespace: Arc<NamespaceInner<T>>,
) -> TaskHandle {
    let name = format!("BucketMonitor-{}", namespace.name());
    let builder = move || {
        let inner = namespace.clone();
        let now = Timestamp::now();

        bucket_monitor_routine(inner, now)
    };

    TimedTask::start_timed_task(name, runtime, period, builder)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_types::bytes::BytesMut;
    use common_util::runtime::{Builder, Runtime};
    use table_kv::{memory::MemoryImpl, KeyBoundary, ScanContext, ScanRequest};

    use super::*;
    use crate::{
        log_batch::{LogWriteEntry, PayloadDecoder},
        table_kv_impl::{consts, encoding::LogEncoding},
        tests::util::{TestPayload, TestPayloadDecoder},
    };

    fn new_runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        Arc::new(runtime)
    }

    fn new_wal_runtimes(runtime: Arc<Runtime>) -> WalRuntimes {
        WalRuntimes {
            read_runtime: runtime.clone(),
            write_runtime: runtime.clone(),
            bg_runtime: runtime,
        }
    }

    struct NamespaceMocker<T> {
        name: String,
        table_kv: T,
        runtime: Arc<Runtime>,
        ttl: Option<Duration>,
    }

    impl<T: TableKv> NamespaceMocker<T> {
        fn new(table_kv: T, runtime: Arc<Runtime>) -> Self {
            Self {
                name: "test".to_string(),
                table_kv,
                runtime,
                ttl: None,
            }
        }

        fn ttl(mut self, ttl: Option<Duration>) -> Self {
            self.ttl = ttl;
            self
        }

        fn build(self) -> Namespace<T> {
            let config = NamespaceConfig {
                wal_shard_num: 4,
                region_meta_shard_num: 4,
                ttl: self.ttl.map(Into::into),
                ..Default::default()
            };
            let wal_runtimes = new_wal_runtimes(self.runtime);

            Namespace::open(&self.table_kv, wal_runtimes, &self.name, config).unwrap()
        }
    }

    #[test]
    fn test_timed_bucket() {
        // Gmt time: 2022-03-28 00:00:00
        let gmt_start_ms = Timestamp::new(1648425600000);
        let entry = BucketEntry::new_timed(4, gmt_start_ms, BUCKET_DURATION_MS).unwrap();
        assert!(!entry.is_permanent());

        let bucket = Bucket::new("test", entry);
        assert_eq!(4, bucket.wal_shard_names.len());
        let expect_names = [
            "wal_test_20220328000000_000000",
            "wal_test_20220328000000_000001",
            "wal_test_20220328000000_000002",
            "wal_test_20220328000000_000003",
        ];
        assert_eq!(&expect_names[..], &bucket.wal_shard_names[..]);
    }

    #[test]
    fn test_permanent_bucket() {
        let entry = BucketEntry::new_permanent(4);
        assert!(entry.is_permanent());

        let bucket = Bucket::new("test", entry);
        assert_eq!(4, bucket.wal_shard_names.len());
        let expect_names = [
            "wal_test_permanent_000000",
            "wal_test_permanent_000001",
            "wal_test_permanent_000002",
            "wal_test_permanent_000003",
        ];
        assert_eq!(&expect_names[..], &bucket.wal_shard_names[..]);
    }

    #[test]
    fn test_permanent_bucket_set() {
        let entry = BucketEntry::new_permanent(4);
        let bucket = Arc::new(Bucket::new("test", entry));

        let mut bucket_set = BucketSet::new(false);
        let buckets = bucket_set.buckets();
        assert!(buckets.is_empty());

        bucket_set.insert_bucket(bucket);

        assert!(bucket_set.get_bucket(Timestamp::ZERO).is_some());
        assert!(bucket_set.get_bucket(Timestamp::MIN).is_some());
        assert!(bucket_set.get_bucket(Timestamp::MAX).is_some());
        assert!(bucket_set
            .get_bucket(Timestamp::new(1648425600000))
            .is_some());

        let buckets = bucket_set.buckets();
        assert_eq!(1, buckets.len());

        assert!(bucket_set
            .expired_buckets(Timestamp::MAX, Duration::from_millis(100))
            .is_empty());

        bucket_set.remove_timed_bucket(Timestamp::ZERO);
        assert!(bucket_set.get_bucket(Timestamp::ZERO).is_some());
    }

    fn new_timed_bucket(ts: Timestamp) -> BucketRef {
        let entry = BucketEntry::new_timed(1, ts, BUCKET_DURATION_MS).unwrap();

        Arc::new(Bucket::new("test", entry))
    }

    #[test]
    fn test_timed_bucket_set() {
        let mut bucket_set = BucketSet::new(true);

        let timestamps = [
            // Gmt time: 2022-03-20 00:00:00
            Timestamp::new(1647734400000),
            // Gmt time: 2022-03-21 00:00:00
            Timestamp::new(1647820800000),
            // Gmt time: 2022-03-22 00:00:00
            Timestamp::new(1647907200000),
            // Gmt time: 2022-03-23 00:00:00
            Timestamp::new(1647993600000),
        ];

        bucket_set.insert_bucket(new_timed_bucket(timestamps[0]));
        bucket_set.insert_bucket(new_timed_bucket(timestamps[1]));
        bucket_set.insert_bucket(new_timed_bucket(timestamps[2]));

        assert_eq!(3, bucket_set.buckets().len());

        for ts in &timestamps[0..3] {
            let bucket = bucket_set.get_bucket(*ts).unwrap();
            assert_eq!(*ts, bucket.entry.gmt_start_ms());
        }
        assert!(bucket_set.get_bucket(timestamps[3]).is_none());

        // Insert bucket of last timestamp.
        bucket_set.insert_bucket(new_timed_bucket(timestamps[3]));

        let ttl_1d = Duration::from_millis(consts::DAY_MS);
        // No expired bucket.
        assert!(bucket_set.expired_buckets(timestamps[0], ttl_1d).is_empty());
        assert!(bucket_set.expired_buckets(timestamps[1], ttl_1d).is_empty());
        // One expired bucket.
        let expired = bucket_set.expired_buckets(timestamps[2], ttl_1d);
        assert_eq!(1, expired.len());
        assert_eq!(timestamps[0], expired[0].entry.gmt_start_ms());
        // No expired bucket.
        assert!(bucket_set
            .expired_buckets(Timestamp::new(timestamps[2].as_i64() - 1), ttl_1d)
            .is_empty());
        // Now: 2022-03-22 08:00:00 GMT , one expired bucket.
        let expired = bucket_set.expired_buckets(Timestamp::new(1647936000000), ttl_1d);
        assert_eq!(1, expired.len());
        assert_eq!(timestamps[0], expired[0].entry.gmt_start_ms());
        // Now: 2022-03-23 08:00:00 GMT , two expired bucekts.
        let expired = bucket_set.expired_buckets(Timestamp::new(1648022400000), ttl_1d);
        assert_eq!(2, expired.len());
        assert_eq!(timestamps[0], expired[0].entry.gmt_start_ms());
        assert_eq!(timestamps[1], expired[1].entry.gmt_start_ms());

        bucket_set.remove_timed_bucket(timestamps[0]);
        assert!(bucket_set.get_bucket(timestamps[0]).is_none());
        // It is okay to remove again.
        bucket_set.remove_timed_bucket(timestamps[0]);
        bucket_set.remove_timed_bucket(timestamps[2]);
        let buckets = bucket_set.buckets();
        assert_eq!(2, buckets.len());
        assert_eq!(timestamps[1], buckets[0].entry.gmt_start_ms());
        assert_eq!(timestamps[3], buckets[1].entry.gmt_start_ms());
    }

    #[test]
    fn test_bucket_monitor_routine_no_ttl() {
        let runtime = new_runtime();
        let table_kv = MemoryImpl::default();

        runtime.block_on(async {
            let namespace = NamespaceMocker::new(table_kv, runtime.clone()).build();

            // Bucket monitor is disabled, log cleaner is enabled.
            assert!(namespace.monitor_handle.is_none());
            assert!(namespace.cleaner_handle.is_some());

            let inner = &namespace.inner;

            // Gmt time: 2022-03-28 00:00:00
            let today = Timestamp::new(1648425600000);

            // Should create permanent bucket.
            bucket_monitor_routine(inner.clone(), today).await;

            let buckets = inner.list_buckets();
            assert_eq!(1, buckets.len());
            assert!(buckets[0].entry.is_permanent());
            assert_eq!(Timestamp::ZERO, buckets[0].entry.gmt_start_ms());
            assert_eq!(Timestamp::MAX, buckets[0].entry.gmt_end_ms());

            let tomorrow = Timestamp::new(today.as_i64() + consts::DAY_MS as i64);
            bucket_monitor_routine(inner.clone(), tomorrow).await;
            assert_eq!(1, buckets.len());
            assert!(buckets[0].entry.is_permanent());

            namespace.close().await.unwrap();
        });
    }

    #[test]
    fn test_bucket_monitor_routine_ttl() {
        let runtime = new_runtime();
        let table_kv = MemoryImpl::default();

        runtime.block_on(async {
            let namespace = NamespaceMocker::new(table_kv, runtime.clone())
                .ttl(Some(Duration::from_millis(BUCKET_DURATION_MS as u64)))
                .build();

            // Bucket monitor is enabled, log cleaner is disabled.
            assert!(namespace.monitor_handle.is_some());
            assert!(namespace.cleaner_handle.is_none());

            let inner = &namespace.inner;

            // Gmt time: 2022-03-28 00:00:00
            let today = Timestamp::new(1648425600000);
            let yesterday = Timestamp::new(today.as_i64() - consts::DAY_MS as i64);
            let tomorrow = Timestamp::new(today.as_i64() + consts::DAY_MS as i64);
            let after_two_days = Timestamp::new(tomorrow.as_i64() + consts::DAY_MS as i64);
            let after_three_days = Timestamp::new(after_two_days.as_i64() + consts::DAY_MS as i64);

            // Create next bucket of yesterday.
            bucket_monitor_routine(inner.clone(), yesterday).await;

            let buckets = inner.list_buckets();
            assert_eq!(1, buckets.len());
            assert!(!buckets[0].entry.is_permanent());
            // This is today's bucket.
            assert_eq!(today, buckets[0].entry.gmt_start_ms());
            assert_eq!(tomorrow, buckets[0].entry.gmt_end_ms());

            // Create next bucket of today.
            bucket_monitor_routine(inner.clone(), today).await;

            let buckets = inner.list_buckets();
            assert_eq!(2, buckets.len());
            assert_eq!(today, buckets[0].entry.gmt_start_ms());
            assert_eq!(tomorrow, buckets[1].entry.gmt_start_ms());

            // Create next bucket of tomorrow.
            bucket_monitor_routine(inner.clone(), tomorrow).await;

            let buckets = inner.list_buckets();
            assert_eq!(3, buckets.len());
            assert_eq!(today, buckets[0].entry.gmt_start_ms());
            assert_eq!(tomorrow, buckets[1].entry.gmt_start_ms());
            assert_eq!(after_two_days, buckets[2].entry.gmt_start_ms());

            // Create next bucket of two days later, expire one bucket.
            bucket_monitor_routine(inner.clone(), after_two_days).await;

            let buckets = inner.list_buckets();
            assert_eq!(3, buckets.len());
            assert_eq!(tomorrow, buckets[0].entry.gmt_start_ms());
            assert_eq!(after_two_days, buckets[1].entry.gmt_start_ms());
            assert_eq!(after_three_days, buckets[2].entry.gmt_start_ms());

            namespace.close().await.unwrap();
        });
    }

    #[test]
    fn test_log_cleanner_routine() {
        let runtime = new_runtime();
        let table_kv = MemoryImpl::default();

        runtime.block_on(async {
            let namespace = NamespaceMocker::new(table_kv.clone(), runtime.clone()).build();
            let region_id = 123;

            let seq1 = write_test_payloads(&namespace, region_id, 1000, 1004).await;
            write_test_payloads(&namespace, region_id, 1005, 1009).await;

            namespace.delete_entries(region_id, seq1).await.unwrap();

            let inner = &namespace.inner;
            log_cleaner_routine(inner.clone()).await;

            let buckets = inner.list_buckets();
            assert_eq!(1, buckets.len());

            let table = buckets[0].wal_shard_table(region_id);
            let key_values = direct_read_logs_from_table(&table_kv, table, region_id).await;

            // Logs from min sequence to seq1 should be deleted from the table.
            let mut expect_seq = seq1 + 1;
            let mut expect_val = 1005;
            for (k, v) in key_values {
                assert_eq!(expect_seq, k);
                assert_eq!(expect_val, v.val);

                expect_seq += 1;
                expect_val += 1;
            }

            namespace.close().await.unwrap();
        });
    }

    async fn direct_read_logs_from_table<T: TableKv>(
        table_kv: &T,
        table_name: &str,
        region_id: RegionId,
    ) -> Vec<(SequenceNumber, TestPayload)> {
        let log_encoding = LogEncoding::newest();

        let mut start_key = BytesMut::new();
        log_encoding
            .encode_key(
                &mut start_key,
                &(region_id, common_types::MIN_SEQUENCE_NUMBER),
            )
            .unwrap();
        let mut end_key = BytesMut::new();
        log_encoding
            .encode_key(
                &mut end_key,
                &(region_id, common_types::MAX_SEQUENCE_NUMBER),
            )
            .unwrap();

        let scan_req = ScanRequest {
            start: KeyBoundary::included(&start_key),
            end: KeyBoundary::included(&end_key),
            reverse: false,
        };
        let mut iter = table_kv
            .scan(ScanContext::default(), table_name, scan_req)
            .unwrap();

        let decoder = TestPayloadDecoder;
        let mut key_values = Vec::new();
        while iter.valid() {
            let decoded_key = log_encoding.decode_key(iter.key()).unwrap();
            let mut raw_value = log_encoding.decode_value(iter.value()).unwrap();
            let decoded_value = decoder.decode(&mut raw_value).unwrap();
            key_values.push((decoded_key.1, decoded_value));

            iter.next().unwrap();
        }

        key_values
    }

    async fn write_test_payloads<T: TableKv>(
        namespace: &Namespace<T>,
        region_id: RegionId,
        start_sequence: u32,
        end_sequence: u32,
    ) -> SequenceNumber {
        let write_ctx = manager::WriteContext::default();
        let mut last_sequence = 0;
        for val in start_sequence..end_sequence {
            let mut wb = LogWriteBatch::new(region_id);
            let payload = TestPayload { val };
            wb.push(LogWriteEntry { payload: &payload });

            last_sequence = namespace.write_log(&write_ctx, &wb).await.unwrap();
        }

        last_sequence
    }
}
