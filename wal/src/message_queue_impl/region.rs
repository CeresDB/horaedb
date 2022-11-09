// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Region in wal on message queue

use std::{cmp, sync::Arc};

use common_types::{bytes::BytesMut, table::TableId, SequenceNumber};
use common_util::define_result;
use log::{debug, info};
use message_queue::{ConsumeIterator, MessageQueue, Offset, OffsetType, StartOffset};
use snafu::{ensure, Backtrace, OptionExt, ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};
use util::*;

use super::region_meta::RegionMetaBuilder;
use crate::{
    kv_encoder::{CommonLogEncoding, CommonLogKey},
    log_batch::{LogEntry, LogWriteBatch},
    manager::{self, RegionId},
    message_queue_impl::{
        self,
        encoding::{format_wal_data_topic_name, format_wal_meta_topic_name, MetaEncoding},
        log_cleaner::LogCleaner,
        region_meta::{
            self, OffsetRange, RegionMeta, RegionMetaDelta, RegionMetaSnapshot, TableMetaData,
        },
        snapshot_synchronizer::{self, SnapshotSynchronizer},
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Write logs to region failed, region id:{}, table id:{}, err:{}",
        region_id,
        table_id,
        source
    ))]
    WriteWithCause {
        region_id: RegionId,
        table_id: TableId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Write logs to region failed with no cause, region id:{}, table id:{}, msg:{}, \nBacktrace:\n{}",
        region_id,
        table_id,
        msg,
        backtrace
    ))]
    WriteNoCause {
        region_id: RegionId,
        table_id: TableId,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Scan logs from region failed, region id:{}, msg:{}\nBacktrace:{}",
        region_id,
        msg,
        backtrace
    ))]
    ScanNoCause {
        region_id: RegionId,
        table_id: Option<TableId>,
        msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Scan logs from region failed with cause, region id:{}, table id:{:?}, msg:{:?}, err:{}",
        region_id,
        table_id,
        msg,
        source
    ))]
    ScanWithCause {
        region_id: RegionId,
        table_id: Option<TableId>,
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Get table meta data failed, region id:{}, table id:{}, err:{}",
        region_id,
        table_id,
        source
    ))]
    GetTableMeta {
        region_id: RegionId,
        table_id: TableId,
        source: region_meta::Error,
    },

    #[snafu(display(
        "Get snapshot from region failed, region id:{}, err:{}",
        region_id,
        source
    ))]
    GetMetaSnapshot {
        region_id: RegionId,
        source: region_meta::Error,
    },

    #[snafu(display(
        "Mark deleted sequence to table failed, region id:{}, table id:{:?}, err:{}",
        region_id,
        table_id,
        source
    ))]
    MarkDeleted {
        region_id: RegionId,
        table_id: TableId,
        source: region_meta::Error,
    },

    #[snafu(display("Sync snapshot of region failed, err:{}", source))]
    SyncSnapshot {
        source: snapshot_synchronizer::Error,
    },

    #[snafu(display("Clean logs of region failed, err:{}", source))]
    CleanLogs {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Open region failed with cause, namespace:{}, region id:{}, msg:{}, err:{}",
        namespace,
        region_id,
        msg,
        source
    ))]
    OpenWithCause {
        namespace: String,
        region_id: RegionId,
        msg: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Open region failed with no cause, namespace:{}, region id:{}, msg:{}, \nBacktrace:\n{}",
        namespace,
        region_id,
        msg,
        backtrace
    ))]
    OpenNoCause {
        namespace: String,
        region_id: RegionId,
        msg: String,
        backtrace: Backtrace,
    },
}

define_result!(Error);

/// Region in wal(message queue based)
#[allow(unused)]
pub struct Region<M: MessageQueue> {
    /// Region inner, see [RegionInner]
    ///
    /// Most of time, lock by `read lock`.
    /// While needing to freeze the region(such as, make a snapshot),
    /// `write lock` will be used.
    inner: RwLock<RegionInner<M>>,

    /// Will synchronize the snapshot to message queue by it
    ///
    /// Lock for forcing the snapshots to be synchronized sequentially.
    snapshot_synchronizer: Mutex<SnapshotSynchronizer<M>>,

    /// Clean the outdated logs which are marked delete
    log_cleaner: LogCleaner<M>,
}

#[allow(unused)]
impl<M: MessageQueue> Region<M> {
    /// Init the region.
    pub async fn open(namespace: &str, region_id: RegionId, message_queue: Arc<M>) -> Result<Self> {
        info!(
            "Open region in namespace, namespace:{}, region id:{}",
            namespace, region_id
        );

        let log_encoding = CommonLogEncoding::newest();
        let meta_encoding = MetaEncoding::newest();

        // Format to the topic name.
        let log_topic = format_wal_data_topic_name(namespace, region_id);
        let meta_topic = format_wal_meta_topic_name(namespace, region_id);
        message_queue
            .create_topic_if_not_exist(&log_topic)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while trying to create topic",
            })?;
        message_queue
            .create_topic_if_not_exist(&meta_topic)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while trying to create topic",
            })?;

        // Build region meta.
        let mut region_meta_builder = RegionMetaBuilder::default();
        let high_watermark_in_snapshot = Self::recover_region_meta_from_meta(
            namespace,
            region_id,
            &mut region_meta_builder,
            message_queue.as_ref(),
            &meta_topic,
            &meta_encoding,
        )
        .await?;
        Self::recover_region_meta_from_log(
            namespace,
            region_id,
            &mut region_meta_builder,
            message_queue.as_ref(),
            high_watermark_in_snapshot,
            &log_topic,
            &log_encoding,
        )
        .await?;

        // Init region inner.
        let inner = RwLock::new(RegionInner::new(
            region_id,
            region_meta_builder.build(),
            CommonLogEncoding::newest(),
            message_queue.clone(),
            log_topic.clone(),
        ));

        // Init others.
        let snapshot_synchronizer = Mutex::new(SnapshotSynchronizer::new(
            region_id,
            message_queue.clone(),
            meta_topic,
            MetaEncoding::newest(),
        ));
        let log_cleaner = LogCleaner::new(region_id, message_queue.clone(), log_topic);

        Ok(Region {
            inner,
            snapshot_synchronizer,
            log_cleaner,
        })
    }

    async fn recover_region_meta_from_meta(
        namespace: &str,
        region_id: RegionId,
        builder: &mut RegionMetaBuilder,
        message_queue: &M,
        meta_topic: &str,
        meta_encoding: &MetaEncoding,
    ) -> Result<Offset> {
        // Fetch earliest offset and high watermark, then check.
        let high_watermark = message_queue
            .fetch_offset(meta_topic, OffsetType::HighWaterMark)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            })?;

        if high_watermark == 0 {
            return Ok(0);
        }

        // Fetch snapshot from meta topic(just fetch the last snapshot).
        let mut iter = message_queue
            .consume(meta_topic, StartOffset::At(high_watermark - 1))
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            })?;

        let (latest_message_and_offset, returned_high_watermark) = iter
            .next_message()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            })?;

        // TODO: maybe should assert it?
        ensure!(returned_high_watermark == high_watermark, OpenNoCause { namespace , region_id, msg: format!(
            "failed while region_meta_from_meta, high watermark shouldn't changed while opening region, 
            origin high watermark:{}, returned high watermark:{}", high_watermark, returned_high_watermark)
        });

        // Decode and apply it to builder.
        let raw_key = latest_message_and_offset
            .message
            .key
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta, key in message shouldn't be None",
            })?;

        let raw_value = latest_message_and_offset
            .message
            .value
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta, value in message shouldn't be None",
            })?;

        let key = meta_encoding
            .decode_key(raw_key.as_slice())
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            })?;

        ensure!(key.0 == region_id, OpenNoCause { namespace , region_id, msg: format!(
            "failed while region_meta_from_meta, region id in key should be equal to the one of current region,
            but now are {} and {}", key.0, region_id)
        });

        let value = meta_encoding
            .decode_value(raw_value.as_slice())
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            })?;

        let high_watermark_in_snapshot = value
            .entries
            .iter()
            .fold(0, |hw, entry| cmp::max(hw, entry.current_high_watermark));

        builder
            .apply_region_meta_snapshot(value)
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_meta",
            });

        Ok(high_watermark_in_snapshot)
    }

    async fn recover_region_meta_from_log(
        namespace: &str,
        region_id: RegionId,
        builder: &mut RegionMetaBuilder,
        message_queue: &M,
        start_offset: Offset,
        log_topic: &str,
        log_encoding: &CommonLogEncoding,
    ) -> Result<()> {
        // Fetch high watermark and check.
        let high_watermark = message_queue
            .fetch_offset(log_topic, OffsetType::HighWaterMark)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log",
            })?;

        ensure!(start_offset <= high_watermark, OpenNoCause { namespace , region_id, msg: format!(
            "failed while region_meta_from_log, start offset should be less than or equal to high watermark, now are:{} and {}",
            start_offset, high_watermark)
        });

        if start_offset == high_watermark {
            return Ok(());
        }

        // Fetch snapshot from meta topic(just fetch the last snapshot).
        let mut iter = message_queue
            .consume(log_topic, StartOffset::At(start_offset))
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log",
            })?;

        let (latest_message_and_offset, returned_high_watermark) = iter
            .next_message()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log",
            })?;

        // TODO: maybe should assert it?
        ensure!(returned_high_watermark == high_watermark, OpenNoCause { namespace , region_id, msg: format!(
            "failed while region_meta_from_log, high watermark shouldn't changed while opening region, 
            origin high watermark:{}, returned high watermark:{}", high_watermark, returned_high_watermark)
        });

        // Decode and apply it to builder.
        let raw_key = latest_message_and_offset
            .message
            .key
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log, key in message shouldn't be None",
            })?;

        let raw_value = latest_message_and_offset
            .message
            .value
            .with_context(|| OpenNoCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log, value in message shouldn't be None",
            })?;

        let key = log_encoding
            .decode_key(raw_key.as_slice())
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: "failed while region_meta_from_log",
            })?;

        ensure!(key.region_id == region_id, OpenNoCause { namespace , region_id, msg: format!(
            "failed while region_meta_from_log, region id in key should be equal to the one of current region,
            but now are {} and {}", key.region_id, region_id)
        });

        // TODO: maybe this clone should be avoided?
        let region_meta_delta = RegionMetaDelta::new(
            key.table_id,
            key.sequence_num,
            latest_message_and_offset.offset + 1,
        );
        builder
            .apply_region_meta_delta(region_meta_delta.clone())
            .map_err(|e| Box::new(e) as _)
            .context(OpenWithCause {
                namespace,
                region_id,
                msg: format!(
                    "failed while region_meta_from_log, region meta delta:{:?}",
                    region_meta_delta
                ),
            });

        Ok(())
    }

    /// Write logs of table to region.
    pub async fn write(
        &self,
        ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        let inner = self.inner.read().await;

        debug!(
            "Begin to write to wal region, ctx:{:?}, region_id:{}, location:{:?}, log_entries_num:{}",
            ctx,
            inner.region_id,
            log_batch.location,
            log_batch.entries.len()
        );

        inner.write(ctx, log_batch).await
    }

    /// Scan all logs from region.
    ///
    /// NOTICE: we get scan range from the region's snapshot, if call
    /// `mark_delete_to` during polling logs concurrently, it may lead to
    /// error.
    pub async fn scan_region(
        &self,
        ctx: &manager::ReadContext,
    ) -> Result<Option<MessageQueueLogIterator<M::ConsumeIterator>>> {
        // Calculate region's scan range from its snapshot.
        let scan_range = {
            let inner = self.inner.write().await;

            info!(
                "Prepare to scan all logs from region, region id:{}, log topic:{}, ctx:{:?}",
                inner.region_id, inner.log_topic, ctx
            );

            let snapshot = inner.make_meta_snapshot().await;
            let mut safe_delete_offset = Offset::MAX;
            let mut high_watermark = 0;
            // Calculate the min offset in message queue.
            for table_meta in &snapshot.entries {
                if let Some(offset) = table_meta.safe_delete_offset {
                    safe_delete_offset = cmp::min(safe_delete_offset, offset);
                }
                high_watermark = cmp::max(high_watermark, table_meta.current_high_watermark);
            }

            if safe_delete_offset == Offset::MAX {
                None
            } else {
                assert!(safe_delete_offset < high_watermark);
                Some(ScanRange::new(safe_delete_offset, high_watermark))
            }
        };

        match scan_range {
            Some(scan_range) => {
                let inner = self.inner.read().await;
                Ok(Some(
                    inner
                        .range_scan(ctx, None, scan_range)
                        .await
                        .context(ScanWithCause {
                            region_id: inner.region_id,
                            table_id: None,
                            msg: format!(
                                "failed while creating iterator, scan range:{:?}",
                                scan_range
                            ),
                        })?,
                ))
            }

            None => Ok(None),
        }
    }

    /// Scan logs of specific table from region.
    ///
    /// NOTICE: we get scan range from the table's snapshot, if call
    /// `mark_delete_to` for the same table during polling logs concurrently, it
    /// may lead to error.
    pub async fn scan_table(
        &self,
        table_id: TableId,
        ctx: &manager::ReadContext,
    ) -> Result<Option<MessageQueueLogIterator<M::ConsumeIterator>>> {
        let (table_id, scan_range) = {
            let inner = self.inner.read().await;

            debug!(
                "Prepare to scan logs of the table from region, region id:{}, table id:{}, log topic:{}, ctx:{:?}",
                inner.region_id, table_id, inner.log_topic, ctx
            );

            let table_meta = inner.get_table_meta(table_id).await?;
            if let Some(start_offset) = table_meta.safe_delete_offset {
                (
                    table_id,
                    Some(ScanRange::new(
                        start_offset,
                        table_meta.current_high_watermark,
                    )),
                )
            } else {
                (table_id, None)
            }
        };

        match scan_range {
            Some(scan_range) => {
                let inner = self.inner.read().await;
                Ok(Some(
                    inner
                        .range_scan(ctx, Some(table_id), scan_range)
                        .await
                        .context(ScanWithCause {
                            region_id: inner.region_id,
                            table_id: Some(table_id),
                            msg: format!(
                                "failed while creating iterator, scan range:{:?}",
                                scan_range
                            ),
                        })?,
                ))
            }

            None => Ok(None),
        }
    }

    /// Mark the entries whose sequence number is in [0, `next sequence number`)
    /// to be deleted in the future.
    pub async fn mark_delete_to(
        &self,
        table_id: TableId,
        sequence_num: SequenceNumber,
    ) -> Result<()> {
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;

            debug!(
                "Mark deleted entries to sequence num:{}, region id:{}, table id:{}",
                sequence_num, inner.region_id, table_id
            );

            inner.mark_delete_to(table_id, sequence_num).await.unwrap();

            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        // TODO: a temporary and rough implementation...
        // just need to sync the snapshot while dropping table, but now we sync while
        // every flushing... Just sync here now, obviously it is not enough.
        synchronizer.sync(snapshot).await.context(SyncSnapshot)
    }

    /// Get meta data by table id.
    pub async fn get_table_meta(&self, table_id: TableId) -> Result<TableMetaData> {
        let inner = self.inner.read().await;
        inner.get_table_meta(table_id).await
    }

    /// Clean outdated logs according to the information in region snapshot.
    #[allow(unused)]
    pub async fn clean_logs(&mut self) -> Result<()> {
        // Get current snapshot.
        let (snapshot, synchronizer) = {
            let inner = self.inner.write().await;
            (
                inner.make_meta_snapshot().await,
                self.snapshot_synchronizer.lock().await,
            )
        };

        // Check and maybe clean logs.
        self.log_cleaner
            .maybe_clean_logs(&snapshot)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(CleanLogs)?;

        // Sync snapshot.
        synchronizer
            .sync(snapshot)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(CleanLogs)
    }

    /// Return snapshot, just used for test.
    #[allow(unused)]
    async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        let inner = self.inner.write().await;
        inner.make_meta_snapshot().await
    }
}

/// Region's inner, all methods of [Region] are mainly implemented in it.
#[allow(unused)]
struct RegionInner<M> {
    /// Id of region
    region_id: RegionId,

    /// Region meta data(such as, tables' next sequence numbers)
    region_meta: RegionMeta,

    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,

    /// Message queue's Client
    message_queue: Arc<M>,

    /// Topic storing logs in message queue
    log_topic: String,
}

#[allow(unused)]
impl<M: MessageQueue> RegionInner<M> {
    pub fn new(
        region_id: RegionId,
        region_meta: RegionMeta,
        log_encoding: CommonLogEncoding,
        message_queue: Arc<M>,
        log_topic: String,
    ) -> Self {
        // TODO: use snapshot to recover `region_meta`.
        Self {
            region_id,
            region_meta,
            log_encoding,
            message_queue,
            log_topic,
        }
    }

    async fn write(
        &self,
        _ctx: &manager::WriteContext,
        log_batch: &LogWriteBatch,
    ) -> Result<SequenceNumber> {
        ensure!(
            !log_batch.is_empty(),
            WriteNoCause {
                region_id: self.region_id,
                table_id: log_batch.location.table_id,
                msg: "log batch passed should not be empty"
            }
        );

        let location = &log_batch.location;
        let log_write_entries = &log_batch.entries;

        // Create messages and prepare for write.
        let mut next_sequence_num = self
            .region_meta
            .prepare_for_table_write(location.table_id)
            .await;

        let mut messages = Vec::with_capacity(log_batch.entries.len());
        let mut key_buf = BytesMut::new();
        for entry in log_write_entries {
            let log_key = CommonLogKey::new(self.region_id, location.table_id, next_sequence_num);
            self.log_encoding
                .encode_key(&mut key_buf, &log_key)
                .map_err(|e| Box::new(e) as _)
                .context(WriteWithCause {
                    region_id: self.region_id,
                    table_id: location.table_id,
                })?;

            let message = message_queue_impl::to_message(key_buf.to_vec(), entry.payload.clone());
            messages.push(message);

            next_sequence_num += 1;
        }

        // Write.
        let offsets = self
            .message_queue
            .produce(&self.log_topic, messages)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteWithCause {
                region_id: self.region_id,
                table_id: location.table_id,
            })?;

        ensure!(
            !offsets.is_empty(),
            WriteNoCause {
                region_id: self.region_id,
                table_id: log_batch.location.table_id,
                msg: "returned offsets after producing to message queue shouldn't be empty"
            }
        );

        debug!(
            "Produce to topic success, ctx:{:?}, region_id:{}, location:{:?}, topic:{}",
            _ctx, self.region_id, log_batch.location, self.log_topic,
        );

        // Update after write.
        self.region_meta
            .update_after_table_write(
                location.table_id,
                OffsetRange::new(*offsets.first().unwrap(), *offsets.last().unwrap()),
            )
            .await
            .map_err(|e| Box::new(e) as _)
            .context(WriteWithCause {
                region_id: self.region_id,
                table_id: location.table_id,
            })?;

        Ok(next_sequence_num - 1)
    }

    // TODO: take each read's timeout in consideration.
    async fn range_scan(
        &self,
        _ctx: &manager::ReadContext,
        table_id: Option<TableId>,
        scan_range: ScanRange,
    ) -> std::result::Result<
        MessageQueueLogIterator<M::ConsumeIterator>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let consume_iter = self
            .message_queue
            .consume(&self.log_topic, StartOffset::At(scan_range.inclusive_start))
            .await
            .map_err(Box::new)?;

        debug!("Create scanning iterator successfully, region id:{}, table id:{:?}, log topic:{}, scan range{:?}", self.region_id, 
            table_id, self.log_topic, scan_range);

        Ok(MessageQueueLogIterator::new(
            self.region_id,
            table_id,
            Some(scan_range.exclusive_end),
            consume_iter,
            self.log_encoding.clone(),
        ))
    }

    async fn mark_delete_to(&self, table_id: TableId, sequence_num: SequenceNumber) -> Result<()> {
        self.region_meta
            .mark_table_deleted(table_id, sequence_num)
            .await
            .context(MarkDeleted {
                region_id: self.region_id,
                table_id,
            })
    }

    async fn get_table_meta(&self, table_id: TableId) -> Result<TableMetaData> {
        self.region_meta
            .get_table_meta_data(table_id)
            .await
            .context(GetTableMeta {
                region_id: self.region_id,
                table_id,
            })
    }

    /// Get meta data snapshot of whole region.
    ///
    /// NOTICE: should freeze whole region before calling.
    async fn make_meta_snapshot(&self) -> RegionMetaSnapshot {
        self.region_meta.make_snapshot().await
    }
}

// TODO: define some high-level iterator based on this,
// such as `RegionScanIterator` placing the high watermark invariant checking
// in it.
#[allow(unused)]
#[derive(Debug)]
pub struct MessageQueueLogIterator<C: ConsumeIterator> {
    /// Id of region
    region_id: RegionId,

    /// Id of table id
    ///
    /// It will be `None` while scanning region,
    /// and will be `Some` while scanning table.
    table_id: Option<TableId>,

    /// Polling's end point
    ///
    /// While fetching in slave node, it will be set to `None`, and
    /// reading will not stop.
    /// Otherwise, it will be set to high watermark.
    terminate_offset: Option<Offset>,

    /// Terminated flag
    is_terminated: bool,

    /// Consume Iterator of message queue
    iter: C,

    /// Used to encode/decode the logs
    log_encoding: CommonLogEncoding,
    // TODO: timeout
}

#[allow(unused)]
impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    fn new(
        region_id: RegionId,
        table_id: Option<TableId>,
        terminate_offset: Option<Offset>,
        iter: C,
        log_encoding: CommonLogEncoding,
    ) -> Self {
        Self {
            region_id,
            table_id,
            terminate_offset,
            iter,
            is_terminated: false,
            log_encoding,
        }
    }
}

#[allow(unused)]
impl<C: ConsumeIterator> MessageQueueLogIterator<C> {
    pub async fn next_log_entry(&mut self) -> Result<Option<LogEntry<Vec<u8>>>> {
        if self.is_terminated && self.terminate_offset.is_some() {
            debug!(
                "Finished to poll all logs from message queue, region id:{}, terminate offset:{:?}",
                self.region_id, self.terminate_offset
            );
            return Ok(None);
        }

        let (message_and_offset, high_watermark) = self
            .iter
            .next_message()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        if let Some(terminate_offset) = &self.terminate_offset {
            ensure!(*terminate_offset <= high_watermark, ScanNoCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: format!("the setting terminate offset is invalid, it should be less than or equals to high watermark, terminate offset:{}, high watermark:{}",
                    terminate_offset, high_watermark),
            });

            if message_and_offset.offset + 1 == *terminate_offset {
                self.is_terminated = true;
            }
        }

        // Decode the message to log key and value, then create the returned log entry.
        // Key and value in message should absolutely exist.
        let log_key = self
            .log_encoding
            .decode_key(&message_and_offset.message.key.unwrap())
            .map_err(|e| Box::new(e) as _)
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        ensure!(
            log_key.region_id == self.region_id,
            ScanNoCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: format!(
                    "invalid region id in message, real:{}, expected:{}",
                    self.region_id, log_key.region_id
                ),
            }
        );

        let log_value = message_and_offset.message.value.unwrap();
        let payload = self
            .log_encoding
            .decode_value(&log_value)
            .map_err(|e| Box::new(e) as _)
            .context(ScanWithCause {
                region_id: self.region_id,
                table_id: self.table_id,
                msg: "failed while polling log",
            })?;

        Ok(Some(LogEntry {
            table_id: log_key.table_id,
            sequence: log_key.sequence_num,
            payload: payload.to_owned(),
        }))
    }
}

mod util {
    use message_queue::Offset;

    #[derive(Debug, Default, Clone, Copy)]
    pub struct ScanRange {
        pub inclusive_start: Offset,
        pub exclusive_end: Offset,
    }

    impl ScanRange {
        pub fn new(inclusive_start: Offset, exclusive_end: Offset) -> Self {
            Self {
                inclusive_start,
                exclusive_end,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_types::table::{ShardId, TableId};
    use message_queue::{
        kafka::{config::Config, kafka_impl::KafkaImpl},
        ConsumeIterator, MessageQueue, OffsetType, StartOffset,
    };

    use crate::{
        log_batch::PayloadDecoder,
        manager::{ReadContext, RegionId, WriteContext},
        message_queue_impl::{encoding::MetaEncoding, test_util::TestContext},
    };

    #[tokio::test]
    #[ignore]
    async fn test_region_kafka_impl() {
        // Test region
        let mut config = Config::default();
        config.client_config.boost_broker = Some("127.0.0.1:9011".to_string());
        let kafka_impl = KafkaImpl::new(config).await.unwrap();
        let message_queue = Arc::new(kafka_impl);
        test_region(message_queue).await;
    }

    async fn test_region<M: MessageQueue>(message_queue: Arc<M>) {
        let shard_id = 42;
        let region_id = 42;
        let test_payloads = vec![42, 43, 44, 45, 46];

        let mut test_datas = Vec::new();
        for table_id in 0..5_u64 {
            test_datas.push((table_id, test_payloads.clone()));
        }

        test_read_write(
            region_id,
            shard_id,
            test_datas.clone(),
            message_queue.clone(),
        )
        .await;

        test_mark_and_delete(
            region_id,
            shard_id,
            test_datas.clone(),
            message_queue.clone(),
        )
        .await;
    }

    async fn test_read_write<M: MessageQueue>(
        region_id: RegionId,
        shard_id: ShardId,
        test_datas: Vec<(TableId, Vec<u32>)>,
        message_queue: Arc<M>,
    ) {
        let table_num = test_datas.len();
        let test_context = TestContext::new(region_id, shard_id, test_datas, message_queue).await;

        // Write.
        let mut mixed_test_payloads = Vec::new();
        for i in 0..table_num {
            let test_log_batch = &test_context.test_datas[i].1.test_log_batch;
            let test_payloads = &test_context.test_datas[i].1.test_payloads;
            // Write.
            let sequence_num = test_context
                .region
                .write(&WriteContext::default(), test_log_batch)
                .await
                .unwrap();
            assert_eq!(sequence_num, test_log_batch.len() as u64 - 1);

            mixed_test_payloads.extend_from_slice(test_payloads);
        }

        // Read and compare.
        let mut mixed_decoded_res = Vec::new();
        let mut msg_iter = test_context
            .region
            .scan_region(&ReadContext::default())
            .await
            .unwrap()
            .unwrap();
        while let Some(log_entry) = msg_iter.next_log_entry().await.unwrap() {
            let mut payload = log_entry.payload.as_slice();
            let decoded_payload = test_context
                .test_payload_encoder
                .decode(&mut payload)
                .unwrap();
            mixed_decoded_res.push(decoded_payload.val);
        }

        assert_eq!(mixed_test_payloads, mixed_decoded_res);
    }

    async fn test_mark_and_delete<M: MessageQueue>(
        region_id: RegionId,
        shard_id: ShardId,
        test_datas: Vec<(TableId, Vec<u32>)>,
        message_queue: Arc<M>,
    ) {
        let table_num = test_datas.len();
        let mut test_context =
            TestContext::new(region_id, shard_id, test_datas, message_queue).await;

        // Mark deleted and check
        for table_idx in 0..table_num {
            mark_deleted_and_check(&test_context, table_idx).await;
        }

        // Check logs have been deleted, its earliest offset should have changed.
        test_context.region.clean_logs().await.unwrap();
        let new_earliest = test_context
            .message_queue
            .fetch_offset(&test_context.log_topic, OffsetType::EarliestOffset)
            .await
            .unwrap();
        assert_eq!(
            new_earliest,
            test_context.test_datas[0].1.test_log_batch.len() as i64
        );

        check_sync_snapshot(&test_context).await;
    }

    async fn check_sync_snapshot<M: MessageQueue>(test_context: &TestContext<M>) {
        // Only one meta record will exist in normal.
        let earliest = test_context
            .message_queue
            .fetch_offset(&test_context.meta_topic, OffsetType::EarliestOffset)
            .await
            .unwrap();
        let latest = test_context
            .message_queue
            .fetch_offset(&test_context.meta_topic, OffsetType::HighWaterMark)
            .await
            .unwrap();
        assert_eq!(earliest + 1, latest);

        // Compare local snapshot and remote one.
        // Local
        let local_snapshot = test_context.region.make_meta_snapshot().await;

        // Remote
        let meta_encoding = MetaEncoding::newest();
        let mut iter = test_context
            .message_queue
            .consume(&test_context.meta_topic, StartOffset::Earliest)
            .await
            .unwrap();
        let (message_and_offset, high_watermark) = iter.next_message().await.unwrap();
        assert_eq!(message_and_offset.offset + 1, high_watermark);
        let decoded_meta_key = meta_encoding
            .decode_key(&message_and_offset.message.key.unwrap())
            .unwrap();
        assert_eq!(test_context.region_id, decoded_meta_key.0);
        let remote_snapshot = meta_encoding
            .decode_value(&message_and_offset.message.value.unwrap())
            .unwrap();

        assert_eq!(local_snapshot, remote_snapshot);
    }

    async fn mark_deleted_and_check<M: MessageQueue>(
        test_context: &TestContext<M>,
        table_idx: usize,
    ) {
        let test_log_batch = &test_context.test_datas[table_idx].1.test_log_batch;
        let table_id = test_context.test_datas[table_idx].0;

        // Write.
        let base_offset = test_log_batch.len() as i64 * 2 * table_idx as i64;
        let sequence_num = test_context
            .region
            .write(&WriteContext::default(), test_log_batch)
            .await
            .unwrap();
        assert_eq!(sequence_num, test_log_batch.len() as u64 - 1);
        let sequence_num = test_context
            .region
            .write(&WriteContext::default(), test_log_batch)
            .await
            .unwrap();
        assert_eq!(sequence_num, test_log_batch.len() as u64 * 2 - 1);

        // Mark deleted.
        test_context
            .region
            .mark_delete_to(table_id, test_log_batch.len() as u64)
            .await
            .unwrap();
        let table_meta = test_context.region.get_table_meta(table_id).await.unwrap();
        assert_eq!(
            table_meta.next_sequence_num,
            test_log_batch.len() as u64 * 2
        );
        assert_eq!(
            table_meta.latest_marked_deleted,
            test_log_batch.len() as u64
        );
        assert_eq!(
            table_meta.current_high_watermark,
            base_offset + test_log_batch.len() as i64 * 2
        );
        assert_eq!(
            table_meta.safe_delete_offset,
            Some(base_offset + test_log_batch.len() as i64)
        );

        check_sync_snapshot(test_context).await;
    }
}
