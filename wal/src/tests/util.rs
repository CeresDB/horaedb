// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! utilities for testing wal module.

use std::{collections::VecDeque, path::Path, str::FromStr, sync::Arc};

use async_trait::async_trait;
use common_types::{
    bytes::{MemBuf, MemBufMut},
    SequenceNumber,
};
use common_util::{
    config::ReadableDuration,
    runtime::{self, Runtime},
};
use snafu::Snafu;
use table_kv::memory::MemoryImpl;
use tempfile::TempDir;

use crate::{
    log_batch::{LogWriteBatch, LogWriteEntry, Payload, PayloadDecoder},
    manager::{
        BatchLogIterator, BatchLogIteratorAdapter, ReadContext, RegionId, WalManager, WriteContext,
    },
    rocks_impl::{self, manager::RocksImpl},
    table_kv_impl::{model::NamespaceConfig, wal::WalNamespaceImpl, WalRuntimes},
};

#[derive(Debug, Snafu)]
pub enum Error {}

#[async_trait]
pub trait WalBuilder: Send + Sync {
    type Wal: WalManager + Send + Sync;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal>;
}

#[derive(Default)]
pub struct RocksWalBuilder;

#[async_trait]
impl WalBuilder for RocksWalBuilder {
    type Wal = RocksImpl;

    async fn build(&self, data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let wal_builder =
            rocks_impl::manager::Builder::with_default_rocksdb_config(data_path, runtime);

        Arc::new(
            wal_builder
                .build()
                .expect("should succeed to build rocksimpl wal"),
        )
    }
}

pub type RocksTestEnv = TestEnv<RocksWalBuilder>;

const WAL_NAMESPACE: &str = "wal";

#[derive(Default)]
pub struct MemoryTableWalBuilder {
    table_kv: MemoryImpl,
    ttl: Option<ReadableDuration>,
}

#[async_trait]
impl WalBuilder for MemoryTableWalBuilder {
    type Wal = WalNamespaceImpl<MemoryImpl>;

    async fn build(&self, _data_path: &Path, runtime: Arc<Runtime>) -> Arc<Self::Wal> {
        let config = NamespaceConfig {
            wal_shard_num: 2,
            region_meta_shard_num: 2,
            ttl: self.ttl,
            ..Default::default()
        };

        let wal_runtimes = WalRuntimes {
            read_runtime: runtime.clone(),
            write_runtime: runtime.clone(),
            bg_runtime: runtime.clone(),
        };
        let namespace_wal =
            WalNamespaceImpl::open(self.table_kv.clone(), wal_runtimes, WAL_NAMESPACE, config)
                .await
                .unwrap();

        Arc::new(namespace_wal)
    }
}

impl MemoryTableWalBuilder {
    pub fn with_ttl(ttl: &str) -> Self {
        Self {
            table_kv: MemoryImpl::default(),
            ttl: Some(ReadableDuration::from_str(ttl).unwrap()),
        }
    }
}

pub type TableKvTestEnv = TestEnv<MemoryTableWalBuilder>;

/// The environment for testing wal.
pub struct TestEnv<B> {
    pub dir: TempDir,
    pub runtime: Arc<Runtime>,
    pub write_ctx: WriteContext,
    pub read_ctx: ReadContext,
    /// Builder for a specific wal.
    builder: B,
}

impl<B: WalBuilder> TestEnv<B> {
    pub fn new(num_workers: usize, builder: B) -> Self {
        let runtime = runtime::Builder::default()
            .worker_threads(num_workers)
            .enable_all()
            .build()
            .unwrap();

        Self {
            dir: tempfile::tempdir().unwrap(),
            runtime: Arc::new(runtime),
            write_ctx: WriteContext::default(),
            read_ctx: ReadContext::default(),
            builder,
        }
    }

    pub async fn build_wal(&self) -> Arc<B::Wal> {
        self.builder
            .build(self.dir.path(), self.runtime.clone())
            .await
    }

    /// Build the log batch with [TestPayload].val range [start, end).
    pub fn build_log_batch<'a>(
        &self,
        region_id: RegionId,
        start: u32,
        end: u32,
        payload_batch: &'a mut Vec<TestPayload>,
    ) -> LogWriteBatch<'a> {
        let mut write_batch = LogWriteBatch::new(region_id);

        for val in start..end {
            let payload = TestPayload { val };
            payload_batch.push(payload);
        }

        for payload in payload_batch.iter() {
            write_batch.entries.push(LogWriteEntry { payload });
        }

        write_batch
    }

    /// Check whether the log entries from the iterator equals the
    /// `write_batch`.
    pub async fn check_log_entries(
        &self,
        max_seq: SequenceNumber,
        write_batch: &LogWriteBatch<'_>,
        mut iter: BatchLogIteratorAdapter,
    ) {
        let mut log_entries = VecDeque::with_capacity(write_batch.entries.len());
        let mut buffer = VecDeque::new();
        loop {
            let dec = TestPayloadDecoder;
            buffer = iter
                .next_log_entries(dec, buffer)
                .await
                .expect("should succeed to fetch next log entry");
            if buffer.is_empty() {
                break;
            }

            log_entries.append(&mut buffer);
        }

        assert_eq!(write_batch.entries.len(), log_entries.len());
        for (idx, (expect_log_write_entry, log_entry)) in write_batch
            .entries
            .iter()
            .zip(log_entries.iter())
            .rev()
            .enumerate()
        {
            // sequence
            assert_eq!(max_seq - idx as u64, log_entry.sequence);

            // payload
            let (mut expected_buf, mut buf) = (Vec::new(), Vec::new());
            expect_log_write_entry
                .payload
                .encode_to(&mut expected_buf)
                .unwrap();
            log_entry.payload.encode_to(&mut buf).unwrap();

            assert_eq!(
                expect_log_write_entry.payload.encode_size(),
                log_entry.payload.encode_size()
            );
            assert_eq!(expected_buf, buf);
        }
    }
}

/// The payload for Wal log entry for testing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestPayload {
    pub val: u32,
}

impl Payload for TestPayload {
    fn encode_size(&self) -> usize {
        4
    }

    fn encode_to(
        &self,
        buf: &mut dyn MemBufMut,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        buf.write_u32(self.val).expect("must write");
        Ok(())
    }
}

pub struct TestPayloadDecoder;

impl PayloadDecoder for TestPayloadDecoder {
    type Error = Error;
    type Target = TestPayload;

    fn decode<B: MemBuf>(&self, buf: &mut B) -> Result<Self::Target, Self::Error> {
        let val = buf.read_u32().expect("should succeed to read u32");
        Ok(TestPayload { val })
    }
}
