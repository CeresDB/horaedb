// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Sst reader implementation based on parquet.

use std::{
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use common_types::{
    projected_schema::ProjectedSchema,
    record_batch::{ArrowRecordBatchProjector, RecordBatchWithKey},
    schema::Schema,
};
use common_util::{runtime::Runtime, time::InstantExt};
use datafusion::{
    datasource::{file_format, object_store::ObjectStoreUrl},
    execution::context::TaskContext,
    physical_plan::{
        display::DisplayableExecutionPlan,
        execute_stream,
        file_format::{
            FileMeta, FileScanConfig, ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory,
        },
        metrics::ExecutionPlanMetricsSet,
        ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::{SessionConfig, SessionContext},
};
use futures::{
    future::{self, BoxFuture},
    FutureExt, Stream, StreamExt, TryFutureExt,
};
use log::{debug, error, info};
use object_store::{ObjectMeta, ObjectStoreRef, Path};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet_ext::{DataCacheRef, MetaCacheRef};
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::predicate::PredicateRef;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    sst::{
        factory::SstReaderOptions,
        file::SstMetaData,
        parquet::{
            encoding::{self, ParquetDecoder},
            hybrid,
        },
        reader::{error::*, Result, SstReader},
    },
    table_options::{StorageFormat, StorageFormatOptions},
};

const CERESDB_SCHEME: &str = "ceresdb";
const CERESDB_HOST: &str = "ceresdb_host";

pub struct Reader<'a> {
    /// The path where the data is persisted.
    path: &'a Path,
    /// The storage where the data is persist.
    storage: &'a ObjectStoreRef,
    projected_schema: ProjectedSchema,
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    meta_cache: Option<MetaCacheRef>,
    predicate: PredicateRef,
    batch_size: usize,
    df_plan: Option<Arc<dyn ExecutionPlan>>,

    /// init this field in `init_if_necessary`
    meta_data: Option<SstMetaData>,
}

impl<'a> Reader<'a> {
    pub fn new(path: &'a Path, storage: &'a ObjectStoreRef, options: &SstReaderOptions) -> Self {
        let reader_factory = Arc::new(CachableParquetFileReaderFactory {
            storage: storage.clone(),
            data_cache: options.data_cache.clone(),
        });
        let batch_size = options.read_batch_row_num;
        Self {
            path,
            storage,
            reader_factory,
            projected_schema: options.projected_schema.clone(),
            meta_cache: options.meta_cache.clone(),
            predicate: options.predicate.clone(),
            batch_size,
            df_plan: None,
            meta_data: None,
        }
    }

    fn construct_arrow_schema(schema: &Schema, opts: &StorageFormatOptions) -> ArrowSchemaRef {
        match opts.format {
            StorageFormat::Columnar => schema.to_arrow_schema_ref(),
            StorageFormat::Hybrid => hybrid::build_hybrid_arrow_schema(schema),
        }
    }

    async fn fetch_record_batch_stream(&mut self) -> Result<SendableRecordBatchStream> {
        assert!(self.meta_data.is_some());

        let meta_data = self.meta_data.as_ref().unwrap();
        let object_meta = ObjectMeta {
            location: self.path.clone(),
            // we don't care modified time
            last_modified: Default::default(),
            size: meta_data.size as usize,
        };

        let arrow_schema =
            Self::construct_arrow_schema(&meta_data.schema, &meta_data.storage_format_opts);
        let row_projector = self
            .projected_schema
            .try_project_with_key(&meta_data.schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        let scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse(format!(
                "{}://{}",
                CERESDB_SCHEME, CERESDB_HOST
            ))
            .expect("valid object store URL"),
            file_schema: arrow_schema,
            file_groups: vec![vec![object_meta.clone().into()]],
            statistics: Statistics::default(),
            projection: Some(row_projector.existed_source_projection()),
            limit: None,
            table_partition_cols: vec![],
        };
        let filter_expr = self.predicate.to_df_expr(meta_data.schema.timestamp_name());
        debug!(
            "fetch_record_batch_stream, object_meta:{:?}, filter:{:?}, scan_config:{:?}",
            object_meta, filter_expr, scan_config
        );

        let exec = ParquetExec::new(scan_config, Some(filter_expr), Some(object_meta.size))
            .with_parquet_file_reader_factory(self.reader_factory.clone());
        let exec = Arc::new(exec);

        // There are some options can be configured for execution, such as
        // `DATAFUSION_EXECUTION_BATCH_SIZE`. More refer:
        // https://arrow.apache.org/datafusion/user-guide/configs.html
        let session_ctx =
            SessionContext::with_config(SessionConfig::from_env().with_batch_size(self.batch_size));
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        task_ctx.runtime_env().register_object_store(
            CERESDB_SCHEME,
            CERESDB_HOST,
            self.storage.clone(),
        );

        self.df_plan = Some(exec.clone());
        execute_stream(exec, task_ctx)
            .await
            .context(DataFusionError {})
    }

    async fn init_if_necessary(&mut self) -> Result<()> {
        if self.meta_data.is_some() {
            return Ok(());
        }

        let sst_meta = Self::read_sst_meta(
            self.storage,
            self.path,
            &self.meta_cache,
            &self.reader_factory,
        )
        .await?;
        self.meta_data = Some(sst_meta);

        Ok(())
    }

    async fn read_sst_meta(
        storage: &ObjectStoreRef,
        path: &Path,
        meta_cache: &Option<MetaCacheRef>,
        reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    ) -> Result<SstMetaData> {
        let object_meta = storage.head(path).await.context(ObjectStoreError {})?;
        let file_size = object_meta.size;

        let get_metadata_from_storage = || async {
            let mut reader = reader_factory
                // We don't care partition_index
                .create_reader(
                    Default::default(),
                    object_meta.into(),
                    None,
                    &ExecutionPlanMetricsSet::new(),
                )
                .context(DataFusionError {})?;
            let metadata = reader.get_metadata().await.context(ParquetError {})?;

            if let Some(cache) = meta_cache {
                cache.put(path.to_string(), metadata.clone());
            }
            Ok(metadata)
        };

        let metadata = if let Some(cache) = meta_cache {
            if let Some(cached_data) = cache.get(path.as_ref()) {
                cached_data
            } else {
                get_metadata_from_storage().await?
            }
        } else {
            get_metadata_from_storage().await?
        };

        let kv_metas = metadata
            .file_metadata()
            .key_value_metadata()
            .context(SstMetaNotFound)?;
        ensure!(!kv_metas.is_empty(), EmptySstMeta);

        let mut sst_meta = encoding::decode_sst_meta_data(&kv_metas[0])
            .map_err(|e| Box::new(e) as _)
            .context(DecodeSstMeta)?;
        // size in sst_meta is always 0, so overwrite it here
        // https://github.com/CeresDB/ceresdb/issues/321
        sst_meta.size = file_size as u64;
        Ok(sst_meta)
    }

    #[cfg(test)]
    pub(crate) async fn row_groups(&mut self) -> Vec<parquet::file::metadata::RowGroupMetaData> {
        let object_meta = self.storage.head(self.path).await.unwrap();
        let mut reader = self
            .reader_factory
            .create_reader(0, object_meta.into(), None, &ExecutionPlanMetricsSet::new())
            .unwrap();

        let metadata = reader.get_metadata().await.unwrap();
        metadata.row_groups().to_vec()
    }
}

impl<'a> Drop for Reader<'a> {
    fn drop(&mut self) {
        if self.df_plan.is_none() {
            return;
        }

        let df_plan = self.df_plan.take().unwrap();
        info!(
            "Reader plan metrics:\n{}",
            DisplayableExecutionPlan::with_metrics(&*df_plan)
                .indent()
                .to_string()
        );
    }
}

#[derive(Debug)]
pub struct CachableParquetFileReaderFactory {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
}

impl CachableParquetFileReaderFactory {
    pub fn new(storage: ObjectStoreRef, data_cache: Option<DataCacheRef>) -> Self {
        Self {
            storage,
            data_cache,
        }
    }
}

impl ParquetFileReaderFactory for CachableParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let parquet_file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        Ok(Box::new(CachableParquetFileReader::new(
            self.storage.clone(),
            self.data_cache.clone(),
            file_meta.object_meta,
            parquet_file_metrics,
            metadata_size_hint,
        )))
    }
}

struct CachableParquetFileReader {
    storage: ObjectStoreRef,
    data_cache: Option<DataCacheRef>,
    meta: ObjectMeta,
    metrics: ParquetFileMetrics,
    metadata_size_hint: Option<usize>,
    cache_hit: usize,
    cache_miss: usize,
}

impl CachableParquetFileReader {
    fn new(
        storage: ObjectStoreRef,
        data_cache: Option<DataCacheRef>,
        meta: ObjectMeta,
        metrics: ParquetFileMetrics,
        metadata_size_hint: Option<usize>,
    ) -> Self {
        Self {
            storage,
            data_cache,
            meta,
            metrics,
            metadata_size_hint,
            cache_hit: 0,
            cache_miss: 0,
        }
    }

    fn cache_key(name: &str, start: usize, end: usize) -> String {
        format!("{}_{}_{}", name, start, end)
    }
}

impl Drop for CachableParquetFileReader {
    fn drop(&mut self) {
        info!(
            "CachableParquetFileReader meta_size_hint:{:?}, cache_hit:{}, cache_miss:{}, bytes_scanned:{}",
            self.metadata_size_hint, self.cache_hit, self.cache_miss, self.metrics.bytes_scanned.value()
        );
    }
}

impl AsyncFileReader for CachableParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.metrics.bytes_scanned.add(range.end - range.start);

        let key = Self::cache_key(self.meta.location.as_ref(), range.start, range.end);
        if let Some(cache) = &self.data_cache {
            if let Some(cached_bytes) = cache.get(&key) {
                self.cache_hit += 1;
                return Box::pin(future::ok(Bytes::from(cached_bytes.to_vec())));
            };
        }

        self.cache_miss += 1;
        self.storage
            .get_range(&self.meta.location, range)
            .map_ok(|bytes| {
                if let Some(cache) = &self.data_cache {
                    cache.put(key, Arc::new(bytes.to_vec()));
                }
                bytes
            })
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_bytes error: {}",
                    e
                ))
            })
            .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>> {
        Box::pin(async move {
            let metadata = file_format::parquet::fetch_parquet_metadata(
                self.storage.as_ref(),
                &self.meta,
                self.metadata_size_hint,
            )
            .await
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "CachableParquetFileReader::get_metadata error: {}",
                    e
                ))
            })?;
            Ok(Arc::new(metadata))
        })
    }
}

struct RecordBatchProjector {
    stream: SendableRecordBatchStream,
    row_projector: ArrowRecordBatchProjector,
    storage_format_opts: StorageFormatOptions,

    row_num: usize,
    start_time: Instant,
}

impl RecordBatchProjector {
    fn new(
        stream: SendableRecordBatchStream,
        row_projector: ArrowRecordBatchProjector,
        storage_format_opts: StorageFormatOptions,
    ) -> Self {
        Self {
            stream,
            row_projector,
            storage_format_opts,
            row_num: 0,
            start_time: Instant::now(),
        }
    }
}

impl Drop for RecordBatchProjector {
    fn drop(&mut self) {
        info!(
            "RecordBatchProjector read {} rows, cost:{}ms",
            self.row_num,
            self.start_time.saturating_elapsed().as_millis(),
        );
    }
}

impl Stream for RecordBatchProjector {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projector = self.get_mut();

        match projector.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(record_batch)) => {
                match record_batch
                    .map_err(|e| Box::new(e) as _)
                    .context(DecodeRecordBatch {})
                {
                    Err(e) => Poll::Ready(Some(Err(e))),
                    Ok(record_batch) => {
                        let parquet_decoder =
                            ParquetDecoder::new(projector.storage_format_opts.clone());
                        let record_batch = parquet_decoder
                            .decode_record_batch(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch)?;

                        projector.row_num += record_batch.num_rows();

                        let projected_batch = projector
                            .row_projector
                            .project_to_record_batch_with_key(record_batch)
                            .map_err(|e| Box::new(e) as _)
                            .context(DecodeRecordBatch {});

                        Poll::Ready(Some(projected_batch))
                    }
                }
            }
            // expected struct `RecordBatchWithKey`, found struct `arrow::record_batch::RecordBatch`
            // other => other
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[async_trait]
impl<'a> SstReader for Reader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.init_if_necessary().await?;

        Ok(self.meta_data.as_ref().unwrap())
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        self.init_if_necessary().await?;

        let stream = self.fetch_record_batch_stream().await?;
        let metadata = &self.meta_data.as_ref().unwrap();
        let row_projector = self
            .projected_schema
            .try_project_with_key(&metadata.schema)
            .map_err(|e| Box::new(e) as _)
            .context(Projection)?;
        let row_projector = ArrowRecordBatchProjector::from(row_projector);
        let storage_format_opts = metadata.storage_format_opts.clone();

        Ok(Box::new(RecordBatchProjector::new(
            stream,
            row_projector,
            storage_format_opts,
        )))
    }
}

struct RecordBatchReceiver {
    rx: Receiver<Result<RecordBatchWithKey>>,
}

impl Stream for RecordBatchReceiver {
    type Item = Result<RecordBatchWithKey>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().rx.poll_recv(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

const DEFAULT_CHANNEL_CAP: usize = 1024;

/// Spawn a new thread to read record_batches
pub struct ThreadedReader<'a> {
    inner: Reader<'a>,
    runtime: Arc<Runtime>,

    channel_cap: usize,
}

impl<'a> ThreadedReader<'a> {
    pub fn new(reader: Reader<'a>, runtime: Arc<Runtime>) -> Self {
        Self {
            inner: reader,
            runtime,
            channel_cap: DEFAULT_CHANNEL_CAP,
        }
    }

    async fn read_record_batches(&mut self, tx: Sender<Result<RecordBatchWithKey>>) -> Result<()> {
        let mut stream = self.inner.read().await?;
        self.runtime.spawn(async move {
            while let Some(batch) = stream.next().await {
                if let Err(e) = tx.send(batch).await {
                    error!("fail to send the fetched record batch result, err:{}", e);
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl<'a> SstReader for ThreadedReader<'a> {
    async fn meta_data(&mut self) -> Result<&SstMetaData> {
        self.inner.meta_data().await
    }

    async fn read(
        &mut self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatchWithKey>> + Send + Unpin>> {
        let (tx, rx) = mpsc::channel::<Result<RecordBatchWithKey>>(self.channel_cap);
        self.read_record_batches(tx).await?;

        Ok(Box::new(RecordBatchReceiver { rx }))
    }
}
