// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Distributed Table implementation

use std::{collections::HashMap, fmt};

use async_trait::async_trait;
use common_types::{
    row::{Row, RowGroupBuilder},
    schema::Schema,
};
use common_util::error::BoxError;
use futures::future::try_join_all;
use snafu::ResultExt;
use table_engine::{
    partition::{
        format_sub_partition_table_name, rule::df_adapter::DfPartitionRuleAdapter, PartitionInfo,
    },
    remote::{
        model::{
            ReadRequest as RemoteReadRequest, TableIdentifier, WriteRequest as RemoteWriteRequest,
        },
        RemoteEngineRef,
    },
    stream::{PartitionedStreams, SendableRecordBatchStream},
    table::{
        AlterSchemaRequest, CreatePartitionRule, FlushRequest, GetRequest, LocatePartitions,
        ReadRequest, Result, Scan, Table, TableId, TableStats, UnexpectedWithMsg,
        UnsupportedMethod, Write, WriteRequest,
    },
};

use crate::metrics::{
    PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM, PARTITION_TABLE_WRITE_DURATION_HISTOGRAM,
};

#[derive(Debug)]
pub struct TableData {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
    pub table_schema: Schema,
    pub partition_info: PartitionInfo,
    pub options: HashMap<String, String>,
    pub engine_type: String,
}

/// Table trait implementation
pub struct PartitionTableImpl {
    table_data: TableData,
    remote_engine: RemoteEngineRef,
}

impl PartitionTableImpl {
    pub fn new(table_data: TableData, remote_engine: RemoteEngineRef) -> Result<Self> {
        Ok(Self {
            table_data,
            remote_engine,
        })
    }

    fn get_sub_table_ident(&self, id: usize) -> TableIdentifier {
        let partition_name = self.table_data.partition_info.get_definitions()[id]
            .name
            .clone();
        TableIdentifier {
            catalog: self.table_data.catalog_name.clone(),
            schema: self.table_data.schema_name.clone(),
            table: format_sub_partition_table_name(&self.table_data.table_name, &partition_name),
        }
    }
}

impl fmt::Debug for PartitionTableImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionTableImpl")
            .field("table_data", &self.table_data)
            .finish()
    }
}

#[async_trait]
impl Table for PartitionTableImpl {
    fn name(&self) -> &str {
        &self.table_data.table_name
    }

    fn id(&self) -> TableId {
        self.table_data.table_id
    }

    fn schema(&self) -> Schema {
        self.table_data.table_schema.clone()
    }

    // TODO: get options from sub partition table with remote engine
    fn options(&self) -> HashMap<String, String> {
        self.table_data.options.clone()
    }

    fn partition_info(&self) -> Option<PartitionInfo> {
        Some(self.table_data.partition_info.clone())
    }

    fn engine_type(&self) -> &str {
        &self.table_data.engine_type
    }

    fn stats(&self) -> TableStats {
        TableStats::default()
    }

    async fn write(&self, request: WriteRequest) -> Result<usize> {
        let _timer = PARTITION_TABLE_WRITE_DURATION_HISTOGRAM
            .with_label_values(&["total"])
            .start_timer();

        // Build partition rule.
        let df_partition_rule = match self.partition_info() {
            None => UnexpectedWithMsg {
                msg: "partition table partition info can't be empty",
            }
            .fail()?,
            Some(partition_info) => {
                DfPartitionRuleAdapter::new(partition_info, &self.table_data.table_schema)
                    .box_err()
                    .context(CreatePartitionRule)?
            }
        };

        // Split write request.
        let partitions = {
            let _locate_timer = PARTITION_TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["locate"])
                .start_timer();
            df_partition_rule
                .locate_partitions_for_write(&request.row_group)
                .box_err()
                .context(LocatePartitions)?
        };

        let mut split_rows = HashMap::new();
        let schema = request.row_group.schema().clone();
        for (partition, row) in partitions.into_iter().zip(request.row_group.into_iter()) {
            split_rows
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(row);
        }

        // Insert split write request through remote engine.
        let mut futures = Vec::with_capacity(split_rows.len());
        for (partition, rows) in split_rows {
            let row_group = RowGroupBuilder::with_rows(schema.clone(), rows)
                .box_err()
                .context(Write {
                    table: self.get_sub_table_ident(partition).table,
                })?
                .build();
            futures.push(async move {
                self.remote_engine
                    .write(RemoteWriteRequest {
                        table: self.get_sub_table_ident(partition),
                        write_request: WriteRequest { row_group },
                    })
                    .await
            });
        }

        let result = {
            let _remote_timer = PARTITION_TABLE_WRITE_DURATION_HISTOGRAM
                .with_label_values(&["remote_write"])
                .start_timer();
            try_join_all(futures).await.box_err().context(Write {
                table: self.name().to_string(),
            })?
        };

        Ok(result.into_iter().sum())
    }

    async fn read(&self, _request: ReadRequest) -> Result<SendableRecordBatchStream> {
        UnsupportedMethod {
            table: self.name(),
            method: "read",
        }
        .fail()
    }

    async fn get(&self, _request: GetRequest) -> Result<Option<Row>> {
        UnsupportedMethod {
            table: self.name(),
            method: "get",
        }
        .fail()
    }

    async fn partitioned_read(&self, request: ReadRequest) -> Result<PartitionedStreams> {
        let _timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
            .with_label_values(&["total"])
            .start_timer();

        // Build partition rule.
        let df_partition_rule = match self.partition_info() {
            None => UnexpectedWithMsg {
                msg: "partition table partition info can't be empty",
            }
            .fail()?,
            Some(partition_info) => {
                DfPartitionRuleAdapter::new(partition_info, &self.table_data.table_schema)
                    .box_err()
                    .context(CreatePartitionRule)?
            }
        };

        // Evaluate expr and locate partition.
        let partitions = {
            let _locate_timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
                .with_label_values(&["locate"])
                .start_timer();
            df_partition_rule
                .locate_partitions_for_read(request.predicate.exprs())
                .box_err()
                .context(LocatePartitions)?
        };

        // Query streams through remote engine.
        let mut futures = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let remote_engine = self.remote_engine.clone();
            let request_clone = request.clone();
            futures.push(async move {
                remote_engine
                    .read(RemoteReadRequest {
                        table: self.get_sub_table_ident(partition),
                        read_request: request_clone,
                    })
                    .await
            })
        }

        let streams = {
            let _remote_timer = PARTITION_TABLE_PARTITIONED_READ_DURATION_HISTOGRAM
                .with_label_values(&["remote_read"])
                .start_timer();
            try_join_all(futures).await.box_err().context(Scan {
                table: self.name().to_string(),
            })?
        };

        Ok(PartitionedStreams { streams })
    }

    async fn alter_schema(&self, _request: AlterSchemaRequest) -> Result<usize> {
        UnsupportedMethod {
            table: self.name(),
            method: "alter_schema",
        }
        .fail()
    }

    async fn alter_options(&self, _options: HashMap<String, String>) -> Result<usize> {
        UnsupportedMethod {
            table: self.name(),
            method: "alter_options",
        }
        .fail()
    }

    async fn flush(&self, _request: FlushRequest) -> Result<()> {
        Ok(())
    }

    async fn compact(&self) -> Result<()> {
        Ok(())
    }
}
