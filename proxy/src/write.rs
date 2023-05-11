// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use bytes::Bytes;
use ceresdbproto::storage::{
    storage_service_client::StorageServiceClient, value, RouteRequest, WriteRequest,
    WriteResponse as WriteResponsePB, WriteSeriesEntry, WriteTableRequest,
};
use cluster::config::SchemaConfig;
use common_types::{
    column_schema::ColumnSchema,
    datum::{Datum, DatumKind},
    request_id::RequestId,
    row::{Row, RowGroupBuilder},
    schema::Schema,
    time::Timestamp,
};
use common_util::error::BoxError;
use futures::{future::try_join_all, FutureExt};
use http::StatusCode;
use interpreters::interpreter::Output;
use log::{debug, error, info};
use query_engine::executor::Executor as QueryExecutor;
use query_frontend::{
    frontend::{Context as FrontendContext, Frontend},
    plan::{AlterTableOperation, AlterTablePlan, InsertPlan, Plan},
    planner::build_schema_from_write_table_request,
    provider::CatalogMetaProvider,
};
use router::endpoint::Endpoint;
use snafu::{ensure, OptionExt, ResultExt};
use table_engine::table::TableRef;
use tonic::transport::Channel;

use crate::{
    error::{ErrNoCause, ErrWithCause, InternalNoCause, Result},
    forward::{ForwardResult, ForwarderRef},
    Context, Proxy,
};

#[derive(Debug)]
pub struct WriteContext {
    pub request_id: RequestId,
    pub deadline: Option<Instant>,
    pub catalog: String,
    pub schema: String,
    pub auto_create_table: bool,
}

#[derive(Debug, Default)]
pub(crate) struct WriteResponse {
    pub success: u32,
    pub failed: u32,
}

impl<Q: QueryExecutor + 'static> Proxy<Q> {
    pub(crate) async fn handle_write_internal(
        &self,
        ctx: Context,
        req: WriteRequest,
    ) -> Result<WriteResponse> {
        let write_context = req.context.clone().context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;

        let (write_request_to_local, write_requests_to_forward) =
            self.split_write_request(req).await?;

        let mut futures = Vec::with_capacity(write_requests_to_forward.len() + 1);

        // Write to remote.
        for (endpoint, table_write_request) in write_requests_to_forward {
            let forwarder = self.forwarder.clone();
            let write_handle = self.engine_runtimes.io_runtime.spawn(async move {
                Self::write_to_remote(forwarder, endpoint, table_write_request).await
            });

            futures.push(write_handle.boxed());
        }

        // Write to local.
        if !write_request_to_local.table_requests.is_empty() {
            let local_handle =
                async move { Ok(self.write_to_local(ctx, write_request_to_local).await) };
            futures.push(local_handle.boxed());
        }

        let resps = try_join_all(futures)
            .await
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Failed to join task",
            })?;

        debug!(
            "Grpc handle write finished, schema:{}, resps:{:?}",
            write_context.database, resps
        );

        let mut success = 0;
        for resp in resps {
            success += resp?.success;
        }

        Ok(WriteResponse {
            success,
            ..Default::default()
        })
    }

    async fn split_write_request(
        &self,
        req: WriteRequest,
    ) -> Result<(WriteRequest, HashMap<Endpoint, WriteRequest>)> {
        // Split write request into multiple requests, each request contains table
        // belong to one remote engine.
        let tables = req
            .table_requests
            .iter()
            .map(|table_request| table_request.table.clone())
            .collect();

        // TODO: Make the router can accept an iterator over the tables to avoid the
        // memory allocation here.
        let route_data = self
            .router
            .route(RouteRequest {
                context: req.context.clone(),
                tables,
            })
            .await?;
        let forwarded_table_routes = route_data
            .into_iter()
            .filter_map(|router| {
                router
                    .endpoint
                    .map(|endpoint| (router.table, endpoint.into()))
            })
            .filter(|router| !self.forwarder.is_local_endpoint(&router.1))
            .collect::<HashMap<_, _>>();

        // No table need to be forwarded.
        if forwarded_table_routes.is_empty() {
            return Ok((req, HashMap::default()));
        }

        let mut table_requests_to_local = WriteRequest {
            table_requests: Vec::with_capacity(max(
                req.table_requests.len() - forwarded_table_routes.len(),
                0,
            )),
            context: req.context.clone(),
        };

        let mut table_requests_to_forward = HashMap::with_capacity(forwarded_table_routes.len());

        let write_context = req.context;
        for table_request in req.table_requests {
            let route = forwarded_table_routes.get(&table_request.table);
            match route {
                Some(endpoint) => {
                    let table_requests = table_requests_to_forward
                        .entry(endpoint.clone())
                        .or_insert_with(|| WriteRequest {
                            context: write_context.clone(),
                            table_requests: Vec::new(),
                        });
                    table_requests.table_requests.push(table_request);
                }
                _ => {
                    table_requests_to_local.table_requests.push(table_request);
                }
            }
        }
        Ok((table_requests_to_local, table_requests_to_forward))
    }

    async fn write_to_remote(
        forwarder: ForwarderRef,
        endpoint: Endpoint,
        table_write_request: WriteRequest,
    ) -> Result<WriteResponse> {
        let do_write = |mut client: StorageServiceClient<Channel>,
                        request: tonic::Request<WriteRequest>,
                        _: &Endpoint| {
            let write = async move {
                client
                    .write(request)
                    .await
                    .map(|resp| resp.into_inner())
                    .box_err()
                    .context(ErrWithCause {
                        code: StatusCode::INTERNAL_SERVER_ERROR,
                        msg: "Forwarded write request failed",
                    })
            }
            .boxed();

            Box::new(write) as _
        };

        let forward_result = forwarder
            .forward_with_endpoint(endpoint, tonic::Request::new(table_write_request), do_write)
            .await;
        let forward_res = forward_result
            .map_err(|e| {
                error!("Failed to forward sql req but the error is ignored, err:{e}");
                e
            })
            .box_err()
            .context(ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Local response is not expected",
            })?;

        match forward_res {
            ForwardResult::Forwarded(resp) => resp.map(|v: WriteResponsePB| WriteResponse {
                success: v.success,
                failed: v.failed,
            }),
            ForwardResult::Local => InternalNoCause {
                msg: "Local response is not expected".to_string(),
            }
            .fail(),
        }
    }

    async fn write_to_local(&self, ctx: Context, req: WriteRequest) -> Result<WriteResponse> {
        let request_id = RequestId::next_id();
        let begin_instant = Instant::now();
        let deadline = ctx.timeout.map(|t| begin_instant + t);
        let catalog = self.instance.catalog_manager.default_catalog_name();
        let req_ctx = req.context.context(ErrNoCause {
            msg: "Missing context",
            code: StatusCode::BAD_REQUEST,
        })?;
        let schema = req_ctx.database;
        let schema_config = self
            .schema_config_provider
            .schema_config(&schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Fail to fetch schema config, schema_name:{schema}"),
            })?;

        debug!(
            "Grpc handle write begin, catalog:{catalog}, schema:{schema}, request_id:{request_id}, first_table:{:?}, num_tables:{}",
            req.table_requests
                .first()
                .map(|m| (&m.table, &m.tag_names, &m.field_names)),
            req.table_requests.len(),
        );

        let write_context = WriteContext {
            request_id,
            deadline,
            catalog: catalog.to_string(),
            schema: schema.clone(),
            auto_create_table: self.auto_create_table,
        };

        let plan_vec = self
            .write_request_to_insert_plan(req.table_requests, schema_config, write_context)
            .await?;

        let mut success = 0;
        for insert_plan in plan_vec {
            success += self
                .execute_insert_plan(request_id, catalog, &schema, insert_plan, deadline)
                .await?;
        }

        Ok(WriteResponse {
            success: success as u32,
            ..Default::default()
        })
    }

    async fn write_request_to_insert_plan(
        &self,
        table_requests: Vec<WriteTableRequest>,
        schema_config: Option<&SchemaConfig>,
        write_context: WriteContext,
    ) -> Result<Vec<InsertPlan>> {
        let mut plan_vec = Vec::with_capacity(table_requests.len());

        let WriteContext {
            request_id,
            catalog,
            schema,
            deadline,
            auto_create_table,
        } = write_context;
        let schema_config = schema_config.cloned().unwrap_or_default();
        for write_table_req in table_requests {
            let table_name = &write_table_req.table;
            self.maybe_open_partition_table_if_not_exist(&catalog, &schema, table_name)
                .await?;
            let mut table = self.try_get_table(&catalog, &schema, table_name)?;

            match table.clone() {
                None => {
                    if auto_create_table {
                        self.create_table(
                            request_id,
                            &catalog,
                            &schema,
                            &write_table_req,
                            &schema_config,
                            deadline,
                        )
                        .await?;
                        // try to get table again
                        table = self.try_get_table(&catalog, &schema, table_name)?;
                    }
                }
                Some(t) => {
                    if auto_create_table {
                        // The reasons for making the decision to add columns before writing are as
                        // follows:
                        // * If judged based on the error message returned, it may cause data that
                        //   has already been successfully written to be written again and affect
                        //   the accuracy of the data.
                        // * Currently, the decision to add columns is made at the request level,
                        //   not at the row level, so the cost is relatively small.
                        let table_schema = t.schema();
                        let columns =
                            find_new_columns(&table_schema, &schema_config, &write_table_req)?;
                        if !columns.is_empty() {
                            self.execute_add_columns_plan(
                                request_id, &catalog, &schema, t, columns, deadline,
                            )
                            .await?;
                        }
                    }
                }
            }

            match table {
                Some(table) => {
                    let plan = write_table_request_to_insert_plan(table, write_table_req)?;
                    plan_vec.push(plan);
                }
                None => {
                    return ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Table not found, schema:{schema}, table:{table_name}"),
                    }
                    .fail();
                }
            }
        }

        Ok(plan_vec)
    }

    async fn execute_insert_plan(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        insert_plan: InsertPlan,
        deadline: Option<Instant>,
    ) -> Result<usize> {
        debug!(
            "Grpc handle write table begin, table:{}, row_num:{}",
            insert_plan.table.name(),
            insert_plan.rows.num_rows()
        );
        let plan = Plan::Insert(insert_plan);
        let output = self
            .execute_plan(request_id, catalog, schema, plan, deadline)
            .await;
        output.and_then(|output| match output {
            Output::AffectedRows(n) => Ok(n),
            Output::Records(_) => ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: "Invalid output type, expect AffectedRows, found Records",
            }
            .fail(),
        })
    }

    fn try_get_table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<Option<TableRef>> {
        self.instance
            .catalog_manager
            .catalog_by_name(catalog)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find catalog, catalog_name:{catalog}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Catalog not found, catalog_name:{catalog}"),
            })?
            .schema_by_name(schema)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find schema, schema_name:{schema}"),
            })?
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Schema not found, schema_name:{schema}"),
            })?
            .table_by_name(table_name)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!("Failed to find table, table:{table_name}"),
            })
    }

    async fn create_table(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        write_table_req: &WriteTableRequest,
        schema_config: &SchemaConfig,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let provider = CatalogMetaProvider {
            manager: self.instance.catalog_manager.clone(),
            default_catalog: catalog,
            default_schema: schema,
            function_registry: &*self.instance.function_registry,
        };
        let frontend = Frontend::new(provider);
        let mut ctx = FrontendContext::new(request_id, deadline);
        let plan = frontend
            .write_req_to_plan(&mut ctx, schema_config, write_table_req)
            .box_err()
            .with_context(|| ErrWithCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: format!(
                    "Failed to build creating table plan, table:{}",
                    write_table_req.table
                ),
            })?;

        debug!("Grpc handle create table begin, plan:{:?}", plan);

        let output = self
            .execute_plan(request_id, catalog, schema, plan, deadline)
            .await;
        output.and_then(|output| match output {
            Output::AffectedRows(_) => Ok(()),
            Output::Records(_) => ErrNoCause {
                code: StatusCode::INTERNAL_SERVER_ERROR,
                msg: "Invalid output type, expect AffectedRows, found Records",
            }
            .fail(),
        })
    }

    async fn execute_add_columns_plan(
        &self,
        request_id: RequestId,
        catalog: &str,
        schema: &str,
        table: TableRef,
        columns: Vec<ColumnSchema>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let table_name = table.name().to_string();
        info!(
            "Add columns start, request_id:{request_id}, table:{table_name}, columns:{columns:?}"
        );

        let plan = Plan::AlterTable(AlterTablePlan {
            table,
            operations: AlterTableOperation::AddColumn(columns),
        });
        let _ = self
            .execute_plan(request_id, catalog, schema, plan, deadline)
            .await?;

        info!("Add columns success, request_id:{request_id}, table:{table_name}");
        Ok(())
    }
}

fn find_new_columns(
    schema: &Schema,
    schema_config: &SchemaConfig,
    write_req: &WriteTableRequest,
) -> Result<Vec<ColumnSchema>> {
    let new_schema = build_schema_from_write_table_request(schema_config, write_req)
        .box_err()
        .context(ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: "Build schema from write table request failed",
        })?;

    let columns = new_schema.columns();
    let old_columns = schema.columns();

    // find new columns:
    // 1. timestamp column can't be a new column;
    // 2. column not in old schema is a new column.
    let new_columns = columns
        .iter()
        .enumerate()
        .filter(|(idx, column)| {
            *idx != new_schema.timestamp_index()
                && !old_columns.iter().any(|c| c.name == column.name)
        })
        .map(|(_, column)| column.clone())
        .collect();
    Ok(new_columns)
}

fn write_table_request_to_insert_plan(
    table: TableRef,
    write_table_req: WriteTableRequest,
) -> Result<InsertPlan> {
    let schema = table.schema();

    let mut rows_total = Vec::new();
    for write_entry in write_table_req.entries {
        let mut rows = write_entry_to_rows(
            &write_table_req.table,
            &schema,
            &write_table_req.tag_names,
            &write_table_req.field_names,
            write_entry,
        )?;
        rows_total.append(&mut rows);
    }
    // The row group builder will checks nullable.
    let row_group = RowGroupBuilder::with_rows(schema, rows_total)
        .box_err()
        .with_context(|| ErrWithCause {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("Failed to build row group, table:{}", table.name()),
        })?
        .build();
    Ok(InsertPlan {
        table,
        rows: row_group,
        default_value_map: BTreeMap::new(),
    })
}

fn write_entry_to_rows(
    table_name: &str,
    schema: &Schema,
    tag_names: &[String],
    field_names: &[String],
    write_series_entry: WriteSeriesEntry,
) -> Result<Vec<Row>> {
    // Init all columns by null.
    let mut rows = vec![
        Row::from_datums(vec![Datum::Null; schema.num_columns()]);
        write_series_entry.field_groups.len()
    ];

    // Fill tsid by default value.
    if let Some(tsid_idx) = schema.index_of_tsid() {
        let kind = &schema.tsid_column().unwrap().data_type;
        let default_datum = Datum::empty(kind);
        for row in &mut rows {
            row[tsid_idx] = default_datum.clone();
        }
    }

    // Fill tags.
    for tag in write_series_entry.tags {
        let name_index = tag.name_index as usize;
        ensure!(
            name_index < tag_names.len(),
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag {tag:?} is not found in tag_names:{tag_names:?}, table:{table_name}",
                ),
            }
        );

        let tag_name = &tag_names[name_index];
        let tag_index_in_schema = schema.index_of(tag_name).with_context(|| ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!("Can't find tag({tag_name}) in schema, table:{table_name}"),
        })?;

        let column_schema = schema.column(tag_index_in_schema);
        ensure!(
            column_schema.is_tag,
            ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Column({tag_name}) is a field rather than a tag, table:{table_name}"),
            }
        );

        let tag_value = tag
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!("Tag({tag_name}) value is needed, table:{table_name}"),
            })?
            .value
            .with_context(|| ErrNoCause {
                code: StatusCode::BAD_REQUEST,
                msg: format!(
                    "Tag({tag_name}) value type is not supported, table_name:{table_name}"
                ),
            })?;
        for row in &mut rows {
            row[tag_index_in_schema] = convert_proto_value_to_datum(
                table_name,
                tag_name,
                tag_value.clone(),
                column_schema.data_type,
            )?;
        }
    }

    // Fill fields.
    let mut field_name_index: HashMap<String, usize> = HashMap::new();
    for (i, field_group) in write_series_entry.field_groups.into_iter().enumerate() {
        // timestamp
        let timestamp_index_in_schema = schema.timestamp_index();
        rows[i][timestamp_index_in_schema] =
            Datum::Timestamp(Timestamp::new(field_group.timestamp));

        for field in field_group.fields {
            if (field.name_index as usize) < field_names.len() {
                let field_name = &field_names[field.name_index as usize];
                let index_in_schema = if field_name_index.contains_key(field_name) {
                    field_name_index.get(field_name).unwrap().to_owned()
                } else {
                    let index_in_schema =
                        schema.index_of(field_name).with_context(|| ErrNoCause {
                            code: StatusCode::BAD_REQUEST,
                            msg: format!(
                                "Can't find field in schema, table:{table_name}, field_name:{field_name}"
                            ),
                        })?;
                    field_name_index.insert(field_name.to_string(), index_in_schema);
                    index_in_schema
                };
                let column_schema = schema.column(index_in_schema);
                ensure!(
                    !column_schema.is_tag,
                    ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Column {field_name} is a tag rather than a field, table:{table_name}"
                        )
                    }
                );
                let field_value = field
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!("Field({field_name}) is needed, table:{table_name}"),
                    })?
                    .value
                    .with_context(|| ErrNoCause {
                        code: StatusCode::BAD_REQUEST,
                        msg: format!(
                            "Field({field_name}) value type is not supported, table:{table_name}"
                        ),
                    })?;

                rows[i][index_in_schema] = convert_proto_value_to_datum(
                    table_name,
                    field_name,
                    field_value,
                    column_schema.data_type,
                )?;
            }
        }
    }

    Ok(rows)
}

/// Convert the `Value_oneof_value` defined in protos into the datum.
fn convert_proto_value_to_datum(
    table_name: &str,
    name: &str,
    value: value::Value,
    data_type: DatumKind,
) -> Result<Datum> {
    match (value, data_type) {
        (value::Value::Float64Value(v), DatumKind::Double) => Ok(Datum::Double(v)),
        (value::Value::StringValue(v), DatumKind::String) => Ok(Datum::String(v.into())),
        (value::Value::Int64Value(v), DatumKind::Int64) => Ok(Datum::Int64(v)),
        (value::Value::Float32Value(v), DatumKind::Float) => Ok(Datum::Float(v)),
        (value::Value::Int32Value(v), DatumKind::Int32) => Ok(Datum::Int32(v)),
        (value::Value::Int16Value(v), DatumKind::Int16) => Ok(Datum::Int16(v as i16)),
        (value::Value::Int8Value(v), DatumKind::Int8) => Ok(Datum::Int8(v as i8)),
        (value::Value::BoolValue(v), DatumKind::Boolean) => Ok(Datum::Boolean(v)),
        (value::Value::Uint64Value(v), DatumKind::UInt64) => Ok(Datum::UInt64(v)),
        (value::Value::Uint32Value(v), DatumKind::UInt32) => Ok(Datum::UInt32(v)),
        (value::Value::Uint16Value(v), DatumKind::UInt16) => Ok(Datum::UInt16(v as u16)),
        (value::Value::Uint8Value(v), DatumKind::UInt8) => Ok(Datum::UInt8(v as u8)),
        (value::Value::TimestampValue(v), DatumKind::Timestamp) => Ok(Datum::Timestamp(Timestamp::new(v))),
        (value::Value::VarbinaryValue(v), DatumKind::Varbinary) => Ok(Datum::Varbinary(Bytes::from(v))),
        (v, _) => ErrNoCause {
            code: StatusCode::BAD_REQUEST,
            msg: format!(
                "Value type is not same, table:{table_name}, value_name:{name}, schema_type:{data_type:?}, actual_value:{v:?}"
            ),
        }
            .fail(),
    }
}

#[cfg(test)]
mod test {
    use ceresdbproto::storage::{value, Field, FieldGroup, Tag, Value, WriteSeriesEntry};
    use common_types::{
        column_schema::{self, ColumnSchema},
        datum::{Datum, DatumKind},
        row::Row,
        schema::Builder,
        time::Timestamp,
    };
    use system_catalog::sys_catalog_table::TIMESTAMP_COLUMN_NAME;

    use super::*;

    const TAG_K: &str = "tagk";
    const TAG_V: &str = "tagv";
    const TAG_K1: &str = "tagk1";
    const TAG_V1: &str = "tagv1";
    const FIELD_NAME: &str = "field";
    const FIELD_NAME1: &str = "field1";
    const FIELD_VALUE_STRING: &str = "stringValue";

    // tag_names field_names write_entry
    fn generate_write_entry() -> (Schema, Vec<String>, Vec<String>, WriteSeriesEntry) {
        let tag_names = vec![TAG_K.to_string(), TAG_K1.to_string()];
        let field_names = vec![FIELD_NAME.to_string(), FIELD_NAME1.to_string()];

        let tag = Tag {
            name_index: 0,
            value: Some(Value {
                value: Some(value::Value::StringValue(TAG_V.to_string())),
            }),
        };
        let tag1 = Tag {
            name_index: 1,
            value: Some(Value {
                value: Some(value::Value::StringValue(TAG_V1.to_string())),
            }),
        };
        let tags = vec![tag, tag1];

        let field = Field {
            name_index: 0,
            value: Some(Value {
                value: Some(value::Value::Float64Value(100.0)),
            }),
        };
        let field1 = Field {
            name_index: 1,
            value: Some(Value {
                value: Some(value::Value::StringValue(FIELD_VALUE_STRING.to_string())),
            }),
        };
        let field_group = FieldGroup {
            timestamp: 1000,
            fields: vec![field],
        };
        let field_group1 = FieldGroup {
            timestamp: 2000,
            fields: vec![field1.clone()],
        };
        let field_group2 = FieldGroup {
            timestamp: 3000,
            fields: vec![field1],
        };

        let write_entry = WriteSeriesEntry {
            tags,
            field_groups: vec![field_group, field_group1, field_group2],
        };

        let schema_builder = Builder::new();
        let schema = schema_builder
            .auto_increment_column_id(true)
            .add_key_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TIMESTAMP_COLUMN_NAME.to_string(),
                data_type: DatumKind::Timestamp,
                is_nullable: false,
                is_tag: false,
                comment: String::new(),
                escaped_name: TIMESTAMP_COLUMN_NAME.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_key_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TAG_K.to_string(),
                data_type: DatumKind::String,
                is_nullable: false,
                is_tag: true,
                comment: String::new(),
                escaped_name: TAG_K.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: TAG_K1.to_string(),
                data_type: DatumKind::String,
                is_nullable: false,
                is_tag: true,
                comment: String::new(),
                escaped_name: TAG_K1.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: FIELD_NAME.to_string(),
                data_type: DatumKind::Double,
                is_nullable: true,
                is_tag: false,
                comment: String::new(),
                escaped_name: FIELD_NAME.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .add_normal_column(ColumnSchema {
                id: column_schema::COLUMN_ID_UNINIT,
                name: FIELD_NAME1.to_string(),
                data_type: DatumKind::String,
                is_nullable: true,
                is_tag: false,
                comment: String::new(),
                escaped_name: FIELD_NAME1.escape_debug().to_string(),
                default_value: None,
            })
            .unwrap()
            .build()
            .unwrap();
        (schema, tag_names, field_names, write_entry)
    }

    #[test]
    fn test_write_entry_to_row_group() {
        let (schema, tag_names, field_names, write_entry) = generate_write_entry();
        let rows =
            write_entry_to_rows("test_table", &schema, &tag_names, &field_names, write_entry)
                .unwrap();
        let row0 = vec![
            Datum::Timestamp(Timestamp::new(1000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Double(100.0),
            Datum::Null,
        ];
        let row1 = vec![
            Datum::Timestamp(Timestamp::new(2000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Null,
            Datum::String(FIELD_VALUE_STRING.into()),
        ];
        let row2 = vec![
            Datum::Timestamp(Timestamp::new(3000)),
            Datum::String(TAG_V.into()),
            Datum::String(TAG_V1.into()),
            Datum::Null,
            Datum::String(FIELD_VALUE_STRING.into()),
        ];

        let expect_rows = vec![
            Row::from_datums(row0),
            Row::from_datums(row1),
            Row::from_datums(row2),
        ];
        assert_eq!(rows, expect_rows);
    }
}
