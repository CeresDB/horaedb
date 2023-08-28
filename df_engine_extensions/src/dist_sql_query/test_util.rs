// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use catalog::{manager::ManagerRef, test_util::MockCatalogManagerBuilder};
use common_types::{projected_schema::ProjectedSchema, tests::build_schema_for_cpu};
use datafusion::{
    logical_expr::{expr_fn, Literal, Operator},
    physical_plan::{
        expressions::{binary, col, lit},
        filter::FilterExec,
        projection::ProjectionExec,
        ExecutionPlan, PhysicalExpr,
    },
    scalar::ScalarValue,
};
use table_engine::{
    memory::MemoryTable,
    predicate::PredicateBuilder,
    remote::model::TableIdentifier,
    table::{ReadOptions, ReadRequest, TableId},
    ANALYTIC_ENGINE_TYPE,
};
use trace_metric::MetricsCollector;

use crate::dist_sql_query::physical_plan::{UnresolvedPartitionedScan, UnresolvedSubTableScan};

// Test context
pub struct TestContext {
    request: ReadRequest,
    sub_tables: Vec<TableIdentifier>,
    physical_filter: Arc<dyn PhysicalExpr>,
    physical_projection: Vec<(Arc<dyn PhysicalExpr>, String)>,
    catalog_manager: ManagerRef,
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TestContext {
    pub fn new() -> Self {
        let test_schema = build_schema_for_cpu();
        let sub_tables = vec![
            "__test_1".to_string(),
            "__test_2".to_string(),
            "__test_3".to_string(),
        ]
        .into_iter()
        .map(|table| TableIdentifier {
            catalog: "test_catalog".to_string(),
            schema: "test_schema".to_string(),
            table,
        })
        .collect::<Vec<_>>();

        // Logical exprs.
        // Projection: [time, tag1, tag2, value, field2]
        let projection = vec![1_usize, 2, 3, 4, 5];
        let projected_schema = ProjectedSchema::new(test_schema.clone(), Some(projection)).unwrap();
        // Filter: time < 1691974518000 and tag1 == 'test_tag'
        let logical_filters = vec![(expr_fn::col("time").lt(ScalarValue::TimestampMillisecond(
            Some(1691974518000),
            None,
        )
        .lit()))
        .and(expr_fn::col("tag1").eq("test_tag".lit()))];

        // Physical exprs.
        let arrow_projected_schema = projected_schema.to_projected_arrow_schema();
        let physical_projection = vec![
            (
                col("time", &arrow_projected_schema).unwrap(),
                "time".to_string(),
            ),
            (
                col("tag1", &arrow_projected_schema).unwrap(),
                "tag1".to_string(),
            ),
            (
                col("tag2", &arrow_projected_schema).unwrap(),
                "tag2".to_string(),
            ),
            (
                col("value", &arrow_projected_schema).unwrap(),
                "value".to_string(),
            ),
            (
                col("field2", &arrow_projected_schema).unwrap(),
                "field2".to_string(),
            ),
        ];

        let physical_filter1: Arc<dyn PhysicalExpr> = binary(
            col("time", &arrow_projected_schema).unwrap(),
            Operator::Lt,
            lit(ScalarValue::TimestampMillisecond(Some(1691974518000), None)),
            &arrow_projected_schema,
        )
        .unwrap();
        let physical_filter2: Arc<dyn PhysicalExpr> = binary(
            col("tag1", &arrow_projected_schema).unwrap(),
            Operator::Eq,
            lit("test_tag"),
            &arrow_projected_schema,
        )
        .unwrap();
        let physical_filter: Arc<dyn PhysicalExpr> = binary(
            physical_filter1,
            Operator::And,
            physical_filter2,
            &arrow_projected_schema,
        )
        .unwrap();

        // Build the physical plan.
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(&logical_filters)
            .extract_time_range(&test_schema, &logical_filters)
            .build();
        let read_request = ReadRequest {
            request_id: 42.into(),
            opts: ReadOptions::default(),
            projected_schema,
            predicate,
            metrics_collector: MetricsCollector::default(),
        };

        // Build the test catalog
        let table = Arc::new(MemoryTable::new(
            "__test_1".to_string(),
            TableId::from(42),
            build_schema_for_cpu(),
            ANALYTIC_ENGINE_TYPE.to_string(),
        ));

        let catalog_manager_builder = MockCatalogManagerBuilder::new(
            "test_catalog".to_string(),
            "test_schema".to_string(),
            vec![table],
        );
        let catalog_manager = catalog_manager_builder.build();

        Self {
            request: read_request,
            sub_tables,
            physical_filter,
            physical_projection,
            catalog_manager,
        }
    }

    // Return test catalog manager
    pub fn catalog_manager(&self) -> ManagerRef {
        self.catalog_manager.clone()
    }

    // Basic plan includes:
    // Projection
    //      Filter
    //          Scan
    pub fn build_basic_partitioned_table_plan(&self) -> Arc<dyn ExecutionPlan> {
        let unresolved_scan = Arc::new(UnresolvedPartitionedScan {
            sub_tables: self.sub_tables.clone(),
            read_request: self.request.clone(),
        });

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(self.physical_filter.clone(), unresolved_scan).unwrap());

        Arc::new(ProjectionExec::try_new(self.physical_projection.clone(), filter).unwrap())
    }

    // Basic plan includes:
    // Projection
    //      Filter
    //          Scan
    pub fn build_basic_sub_table_plan(&self) -> Arc<dyn ExecutionPlan> {
        let unresolved_scan = Arc::new(UnresolvedSubTableScan {
            table: self.sub_tables[0].clone(),
            read_request: self.request.clone(),
        });

        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(self.physical_filter.clone(), unresolved_scan).unwrap());

        Arc::new(ProjectionExec::try_new(self.physical_projection.clone(), filter).unwrap())
    }

    pub fn read_request(&self) -> ReadRequest {
        self.request.clone()
    }
}
