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

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    error::Result as DfResult,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
};
use table_engine::{
    remote::model::TableIdentifier,
    table::{ReadRequest, TableRef},
};

pub mod codec;
pub mod physical_plan;
pub mod resolver;
#[cfg(test)]
pub mod test_util;

/// Remote datafusion physical plan executor
#[async_trait]
pub trait RemotePhysicalPlanExecutor: Clone + fmt::Debug + Send + Sync + 'static {
    async fn execute(
        &self,
        table: TableIdentifier,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> DfResult<SendableRecordBatchStream>;
}

/// Executable scan's builder
///
/// It is not suitable to restrict the detailed implementation of executable
/// scan, so we define a builder here which return the general `ExecutionPlan`.
pub trait ExecutableScanBuilder: fmt::Debug + Send + Sync + 'static {
    fn build(&self, table: TableRef, read_request: ReadRequest)
        -> DfResult<Arc<dyn ExecutionPlan>>;
}
