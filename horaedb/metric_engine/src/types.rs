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

use std::{ops::Range, pin::Pin, sync::Arc};

use arrow::{array::RecordBatch, datatypes::Schema};
use futures::Stream;
use object_store::ObjectStore;

use crate::error::Result;

pub enum Value {
    Int64(i64),
    Int32(i32),
    Float64(f64),
    Bytes(Vec<u8>),
}

pub enum Predicate {
    Equal(String, Value),
    NotEqual(String, Value),
    RegexMatch(String, Vec<u8>),
    NotRegexMatch(String, Vec<u8>),
}

pub type TimeRange = Range<i64>;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> &Schema;
}

/// Trait for a [`Stream`] of [`RecordBatch`]es
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;
