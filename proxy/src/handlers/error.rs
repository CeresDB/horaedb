// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Error of handlers

use macros::define_result;
use snafu::{Backtrace, Snafu};
use warp::reject::Reject;

use crate::limiter;
// TODO(yingwen): Avoid printing huge sql string
// TODO(yingwen): Maybe add an error type to sql sub mod

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to parse sql, err:{}", source))]
    ParseSql {
        source: query_frontend::frontend::Error,
    },

    #[snafu(display("Failed to create plan, query:{}, err:{}", query, source))]
    CreatePlan {
        query: String,
        source: query_frontend::frontend::Error,
    },

    #[snafu(display(
        "Only support execute one statement now, current num:{}, query:{}.\nBacktrace:\n{}",
        len,
        query,
        backtrace,
    ))]
    TooMuchStmt {
        len: usize,
        query: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to execute interpreter, query:{}, err:{}", query, source))]
    InterpreterExec {
        query: String,
        source: interpreters::interpreter::Error,
    },

    #[snafu(display(
        "Failed to convert arrow to string, query:{}, err:{}.\nBacktrace:\n{}",
        query,
        source,
        backtrace
    ))]
    ArrowToString {
        query: String,
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Query limited by block list, query:{}, err:{}", query, source))]
    QueryBlock {
        query: String,
        source: limiter::Error,
    },

    #[snafu(display(
        "Query timeout, query:{}, err:{}\nBacktrace:\n{}",
        query,
        source,
        backtrace
    ))]
    QueryTimeout {
        query: String,
        source: tokio::time::error::Elapsed,
        backtrace: Backtrace,
    },
}

define_result!(Error);

impl Reject for Error {}
