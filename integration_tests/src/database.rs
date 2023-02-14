// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{
    env,
    fmt::Display,
    fs::File,
    path::Path,
    process::{Child, Command},
    sync::Arc,
};

use async_trait::async_trait;
use ceresdb_client_rs::{
    db_client::{Builder, DbClient, Mode},
    model::sql_query::{display::CsvFormatter, Request},
    RpcContext,
};
use sqlness::Database;

const BINARY_PATH_ENV: &str = "CERESDB_BINARY_PATH";
const SERVER_ENDPOINT_ENV: &str = "CERESDB_SERVER_ENDPOINT";
const CERESDB_STDOUT_FILE: &str = "CERESDB_STDOUT_FILE";
const CERESDB_STDERR_FILE: &str = "CERESDB_STDERR_FILE";

pub struct CeresDB {
    server_process: Child,
    db_client: Arc<dyn DbClient>,
}

#[async_trait]
impl Database for CeresDB {
    async fn query(&self, query: String) -> Box<dyn Display> {
        Self::execute(query, self.db_client.clone()).await
    }
}

impl CeresDB {
    pub fn new(config: Option<&Path>) -> Self {
        let config = config.unwrap().to_string_lossy();
        let bin = env::var(BINARY_PATH_ENV).expect("Cannot parse binary path env");
        let stdout = env::var(CERESDB_STDOUT_FILE).expect("Cannot parse stdout env");
        let stderr = env::var(CERESDB_STDERR_FILE).expect("Cannot parse stderr env");
        let stdout = File::create(stdout).expect("Cannot create stdout");
        let stderr = File::create(stderr).expect("Cannot create stderr");

        println!("Start {bin} with {config}...");

        let server_process = Command::new(&bin)
            .args(["--config", &config])
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap_or_else(|_| panic!("Failed to start server at {bin:?}"));

        // Wait for a while
        std::thread::sleep(std::time::Duration::from_secs(5));
        let endpoint = env::var(SERVER_ENDPOINT_ENV).unwrap_or_else(|_| {
            panic!(
                "Cannot read server endpoint from env {SERVER_ENDPOINT_ENV:?}"
            )
        });

        let db_client = Builder::new(endpoint, Mode::Proxy).build();

        CeresDB {
            db_client,
            server_process,
        }
    }

    pub fn stop(mut self) {
        self.server_process.kill().unwrap()
    }

    async fn execute(query: String, client: Arc<dyn DbClient>) -> Box<dyn Display> {
        let query_ctx = RpcContext {
            database: Some("public".to_string()),
            timeout: None,
        };
        let query_req = Request {
            tables: vec![],
            sql: query,
        };
        let result = client.sql_query(&query_ctx, &query_req).await;

        Box::new(match result {
            Ok(resp) => {
                if resp.rows.is_empty() {
                    format!("affected_rows: {}", resp.affected_rows)
                } else {
                    format!("{}", CsvFormatter { resp })
                }
            }
            Err(e) => format!("Failed to execute query, err: {e:?}"),
        })
    }
}
