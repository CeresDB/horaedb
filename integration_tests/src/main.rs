// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::{env, path::Path};

use anyhow::Result;
use async_trait::async_trait;
use database::CeresDB;
use sqlness::{EnvController, Runner};

mod database;

const CASE_ROOT_PATH_ENV: &str = "CERESDB_TEST_CASE_PATH";

pub struct CeresDBController;

#[async_trait]
impl EnvController for CeresDBController {
    type DB = CeresDB;

    async fn start(&self, _env: &str, config: Option<&Path>) -> Self::DB {
        CeresDB::new(config)
    }

    async fn stop(&self, _env: &str, database: Self::DB) {
        database.stop();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let case_dir = env::var(CASE_ROOT_PATH_ENV)?;
    let env = CeresDBController;
    let config = sqlness::ConfigBuilder::default()
        .case_dir(case_dir)
        .build()?;
    let runner = Runner::new_with_config(config, env).await?;
    runner.run().await?;

    Ok(())
}
