// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Manage meta data of the engine

pub mod details;
pub mod meta_data;
pub mod meta_update;

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use common_types::table::Location;

use crate::meta::{meta_data::TableManifestData, meta_update::MetaUpdateRequest};

/// Manifest holds meta data of all tables.
#[async_trait]
pub trait Manifest: Send + Sync + fmt::Debug {
    /// Store update to manifest
    async fn store_update(
        &self,
        request: MetaUpdateRequest,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Load table meta data from manifest.
    ///
    /// If `do_snapshot` is true, the manifest will try to create a snapshot of
    /// the manifest data.
    async fn load_data(
        &self,
        location: Location,
        do_snapshot: bool,
    ) -> Result<Option<TableManifestData>, Box<dyn std::error::Error + Send + Sync>>;
}

pub type ManifestRef = Arc<dyn Manifest>;
