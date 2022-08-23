use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use table_engine::table::{AlterSchemaRequest, WriteRequest};

use crate::{
    instance::{
        alter::TableAlterSchemaPolicy,
        flush_compaction::{TableFlushOptions, TableFlushPolicy},
        write::TableWritePolicy,
        write_worker::WorkerLocal,
        Instance, InstanceRef,
    },
    role_table::{Result, RoleTable, RoleTableRef, TableRole},
    table::data::TableDataRef,
};

pub struct LeaderTable {
    inner: Arc<LeaderTableInner>,
}

impl LeaderTable {
    pub fn new(table_data: TableDataRef) -> RoleTableRef {
        let inner = Arc::new(LeaderTableInner {
            state: AtomicU8::new(LeaderTableInner::ROLE),
            table_data,
        });
        Arc::new(Self { inner }) as _
    }
}

impl std::fmt::Debug for LeaderTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderTable")
            .field("table_id", &self.inner.table_data.id)
            .finish()
    }
}

impl Drop for LeaderTable {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            // TODO: notify the state is completely changed
        }
    }
}

struct LeaderTableInner {
    state: AtomicU8,
    table_data: TableDataRef,
}

// TODO: handle `Result`
impl LeaderTableInner {
    const ROLE: u8 = TableRole::Leader as u8;

    fn check_state(&self) -> bool {
        self.state.load(Ordering::Relaxed) == Self::ROLE
    }

    async fn change_role(&self) -> Result<()> {
        todo!()
    }

    async fn write(&self, request: WriteRequest, instance: &Arc<Instance>) -> Result<usize> {
        let policy = TableWritePolicy::Full;

        let res = instance
            .write_to_table(self.table_data.clone(), request, policy)
            .await
            .unwrap();

        Ok(res)
    }

    async fn flush(
        &self,
        mut flush_opts: TableFlushOptions,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
    ) -> Result<()> {
        // Leader Table will dump memtable to storage.
        flush_opts.policy = TableFlushPolicy::Dump;

        instance
            .flush_table_in_worker(worker_local, &self.table_data, flush_opts)
            .await
            .unwrap();

        Ok(())
    }

    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        instance
            .process_alter_schema_command(
                worker_local,
                &self.table_data,
                request,
                TableAlterSchemaPolicy::Alter,
            )
            .await
            .unwrap();

        Ok(())
    }

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        options: HashMap<String, String>,
    ) -> Result<()> {
        instance
            .process_alter_options_command(worker_local, &self.table_data, options)
            .await
            .unwrap();

        Ok(())
    }
}

#[async_trait]
impl RoleTable for LeaderTable {
    fn check_state(&self) -> bool {
        self.inner.check_state()
    }

    async fn change_role(&self) -> Result<()> {
        self.inner.change_role().await
    }

    async fn write(&self, request: WriteRequest, instance: &InstanceRef) -> Result<usize> {
        self.inner.write(request, instance).await
    }

    /// This method is expected to be called by [Instance]
    async fn flush(
        &self,
        flush_opts: TableFlushOptions,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
    ) -> Result<()> {
        self.inner.flush(flush_opts, instance, worker_local).await
    }

    /// This method is expected to be called by [Instance]
    async fn alter_schema(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        request: AlterSchemaRequest,
    ) -> Result<()> {
        self.inner
            .alter_schema(instance, worker_local, request)
            .await
    }

    async fn alter_options(
        &self,
        instance: &Arc<Instance>,
        worker_local: &mut WorkerLocal,
        options: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .alter_options(instance, worker_local, options)
            .await
    }

    fn table_data(&self) -> TableDataRef {
        self.inner.table_data.clone()
    }
}
