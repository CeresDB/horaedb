// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Server

use std::sync::Arc;

use catalog::manager::ManagerRef;
use cluster::ClusterRef;
use df_operator::registry::FunctionRegistryRef;
use log::warn;
use query_engine::executor::Executor as QueryExecutor;
use snafu::{Backtrace, OptionExt, ResultExt, Snafu};
use table_engine::engine::{EngineRuntimes, TableEngineRef};

use crate::{
    config::{Config, Endpoint},
    grpc::{self, RpcServices},
    http::{self, Service},
    instance::{Instance, InstanceRef},
    limiter::Limiter,
    mysql,
    mysql::error::Error as MysqlError,
    route::RouterRef,
    schema_config_provider::SchemaConfigProviderRef,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing runtimes.\nBacktrace:\n{}", backtrace))]
    MissingRuntimes { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },

    #[snafu(display("Missing schema config provider.\nBacktrace:\n{}", backtrace))]
    MissingSchemaConfigProvider { backtrace: Backtrace },

    #[snafu(display("Missing catalog manager.\nBacktrace:\n{}", backtrace))]
    MissingCatalogManager { backtrace: Backtrace },

    #[snafu(display("Missing query executor.\nBacktrace:\n{}", backtrace))]
    MissingQueryExecutor { backtrace: Backtrace },

    #[snafu(display("Missing table engine.\nBacktrace:\n{}", backtrace))]
    MissingTableEngine { backtrace: Backtrace },

    #[snafu(display("Missing function registry.\nBacktrace:\n{}", backtrace))]
    MissingFunctionRegistry { backtrace: Backtrace },

    #[snafu(display("Missing limiter.\nBacktrace:\n{}", backtrace))]
    MissingLimiter { backtrace: Backtrace },

    #[snafu(display("Failed to start http service, err:{}", source))]
    StartHttpService { source: crate::http::Error },

    #[snafu(display("Failed to build mysql service, err:{}", source))]
    BuildMysqlService { source: MysqlError },

    #[snafu(display("Failed to start mysql service, err:{}", source))]
    StartMysqlService { source: MysqlError },

    #[snafu(display("Failed to register system catalog, err:{}", source))]
    RegisterSystemCatalog { source: catalog::manager::Error },

    #[snafu(display("Failed to build grpc service, err:{}", source))]
    BuildGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start grpc service, err:{}", source))]
    StartGrpcService { source: crate::grpc::Error },

    #[snafu(display("Failed to start cluster, err:{}", source))]
    StartCluster { source: cluster::Error },
}

define_result!(Error);

// TODO(yingwen): Consider a config manager
/// Server
pub struct Server<Q: QueryExecutor + 'static> {
    http_service: Service<Q>,
    rpc_services: RpcServices<Q>,
    mysql_service: mysql::MysqlService<Q>,
    instance: InstanceRef<Q>,
    cluster: Option<ClusterRef>,
}

impl<Q: QueryExecutor + 'static> Server<Q> {
    pub async fn stop(mut self) {
        self.rpc_services.shutdown().await;
        self.http_service.stop();
        self.mysql_service.shutdown();

        if let Some(cluster) = &self.cluster {
            cluster.stop().await.expect("fail to stop cluster");
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        if let Some(cluster) = &self.cluster {
            cluster.start().await.context(StartCluster)?;
        }

        // TODO: Is it necessary to create default schema in cluster mode?
        self.create_default_schema_if_not_exists().await;

        self.mysql_service
            .start()
            .await
            .context(StartMysqlService)?;
        self.rpc_services.start().await.context(StartGrpcService)?;

        Ok(())
    }

    async fn create_default_schema_if_not_exists(&self) {
        let catalog_mgr = &self.instance.catalog_manager;
        let default_catalog = catalog_mgr
            .catalog_by_name(catalog_mgr.default_catalog_name())
            .expect("Fail to retrieve default catalog")
            .expect("Default catalog doesn't exist");

        if default_catalog
            .schema_by_name(catalog_mgr.default_schema_name())
            .expect("Fail to retrieve default schema")
            .is_none()
        {
            warn!("Default schema doesn't exist and create it");
            default_catalog
                .create_schema(catalog_mgr.default_schema_name())
                .await
                .expect("Fail to create default schema");
        }
    }
}

#[must_use]
pub struct Builder<Q> {
    config: Config,
    runtimes: Option<Arc<EngineRuntimes>>,
    catalog_manager: Option<ManagerRef>,
    query_executor: Option<Q>,
    table_engine: Option<TableEngineRef>,
    function_registry: Option<FunctionRegistryRef>,
    limiter: Limiter,
    cluster: Option<ClusterRef>,
    router: Option<RouterRef>,
    schema_config_provider: Option<SchemaConfigProviderRef>,
}

impl<Q: QueryExecutor + 'static> Builder<Q> {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            runtimes: None,
            catalog_manager: None,
            query_executor: None,
            table_engine: None,
            function_registry: None,
            limiter: Limiter::default(),
            cluster: None,
            router: None,
            schema_config_provider: None,
        }
    }

    pub fn runtimes(mut self, runtimes: Arc<EngineRuntimes>) -> Self {
        self.runtimes = Some(runtimes);
        self
    }

    pub fn catalog_manager(mut self, val: ManagerRef) -> Self {
        self.catalog_manager = Some(val);
        self
    }

    pub fn query_executor(mut self, val: Q) -> Self {
        self.query_executor = Some(val);
        self
    }

    pub fn table_engine(mut self, val: TableEngineRef) -> Self {
        self.table_engine = Some(val);
        self
    }

    pub fn function_registry(mut self, val: FunctionRegistryRef) -> Self {
        self.function_registry = Some(val);
        self
    }

    pub fn limiter(mut self, val: Limiter) -> Self {
        self.limiter = val;
        self
    }

    pub fn cluster(mut self, cluster: ClusterRef) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn router(mut self, router: RouterRef) -> Self {
        self.router = Some(router);
        self
    }

    pub fn schema_config_provider(
        mut self,
        schema_config_provider: SchemaConfigProviderRef,
    ) -> Self {
        self.schema_config_provider = Some(schema_config_provider);
        self
    }

    /// Build and run the server
    pub fn build(self) -> Result<Server<Q>> {
        // Build instance
        let catalog_manager = self.catalog_manager.context(MissingCatalogManager)?;
        let query_executor = self.query_executor.context(MissingQueryExecutor)?;
        let table_engine = self.table_engine.context(MissingTableEngine)?;
        let function_registry = self.function_registry.context(MissingFunctionRegistry)?;
        let instance = Instance {
            catalog_manager,
            query_executor,
            table_engine,
            function_registry,
            limiter: self.limiter,
        };
        let instance = InstanceRef::new(instance);

        // Create http config
        let http_config = Endpoint {
            addr: self.config.bind_addr.clone(),
            port: self.config.http_port,
        };

        // Start http service
        let runtimes = self.runtimes.context(MissingRuntimes)?;
        let http_service = http::Builder::new(http_config)
            .runtimes(runtimes.clone())
            .instance(instance.clone())
            .build()
            .context(StartHttpService)?;

        let mysql_config = mysql::MysqlConfig {
            ip: self.config.bind_addr.clone(),
            port: self.config.mysql_port,
        };

        let mysql_service = mysql::Builder::new(mysql_config)
            .runtimes(runtimes.clone())
            .instance(instance.clone())
            .build()
            .context(BuildMysqlService)?;

        let router = self.router.context(MissingRouter)?;
        let provider = self
            .schema_config_provider
            .context(MissingSchemaConfigProvider)?;
        let rpc_services = grpc::Builder::new()
            .endpoint(Endpoint::new(self.config.bind_addr, self.config.grpc_port).to_string())
            .runtimes(runtimes)
            .instance(instance.clone())
            .router(router)
            .cluster(self.cluster.clone())
            .schema_config_provider(provider)
            .build()
            .context(BuildGrpcService)?;

        let server = Server {
            http_service,
            rpc_services,
            mysql_service,
            instance,
            cluster: self.cluster,
        };
        Ok(server)
    }
}
