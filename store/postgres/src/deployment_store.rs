use blake3::Hash;
use detail::DeploymentDetail;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use graph::blockchain::block_stream::FirehoseCursor;
use graph::components::store::{EntityKey, EntityType, PruneReporter, StoredDynamicDataSource};
use graph::components::versions::VERSIONS;
use graph::data::query::Trace;
use graph::data::subgraph::{status, SPEC_VERSION_0_0_6};
use graph::prelude::{
    tokio, ApiVersion, CancelHandle, CancelToken, CancelableError, EntityOperation, PoolWaitStats,
    SubgraphDeploymentEntity,
};
use graph::semver::Version;
use lru_time_cache::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::Into;
use std::iter::FromIterator;
use std::ops::Bound;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Instant;

use graph::components::store::EntityCollection;
use graph::components::subgraph::{ProofOfIndexingFinisher, ProofOfIndexingVersion};
use graph::constraint_violation;
use graph::data::subgraph::schema::{DeploymentCreate, SubgraphError, POI_OBJECT};
use graph::prelude::{
    anyhow, debug, info, o, warn, web3, ApiSchema, AttributeNames, BlockNumber, BlockPtr,
    CheapClone, DeploymentHash, DeploymentState, Entity, EntityModification, EntityQuery, Error,
    Logger, QueryExecutionError, Schema, StopwatchMetrics, StoreError, StoreEvent, UnfailOutcome,
    Value, ENV_VARS,
};
use graph_graphql::prelude::api_schema;
use web3::types::Address;
use nebula_rust::graph_client::{pool_config, connection_pool, connection::Connection as Connection_nebula,session, nebula_schema::{ColType, Tag, DataType, InsertTagQuery, InsertEdgeQueryWithRank}};
use rand::Rng;

use crate::block_range::block_number;
use crate::catalog;
use crate::deployment;
use crate::detail::ErrorDetail;
use crate::dynds::DataSourcesTable;
use crate::relational::{Layout, LayoutCache, SqlName, Table};
use crate::relational_queries::FromEntityData;
use crate::{connection_pool::ConnectionPool, detail};
use crate::{dynds, primary::Site};

/// When connected to read replicas, this allows choosing which DB server to use for an operation.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ReplicaId {
    /// The main server has write and read access.
    Main,

    /// A read replica identified by its index.
    ReadOnly(usize),
}

/// Commonly needed information about a subgraph that we cache in
/// `Store.subgraph_cache`. Only immutable subgraph data can be cached this
/// way as the cache lives for the lifetime of the `Store` object
#[derive(Clone)]
pub(crate) struct SubgraphInfo {
    /// The schema as supplied by the user
    pub(crate) input: Arc<Schema>,
    /// The schema we derive from `input` with `graphql::schema::api::api_schema`
    pub(crate) api: HashMap<ApiVersion, Arc<ApiSchema>>,
    /// The block number at which this subgraph was grafted onto
    /// another one. We do not allow reverting past this block
    pub(crate) graft_block: Option<BlockNumber>,
    /// The deployment hash of the remote subgraph whose store
    /// will be GraphQL queried, for debugging purposes.
    pub(crate) debug_fork: Option<DeploymentHash>,
    pub(crate) description: Option<String>,
    pub(crate) repository: Option<String>,
    pub(crate) poi_version: ProofOfIndexingVersion,
}

pub struct StoreInner {
    logger: Logger,

    pool: ConnectionPool,
    read_only_pools: Vec<ConnectionPool>,

    /// A list of the available replicas set up such that when we run
    /// through the list once, we picked each replica according to its
    /// desired weight. Each replica can appear multiple times in the list
    replica_order: Vec<ReplicaId>,
    /// The current position in `replica_order` so we know which one to
    /// pick next
    conn_round_robin_counter: AtomicUsize,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<DeploymentHash, SubgraphInfo>>,

    /// A cache for the layout metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) layout_cache: LayoutCache,

    // pool_nebula: connection_test::ConnectionPool,
    conf_nebula: pool_config::PoolConfig,


}

/// Storage of the data for individual deployments. Each `DeploymentStore`
/// corresponds to one of the database shards that `SubgraphStore` manages.
#[derive(Clone)]
pub struct DeploymentStore(Arc<StoreInner>);

impl CheapClone for DeploymentStore {}

impl Deref for DeploymentStore {
    type Target = StoreInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DeploymentStore {
    pub fn new(
        logger: &Logger,
        pool: ConnectionPool,
        read_only_pools: Vec<ConnectionPool>,
        mut pool_weights: Vec<usize>,
        nebula_url: String,
        // pool_nebula: connection_test::ConnectionPool
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Create a list of replicas with repetitions according to the weights
        // and shuffle the resulting list. Any missing weights in the list
        // default to 1
        pool_weights.resize(read_only_pools.len() + 1, 1);
        let mut replica_order: Vec<_> = pool_weights
            .iter()
            .enumerate()
            .map(|(i, weight)| {
                let replica = if i == 0 {
                    ReplicaId::Main
                } else {
                    ReplicaId::ReadOnly(i - 1)
                };
                vec![replica; *weight]
            })
            .flatten()
            .collect();
        let mut rng = thread_rng();
        replica_order.shuffle(&mut rng);
        debug!(logger, "Using postgres host order {:?}", replica_order);



        // init nebula connection configuration
        // let pool_nebula = connection_pool::ConnectionPool_nebula::new_pool(nebula_url.as_str());
        let conf_nebula = pool_config::PoolConfig::new_conf(nebula_url.as_str());


        // Create the store
        let store = StoreInner {
            logger: logger.clone(),
            pool,
            read_only_pools,
            replica_order,
            conn_round_robin_counter: AtomicUsize::new(0),
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            layout_cache: LayoutCache::new(ENV_VARS.store.query_stats_refresh_interval),
            conf_nebula,
        };

        DeploymentStore(Arc::new(store))
    }

    pub(crate) async fn create_deployment(
        &self,
        schema: &Schema,
        deployment: DeploymentCreate,
        site: Arc<Site>,
        graft_base: Option<Arc<Layout>>,
        replace: bool,
    ) -> Result<(), StoreError> {

        let conn = self.get_conn()?;

        let conf_nebula = &self.conf_nebula;
        // get nebula session
        let conn_nebula = Connection_nebula::new_from_conf(conf_nebula).await.unwrap();

        let resp = conn_nebula.authenticate(conf_nebula.username.clone().as_str(), conf_nebula.password.clone().as_str()).await.unwrap();

        let session_id = resp.session_id.unwrap();

        // CREATE SPACE `token_transfer` (partition_num = 1, replica_factor = 1, vid_type = FIXED_STRING(50))

        let mut tables: Vec<Arc<Table>> = Vec::new();

        let res:Result<(), StoreError> = conn.transaction(|| -> Result<_, StoreError> {
            let exists = deployment::exists(&conn, &site)?;

            // Create (or update) the metadata. Update only happens in tests
            if replace || !exists {
                deployment::create_deployment(&conn, &site, deployment, exists, replace)?;
            };

            // Create the schema for the subgraph data
            if !exists {
                let query = format!("create schema {}", &site.namespace);
                conn.batch_execute(&query)?;

                let layout = Layout::create_relational_schema(&conn, site.clone(), schema)?;


                // See if we are grafting and check that the graft is permissible
                if let Some(base) = graft_base {
                    let errors = layout.can_copy_from(&base);
                    if !errors.is_empty() {
                        return Err(StoreError::Unknown(anyhow!(
                            "The subgraph `{}` cannot be used as the graft base \
                             for `{}` because the schemas are incompatible:\n    - {}",
                            &base.catalog.site.namespace,
                            &layout.catalog.site.namespace,
                            errors.join("\n    - ")
                        )));
                    }
                }

                // Create data sources table
                if site.schema_version.private_data_sources() {
                    conn.batch_execute(&DataSourcesTable::new(site.namespace.clone()).as_ddl())?;
                }


                for (_, v) in layout.tables{
                    tables.push(v);
                }
            }
            Ok(())
        });

        for table in tables{
            
            if table.object.to_string() == String::from("Poi$"){
                continue;
            }

            let mut all_queries = String::from("");

            all_queries += conn_nebula.get_create_space_query(table.object.as_str(), 1, 1, true, 50, "").as_str();

            // create tag
            // only create one property (id)
            let space_name = table.object.as_str();
            let col_type = ColType::Tag;
            let mut tag_name = String::from(table.object.as_str()) + "_";
            tag_name += ColType::Tag.to_string().as_str();
            let tag_name = tag_name.as_str();
            let comment = "";
            let mut tags: Vec<Tag> = Vec::new();
            for column in &table.columns{
                if column.name.as_str()!="id"{
                    continue;
                }
                let property_name = column.name.as_str();
                let data_type = column.column_type.to_nebula_type();
                let allow_null = false;
                let defaults = "";
                let comment = "";
                let tag_id = Tag::new(property_name, data_type, allow_null, defaults, comment);
                let tag_value = Tag::new("value", DataType::Int32, allow_null, "", comment);
                tags.push(tag_id);
                tags.push(tag_value);
            }

            all_queries += conn_nebula.get_create_tag_or_edge(space_name, col_type, tag_name, comment, tags).as_str();

            // create edge (custom)
            if table.object.as_str() == "TokenTransfer"{
                let space_name = table.object.as_str();
                let col_type = ColType::Edge;
                let tag_name = "tx";
                let comment = "";
                let mut tags: Vec<Tag> = Vec::new();
                tags.push(Tag::new("from_account", DataType::String, false, "", ""));
                tags.push(Tag::new("to_account", DataType::String, false, "", ""));
                tags.push(Tag::new("transactions", DataType::String, false, "", ""));
                all_queries += conn_nebula.get_create_tag_or_edge(space_name, col_type, tag_name, comment, tags).as_str();
            }

            println!("create table:{:?}",all_queries);
            let _resp = conn_nebula.execute(session_id, all_queries.as_str(), ).await.unwrap();
            println!("create table:{:?}",_resp);
            conn_nebula.signout(session_id).await;
            std::thread::sleep(std::time::Duration::from_millis(5000));
        }
        let res = Ok(());
        res
    }
        

    pub(crate) fn load_deployment(
        &self,
        site: &Site,
    ) -> Result<SubgraphDeploymentEntity, StoreError> {
        let conn = self.get_conn()?;
        detail::deployment_entity(&conn, site)
    }

    // Remove the data and metadata for the deployment `site`. This operation
    // is not reversible
    pub(crate) fn drop_deployment(&self, site: &Site) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| {
            crate::deployment::drop_schema(&conn, &site.namespace)?;
            if !site.schema_version.private_data_sources() {
                crate::dynds::shared::drop(&conn, &site.deployment)?;
            }
            crate::deployment::drop_metadata(&conn, site)
        })
    }

    pub(crate) fn execute_query<T: FromEntityData>(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
        query: EntityQuery,
    ) -> Result<(Vec<T>, Trace), QueryExecutionError> {
        let layout = self.layout(conn, site)?;

        let logger = query.logger.unwrap_or_else(|| self.logger.clone());
        layout.query(
            &logger,
            conn,
            query.collection,
            query.filter,
            query.order,
            query.range,
            query.block,
            query.query_id,
        )
    }

    fn check_interface_entity_uniqueness(
        &self,
        conn: &PgConnection,
        layout: &Layout,
        key: &EntityKey,
    ) -> Result<(), StoreError> {
        // Collect all types that share an interface implementation with this
        // entity type, and make sure there are no conflicting IDs.
        //
        // To understand why this is necessary, suppose that `Dog` and `Cat` are
        // types and both implement an interface `Pet`, and both have instances
        // with `id: "Fred"`. If a type `PetOwner` has a field `pets: [Pet]`
        // then with the value `pets: ["Fred"]`, there's no way to disambiguate
        // if that's Fred the Dog, Fred the Cat or both.
        //
        // This assumes that there are no concurrent writes to a subgraph.
        let schema = self
            .subgraph_info_with_conn(conn, &layout.site)?
            .api
            .get(&Default::default())
            .expect("API schema should be present")
            .clone();
        let types_for_interface = schema.types_for_interface();
        let entity_type = key.entity_type.to_string();
        let types_with_shared_interface = Vec::from_iter(
            schema
                .interfaces_for_type(&key.entity_type)
                .into_iter()
                .flatten()
                .map(|interface| &types_for_interface[&interface.into()])
                .flatten()
                .map(EntityType::from)
                .filter(|type_name| type_name != &key.entity_type),
        );

        if !types_with_shared_interface.is_empty() {
            if let Some(conflicting_entity) =
                layout.conflicting_entity(conn, &key.entity_id, types_with_shared_interface)?
            {
                return Err(StoreError::ConflictingId(
                    entity_type,
                    key.entity_id.to_string(),
                    conflicting_entity,
                ));
            }
        }
        Ok(())
    }

    /// Execute a closure with a connection to the database.
    ///
    /// # API
    ///   The API of using a closure to bound the usage of the connection serves several
    ///   purposes:
    ///
    ///   * Moves blocking database access out of the `Future::poll`. Within
    ///     `Future::poll` (which includes all `async` methods) it is illegal to
    ///     perform a blocking operation. This includes all accesses to the
    ///     database, acquiring of locks, etc. Calling a blocking operation can
    ///     cause problems with `Future` combinators (including but not limited
    ///     to select, timeout, and FuturesUnordered) and problems with
    ///     executors/runtimes. This method moves the database work onto another
    ///     thread in a way which does not block `Future::poll`.
    ///
    ///   * Limit the total number of connections. Because the supplied closure
    ///     takes a reference, we know the scope of the usage of all entity
    ///     connections and can limit their use in a non-blocking way.
    ///
    /// # Cancellation
    ///   The normal pattern for futures in Rust is drop to cancel. Once we
    ///   spawn the database work in a thread though, this expectation no longer
    ///   holds because the spawned task is the independent of this future. So,
    ///   this method provides a cancel token which indicates that the `Future`
    ///   has been dropped. This isn't *quite* as good as drop on cancel,
    ///   because a drop on cancel can do things like cancel http requests that
    ///   are in flight, but checking for cancel periodically is a significant
    ///   improvement.
    ///
    ///   The implementation of the supplied closure should check for cancel
    ///   between every operation that is potentially blocking. This includes
    ///   any method which may interact with the database. The check can be
    ///   conveniently written as `token.check_cancel()?;`. It is low overhead
    ///   to check for cancel, so when in doubt it is better to have too many
    ///   checks than too few.
    ///
    /// # Panics:
    ///   * This task will panic if the supplied closure panics
    ///   * This task will panic if the supplied closure returns Err(Cancelled)
    ///     when the supplied cancel token is not cancelled.
    pub(crate) async fn with_conn<T: Send + 'static>(
        &self,
        f: impl 'static
            + Send
            + FnOnce(
                &PooledConnection<ConnectionManager<PgConnection>>,
                &CancelHandle,
            ) -> Result<T, CancelableError<StoreError>>,
    ) -> Result<T, StoreError> {
        self.pool.with_conn(f).await
    }

    /// Deprecated. Use `with_conn` instead.
    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        self.pool.get()
    }

    /// Panics if `idx` is not a valid index for a read only pool.
    fn read_only_conn(
        &self,
        idx: usize,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        self.read_only_pools[idx].get().map_err(Error::from)
    }

    pub(crate) fn get_replica_conn(
        &self,
        replica: ReplicaId,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        let conn = match replica {
            ReplicaId::Main => self.get_conn()?,
            ReplicaId::ReadOnly(idx) => self.read_only_conn(idx)?,
        };
        Ok(conn)
    }

    pub(crate) async fn query_permit(
        &self,
        replica: ReplicaId,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, StoreError> {
        let pool = match replica {
            ReplicaId::Main => &self.pool,
            ReplicaId::ReadOnly(idx) => &self.read_only_pools[idx],
        };
        pool.query_permit().await
    }

    pub(crate) fn wait_stats(&self, replica: ReplicaId) -> Result<PoolWaitStats, StoreError> {
        match replica {
            ReplicaId::Main => self.pool.wait_stats(),
            ReplicaId::ReadOnly(idx) => self.read_only_pools[idx].wait_stats(),
        }
    }

    /// Return the layout for a deployment. Since constructing a `Layout`
    /// object takes a bit of computation, we cache layout objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Layout objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    pub(crate) fn layout(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
    ) -> Result<Arc<Layout>, StoreError> {
        self.layout_cache.get(&self.logger, conn, site)
    }

    /// Return the layout for a deployment. This might use a database
    /// connection for the lookup and should only be called if the caller
    /// does not have a connection currently. If it does, use `layout`
    pub(crate) fn find_layout(&self, site: Arc<Site>) -> Result<Arc<Layout>, StoreError> {
        if let Some(layout) = self.layout_cache.find(site.as_ref()) {
            return Ok(layout);
        }

        let conn = self.get_conn()?;
        self.layout(&conn, site)
    }

    fn subgraph_info_with_conn(
        &self,
        conn: &PgConnection,
        site: &Site,
    ) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&site.deployment) {
            return Ok(info.clone());
        }

        let (input_schema, description, repository, spec_version) =
            deployment::manifest_info(conn, site)?;

        let graft_block =
            deployment::graft_point(conn, &site.deployment)?.map(|(_, ptr)| ptr.number as i32);

        let debug_fork = deployment::debug_fork(conn, &site.deployment)?;

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        let mut api: HashMap<ApiVersion, Arc<ApiSchema>> = HashMap::new();

        for version in VERSIONS.iter() {
            let api_version = ApiVersion::from_version(version).expect("Invalid API version");
            let mut schema = input_schema.clone();
            schema.document =
                api_schema(&schema.document).map_err(|e| StoreError::Unknown(e.into()))?;
            schema.add_subgraph_id_directives(site.deployment.clone());
            api.insert(api_version, Arc::new(ApiSchema::from_api_schema(schema)?));
        }

        let spec_version = Version::from_str(&spec_version).map_err(anyhow::Error::from)?;
        let poi_version = if spec_version.ge(&SPEC_VERSION_0_0_6) {
            ProofOfIndexingVersion::Fast
        } else {
            ProofOfIndexingVersion::Legacy
        };

        let info = SubgraphInfo {
            input: Arc::new(input_schema),
            api,
            graft_block,
            debug_fork,
            description,
            repository,
            poi_version,
        };

        // Insert the schema into the cache.
        let mut cache = self.subgraph_cache.lock().unwrap();
        cache.insert(site.deployment.clone(), info);

        Ok(cache.get(&site.deployment).unwrap().clone())
    }

    pub(crate) fn subgraph_info(&self, site: &Site) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&site.deployment) {
            return Ok(info.clone());
        }

        let conn = self.get_conn()?;
        self.subgraph_info_with_conn(&conn, site)
    }

    fn block_ptr_with_conn(
        conn: &PgConnection,
        site: Arc<Site>,
    ) -> Result<Option<BlockPtr>, StoreError> {
        deployment::block_ptr(conn, &site.deployment)
    }

    pub(crate) fn deployment_details(
        &self,
        ids: Vec<String>,
    ) -> Result<Vec<DeploymentDetail>, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<_, StoreError> { detail::deployment_details(&conn, ids) })
    }

    pub(crate) fn deployment_statuses(
        &self,
        sites: &[Arc<Site>],
    ) -> Result<Vec<status::Info>, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<Vec<status::Info>, StoreError> {
            detail::deployment_statuses(&conn, sites)
        })
    }

    pub(crate) fn deployment_exists_and_synced(
        &self,
        id: &DeploymentHash,
    ) -> Result<bool, StoreError> {
        let conn = self.get_conn()?;
        deployment::exists_and_synced(&conn, id.as_str())
    }

    pub(crate) fn deployment_synced(&self, id: &DeploymentHash) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::set_synced(&conn, id))
    }

    // Only used for tests
    #[cfg(debug_assertions)]
    pub(crate) fn drop_deployment_schema(
        &self,
        namespace: &crate::primary::Namespace,
    ) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        deployment::drop_schema(&conn, namespace)
    }

    // Only used for tests
    #[cfg(debug_assertions)]
    pub(crate) fn drop_all_metadata(&self) -> Result<(), StoreError> {
        // Delete metadata entities in each shard

        // This needs to touch all the tables in the subgraphs schema
        const QUERY: &str = "
        delete from subgraphs.dynamic_ethereum_contract_data_source;
        delete from subgraphs.subgraph;
        delete from subgraphs.subgraph_deployment;
        delete from subgraphs.subgraph_deployment_assignment;
        delete from subgraphs.subgraph_version;
        delete from subgraphs.subgraph_manifest;
        delete from subgraphs.copy_table_state;
        delete from subgraphs.copy_state;
        delete from active_copies;
    ";

        let conn = self.get_conn()?;
        conn.batch_execute(QUERY)?;
        conn.batch_execute("delete from deployment_schemas;")?;
        Ok(())
    }

    pub(crate) async fn vacuum(&self) -> Result<(), StoreError> {
        self.with_conn(|conn, _| {
            conn.batch_execute("vacuum (analyze) subgraphs.subgraph_deployment")?;
            Ok(())
        })
        .await
    }

    /// Runs the SQL `ANALYZE` command in a table.
    pub(crate) fn analyze(&self, site: Arc<Site>, entity_name: &str) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        self.analyze_with_conn(site, entity_name, &conn)
    }

    /// Runs the SQL `ANALYZE` command in a table, with a shared connection.
    pub(crate) fn analyze_with_conn(
        &self,
        site: Arc<Site>,
        entity_name: &str,
        conn: &PgConnection,
    ) -> Result<(), StoreError> {
        let store = self.clone();
        let entity_name = entity_name.to_owned();
        let layout = store.layout(&conn, site)?;
        let table = resolve_table_name(&layout, &entity_name)?;
        table.analyze(conn)
    }

    /// Creates a new index in the specified Entity table if it doesn't already exist.
    ///
    /// This is a potentially time-consuming operation.
    pub(crate) async fn create_manual_index(
        &self,
        site: Arc<Site>,
        entity_name: &str,
        field_names: Vec<String>,
        index_method: String,
    ) -> Result<(), StoreError> {
        let store = self.clone();
        let entity_name = entity_name.to_owned();
        self.with_conn(move |conn, _| {
            let schema_name = site.namespace.clone();
            let layout = store.layout(conn, site)?;
            let table = resolve_table_name(&layout, &entity_name)?;
            let column_names = resolve_column_names(table, &field_names)?;
            let column_names_sep_by_underscores = column_names.join("_");
            let column_names_sep_by_commas = column_names.join(", ");
            let table_name = &table.name;
            let index_name = format!("manual_{table_name}_{column_names_sep_by_underscores}");
            let sql = format!(
                "create index concurrently if not exists {index_name} \
                 on {schema_name}.{table_name} using {index_method} \
                 ({column_names_sep_by_commas})"
            );
            // This might take a long time.
            conn.execute(&sql)?;
            // check if the index creation was successfull
            let index_is_valid =
                catalog::check_index_is_valid(conn, schema_name.as_str(), &index_name)?;
            if index_is_valid {
                Ok(())
            } else {
                // Index creation falied. We should drop the index before returning.
                let drop_index_sql =
                    format!("drop index concurrently if exists {schema_name}.{index_name}");
                conn.execute(&drop_index_sql)?;
                Err(StoreError::Canceled)
            }
            .map_err(Into::into)
        })
        .await
    }

    /// Returns a list of all existing indexes for the specified Entity table.
    pub(crate) async fn indexes_for_entity(
        &self,
        site: Arc<Site>,
        entity_name: &str,
    ) -> Result<Vec<String>, StoreError> {
        let store = self.clone();
        let entity_name = entity_name.to_owned();
        self.with_conn(move |conn, _| {
            let schema_name = site.namespace.clone();
            let layout = store.layout(conn, site)?;
            let table = resolve_table_name(&layout, &entity_name)?;
            let table_name = &table.name;
            catalog::indexes_for_table(conn, schema_name.as_str(), table_name.as_str())
                .map_err(Into::into)
        })
        .await
    }

    /// Drops an index for a given deployment, concurrently.
    pub(crate) async fn drop_index(
        &self,
        site: Arc<Site>,
        index_name: &str,
    ) -> Result<(), StoreError> {
        let index_name = String::from(index_name);
        self.with_conn(move |conn, _| {
            let schema_name = site.namespace.clone();
            catalog::drop_index(conn, schema_name.as_str(), &index_name).map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn set_account_like(
        &self,
        site: Arc<Site>,
        table: &str,
        is_account_like: bool,
    ) -> Result<(), StoreError> {
        let store = self.clone();
        let table = table.to_string();
        self.with_conn(move |conn, _| {
            let layout = store.layout(conn, site.clone())?;
            let table = resolve_table_name(&layout, &table)?;
            catalog::set_account_like(conn, &site, &table.name, is_account_like).map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn prune(
        self: &Arc<Self>,
        mut reporter: Box<dyn PruneReporter>,
        site: Arc<Site>,
        earliest_block: BlockNumber,
        reorg_threshold: BlockNumber,
        prune_ratio: f64,
    ) -> Result<Box<dyn PruneReporter>, StoreError> {
        let store = self.clone();
        self.with_conn(move |conn, cancel| {
            let layout = store.layout(conn, site.clone())?;
            cancel.check_cancel()?;
            let state = deployment::state(&conn, site.deployment.clone())?;

            if state.latest_block.number <= reorg_threshold {
                return Ok(reporter);
            }

            if state.earliest_block_number > earliest_block {
                return Err(constraint_violation!("earliest block can not move back from {} to {}", state.earliest_block_number, earliest_block).into());
            }

            let final_block = state.latest_block.number - reorg_threshold;
            if final_block <= earliest_block {
                return Err(constraint_violation!("the earliest block {} must be at least {} blocks before the current latest block {}", earliest_block, reorg_threshold, state.latest_block.number).into());
            }

            if let Some((_, graft)) = deployment::graft_point(conn, &site.deployment)? {
                if graft.block_number() >= earliest_block {
                    return Err(constraint_violation!("the earliest block {} must be after the graft point {}", earliest_block, graft.block_number()).into());
                }
            }

            cancel.check_cancel()?;

            conn.transaction(|| {
                deployment::set_earliest_block(conn, site.as_ref(), earliest_block)
            })?;

            cancel.check_cancel()?;

            layout.prune_by_copying(
                &store.logger,
                reporter.as_mut(),
                conn,
                earliest_block,
                final_block,
                prune_ratio,
                cancel,
            )?;
            Ok(reporter)
        })
        .await
    }
}

/// Methods that back the trait `graph::components::Store`, but have small
/// variations in their signatures
impl DeploymentStore {
    pub(crate) async fn block_ptr(&self, site: Arc<Site>) -> Result<Option<BlockPtr>, StoreError> {
        let site = site.cheap_clone();

        self.with_conn(|conn, cancel| {
            cancel.check_cancel()?;

            Self::block_ptr_with_conn(&conn, site).map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn block_cursor(&self, site: Arc<Site>) -> Result<FirehoseCursor, StoreError> {
        let site = site.cheap_clone();

        self.with_conn(|conn, cancel| {
            cancel.check_cancel()?;

            deployment::get_subgraph_firehose_cursor(&conn, site)
                .map(FirehoseCursor::from)
                .map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn supports_proof_of_indexing<'a>(
        &self,
        site: Arc<Site>,
    ) -> Result<bool, StoreError> {
        let store = self.clone();
        self.with_conn(move |conn, cancel| {
            cancel.check_cancel()?;
            let layout = store.layout(conn, site)?;
            Ok(layout.supports_proof_of_indexing())
        })
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn get_proof_of_indexing(
        &self,
        site: Arc<Site>,
        indexer: &Option<Address>,
        block: BlockPtr,
    ) -> Result<Option<[u8; 32]>, StoreError> {
        let indexer = *indexer;
        let site3 = site.cheap_clone();
        let site4 = site.cheap_clone();
        let site5 = site.cheap_clone();
        let store = self.cheap_clone();
        let block2 = block.cheap_clone();

        let entities = self
            .with_conn(move |conn, cancel| {
                cancel.check_cancel()?;

                let layout = store.layout(conn, site4.cheap_clone())?;

                if !layout.supports_proof_of_indexing() {
                    return Ok(None);
                }

                conn.transaction::<_, CancelableError<anyhow::Error>, _>(move || {
                    let latest_block_ptr =
                        match Self::block_ptr_with_conn(conn, site4.cheap_clone())? {
                            Some(inner) => inner,
                            None => return Ok(None),
                        };

                    cancel.check_cancel()?;

                    // FIXME: (Determinism)
                    //
                    // It is vital to ensure that the block hash given in the query
                    // is a parent of the latest block indexed for the subgraph.
                    // Unfortunately the machinery needed to do this is not yet in place.
                    // The best we can do right now is just to make sure that the block number
                    // is high enough.
                    if latest_block_ptr.number < block.number {
                        return Ok(None);
                    }

                    let query = EntityQuery::new(
                        site4.deployment.cheap_clone(),
                        block.number,
                        EntityCollection::All(vec![(
                            POI_OBJECT.cheap_clone(),
                            AttributeNames::All,
                        )]),
                    );
                    let entities = store
                        .execute_query::<Entity>(conn, site4, query)
                        .map(|(entities, _)| entities)
                        .map_err(anyhow::Error::from)?;

                    Ok(Some(entities))
                })
                .map_err(Into::into)
            })
            .await?;

        let entities = if let Some(entities) = entities {
            entities
        } else {
            return Ok(None);
        };

        let mut by_causality_region = entities
            .into_iter()
            .map(|e| {
                let causality_region = e.id()?;
                let digest = match e.get("digest") {
                    Some(Value::Bytes(b)) => Ok(b.to_owned()),
                    other => Err(anyhow::anyhow!(
                        "Entity has non-bytes digest attribute: {:?}",
                        other
                    )),
                }?;

                Ok((causality_region, digest))
            })
            .collect::<Result<HashMap<_, _>, anyhow::Error>>()?;

        let info = self.subgraph_info(&site5).map_err(anyhow::Error::from)?;

        let mut finisher = ProofOfIndexingFinisher::new(
            &block2,
            &site3.deployment,
            &indexer,
            info.poi_version.clone(),
        );
        for (name, region) in by_causality_region.drain() {
            finisher.add_causality_region(&name, &region);
        }

        Ok(Some(finisher.finish()))
    }

    /// Get the entity matching `key` from the deployment `site`. Only
    /// consider entities as of the given `block`
    pub(crate) fn get(
        &self,
        site: Arc<Site>,
        key: &EntityKey,
        block: BlockNumber,
    ) -> Result<Option<Entity>, StoreError> {
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;
        layout.find(&conn, &key.entity_type, &key.entity_id, block)
    }

    /// Retrieve all the entities matching `ids_for_type` from the
    /// deployment `site`. Only consider entities as of the given `block`
    pub(crate) fn get_many(
        &self,
        site: Arc<Site>,
        ids_for_type: &BTreeMap<&EntityType, Vec<&str>>,
        block: BlockNumber,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;

        layout.find_many(&conn, ids_for_type, block)
    }

    pub(crate) fn get_changes(
        &self,
        site: Arc<Site>,
        block: BlockNumber,
    ) -> Result<Vec<EntityOperation>, StoreError> {
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;
        let changes = layout.find_changes(&conn, block)?;

        Ok(changes)
    }

    // Only used by tests
    #[cfg(debug_assertions)]
    pub(crate) fn find(
        &self,
        site: Arc<Site>,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self.get_conn()?;
        self.execute_query(&conn, site, query)
            .map(|(entities, _)| entities)
    }

    fn apply_entity_modifications(
        &self,
        conn: &PgConnection,
        layout: &Layout,
        mods: &[EntityModification],
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
        entities: & mut Vec<EntityWithSpaceName>
    ) -> Result<i32, StoreError> {
        use EntityModification::*;
        let mut count = 0;

        // Group `Insert`s and `Overwrite`s by key, and accumulate `Remove`s.
        for modification in mods.into_iter() {
            match modification {
                Insert { key, data } => {
                    entities.push(EntityWithSpaceName::new(key.entity_type.to_string(), data.clone(), ptr.block_number()));
                }
                Overwrite { key, data } => {
                    entities.push(EntityWithSpaceName::new(key.entity_type.to_string(), data.clone(), ptr.block_number()));
                }
                Remove { key } => {
                    continue;
                }
            }
        }
        Ok(entities.len() as i32)
    }

    fn insert_entities<'a>(
        &'a self,
        entity_type: &'a EntityType,
        data: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        conn: &PgConnection,
        layout: &'a Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let section = stopwatch.start_section("check_interface_entity_uniqueness");
        for (key, _) in data.iter() {
            // WARNING: This will potentially execute 2 queries for each entity key.
            self.check_interface_entity_uniqueness(conn, layout, key)?;
        }
        section.end();

        let _section = stopwatch.start_section("apply_entity_modifications_insert");
        layout.insert(conn, entity_type, data, block_number(ptr), stopwatch)
    }
    fn overwrite_entities<'a>(
        &'a self,
        entity_type: &'a EntityType,
        data: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        conn: &PgConnection,
        layout: &'a Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let section = stopwatch.start_section("check_interface_entity_uniqueness");
        for (key, _) in data.iter() {
            // WARNING: This will potentially execute 2 queries for each entity key.
            self.check_interface_entity_uniqueness(conn, layout, key)?;
        }
        section.end();

        let _section = stopwatch.start_section("apply_entity_modifications_update");
        layout.update(conn, entity_type, data, block_number(ptr), stopwatch)
    }

    fn remove_entities(
        &self,
        entity_type: &EntityType,
        entity_keys: &[&str],
        conn: &PgConnection,
        layout: &Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let _section = stopwatch.start_section("apply_entity_modifications_delete");
        layout
            .delete(conn, entity_type, entity_keys, block_number(ptr), stopwatch)
            .map_err(|_error| anyhow!("Failed to remove entities: {:?}", entity_keys).into())
    }
    pub(crate) fn transact_block_operations(
        &self,
        site: Arc<Site>,
        block_ptr_to: &BlockPtr,
        firehose_cursor: &FirehoseCursor,
        mods: &[EntityModification],
        stopwatch: &StopwatchMetrics,
        data_sources: &[StoredDynamicDataSource],
        deterministic_errors: &[SubgraphError],
        manifest_idx_and_name: &[(u32, String)],
        offchain_to_remove: &[StoredDynamicDataSource],
    ) -> Result<StoreEvent, StoreError> {


        let start_time = Instant::now();


        let conn = {
            let _section = stopwatch.start_section("transact_blocks_get_conn");
            self.get_conn()?
        };

        let conf_nebula = &self.conf_nebula;

        let mut entities: Vec<EntityWithSpaceName> = Vec::new();

        let event = conn.transaction(|| -> Result<_, StoreError> {
            // Emit a store event for the changes we are about to make. We
            // wait with sending it until we have done all our other work
            // so that we do not hold a lock on the notification queue
            // for longer than we have to
            let event: StoreEvent = StoreEvent::from_mods(&site.deployment, mods);

            // Make the changes
            let layout = self.layout(&conn, site.clone())?;

            //  see also: deployment-lock-for-update
            deployment::lock(&conn, &site)?;

            let section = stopwatch.start_section("apply_entity_modifications");
            
            let count = self.apply_entity_modifications(
                &conn,
                layout.as_ref(),
                mods,
                block_ptr_to,
                stopwatch,
                & mut entities,
            )?;
            section.end();
            dynds::insert(
                &conn,
                &site,
                data_sources,
                block_ptr_to,
                manifest_idx_and_name,
            )?;
            dynds::remove_offchain(&conn, &site, offchain_to_remove)?;
            if !deterministic_errors.is_empty() {
                deployment::insert_subgraph_errors(
                    &conn,
                    &site.deployment,
                    deterministic_errors,
                    block_ptr_to.block_number(),
                )?;
            }
            deployment::transact_block(
                &conn,
                &site,
                block_ptr_to,
                firehose_cursor,
                layout.count_query.as_str(),
                count,
            )?;
            Ok(event)
        })?;

        // insert tag
        let insert_tag_queries = EntityWithSpaceName::entity_to_insert_tag_query(&entities);
  
        // insert edge
        let insert_edge_queries = EntityWithSpaceName::entity_to_insert_edge_queries(&entities);


        let start_time2 = Instant::now();

        // run nebula execution
        tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            // get nebula session
            let conn_nebula = Connection_nebula::new_from_conf(conf_nebula).await.unwrap();
            let resp = conn_nebula.authenticate(conf_nebula.username.clone().as_str(), conf_nebula.password.clone().as_str()).await.unwrap();
            let session_id = resp.session_id.unwrap();
            conn_nebula.insert_tags(insert_tag_queries, session_id).await;
            conn_nebula.insert_edges(insert_edge_queries, session_id).await;
            conn_nebula.signout(session_id).await;
        });

        println!("insert_into_nebula:{}", start_time2.elapsed().as_secs_f64());


        println!("transact_block_operations:{}", start_time.elapsed().as_secs_f64());


        Ok(event)
    }

    fn rewind_with_conn(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
        firehose_cursor: &FirehoseCursor,
    ) -> Result<StoreEvent, StoreError> {
        let event = conn.transaction(|| -> Result<_, StoreError> {
            //  see also: deployment-lock-for-update
            deployment::lock(conn, &site)?;

            // Don't revert past a graft point
            let info = self.subgraph_info_with_conn(conn, site.as_ref())?;
            if let Some(graft_block) = info.graft_block {
                if graft_block > block_ptr_to.number {
                    return Err(anyhow!(
                        "Can not revert subgraph `{}` to block {} as it was \
                        grafted at block {} and reverting past a graft point \
                        is not possible",
                        site.deployment.clone(),
                        block_ptr_to.number,
                        graft_block
                    )
                    .into());
                }
            }

            // The revert functions want the number of the first block that we need to get rid of
            let block = block_ptr_to.number + 1;

            deployment::revert_block_ptr(conn, &site.deployment, block_ptr_to, firehose_cursor)?;

            // Revert the data
            let layout = self.layout(conn, site.clone())?;

            let (event, count) = layout.revert_block(conn, block)?;

            // Revert the meta data changes that correspond to this subgraph.
            // Only certain meta data changes need to be reverted, most
            // importantly creation of dynamic data sources. We ensure in the
            // rest of the code that we only record history for those meta data
            // changes that might need to be reverted
            Layout::revert_metadata(&conn, &site, block)?;

            deployment::update_entity_count(
                conn,
                site.as_ref(),
                layout.count_query.as_str(),
                count,
            )?;
            Ok(event)
        })?;

        Ok(event)
    }

    pub(crate) fn rewind(
        &self,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;

        // Unwrap: If we are reverting then the block ptr is not `None`.
        let block_ptr_from = Self::block_ptr_with_conn(&conn, site.cheap_clone())?.unwrap();

        // Sanity check on block numbers
        if block_ptr_from.number <= block_ptr_to.number {
            constraint_violation!(
                "rewind must go backwards, but would go from block {} to block {}",
                block_ptr_from.number,
                block_ptr_to.number
            );
        }

        // When rewinding, we reset the firehose cursor. That way, on resume, Firehose will start
        // from the block_ptr instead (with sanity check to ensure it's resume at the exact block).
        self.rewind_with_conn(&conn, site, block_ptr_to, &FirehoseCursor::None)
    }

    pub(crate) fn revert_block_operations(
        &self,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
        firehose_cursor: &FirehoseCursor,
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;
        // Unwrap: If we are reverting then the block ptr is not `None`.
        let deployment_head = Self::block_ptr_with_conn(&conn, site.cheap_clone())?.unwrap();

        // Confidence check on revert to ensure we go backward only
        if block_ptr_to.number >= deployment_head.number {
            panic!("revert_block_operations must revert only backward, you are trying to revert forward going from subgraph block {} to new block {}", deployment_head, block_ptr_to);
        }

        self.rewind_with_conn(&conn, site, block_ptr_to, firehose_cursor)
    }

    pub(crate) async fn deployment_state_from_id(
        &self,
        id: DeploymentHash,
    ) -> Result<DeploymentState, StoreError> {
        self.with_conn(|conn, _| deployment::state(conn, id).map_err(|e| e.into()))
            .await
    }

    pub(crate) async fn fail_subgraph(
        &self,
        id: DeploymentHash,
        error: SubgraphError,
    ) -> Result<(), StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| deployment::fail(&conn, &id, &error))
                .map_err(Into::into)
        })
        .await?;
        Ok(())
    }

    pub(crate) fn replica_for_query(
        &self,
        for_subscription: bool,
    ) -> Result<ReplicaId, StoreError> {
        use std::sync::atomic::Ordering;

        let replica_id = match for_subscription {
            // Pick a weighted ReplicaId. `replica_order` contains a list of
            // replicas with repetitions according to their weight
            false => {
                let weights_count = self.replica_order.len();
                let index =
                    self.conn_round_robin_counter.fetch_add(1, Ordering::SeqCst) % weights_count;
                *self.replica_order.get(index).unwrap()
            }
            // Subscriptions always go to the main replica.
            true => ReplicaId::Main,
        };

        Ok(replica_id)
    }

    pub(crate) async fn load_dynamic_data_sources(
        &self,
        site: Arc<Site>,
        block: BlockNumber,
        manifest_idx_and_name: Vec<(u32, String)>,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| crate::dynds::load(&conn, &site, block, manifest_idx_and_name))
                .map_err(Into::into)
        })
        .await
    }

    pub(crate) async fn exists_and_synced(&self, id: DeploymentHash) -> Result<bool, StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| deployment::exists_and_synced(conn, &id))
                .map_err(Into::into)
        })
        .await
    }

    pub(crate) fn graft_pending(
        &self,
        id: &DeploymentHash,
    ) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
        let conn = self.get_conn()?;
        deployment::graft_pending(&conn, id)
    }

    /// Bring the subgraph into a state where we can start or resume
    /// indexing.
    ///
    /// If `graft_src` is `Some(..)`, copy data from that subgraph. It
    /// should only be `Some(..)` if we know we still need to copy data. The
    /// code is idempotent so that a copy process that has been interrupted
    /// can be resumed seamlessly, but the code sets the block pointer back
    /// to the graph point, so that calling this needlessly with `Some(..)`
    /// will remove any progress that might have been made since the last
    /// time the deployment was started.
    pub(crate) fn start_subgraph(
        &self,
        logger: &Logger,
        site: Arc<Site>,
        graft_src: Option<(Arc<Layout>, BlockPtr)>,
    ) -> Result<(), StoreError> {
        let dst = self.find_layout(site.cheap_clone())?;

        // Do any cleanup to bring the subgraph into a known good state
        if let Some((src, block)) = graft_src {
            info!(
                logger,
                "Initializing graft by copying data from {} to {}",
                src.catalog.site.namespace,
                dst.catalog.site.namespace
            );

            // Copy subgraph data
            // We allow both not copying tables at all from the source, as well
            // as adding new tables in `self`; we only need to check that tables
            // that actually need to be copied from the source are compatible
            // with the corresponding tables in `self`
            let copy_conn = crate::copy::Connection::new(
                logger,
                self.pool.clone(),
                src.clone(),
                dst.clone(),
                block.clone(),
            )?;
            let status = copy_conn.copy_data()?;
            if status == crate::copy::Status::Cancelled {
                return Err(StoreError::Canceled);
            }

            let conn = self.get_conn()?;
            conn.transaction(|| -> Result<(), StoreError> {
                // Copy shared dynamic data sources and adjust their ID; if
                // the subgraph uses private data sources, that is done by
                // `copy::Connection::copy_data` since it requires access to
                // the source schema which in sharded setups is only
                // available while that function runs
                let start = Instant::now();
                let count = dynds::shared::copy(&conn, &src.site, &dst.site, block.number)?;
                info!(logger, "Copied {} dynamic data sources", count;
                      "time_ms" => start.elapsed().as_millis());

                // Copy errors across
                let start = Instant::now();
                let count = deployment::copy_errors(&conn, &src.site, &dst.site, &block)?;
                info!(logger, "Copied {} existing errors", count;
                      "time_ms" => start.elapsed().as_millis());

                catalog::copy_account_like(&conn, &src.site, &dst.site)?;

                // Rewind the subgraph so that entity versions that are
                // clamped in the future (beyond `block`) become valid for
                // all blocks after `block`. `revert_block` gets rid of
                // everything including the block passed to it. We want to
                // preserve `block` and therefore revert `block+1`
                let start = Instant::now();
                let block_to_revert: BlockNumber = block
                    .number
                    .checked_add(1)
                    .expect("block numbers fit into an i32");
                dst.revert_block(&conn, block_to_revert)?;
                info!(logger, "Rewound subgraph to block {}", block.number;
                      "time_ms" => start.elapsed().as_millis());

                let start = Instant::now();
                deployment::set_entity_count(&conn, &dst.site, &dst.count_query)?;
                info!(logger, "Counted the entities";
                      "time_ms" => start.elapsed().as_millis());

                // Analyze all tables for this deployment
                for entity_name in dst.tables.keys() {
                    self.analyze_with_conn(site.cheap_clone(), entity_name.as_str(), &conn)?;
                }

                // Set the block ptr to the graft point to signal that we successfully
                // performed the graft
                crate::deployment::forward_block_ptr(&conn, &dst.site.deployment, &block)?;
                info!(logger, "Subgraph successfully initialized";
                    "time_ms" => start.elapsed().as_millis());
                Ok(())
            })?;
        }
        // Make sure the block pointer is set. This is important for newly
        // deployed subgraphs so that we respect the 'startBlock' setting
        // the first time the subgraph is started
        let conn = self.get_conn()?;
        conn.transaction(|| crate::deployment::initialize_block_ptr(&conn, &dst.site))?;
        Ok(())
    }

    // If the current block of the deployment is the same as the fatal error,
    // we revert all block operations to it's parent/previous block.
    //
    // This should be called once per subgraph on `graph-node` initialization,
    // before processing the first block on start.
    //
    // It will do nothing (early return) if:
    //
    // - There's no fatal error for the subgraph
    // - The error is NOT deterministic
    pub(crate) fn unfail_deterministic_error(
        &self,
        site: Arc<Site>,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        let conn = &self.get_conn()?;
        let deployment_id = &site.deployment;

        conn.transaction(|| {
            // We'll only unfail subgraphs that had fatal errors
            let subgraph_error = match ErrorDetail::fatal(conn, deployment_id)? {
                Some(fatal_error) => fatal_error,
                // If the subgraph is not failed then there is nothing to do.
                None => return Ok(UnfailOutcome::Noop),
            };

            // Confidence check
            if !subgraph_error.deterministic {
                return Ok(UnfailOutcome::Noop); // Nothing to do
            }

            use deployment::SubgraphHealth::*;
            // Decide status based on if there are any errors for the previous/parent block
            let prev_health =
                if deployment::has_deterministic_errors(conn, deployment_id, parent_ptr.number)? {
                    Unhealthy
                } else {
                    Healthy
                };

            match &subgraph_error.block_hash {
                // The error happened for the current deployment head.
                // We should revert everything (deployment head, subgraph errors, etc)
                // to the previous/parent hash/block.
                Some(bytes) if bytes == current_ptr.hash.as_slice() => {
                    info!(
                        self.logger,
                        "Reverting errored block";
                        "subgraph_id" => deployment_id,
                        "from_block_number" => format!("{}", current_ptr.number),
                        "from_block_hash" => format!("{}", current_ptr.hash),
                        "to_block_number" => format!("{}", parent_ptr.number),
                        "to_block_hash" => format!("{}", parent_ptr.hash),
                    );

                    // We ignore the StoreEvent that's being returned, we'll not use it.
                    //
                    // We reset the firehose cursor. That way, on resume, Firehose will start from
                    // the block_ptr instead (with sanity checks to ensure it's resuming at the
                    // correct block).
                    let _ = self.revert_block_operations(site.clone(), parent_ptr.clone(), &FirehoseCursor::None)?;

                    // Unfail the deployment.
                    deployment::update_deployment_status(conn, deployment_id, prev_health, None)?;

                    Ok(UnfailOutcome::Unfailed)
                }
                // Found error, but not for deployment head, we don't need to
                // revert the block operations.
                //
                // If you find this warning in the logs, something is wrong, this
                // shoudn't happen.
                Some(hash_bytes) => {
                    warn!(self.logger, "Subgraph error does not have same block hash as deployment head";
                        "subgraph_id" => deployment_id,
                        "error_id" => &subgraph_error.id,
                        "error_block_hash" => format!("0x{}", hex::encode(&hash_bytes)),
                        "deployment_head" => format!("{}", current_ptr.hash),
                    );

                    Ok(UnfailOutcome::Noop)
                }
                // Same as branch above, if you find this warning in the logs,
                // something is wrong, this shouldn't happen.
                None => {
                    warn!(self.logger, "Subgraph error should have block hash";
                        "subgraph_id" => deployment_id,
                        "error_id" => &subgraph_error.id,
                    );

                    Ok(UnfailOutcome::Noop)
                }
            }
        })
    }

    // If a non-deterministic error happens and the deployment head advances,
    // we should unfail the subgraph (status: Healthy, failed: false) and delete
    // the error itself.
    //
    // This should be called after successfully processing a block for a subgraph.
    //
    // It will do nothing (early return) if:
    //
    // - There's no fatal error for the subgraph
    // - The error IS deterministic
    pub(crate) fn unfail_non_deterministic_error(
        &self,
        site: Arc<Site>,
        current_ptr: &BlockPtr,
    ) -> Result<UnfailOutcome, StoreError> {
        let conn = &self.get_conn()?;
        let deployment_id = &site.deployment;

        conn.transaction(|| {
            // We'll only unfail subgraphs that had fatal errors
            let subgraph_error = match ErrorDetail::fatal(conn, deployment_id)? {
                Some(fatal_error) => fatal_error,
                // If the subgraph is not failed then there is nothing to do.
                None => return Ok(UnfailOutcome::Noop),
            };

            // Confidence check
            if subgraph_error.deterministic {
                return Ok(UnfailOutcome::Noop); // Nothing to do
            }

            match subgraph_error.block_range {
                // Deployment head (current_ptr) advanced more than the error.
                // That means it's healthy, and the non-deterministic error got
                // solved (didn't happen on another try).
                (Bound::Included(error_block_number), _)
                    if current_ptr.number >= error_block_number =>
                    {
                        info!(
                            self.logger,
                            "Unfailing the deployment status";
                            "subgraph_id" => deployment_id,
                        );

                        // Unfail the deployment.
                        deployment::update_deployment_status(
                            conn,
                            deployment_id,
                            deployment::SubgraphHealth::Healthy,
                            None,
                        )?;

                        // Delete the fatal error.
                        deployment::delete_error(conn, &subgraph_error.id)?;

                        Ok(UnfailOutcome::Unfailed)
                    }
                // NOOP, the deployment head is still before where non-deterministic error happened.
                block_range => {
                    info!(
                        self.logger,
                        "Subgraph error is still ahead of deployment head, nothing to unfail";
                        "subgraph_id" => deployment_id,
                        "block_number" => format!("{}", current_ptr.number),
                        "block_hash" => format!("{}", current_ptr.hash),
                        "error_block_range" => format!("{:?}", block_range),
                        "error_block_hash" => subgraph_error.block_hash.as_ref().map(|hash| format!("0x{}", hex::encode(hash))),
                    );

                    Ok(UnfailOutcome::Noop)
                }
            }
        })
    }

    #[cfg(debug_assertions)]
    pub fn error_count(&self, id: &DeploymentHash) -> Result<usize, StoreError> {
        let conn = self.get_conn()?;
        deployment::error_count(&conn, id)
    }

    pub(crate) async fn mirror_primary_tables(&self, logger: &Logger) {
        self.pool.mirror_primary_tables().await.unwrap_or_else(|e| {
            warn!(logger, "Mirroring primary tables failed. We will try again in a few minutes";
                  "error" => e.to_string(),
                  "shard" => self.pool.shard.as_str())
        });
    }

    pub(crate) async fn health(
        &self,
        site: &Site,
    ) -> Result<deployment::SubgraphHealth, StoreError> {
        let id = site.id.clone();
        self.with_conn(move |conn, _| deployment::health(conn, id).map_err(Into::into))
            .await
    }
}

/// Tries to fetch a [`Table`] either by its Entity name or its SQL name.
///
/// Since we allow our input to be either camel-case or snake-case, we must retry the
/// search using the latter if the search for the former fails.
fn resolve_table_name<'a>(layout: &'a Layout, name: &'_ str) -> Result<&'a Table, StoreError> {
    layout
        .table_for_entity(&EntityType::new(name.to_owned()))
        .map(Deref::deref)
        .or_else(|_error| {
            let sql_name = SqlName::from(name);
            layout
                .table(&sql_name)
                .ok_or_else(|| StoreError::UnknownTable(name.to_owned()))
        })
}

// Resolves column names.
//
// Since we allow our input to be either camel-case or snake-case, we must retry the
// search using the latter if the search for the former fails.
fn resolve_column_names<'a, T: AsRef<str>>(
    table: &'a Table,
    field_names: &[T],
) -> Result<Vec<&'a str>, StoreError> {
    field_names
        .iter()
        .map(|f| {
            table
                .column_for_field(f.as_ref())
                .or_else(|_error| {
                    let sql_name = SqlName::from(f.as_ref());
                    table
                        .column(&sql_name)
                        .ok_or_else(|| StoreError::UnknownField(f.as_ref().to_string()))
                })
                .map(|column| column.name.as_str())
        })
        .collect()
}

pub struct EntityWithSpaceName{
    pub space_name: String,
    pub entity: Entity,
    pub block_number: BlockNumber,
}
impl EntityWithSpaceName{
    pub fn new(
        space_name: String,
        entity: Entity, 
        block_number: BlockNumber,
    ) -> Self{
        EntityWithSpaceName{
            space_name,
            entity,
            block_number
        }
    }
    pub fn get_random_string(len: usize) -> String{
        let mut rng = rand::thread_rng();
        let mut test: Vec<u8> = vec![0; len];
        for i in &mut test{
            let dig_or_char: u8 = rng.gen_range(0..=1);
            match dig_or_char{
                0 => *i = rng.gen_range(48..=57),
                _ => *i = rng.gen_range(97..=122),
            }
        }
        String::from_utf8(test).unwrap()
    }

    pub fn entity_to_insert_tag_query(entities: &Vec<EntityWithSpaceName>) -> Vec<InsertTagQuery>{
        let mut insert_tag_queries: Vec<InsertTagQuery> = Vec::new();
        for entity in entities{
            if entity.space_name == String::from("Poi$"){
                continue;
            }
            let mut properties_from: HashMap<String, String> = HashMap::new();
            let mut properties_to: HashMap<String, String> = HashMap::new();
            let mut value = String::from("");
            if entity.entity.0.get("operation").clone().unwrap().to_string()==String::from("1"){
                for (k,v) in &entity.entity.0{
                    // println!("------------kv-------------");
                    // println!("{}",k.to_string());
                    // println!("{}",v.to_string());
                    // (id) VALUES "from_account"(from_account)
                    if k.clone()==String::from("from_account"){
                        properties_from.insert("from_account".to_string(), v.clone().to_string());
                    }
                    else if k.clone()==String::from("to_account"){
                        properties_to.insert("to_account".to_string(), v.clone().to_string());
                    }
                    else if k.clone()==String::from("value"){
                        value = v.to_string();
                    } 
                }
                let space_name = entity.space_name.clone();
                let tag_name = space_name.clone() + "_tag";
                let vid_from = properties_from.get("from_account").unwrap().clone().replace("\"", "");
                let vid_to = properties_to.get("to_account").unwrap().clone().replace("\"", "");
                let insert_tag_query_from = InsertTagQuery::new(space_name.clone(), tag_name.clone(), properties_from, vid_from, true, value.parse::<i32>().unwrap());
                let insert_tag_query_to = InsertTagQuery::new(space_name, tag_name, properties_to, vid_to,true, value.parse::<i32>().unwrap());
                insert_tag_queries.push(insert_tag_query_from);
                insert_tag_queries.push(insert_tag_query_to);
            }
        }
        insert_tag_queries
    }

    pub fn entity_to_insert_edge_queries(entities: &Vec<EntityWithSpaceName>) -> Vec<InsertEdgeQueryWithRank>{

        pub fn get_random_string(len: usize) -> String{
            let mut rng = rand::thread_rng();
            let mut test: Vec<u8> = vec![0; len];
            for i in &mut test{
                let dig_or_char: u8 = rng.gen_range(0..=1);
                match dig_or_char{
                    0 => *i = rng.gen_range(48..=57),
                    _ => *i = rng.gen_range(97..=122),
                }
            }
            String::from_utf8(test).unwrap()
        }

        let mut insert_edge_queries: Vec<InsertEdgeQueryWithRank> = Vec::new();
        for entity in entities{
            if entity.space_name == String::from("Poi$"){
                continue;
            }
            let mut properties: HashMap<String, String> = HashMap::new();
            if entity.entity.0.get("operation").unwrap().to_string()==String::from("1"){
                for (k,v) in &entity.entity.0{
                    if k.clone()==String::from("id") || k.clone()==String::from("operation"){
                        continue;
                    }else if k.clone()==String::from("value"){
                        let mut transactions = String::from("(");
                        transactions += &v.to_string();
                        transactions += ",";
                        transactions += &get_random_string(20);
                        transactions += ",";
                        transactions += "+";
                        transactions += ")";
                        properties.insert("transactions".to_string(),transactions);
                        properties.insert(k.clone(), v.to_string());
                    }else{
                        properties.insert(k.clone(), v.to_string());
                    }
                }
                let space_name = entity.space_name.clone();
                let to_vertex = properties.get("from_account").unwrap().clone().replace("\"", "");
                let from_vertex = properties.get("to_account").unwrap().clone().replace("\"", "");
                //value_map
                let value = properties.get("value").unwrap().clone().replace("\"", "");
                
                
                let insert_edge_query = InsertEdgeQueryWithRank::new(
                    space_name,
                    "tx".to_string(),
                    properties,
                    from_vertex,
                    to_vertex,
                    entity.block_number,
                    value.parse::<i32>().unwrap(),
                );
                insert_edge_queries.push(insert_edge_query);
            }
        }
        insert_edge_queries
    }
}
