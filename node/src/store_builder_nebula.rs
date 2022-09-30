use std::iter::FromIterator;
use std::{collections::HashMap, sync::Arc};

use futures::future::join_all;
use graph::blockchain::ChainIdentifier;
use graph::prelude::{o, MetricsRegistry, NodeId};
use graph::url::Url;
use graph::{
    prelude::{info, CheapClone, Logger},
    util::security::SafeDisplay,
};
use graph_store_postgres::connection_pool::{ConnectionPool, ForeignServer, PoolName};
use graph_store_postgres::{
    BlockStore as DieselBlockStore, ChainHeadUpdateListener as PostgresChainHeadUpdateListener,
    NotificationSender, Shard as ShardName, Store as DieselStore, SubgraphStore,
    SubscriptionManager, PRIMARY_SHARD,
};
use nebula_rust::graph_client::{pool_config, connection_pool, session};

use crate::config::{Config, Shard};
use tokio::*;


pub struct StoreBuilder_nebula {
    pub logger: Logger,
    pub connection_pool: connection_pool::ConnectionPool_nebula,
}

impl StoreBuilder_nebula{
    pub async fn new(
        logger: &Logger,
        config: &Config,
    ) -> Self{
        // 
        let primary_shard = config.primary_store_nebula().clone();
        // root:root@localhost:9669/basketballplayer
        let conn_info = primary_shard.connection.as_str();
        let v:Vec<&str> = conn_info.split('@').collect();
        let v2:Vec<&str> = v[1].split('/').collect();
        let add = String::from(v2[0]);
        let v3:Vec<&str> = v[0].split(':').collect();
        let username = String::from(v3[0]);
        let password = String::from(v3[1]);

        let mut conf = pool_config::PoolConfig::new();
        conf.min_connection_pool_size(2)
            .max_connection_pool_size(10)
            .address(add)
            .set_username(username)
            .set_password(password);
    
        // println!("===============PoolConfig============");
        // println!("{:?}", conf);

        let pool = connection_pool::ConnectionPool_nebula::new(&conf);
        pool.create_new_connection().await;
        info!(logger, "Successfully connecting to NebulaGraph!");
        // let session = pool.get_session("root", "nebula", true).await.unwrap();

        Self {
            logger: logger.cheap_clone(),
            connection_pool: pool
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_connection() {
        use nebula_rust::graph_client;
        let mut conf = graph_client::pool_config::PoolConfig::new();
        conf.min_connection_pool_size(2)
            .max_connection_pool_size(10)
            .address("localhost:9669".to_string());
    
        let pool = graph_client::connection_pool::ConnectionPool_nebula::new(&conf);
        pool.create_new_connection().await;
        let session = pool.get_session("root", "nebula", true).await.unwrap();
    
    }
}