use crate::graph_client::connection::Connection;
use crate::graph_client::pool_config::PoolConfig;
use crate::graph_client::session::Session;
use crate::graph_client::connection_pool::ConnectionPool;

pub struct ConnectionPool_nebula{
        /// The connections
    /// The interior mutable to enable could get multiple sessions in one scope
    /// 内部可变启用可以在一个范围内获得多个会话
    conns: std::sync::Mutex<std::cell::RefCell<std::collections::LinkedList<Connection>>>,
    /// It should be immutable
    /// 它应该是不可变的
    config: PoolConfig,
}