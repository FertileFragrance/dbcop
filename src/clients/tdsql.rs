use std::fmt::format;
use crate::db::cluster::{Cluster, ClusterNode, Node};
use crate::db::history::{HistParams, Transaction};

use log::info;
use mysql::{Conn, TxOpts, prelude::*};
use std::thread;

#[derive(Debug)]
pub struct TDSQLNode {
    addr: String,
}

fn get_mysql_conn_string(username: &str, password: &str, node: &Node) -> String {
    format!("mysql://{}:{}@{}", username, password, node.addr)
}

impl From<Node> for TDSQLNode {
    fn from(node: Node) -> Self {
        TDSQLNode {
            addr: get_mysql_conn_string("test", "test123", &node),
        }
    }
}

impl ClusterNode for TDSQLNode {
    fn exec_session(&self, hist: &mut Vec<Transaction>) {
        let txn_number = hist.len();
        let mut counter = 0;
        let mut log_threshold = 0.1;
        info!("Start executing {} transactions in {:?}", txn_number, thread::current().name().unwrap());

        let mut conn = Conn::new(self.addr.as_str()).unwrap();
        let txnopts = TxOpts::default()
            .set_isolation_level(Some(mysql::IsolationLevel::RepeatableRead));
            // .set_access_mode(Some(mysql::AccessMode::ReadWrite))
            // .set_with_consistent_snapshot(true);
        let read_stmt = conn.prep("SELECT * FROM dbcop.variables WHERE var=?").unwrap();
        let write_stmt = conn.prep("UPDATE dbcop.variables SET val=? WHERE var=?").unwrap();

        for transaction in hist.iter_mut() {
            while !transaction.success {
                let mut sqltxn = conn.start_transaction(txnopts).unwrap();
                transaction.success = true;

                for event in transaction.events.iter_mut() {
                    if event.write {
                        if let Err(error) = sqltxn.exec_drop(&write_stmt, (event.value, event.variable)) {
                            println!("{error}");
                            transaction.success = false;
                            break;
                        }
                        event.success = true;
                    } else {
                        match sqltxn.exec_first(&read_stmt, (event.variable,)) {
                            Ok(result) => {
                                let mut row: mysql::Row = result.unwrap();
                                event.value = row.take("val").unwrap();
                                event.success = true;
                            },
                            Err(error) => {
                                println!("{error}");
                                transaction.success = false;
                                break;
                            }
                        }
                    }
                }

                transaction.success = transaction.success && sqltxn.commit().is_ok();
            }
            counter += 1;
            let progress = counter as f64 / txn_number as f64;
            if progress >= log_threshold {
                info!("[{:?}] Finish {:.0}% transactions", thread::current().name().unwrap(), progress * 100.0);
                log_threshold += 0.1; 
            }
        }
    }
}

#[derive(Debug)]
pub struct TDSQLCluster{
    nodes: Vec<Node>,
}

impl TDSQLCluster {
    pub fn new(ips: &Vec<&str>) -> Self {
        //  info!("Creating new TDSQLCluster with ip:port list: {:?}", ips);
        let nodes = TDSQLCluster::node_vec(ips);
        TDSQLCluster{ nodes }
    }

    fn create_table(&self) -> bool {
        info!("Create table for testing");
        let addr = self.get_mysql_addr(0).unwrap();
        let mut conn = mysql::Conn::new(addr.as_str()).unwrap();

        conn.exec_drop("CREATE DATABASE IF NOT EXISTS dbcop", ()).unwrap();
        conn.exec_drop("DROP TABLE IF EXISTS dbcop.variables", ()).unwrap();
        conn.exec_drop(
            "CREATE TABLE IF NOT EXISTS dbcop.variables (var BIGINT(64) UNSIGNED NOT NULL PRIMARY KEY, val BIGINT(64) UNSIGNED NOT NULL)", ()
        ).unwrap();
        true
    }

    fn create_variables(&self, n_variable: usize) {
        info!("Insert initial values into table");
        let addr = self.get_mysql_addr(0).unwrap();
        let mut conn = mysql::Conn::new(addr.as_str()).unwrap();

        conn.exec_batch(
            "INSERT INTO dbcop.variables (var, val) values (?, 0)",
            (0..n_variable).map(|v| (v,))
        ).unwrap();
    }

    fn drop_database(&self) {
        info!("Drop table for testing");
        let addr = self.get_mysql_addr(0).unwrap();
        let mut conn = mysql::Conn::new(addr.as_str()).unwrap();

        conn.exec_drop("DROP DATABASE dbcop", ()).unwrap();
    }

    fn get_mysql_addr(&self, i: usize) -> Option<String> {
        match self.nodes.get(i) {
            Some(ref node) => Some(get_mysql_conn_string("test", "test123", &node)),
            None => None,
        }
    }
}

impl Cluster<TDSQLNode> for TDSQLCluster {
    fn n_node(&self) -> usize {
        self.nodes.len()
    }
    fn setup(&self) -> bool {
        self.create_table()
    }
    fn get_node(&self, id: usize) -> Node {
        self.nodes[id].clone()
    }
    fn get_cluster_node(&self, id: usize) -> TDSQLNode {
        From::from(self.get_node(id))
    }
    fn setup_test(&mut self, p: &HistParams) {
        self.create_variables(p.get_n_variable());
    }
    fn cleanup(&self) {
        self.drop_database();
    }
    fn info(&self) -> String {
        "TDSQL".to_string()
    }
}
