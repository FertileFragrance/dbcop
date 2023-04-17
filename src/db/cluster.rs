use super::history::{HistParams, History, Session};
// use verifier::Verifier;

// use std::collections::HashMap;

use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use std::net::SocketAddr;

// use rand::distributions::{Distribution, Uniform};
// use rand::Rng;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
// use std::convert::From;

// use serde_yaml;
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct Node {
    pub addr: SocketAddr,
    pub id: usize,
}

pub trait ClusterNode {
    fn exec_session(&self, hist: &mut Session);
}

pub trait Cluster<N>
where
    N: 'static + Send + ClusterNode,
{
    fn n_node(&self) -> usize;
    fn setup(&self) -> bool;
    fn setup_test(&mut self, p: &HistParams);
    fn get_node(&self, id: usize) -> Node;
    fn get_cluster_node(&self, id: usize) -> N;
    fn cleanup(&self);
    fn info(&self) -> String;

    fn node_vec(ips: &[&str]) -> Vec<Node> where Self: Sized {
        ips.iter()
            .enumerate()
            .map(|(i, ip)| Node {
                addr: ip.parse().unwrap(),
                id: i + 1,
            })
            .collect()
    }

    fn execute_all(&mut self, r_dir: &Path, o_dir: &Path, millisec: u64) -> Option<usize> {
        info!("Reading all histories from {:?}", r_dir);
        let histories: Vec<History> = fs::read_dir(r_dir)
            .unwrap()
            .filter_map(|entry_res| match entry_res {
                Ok(ref entry) if !&entry.path().is_dir() => {
                    let file = File::open(entry.path()).unwrap();
                    let buf_reader = BufReader::new(file);
                    Some(bincode::deserialize_from(buf_reader).unwrap())
                }
                _ => None,
            })
            .collect();

        info!("Successfully reading {} histories", histories.len());
    
        let mut executed_count = 0;
    
        for history in histories.iter() {
            let curr_dir = o_dir.join(format!("hist-{:05}", history.get_id()));
            info!("Create output directory of {:?}", curr_dir);
            if fs::create_dir(&curr_dir).is_ok() {
                info!("Created successfully! Ready to execute this history");
                self.execute(history, &curr_dir);
                executed_count += 1;
                sleep(Duration::from_millis(millisec));
            } else {
                warn!("The output directory is not empty, so skipping");
            }
        }

        info!("Successfully Executed {} histories", executed_count);

        None
    }

    fn execute(&mut self, hist: &History, dir: &Path) -> Option<usize> {
        info!("Step-1: setup");
        self.setup();

        info!("Step-2: setup-test");
        self.setup_test(hist.get_params());

        let mut exec = hist.get_cloned_data();

        let start_time = chrono::Local::now();

        info!("Step-3: exec-history");
        self.exec_history(&mut exec);

        let end_time = chrono::Local::now();

        info!("Step-4: clean up");
        self.cleanup();

        info!("Step-5: write out");
        let exec_hist = History::new(
            hist.get_cloned_params(),
            self.info(),
            start_time,
            end_time,
            exec,
        );

        let file = File::create(dir.join("history.bincode")).unwrap();
        let buf_writer = BufWriter::new(file);
        bincode::serialize_into(buf_writer, &exec_hist).expect("dumping to bincode went wrong");

        None
    }

    fn exec_history(&self, hist: &mut Vec<Session>) {
        let mut threads = (0..self.n_node())
            .cycle()
            .zip(hist.drain(..))
            .enumerate()
            .map(|(index, (node_id, mut single_hist))| {
                let cluster_node = self.get_cluster_node(node_id);
                let session_name = format!("session-{}", index);
                thread::Builder::new()
                    .name(session_name)
                    .spawn(move || {
                        cluster_node.exec_session(&mut single_hist);
                        single_hist
                    }).unwrap()
            })
            .collect::<Vec<_>>();
        hist.extend(threads.drain(..).map(|t| t.join().unwrap()));
    }
}
