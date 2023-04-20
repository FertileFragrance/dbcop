mod clients;
mod db;

use clap::{Parser, Subcommand, ValueEnum};
use clients::{DynCluster, DynNode, PostgresCluster, PostgresSERCluster, DGraphCluster, GaleraCluster, MySQLCluster, TDSQLCluster};
use db::cluster::Cluster;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use rand::distributions::{Bernoulli, Distribution, Uniform};

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use std::fs;

use db::distribution::{MyDistribution, MyDistributionTrait};
use db::history::{generate_mult_histories, HistoryParams};
use db::history::History;

use zipf::ZipfDistribution;

use env_logger::{Builder, Target};
use log::{info};

struct HotspotDistribution {
    hot_probability: Bernoulli,
    hot_key: Uniform<usize>,
    non_hot_key: Uniform<usize>,
}

impl Distribution<usize> for HotspotDistribution {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> usize {
        if self.hot_probability.sample(rng) {
            self.hot_key.sample(rng)
        } else {
            self.non_hot_key.sample(rng)
        }
    }
}

impl HotspotDistribution {
    fn new(n_variables: usize) -> HotspotDistribution {
        let hot_key_max = n_variables / 5;
        HotspotDistribution {
            hot_probability: Bernoulli::new(0.8).unwrap(),
            hot_key: Uniform::new(0, hot_key_max),
            non_hot_key: Uniform::new(hot_key_max, n_variables),
        }
    }
}

#[derive(Parser)]
#[clap(name = "dbcop", author = "Ranadeep", about = "Generates histories or verifies executed histories")]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[clap(about = "Generate histories")]
    Generate {
        #[clap(short = 'd', long = "gen_dir", help = "Directory to generate histories")]
        g_directory: PathBuf,

        #[clap(long = "nhist", default_value_t = 1, help = "Number of histories to generate")]
        n_history: usize,

        #[clap(long = "nnode", short = 'n', help = "Number of nodes per history")]
        n_node: usize,

        #[clap(long = "nvar", short = 'v', help = "Number of variables per history")]
        n_variable: usize,

        #[clap(long = "ntxn", short = 't', help = "Number of transactions per history")]
        n_transaction: usize,

        #[clap(long = "nevt", short = 'e', help = "Number of events per transactions")]
        n_event: usize,

        #[clap(long = "readp", default_value_t = 0.5, help = "Probability for an event to be a read")]
        read_probability: f64,

        #[clap(value_enum, long = "key_distrib", default_value_t = KeyDistribution::Uniform, help = "Key access distribution")]
        key_distribution: KeyDistribution,

        #[clap(long, default_value_t = 0.0, help = "Proportion of long transactions")]
        longtxn_proportion: f64,

        #[clap(long, default_value_t = 10.0, help = "Times of size of long transactions compared to regular txns")]
        longtxn_size: f64,

        #[clap(long, action, help = "Randomize size of transactions")]
        random_txn_size: bool,
    },
    #[clap(about = "Print executed history")]
    Print {
        #[clap(short = 'd', help = "Directory containing executed history")]
        directory: PathBuf,
    },
    #[clap(about = "Convert bincode testcase into json one OR json history into bincode one")]
    Convert {
        #[clap(short = 'd', help = "Directory containing testcases or history")]
        directory: PathBuf,

        #[clap(value_enum, long = "from", help = "Source format")]
        format: FileFormat,
    },
    #[clap(about = "Execute operations on db")]
    Run {
        #[clap(long = "dir", short = 'd')]
        hist_dir: PathBuf,

        #[clap(long = "out", short = 'o')]
        hist_out: PathBuf,

        #[clap(value_name = "ip:port", help = "DB addr")]
        addrs: Vec<String>,

        #[clap(long = "db", value_enum)]
        database: Database,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum KeyDistribution {
    Uniform, Zipf, Hotspot
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Database {
    Postgres, PostgresSer, Dgraph, Galera, Mysql, Tdsql
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum FileFormat {
    Bincode, Json
}

fn main() {
    Builder::new()
        .filter_level(log::LevelFilter::Info)
        .target(Target::Stdout)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Print { directory } => {
            let v_path = directory.join("history.bincode");
            let file = File::open(v_path).unwrap();
            let buf_reader = BufReader::new(file);
            let hist: History = bincode::deserialize_from(buf_reader).unwrap();

            println!("{:?}", hist);
        }
        Commands::Convert { directory, format  } => {
            WalkDir::new(&directory).into_iter().for_each(
                |entry| {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_file() {
                        let file_name = path.file_name().unwrap().to_str().unwrap();
                        match format {
                            FileFormat::Bincode => {
                                if file_name.starts_with("hist") && file_name.ends_with(".bincode") {
                                    // convert bincode to json
                                    let input_path = &directory.join(file_name);
                                    let file = File::open(input_path).unwrap();
                                    let buf_reader = BufReader::new(file);
                                    let hist: History = bincode::deserialize_from(buf_reader).unwrap();
                                    let output_path: &PathBuf = &directory.join(file_name.replace("bincode", "json"));
                                    fs::write(
                                        output_path,
                                        serde_json::to_string_pretty(&hist).unwrap()
                                    ).unwrap();
                                    info!("Converting {:?} to {:?}", file_name, output_path.as_path().file_name().unwrap());
                                }
                            },
                            FileFormat::Json => {
                                if file_name.starts_with("hist") && file_name.ends_with(".json") {
                                    // convert json to bincode
                                    let input_path = &directory.join(file_name);
                                    let file = File::open(input_path).unwrap();
                                    let buf_reader = BufReader::new(file);
                                    let hist: History = serde_json::from_reader(buf_reader).unwrap();
                                    let output_path: &PathBuf = &directory.join(file_name.replace(".json", "-back.bincode"));
                                    fs::write(
                                        output_path, 
                                        bincode::serialize(&hist).unwrap())
                                    .unwrap();
                                    info!("Converting {:?} to {:?}", file_name, output_path.as_path().file_name().unwrap());
                                }
                            },
                        }
                    } 
                }
            );
        }
        Commands::Generate { g_directory, n_history, n_node, n_variable, n_transaction, n_event, read_probability, key_distribution, longtxn_proportion, longtxn_size, random_txn_size } => {
            if !g_directory.is_dir() {
                fs::create_dir_all(&g_directory).expect("failed to create directory");
            }

            let distribution: Box<dyn MyDistributionTrait> =
                match key_distribution {
                    KeyDistribution::Uniform => Box::new(MyDistribution::new(Uniform::new(0, n_variable))),
                    KeyDistribution::Zipf => Box::new(MyDistribution::new(
                        ZipfDistribution::new(n_variable, 0.5)
                            .unwrap()
                            .map(|x| x - 1),
                    )),
                    KeyDistribution::Hotspot => {
                        Box::new(MyDistribution::new(HotspotDistribution::new(n_variable)))
                    }
                };

            let mut histories = generate_mult_histories(
                HistoryParams {
                    n_hist: n_history,
                    n_node,
                    n_variable,
                    n_transaction,
                    n_event,
                    read_probability,
                    key_distribution: distribution.as_ref(),
                    longtxn_proportion,
                    longtxn_size,
                    random_txn_size,
                }
            );

            for hist in histories.drain(..) {
                let file = File::create(g_directory.join(format!("hist-{:05}.bincode", hist.get_id())))
                    .expect("couldn't create bincode file");
                let buf_writer = BufWriter::new(file);
                bincode::serialize_into(buf_writer, &hist)
                    .expect("dumping history to bincode file went wrong");
            }
        }
        Commands::Run { hist_dir, hist_out, addrs, database } => {
            fs::create_dir_all(&hist_out).expect("couldn't create directory");
            let addrs_str = addrs.iter().map(|addr| addr.as_str()).collect();

            let mut cluster: Box<dyn Cluster<DynNode>> = match database {
                Database::Postgres => Box::new(DynCluster::new(PostgresCluster::new(&addrs_str))),
                Database::PostgresSer => Box::new(DynCluster::new(PostgresSERCluster::new(&addrs_str))),
                Database::Dgraph => Box::new(DynCluster::new(DGraphCluster::new(&addrs_str))),
                Database::Galera => Box::new(DynCluster::new(GaleraCluster::new(&addrs_str))),
                Database::Mysql => Box::new(DynCluster::new(MySQLCluster::new(&addrs_str))),
                Database::Tdsql => Box::new(DynCluster::new(TDSQLCluster::new(&addrs_str))),
            };

            cluster.execute_all(&hist_dir.as_path(), &hist_out.as_path(), 100);
        }
    }
}
