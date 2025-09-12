use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, Writer};
use std::collections::{HashMap, HashSet};
use std::error::Error;

mod block_data_structure;
mod bmssp;
mod dijkstra;
mod pq_block_list;

#[derive(Parser, Debug)]
#[command(name = "ssps")]
#[command(about = "Build a graph from a CSV with node_id,neighbors, and weights per row run an SSP algorithm from a source node id.", long_about = None)]
struct Cli {
    /// Path to the .csv file
    #[arg(short, long)]
    csv: String,

    /// Number of runs. Picks a new (deterministic) source id per run (0, 1, 2, 3, ...num_runs).
    #[arg(short, long)]
    num_runs: usize,

    #[arg(short, long, default_value_t = String::from("bmssp"))]
    algorithm: String,
}

enum SspAlgorithm {
    Bmssp,
    Dijkstra,
}

impl SspAlgorithm {
    fn from(string: &str) -> Self {
        match string {
            "bmssp" => SspAlgorithm::Bmssp{},
            "dijkstra" => SspAlgorithm::Dijkstra{},
            _ => panic!("Algorithm not found for input string: {}, possible options are: (\"bmssp\", \"dijkstra\")", string),
        }
    }

    fn run(self: &Self, neighbors: &Vec<Vec<(usize, f64)>>, start: usize) -> Vec<f64> {
        match self {
            SspAlgorithm::Bmssp => bmssp::bmssp_all(neighbors, start),
            SspAlgorithm::Dijkstra => dijkstra::dijkstra_all(neighbors, start),
        }
    }
}

fn build_adjacency_list(edges: &[(usize, usize, f64)]) -> Vec<Vec<(usize, f64)>> {
    // First, determine how many nodes we have.
    let max_node = edges
        .iter()
        .map(|(u, v, _)| std::cmp::max(*u, *v))
        .max()
        .unwrap_or(0);

    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); max_node + 1];

    for &(u, v, w) in edges {
        adj[u].push((v, w));
    }

    adj
}

fn parse_csv_and_build_adjacency_list(
    path: &str,
) -> Result<Vec<Vec<(usize, f64)>>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true) // important: skip header line
        .from_path(path)?;

    let mut edges = Vec::new();

    for result in rdr.records() {
        let record = result?;
        let node_id: usize = record[0].parse()?;
        let neighbor_node_id: usize = record[1].parse()?;
        let weight: f64 = record[2].parse()?;
        edges.push((node_id, neighbor_node_id, weight));
    }

    Ok(build_adjacency_list(&edges))
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // Set the algorithm.
    let ssp = SspAlgorithm::from(&cli.algorithm);

    let adj = parse_csv_and_build_adjacency_list(&cli.csv)?;

    let mut src_idx = 0;
    let mut duration_millis = Vec::new();
    for src_idx in 0..cli.num_runs {
        use std::time::SystemTime;
        let now = SystemTime::now();
        let dist = ssp.run(&adj, src_idx);
        if let Ok(elapsed) = now.elapsed() {
            duration_millis.push(elapsed.as_secs_f64() * 1000.0);
        }
    }
    println!("{:?}", duration_millis);

    Ok(())
}
