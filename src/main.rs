use anyhow::{Context, Result};
use clap::Parser;
use csv::Writer;
use fnv::FnvHashMap;
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, Tags, WayId};
use std::collections::HashSet;
use std::fs::File;

mod block_data_structure;
mod bmssp;
mod dijkstra;
mod geo;
mod pq_block_list;
mod tree_block_list;

#[derive(Parser, Debug)]
#[command(name = "ssps")]
#[command(about = "Build a graph from an OSM .pbf and run an SSP algorithm from a source node id.", long_about = None)]
struct Cli {
    /// Path to the .osm.pbf file
    #[arg(short, long)]
    pbf: String,

    /// Source node id to run SSP from
    #[arg(short, long)]
    source: i64,

    #[arg(short, long, default_value_t = String::from("bmssp"))]
    algorithm: String,

    /// Output CSV (node_id, distance_m). If omitted, prints a summary to stdout.
    #[arg(short, long)]
    out: Option<String>,

    /// Include unreachable nodes in output with infinite distance
    #[arg(long, default_value_t = false)]
    include_unreachable: bool,

    /// Only include 'highway' ways (recommended). If false, attempts to include all linear ways.
    #[arg(long, default_value_t = true)]
    only_highways: bool,
}

#[derive(Clone, Debug)]
struct WayLite {
    id: WayId,
    nodes: Vec<NodeId>,
    tags: Tags,
}

fn is_way_routable(tags: &Tags, only_highways: bool) -> bool {
    if only_highways && !tags.contains_key("highway") {
        return false;
    }
    // Exclude areas and non-linear ways
    if tags.get("area").map(|v| v == "yes").unwrap_or(false) {
        return false;
    }
    true
}

fn is_oneway(tags: &Tags) -> Option<i8> {
    if let Some(v) = tags.get("oneway") {
        match v.as_str() {
            "yes" | "true" | "1" => return Some(1),
            "-1" => return Some(-1),
            _ => {}
        }
    }
    if tags
        .get("junction")
        .map(|v| v == "roundabout")
        .unwrap_or(false)
    {
        return Some(1);
    }
    None
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

    fn run(&self, neighbors: &Vec<Vec<(usize, f64)>>, start: usize) -> Vec<f64> {
        match self {
            SspAlgorithm::Bmssp => bmssp::bmssp_all(neighbors, start),
            SspAlgorithm::Dijkstra => dijkstra::dijkstra_all(neighbors, start),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set Algorithm.
    let ssp = SspAlgorithm::from(&cli.algorithm);

    // Pass 1: collect routable ways and the set of node ids they reference
    let file = File::open(&cli.pbf).with_context(|| format!("opening {}", &cli.pbf))?;
    let mut pbf = OsmPbfReader::new(file);

    let mut needed_nodes: HashSet<NodeId> = HashSet::new();
    let mut ways: Vec<WayLite> = Vec::new();

    for obj in pbf.iter() {
        let obj = obj?;
        if let OsmObj::Way(w) = obj {
            if is_way_routable(&w.tags, cli.only_highways) {
                for nid in &w.nodes {
                    needed_nodes.insert(*nid);
                }
                ways.push(WayLite {
                    id: w.id,
                    nodes: w.nodes.clone(),
                    tags: w.tags.clone(),
                });
            }
        }
    }

    println!(
        "Collected {} routable ways; {} unique node refs",
        ways.len(),
        needed_nodes.len()
    );

    // Pass 2: read coordinates for needed nodes
    let file2 = File::open(&cli.pbf).with_context(|| format!("reopening {}", &cli.pbf))?;
    let mut pbf2 = OsmPbfReader::new(file2);

    let mut coords: FnvHashMap<NodeId, (f64, f64)> = FnvHashMap::default();
    for obj in pbf2.iter() {
        let obj = obj?;
        if let OsmObj::Node(n) = obj {
            if needed_nodes.contains(&n.id) {
                coords.insert(n.id, (n.lat(), n.lon()));
            }
        }
    }

    println!(
        "Loaded coordinates for {} nodes actually present",
        coords.len()
    );

    // Build index mapping and adjacency
    let mut id_to_idx: FnvHashMap<NodeId, usize> = FnvHashMap::default();
    let mut idx_to_id: Vec<NodeId> = Vec::with_capacity(coords.len());

    for (&nid, _) in coords.iter() {
        let idx = idx_to_id.len();
        idx_to_id.push(nid);
        id_to_idx.insert(nid, idx);
    }

    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); idx_to_id.len()];

    let mut edges_added: usize = 0;
    for w in &ways {
        if w.nodes.len() < 2 {
            continue;
        }
        let oneway = is_oneway(&w.tags);
        for pair in w.nodes.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let (&(alat, alon), &(blat, blon)) = match (coords.get(&a), coords.get(&b)) {
                (Some(ca), Some(cb)) => (ca, cb),
                _ => continue,
            };
            let weight = geo::haversine_meters(alat, alon, blat, blon);
            if weight.is_finite() && weight > 0.0 {
                if let (Some(&u), Some(&v)) = (id_to_idx.get(&a), id_to_idx.get(&b)) {
                    match oneway {
                        Some(1) => {
                            adj[u].push((v, weight));
                            edges_added += 1;
                        }
                        Some(-1) => {
                            adj[v].push((u, weight));
                            edges_added += 1;
                        }
                        None => {
                            adj[u].push((v, weight));
                            adj[v].push((u, weight));
                            edges_added += 2;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    println!("Graph: {} nodes, {} directed edges", adj.len(), edges_added);

    // Source mapping
    let src_idx = cli.source as usize;

    use std::time::SystemTime;
    let now = SystemTime::now();
    let dist = ssp.run(&adj, src_idx);
    if let Ok(elapsed) = now.elapsed() {
        println!("{} s", elapsed.as_secs_f64());
    }

    if let Some(out_path) = cli.out {
        let mut wtr =
            Writer::from_path(&out_path).with_context(|| format!("creating CSV {}", &out_path))?;
        wtr.write_record(["node_id", "distance_m"])?;
        let mut dist_with_idx: Vec<(usize, &f64)> = dist.iter().enumerate().collect();
        dist_with_idx.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap());
        for (idx, d) in &dist_with_idx {
            let nid = idx_to_id[*idx].0;
            if d.is_finite() || cli.include_unreachable {
                let val = if d.is_finite() {
                    format!("{:.6}", d)
                } else {
                    String::from("inf")
                };
                wtr.write_record(&[nid.to_string(), val])?;
            }
        }
        wtr.flush()?;
        println!(
            "Wrote distances for {} nodes to {}",
            dist_with_idx.len(),
            out_path
        );
    } else {
        let reachable = dist.iter().filter(|x| x.is_finite()).count();
        println!("Nodes: {}", dist.len());
        println!("Reachable from {}: {}", cli.source, reachable);
        if reachable > 0 {
            let mut maxd = 0.0_f64;
            for d in dist.iter().copied().filter(|x| x.is_finite()) {
                if d > maxd {
                    maxd = d;
                }
            }
            println!("Max finite distance (m): {:.2}", maxd);
        }
    }

    Ok(())
}
