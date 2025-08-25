# ssps

Includes some SSP algorithms for benchmarking.
- BMSSP: The algorithm outlined in this [paper](https://arxiv.org/pdf/2504.17033) which claims a better time complexity than Dijkstra's on sparse graphs
- Dijkstra's: Just your standard Dijkstra's.

The CLI reads an OSM `.osm.pbf` file, builds a graph, and runs an SSP from a given source index to compute the distance cost to every node in the graph.

Unreachable nodes are omitted by default; use `--include-unreachable` to include them with `distance_m` = `inf`.

## Posts
1. [High-Level Overview](https://rohanparanjpe.substack.com/p/a-new-shortest-path-algorithm)
2. [Technical Deep Dive](https://rohanparanjpe.substack.com/p/breaking-the-shortest-path-barrier)

## Build and Run
e.g. Run BMSSP on the DC extract and output to a CSV:
```bash
cargo run --release -- --pbf data/district-of-columbia-latest.osm.pbf --source 100 --out distances_bmssp.csv  --algorithm bmssp
```

## Run unit tests
```bash
cargo test
```

## Current Statistics
Average Runtimes:


|   Graph    | Dijkstra's  | BMSSP| Slowdown (x)
|-------------|-------------|------------- | ------------- | 
| D.C.    | ~0.06s  | ~0.38s  | ~*6.3x*
| NorCal | ~7s | ~40s  | ~*5.7x* |

## Improvements to be Made

- Improved memory allocations for BMSSP
- Single Shortest Path Routes
- Storing backpointers to provide routes instead of just distances
- A* for BMSSP
- Bidirectional Searches in BMSSP
- Better benchmarking
- General cleanliness

If you'd like to help out or contribute in any way, please feel free to open up a PR!
