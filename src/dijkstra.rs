
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Copy, Clone, PartialEq)]
struct State {
    cost: f64,
    node_id: usize,
}

// Min-heap by cost
impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse ordering for min-heap
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra from `start` to all nodes. Returns distances (meters), `f64::INFINITY` when unreachable.
pub fn dijkstra_all(adj: &Vec<Vec<(usize, f64)>>, start: usize) -> Vec<f64> {
    let n = adj.len();
    let mut dist = vec![f64::INFINITY; n];
    let mut heap = BinaryHeap::new();

    dist[start] = 0.0;
    heap.push(State { cost: 0.0, node_id: start });

    while let Some(State { cost, node_id }) = heap.pop() {
        if cost > dist[node_id] {
            continue;
        }
        for &(next, w) in &adj[node_id] {
            let next_cost = cost + w;
            if next_cost < dist[next] {
                dist[next] = next_cost;
                heap.push(State { cost: next_cost, node_id: next });
            }
        }
    }
    dist
}
