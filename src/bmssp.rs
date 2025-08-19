// BMSSSP (distance-only), implemented from 
// "Breaking the Sorting Barrier for Directed Single-Source Shortest Paths"
// (Duan, Mao, Mao, Shu, Yin, April 2025) (arXiv:2504.17033v1)
use std::cmp;
use hashbrown::{HashMap, HashSet};
use std::collections::BinaryHeap;
use crate::block_data_structure::{BlockList, PullResult};

#[derive(Copy, Clone, Debug, PartialEq)]
struct State {
    node_id: usize,
    cost: f64,
}

impl State {
    fn from(node_id: usize, cost: f64) -> Self {
        Self {
            node_id: node_id,
            cost: cost,
        }
    }
}

// Min-heap by cost
impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        // reverse ordering for min-heap
        other.cost.partial_cmp(&self.cost).unwrap_or(cmp::Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}


// Returns a set of pivots and set W such that d(w) < B.
pub fn find_pivots(bound: f64, frontier: &Vec<usize>, k:usize, neighbors: &Vec<Vec<(usize, f64)>>, min_cost_map: &mut HashMap<usize, f64>) -> (Vec<usize>, HashSet<usize>){
    // Build out the "lookahead" layers in our search k-times forward from the frontier.
    let mut layers = Vec::new();
    layers.push(frontier.iter().cloned().collect::<HashSet<usize>>());
    let mut all_layers = layers[0].clone();
    let mut total_added = layers[0].len();
    let mut bp_map = HashMap::new();
    let mut last_layer = &layers[0];
    for i in 1..=k {
        let mut new_layer = HashSet::new();
        for &node_id in last_layer {
            // Relax neighboring edges.
            let cost_to_node_id = min_cost_map[&node_id];
            for &(neighbor_id, cost) in &neighbors[node_id] {
                let cost_to_neighbor = cost_to_node_id + cost;
                if cost_to_neighbor <= min_cost_map[&neighbor_id] {
                    min_cost_map.insert(neighbor_id, cost_to_neighbor);
                    if cost_to_neighbor < bound {
                        // Add to the layer!
                        new_layer.insert(neighbor_id);
                        // Keep back pointers so that we can traverse our forest to find pivots later.
                        bp_map.insert(neighbor_id, node_id);
                    }
                }
            }
        }
        all_layers.extend(new_layer.iter());
        layers.push(new_layer);
        last_layer = &layers[i];
        // If we're doing too much work we need to exit early.
        if all_layers.len() > k * frontier.len() {
            return (frontier.clone(), layers.into_iter().flatten().collect());
        }
    }

    // Otherwise, we want to return the nodes in W0 (S) that are the roots of trees of size >= k.
    // We start by iterating through Wk and traversing until we hit a root and count the number of unique nodes in that tree.
    let mut pivots = HashMap::new();
    let mut node_to_root: HashMap<usize, usize> = HashMap::new();
    for &leaf in layers.last().unwrap() {
        let mut cur = leaf;
        let mut branch = vec![leaf];
        while let Some(&next_node) = bp_map.get(&cur) {
            if let Some(&root_id) = node_to_root.get(&next_node) {
                // This means we can break early. We just need to add the size of this branch to the pivots.
                for &node_id in branch.iter() {
                    node_to_root.insert(node_id, root_id);
                }
                cur = root_id;
            } else {
                branch.push(next_node);
                cur = next_node;
            }
        }

        // This is a way to count the size of the tree (by adding branches uniquely).
        pivots.insert(cur, pivots.get(&cur).unwrap_or(&0) + branch.len());
    }

    // Our pivots are any roots who have trees of size >= k.
    let returned_pivots = pivots.into_iter().filter(|kv| kv.1 >= k).map(|kv| kv.0).collect();
    (returned_pivots, layers.into_iter().flatten().collect::<HashSet<_>>())
}

/*
* Runs on l=0 off a singleton. Effectively a "mini-Dijkstra's". 
* One big assumption here is that node_id is closed.
* Returns: a new boundary B' < upper_bound and a set U.
*/
fn base_bmssp(upper_bound: f64, node_id: usize, k: usize, neighbors: &Vec<Vec<(usize, f64)>>, min_cost_map: &mut HashMap<usize, f64>) -> (f64, HashSet<usize>) {
    let mut u_init = HashSet::new();
    u_init.insert(node_id);
    let mut heap = BinaryHeap::new();
    let mut visited_set = HashSet::new();
    let mut max_cost_so_far = min_cost_map[&node_id];
    heap.push(State::from(node_id, max_cost_so_far));
    while let Some(State {node_id, cost}) = heap.pop() {
        if u_init.len() > k || visited_set.contains(&node_id) {
            break;
        }
        visited_set.insert(node_id);
        u_init.insert(node_id);
        max_cost_so_far = max_cost_so_far.max(min_cost_map[&node_id]);
        for (neighbor_node_id, weight) in &neighbors[node_id] {
            let cost_to_neighbor = cost + weight;
            if cost_to_neighbor <= min_cost_map[neighbor_node_id] && cost_to_neighbor < upper_bound {
                min_cost_map.insert(*neighbor_node_id, cost_to_neighbor);
                heap.push(State::from(*neighbor_node_id, cost_to_neighbor));
            }
        }
    }

    if u_init.len() <= k {
        (upper_bound, u_init)
    } else {
        (max_cost_so_far, u_init.into_iter().filter(|node_id| min_cost_map[node_id] < max_cost_so_far).collect())
    }
}

/*
* Requirements:
* |frontier| <= 2^(l*t) ~ 4096 for the top level with 100k nodes. 
*
* Returns: a new boundary B' < upper_bound and a set U.
*/
fn bmssp_bounded(l: usize, upper_bound: f64, frontier: &Vec<usize>, k: usize, t: usize, neighbors: &Vec<Vec<(usize, f64)>>, min_cost_map: &mut HashMap<usize, f64>) -> (f64, HashSet<usize>) {
    if l == 0 {
        assert_eq!(frontier.len(), 1);
        return base_bmssp(upper_bound, frontier[0], k, neighbors, min_cost_map);
    }

    let (pivots, layer_set) = find_pivots(upper_bound, frontier, k, neighbors, min_cost_map);
    let M = 2_usize.pow((t * (l - 1)).try_into().unwrap());
    let max_size_u_set = k * 2_usize.pow((t * l).try_into().unwrap());
    let mut block_list = BlockList::new(M, upper_bound);
    // Add the pivots to the queue.
    let mut min_upper_bound = upper_bound;
    for pivot in pivots {
        let dist = min_cost_map[&pivot];
        if dist > upper_bound {
            assert!(dist < upper_bound, "Pivot distance can't be greater than B {} >= {}", dist, upper_bound);
        }
        block_list.insert(pivot, dist);
        min_upper_bound = min_upper_bound.min(dist);
    }

    let mut u_set = HashSet::new();
    while u_set.len() < max_size_u_set && !block_list.is_empty() {
        let PullResult(new_frontier, current_upper_bound) = block_list.pull();
        let (new_upper_bound, new_uset) = bmssp_bounded(l - 1, current_upper_bound, &new_frontier, k, t, neighbors, min_cost_map);
        min_upper_bound = new_upper_bound;
        let mut batch_prepend_elements = HashMap::new();
        for &node_id in new_uset.iter() {
            u_set.insert(node_id);
            for (neighbor_node_id, weight) in &neighbors[node_id] {
                let proposed_weight = min_cost_map[&node_id] + weight;
                if proposed_weight <= min_cost_map[neighbor_node_id] {
                    min_cost_map.insert(*neighbor_node_id, proposed_weight);
                    if current_upper_bound <= proposed_weight && proposed_weight < upper_bound {
                        block_list.insert(*neighbor_node_id, proposed_weight)
                    } else if new_upper_bound <= proposed_weight && proposed_weight < current_upper_bound {
                        // Element is cheaper than anything in the block_list currently, so we can batch prepend.
                        batch_prepend_elements.insert(*neighbor_node_id, proposed_weight);
                    }
                }
            }
        }

        for node_id in new_frontier.iter() {
            let cost = min_cost_map[node_id];
            if new_upper_bound <= cost && cost < current_upper_bound {
                // These frontier nodes are cheaper than anything in the block_list, and we can batch prepend.
                batch_prepend_elements.insert(*node_id, cost);
            }
        }
        block_list.batch_prepend(batch_prepend_elements.into_iter().collect());
    }

    // Add any elements in our layer_set that might have a distance estimate less than the min_upper_bound.
    for node_id in layer_set {
        let cost = min_cost_map[&node_id];
        if cost < min_upper_bound {
            u_set.insert(node_id);
        }
    }

    // Return a new boundary and uset.
    (min_upper_bound, u_set)
}

// Convenience function to call from a single source ID.
pub fn bmssp_all(neighbors: &Vec<Vec<(usize, f64)>>, start: usize) -> Vec<f64> {
    let N = neighbors.len() as f64;
    // TODO: Explore why k=1 loops infinitely. Probably some bad condition in the code.
    let k = N.log2().powf(1.0 / 3.0).floor().max(2.0) as usize;
    let t = N.log2().powf(2.0 / 3.0).floor() as usize;
    let starting_l = (N.log2() / (t as f64)).ceil() as usize;
    let mut min_cost_map = HashMap::new();
    // Initialize min_cost_map to infinity.
    for node_id in 0..neighbors.len() {
        min_cost_map.insert(node_id, f64::INFINITY);
    }
    min_cost_map.insert(start, 0.0);
    let B = f64::INFINITY;
    let (min_upper_bound,uset) = bmssp_bounded(starting_l, B, &vec![start], k, t, neighbors, &mut min_cost_map);
    // Now we have a min_cost_map so we can convert to a vec of distances.
    let mut dist = vec![0.0; neighbors.len()];
    for i in 0..dist.len() {
        dist[i] = min_cost_map[&i];
    }
    dist
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_graph() {
        let mut neighbors = vec![Vec::new(); 11];
        neighbors[0] = vec![(1, 0.0), (2, 1.0), (7, 5.0)];
        neighbors[1] = vec![(3, 3.0), (4, 2.0)];
        neighbors[2] = vec![(4, 3.0), (5, 2.0)];
        neighbors[3] = vec![(6, 2.0)];
        neighbors[4] = vec![(6, 2.0)];
        neighbors[5] = vec![];
        neighbors[6] = vec![(8, 3.0)];
        neighbors[7] = vec![(9, 2.0)];
        neighbors[8] = vec![(10, 1.0)];
        neighbors[9] = vec![(10, 2.0)];
        neighbors[10] = vec![];

        let start = 0;

        let dist = bmssp_all(&neighbors, start);

        assert_eq!(dist[0], 0.0);
        assert_eq!(dist[1], 0.0);
        assert_eq!(dist[2], 1.0);
        assert_eq!(dist[3], 3.0);
        assert_eq!(dist[4], 2.0);
        assert_eq!(dist[5], 3.0);
        assert_eq!(dist[6], 4.0);
        assert_eq!(dist[7], 5.0);
        assert_eq!(dist[8], 7.0);
        assert_eq!(dist[9], 7.0);
        assert_eq!(dist[10], 8.0);
    }
}