/*
Block data structure proposed in https://arxiv.org/pdf/2504.17033v1, but uses a BtreeMap to back.

Parameterized by M, and an upper bound B over all values (assuming values are floats) in the block.
Supported operations are Add (Insert), BatchPreprend, and Pull
*/

use hashbrown::HashMap;
use std::collections::BTreeMap;

pub type NodeId = usize;
pub type Cost = f64;

pub struct PullResult(pub Vec<NodeId>, pub Cost);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct OrderedCost(u64);

impl OrderedCost {
    #[inline(always)]
    fn new(f: f64) -> Self {
        debug_assert!(!f.is_nan());
        let bits = f.to_bits();
        // Transform so that integer comparison gives float ordering
        let bits = if (bits as i64) < 0 {
            !bits
        } else {
            bits | (1u64 << 63)
        };
        OrderedCost(bits)
    }

    #[inline(always)]
    fn as_f64(self) -> f64 {
        let bits = if self.0 & (1u64 << 63) != 0 {
            self.0 & !(1u64 << 63)
        } else {
            !self.0
        };
        f64::from_bits(bits)
    }
}

impl PartialOrd for OrderedCost {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedCost {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Debug)]
pub struct BlockList {
    M: usize,
    B: Cost,
    tree_map: BTreeMap<OrderedCost, NodeId>,
    nodes_to_costs: HashMap<NodeId, OrderedCost>,
}

impl BlockList {
    pub fn new(M: usize, B: Cost) -> Self {
        Self {
            M,
            B,
            tree_map: BTreeMap::new(),
            nodes_to_costs: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.tree_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree_map.is_empty()
    }

    pub fn insert(&mut self, node_id: NodeId, cost: f64) {
        debug_assert!(cost <= self.B, "inserted cost {} >= B {}", cost, self.B);

        let new_cost = OrderedCost::new(cost);

        // Use entry API to avoid double lookup
        match self.nodes_to_costs.entry(node_id) {
            hashbrown::hash_map::Entry::Occupied(mut entry) => {
                let old_cost = *entry.get();
                if new_cost >= old_cost {
                    return; // No update needed
                }

                // Remove old entry from tree
                self.tree_map.remove(&old_cost);

                // Update to new cost
                entry.insert(new_cost);
                self.tree_map.insert(new_cost, node_id);
            }
            hashbrown::hash_map::Entry::Vacant(entry) => {
                entry.insert(new_cost);
                self.tree_map.insert(new_cost, node_id);
            }
        }
    }

    pub fn batch_prepend(&mut self, mut nodes_to_prepend: Vec<(NodeId, f64)>) {
        // Sort by node_id to handle duplicates efficiently
        nodes_to_prepend.sort_unstable_by_key(|(id, _)| *id);

        // Keep only the minimum cost for each node
        nodes_to_prepend.dedup_by(|a, b| {
            if a.0 == b.0 {
                b.1 = a.1.min(b.1); // Keep minimum
                true
            } else {
                false
            }
        });

        self.nodes_to_costs.reserve(nodes_to_prepend.len());

        // Sort by cost for better BTree insertion pattern
        nodes_to_prepend.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        for (node_id, cost) in nodes_to_prepend {
            self.insert(node_id, cost);
        }
    }

    fn get_minimum_bound(&self) -> Cost {
        self.tree_map
            .first_key_value()
            .map_or(self.B, |(cost, _)| cost.as_f64()) // Avoid the intermediate map
    }

    pub fn pull(&mut self) -> PullResult {
        // Pull M elements.
        let mut pulled_elements = Vec::with_capacity(self.M);
        for _ in 0..self.M {
            if let Some((cost, node_id)) = self.tree_map.pop_first() {
                pulled_elements.push(node_id);
                self.nodes_to_costs.remove(&node_id);
            } else {
                break;
            }
        }

        PullResult(pulled_elements, self.get_minimum_bound())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut block_list = BlockList::new(2, 100.0);
        block_list.insert(0, 10.0);
        block_list.insert(3, 5.0);
        block_list.insert(2, 7.5);
        block_list.insert(4, 8.0);
        block_list.insert(4, 2.5);
        println!("{:?}", block_list);
        let PullResult(nodes, upper_bound) = block_list.pull();
        assert_eq!(nodes, vec![4, 3]);
        assert_eq!(upper_bound, 7.5);
    }
}
