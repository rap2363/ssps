/*
Block data structure proposed in https://arxiv.org/pdf/2504.17033v1.

Parameterized by M, and an upper bound B over all values (assuming values are floats) in the block.
Supported operations are Add (Insert), BatchPreprend, and Pull

Insert(k, v): Update the value if the key exists in a block by first deleting it, then adding it. Adding
the key-value pair means finding the right block (O(log(N/M)), and then inserting it in the block while potentially
updating its upper bound.
Batch-Prepend(L): Adds L elements to D0, assuming they are all currently cheaper than all other elements in the data structure.
Pull: Pulls the least M costliest elements and returns the minimum upper bound after the pull. 
      This traverses the block lists D0 and D1 in order and pulls the number of elements needed.
*/

use hashbrown::HashMap;
use std::collections::VecDeque;
use std::cmp::Ordering;

pub type NodeId = usize;
pub type Cost = f64;

#[derive(Debug)]
struct Block {
    nodes: Vec<(NodeId, Cost)>,
    upper_bound: Cost,
    capacity: usize,
}

#[derive(Debug)]
enum BlockAdditionResult<'a> {
    Success(&'a Block),
    SplitBlocks(Block, Block),
}

#[derive(Debug)]
enum BlockRemovalResult {
    FullRemoval(Vec<NodeId>, Cost),
    PartialRemoval(Vec<NodeId>, Cost),
    NoElementsLeft(Cost),
}

pub struct PullResult(pub Vec<NodeId>, pub Cost);

impl Block {
    fn new(M: usize, upper_bound: Cost) -> Self {
        Block::from_existing(M, upper_bound, Vec::with_capacity(M))
    }

    fn from_existing(M: usize, upper_bound: Cost, nodes: Vec<(NodeId, Cost)>) -> Self {
        Self {
            nodes: nodes,
            upper_bound: upper_bound,
            capacity: M,
        }
    }

    fn add(self: &mut Self, node_id: NodeId, cost: Cost) -> BlockAdditionResult {
        if self.nodes.len() < self.capacity {
            self.nodes.push((node_id, cost));
            BlockAdditionResult::Success(self)
        } else {
            // We must split the block in two.
            // NOTE: Optimized, this could be O(M), but we just sort the block for simplicity and split it.
            let mut left_nodes = self.nodes.clone();
            left_nodes.push((node_id, cost));
            left_nodes.sort_by(|&a, &b| a.1.partial_cmp(&b.1).unwrap());
            // Take M/2 nodes in the left and M/2 in the right.
            let right_nodes: Vec<_> = left_nodes.drain((self.capacity / 2 + 1)..).collect();
            BlockAdditionResult::SplitBlocks(
                Block::from_existing(self.capacity, right_nodes[0].1, left_nodes),
                Block::from_existing(self.capacity, self.upper_bound, right_nodes),
            )
        }
    }
}

#[derive(Debug)]
enum BlockLocation {
    Prepend(Cost),
    Insert(Cost),
}

#[derive(Debug)]
pub struct BlockList {
    M: usize,
    B: Cost,
    prepend_blocks: VecDeque<Block>,
    insert_blocks: VecDeque<Block>,
    cost_map: HashMap<usize, BlockLocation>, // map of node ids to existing locations.
    len: usize,
}

impl BlockList {
    pub fn new(M: usize, B: Cost) -> Self {
        Self {
            M: M,
            B: B,
            prepend_blocks: VecDeque::new(),
            insert_blocks: vec![Block::new(M, B)].into(),
            cost_map: HashMap::new(),
            len: 0,
        }
    }

    pub fn len(self: &Self) -> usize {
        self.cost_map.len()
        // self.len
    }

    pub fn is_empty(self: &Self) -> bool {
        self.cost_map.is_empty()
        // self.len() == 0
    }

    fn remove_from_prepend_list(self: &mut Self, node_id: NodeId, cost: Cost) {
        let prepend_idx = self.prepend_blocks.partition_point(|block| block.upper_bound < cost);
        // This means it's not in the prepend block!
        assert_ne!(prepend_idx, self.prepend_blocks.len());

        // Now remove the node and its old cost from the prepend block.
        let mut block = &mut self.prepend_blocks[prepend_idx];
        if let Some(i) = block.nodes.iter().position(|&n| n.0 == node_id) {
            block.nodes.swap_remove(i);
        }

        // If the vec was empty, we need to remove the block and "move" its upper bound to the previous block (if it exists).
        if block.nodes.is_empty() {
            if prepend_idx > 0 {
                self.prepend_blocks[prepend_idx - 1].upper_bound = block.upper_bound;
            }
            self.prepend_blocks.remove(prepend_idx);
        }
    }

    fn remove_from_insert_list(self: &mut Self, node_id: NodeId, cost: Cost) {
        let insert_idx = self.insert_blocks.partition_point(|block| block.upper_bound < cost);
        // This means it's not in the insert block!
        assert_ne!(insert_idx, self.insert_blocks.len());

        // Now remove the node and its old cost from the prepend block.
        let mut block = &mut self.insert_blocks[insert_idx];
        if let Some(i) = block.nodes.iter().position(|&n| n.0 == node_id) {
            block.nodes.swap_remove(i);
        }
        // If the vec was empty, we need to remove the block and "move" its upper bound to the previous block if it exists.
        if block.nodes.is_empty() {
            if insert_idx > 0 {
                self.insert_blocks[insert_idx - 1].upper_bound = block.upper_bound;
            }
            if insert_idx != self.insert_blocks.len() - 1 && self.insert_blocks.len() != 1 {
                self.insert_blocks.remove(insert_idx);
            }
        }
    }

    fn update(self: &mut Self, node_id: NodeId, new_cost: Cost) -> bool {
        match self.cost_map.get(&node_id) {
            Some(BlockLocation::Prepend(prepend_cost)) => {
                if new_cost < *prepend_cost {
                    self.remove_from_prepend_list(node_id, *prepend_cost);
                    true
                } else {
                    false
                }
            },
            Some(BlockLocation::Insert(insert_cost)) => {
                if new_cost < *insert_cost {
                    self.remove_from_insert_list(node_id, *insert_cost);
                    true
                } else {
                    false
                }
            },
            _ => true, // Node isn't here, so we can add this node to the cost map.
        }
    }

    pub fn insert(self: &mut Self, node_id: NodeId, cost: Cost) {
        // it should *never* be >= B for D1 inserts.
        assert!(cost <= self.B, "inserted cost {} >= B {} into D1", cost, self.B);
        assert_ne!(self.insert_blocks.len(), 0);
        // First update the node if it exists.
        if !self.update(node_id, cost) {
            // Cost is not less, return early!
            return;
        }
        // Now insert it.
        self.cost_map.insert(node_id, BlockLocation::Insert(cost));
        // First find the block we want to insert into using the partition search.
        let i = self.insert_blocks.partition_point(|block| block.upper_bound < cost);
        let block_to_add_to = &mut self.insert_blocks[i];
        match block_to_add_to.add(node_id, cost) {
            BlockAdditionResult::SplitBlocks(left_block, right_block) => {
                self.insert_blocks[i] = left_block;
                self.insert_blocks.insert(i + 1, right_block);
            },
            _ => {}
        }
    }

    fn get_minimum_block(self: &Self) -> &Block {
        self.prepend_blocks.front().unwrap_or(self.insert_blocks.front().unwrap())
    }

    fn get_minimum_upper_bound(self: &Self) -> Cost {
        let block = self.get_minimum_block();
        block.nodes.iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less))
            .map_or(block.upper_bound, |&n| n.1)
    }

    pub fn batch_prepend(self: &mut Self, mut nodes_to_prepend: Vec<(NodeId, Cost)>) {
        // Remove any nodes that we might replace.
        let mut nodes_to_actually_prepend = Vec::new();
        for (node_id, cost) in &nodes_to_prepend {
            if self.update(*node_id, *cost) {
                nodes_to_actually_prepend.push((*node_id, *cost));
                self.cost_map.insert(*node_id, BlockLocation::Prepend(*cost));
            }
        }

        if nodes_to_actually_prepend.is_empty() {
            // Return early!
            return;
        }

        if nodes_to_actually_prepend.len() <= self.M {
            // Just add a new block in the very front.
            let upper_bound = self.get_minimum_upper_bound();
            self.prepend_blocks.push_front(Block::from_existing(self.M, upper_bound, nodes_to_actually_prepend));
            return;
        }
        // Otherwise, we need to sort these nodes in reverse order and add them one by one into blocks.
        // Technically we could do this in O(|nodes_to_actually_prepend|) with repeated medians, but we just sort
        // here for simplicity.
        nodes_to_actually_prepend.sort_by(|&a, &b| b.1.partial_cmp(&a.1).unwrap());
        // Continually drain M elements and add into a new block until we're finished.
        while !nodes_to_actually_prepend.is_empty() {
            let block_nodes = nodes_to_actually_prepend.drain(..(((self.M as f64) / 2.0).ceil() as usize).min(nodes_to_actually_prepend.len())).collect();
            let upper_bound = self.get_minimum_upper_bound();
            self.prepend_blocks.push_front(Block::from_existing(self.M, upper_bound, block_nodes));
        }
    }

    // Returns the minimum cost across both block lists.
    fn get_minimum_cost(self: &Self) -> Cost {
        let mut min_prepend = self.B;
        let mut min_insert = self.B;
        if let Some(block) = self.prepend_blocks.front() {
            min_prepend = block.nodes.iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less))
            .map_or(block.upper_bound, |&n| n.1);
        }
        if let Some(block) = self.insert_blocks.front() {
            min_insert = block.nodes.iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Less))
            .map_or(block.upper_bound, |&n| n.1);
        }

        min_prepend.min(min_insert)
    }

    fn pull_elements(self: &mut Self, num_to_pull: usize) -> Vec<usize> {
        let mut prepend_block_elements = VecDeque::new();
        let mut insert_block_elements = VecDeque::new();

        // Consider some elements from the prepend list.
        for p_i in 0..self.prepend_blocks.len() {
            // Sort the nodes so we can take as many as needed.
            let mut block_nodes = &mut self.prepend_blocks[p_i].nodes;
            block_nodes.sort_by(|&a, &b| a.1.partial_cmp(&b.1).unwrap());
            let num_to_take = num_to_pull.min(block_nodes.len());
            for i in 0..num_to_take {
                prepend_block_elements.push_back(block_nodes[i]);
            }
            if prepend_block_elements.len() == num_to_pull {
                break;
            }
        }

        // Consider some elements from the insert list.
        for b_i in 0..self.insert_blocks.len() {
            // Sort the nodes so we can take as many as needed.
            let mut block_nodes = &mut self.insert_blocks[b_i].nodes;
            block_nodes.sort_by(|&a, &b| a.1.partial_cmp(&b.1).unwrap());
            let num_to_take = num_to_pull.min(block_nodes.len());
            for i in 0..num_to_take {
                insert_block_elements.push_back(block_nodes[i]);
            }
            if insert_block_elements.len() == num_to_pull {
                break;
            }
        }

        // Now we can effectively "merge" sort and pull from the appropriate list as needed.
        let mut pulled_elements = Vec::new();

        while !(prepend_block_elements.is_empty() && insert_block_elements.is_empty()) && pulled_elements.len() < num_to_pull {
            let min_prepend_cost = prepend_block_elements.front().map_or(self.B, |&n| n.1);
            let min_insert_cost = insert_block_elements.front().map_or(self.B, |&n| n.1);

            let node_id = if min_prepend_cost < min_insert_cost {
                let (node_id, cost) = prepend_block_elements.pop_front().unwrap();
                self.remove_from_prepend_list(node_id, cost);
                node_id
            } else {
                let (node_id, cost) = insert_block_elements.pop_front().unwrap();
                self.remove_from_insert_list(node_id, cost);
                node_id
            };

            // Remove the node from our cost map.
            self.cost_map.remove(&node_id);
            pulled_elements.push(node_id);
        }
        pulled_elements
    }

    pub fn pull(self: &mut Self) -> PullResult {
        let mut pulled_elements = Vec::new();
        let mut num_elements_pulled = 0;
        while num_elements_pulled < self.M {
            // Try to drain up to a number of elements
            let num_to_drain = self.M - num_elements_pulled;
            let mut nodes = self.pull_elements(num_to_drain);
            if nodes.len() == 0 {
                // We're done here.
                break;
            }
            num_elements_pulled += nodes.len();
            pulled_elements.append(&mut nodes);
        }

        PullResult(pulled_elements, self.get_minimum_cost())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let block = Block::new(2, 3.0);
        assert_eq!(block.upper_bound, 3.0);
    }

    #[test]
    fn block_addition_no_split() {
        let mut block = Block::from_existing(4, 10.0, vec![(0, 1.0), (5, 5.0), (3, 3.0)]);
        if let BlockAdditionResult::Success(block_added) = block.add(4, 4.0) {
            assert_eq!(block_added.upper_bound, 10.0);
            assert_eq!(block_added.nodes[3], (4, 4.0));
        } else {
            panic!("We should have gotten a success!");
        }
    }

    #[test]
    fn block_addition_triggers_split() {
        let mut block = Block::from_existing(3, 10.0, vec![(0, 1.0), (5, 5.0), (3, 3.0)]);
        if let BlockAdditionResult::SplitBlocks(left_block, right_block) = block.add(4, 4.0) {
            assert_eq!(left_block.upper_bound, 4.0);
            assert_eq!(left_block.nodes.len(), 2);
            assert_eq!(right_block.upper_bound, 10.0);
            assert_eq!(right_block.nodes.len(), 2);
        } else {
            panic!("We should have gotten a success!");
        }
    }

    #[test]
    fn block_list_addition() {
        let B = 100.0;
        let mut block_list = BlockList::new(3, B);
        block_list.insert(3, 3.0);
        block_list.insert(10, 10.0);
        block_list.insert(1, 1.0);
        block_list.insert(4, 4.0);
        block_list.insert(5, 5.3);        
        block_list.insert(7, 7.0);
        block_list.insert(5, 2.2); // Note the change.
        block_list.insert(9, 9.0); 
        // Sorts into blocks like:
        // [1, 3] -> [4, 5], [7, 9, 10]
        assert_eq!(block_list.insert_blocks.len(), 3);
        assert_eq!(block_list.insert_blocks[0].upper_bound, 4.0);
        assert_eq!(block_list.insert_blocks[1].upper_bound, 7.0);
        assert_eq!(block_list.insert_blocks[2].upper_bound, B);
    }

    #[test]
    fn block_list_prepends() {
        let B = 100.0;
        let mut block_list = BlockList::new(3, B);
        block_list.insert(30, 30.0);
        block_list.insert(10, 10.0);

        // Now prepend some values.
        block_list.batch_prepend(vec![(8, 8.0), (7, 7.0), (9, 9.0)]);
        block_list.insert(50, 50.0);
        block_list.insert(60, 60.0);
        block_list.batch_prepend(vec![(1, 1.0), (3, 3.0), (2, 2.0), (4, 4.0)]);

        // Now prepend some values.
        // Sorts into blocks into:
        // (D0) [1, 2] -> [3, 4] -> [7, 8, 9] -> (D1) [10, 30] -> [50, 60]
        assert_eq!(block_list.prepend_blocks.len(), 3);
        assert_eq!(block_list.prepend_blocks[0].upper_bound, 3.0);
        assert_eq!(block_list.prepend_blocks[1].upper_bound, 7.0);
        assert_eq!(block_list.prepend_blocks[2].upper_bound, 10.0);

        assert_eq!(block_list.insert_blocks.len(), 2);
        assert_eq!(block_list.insert_blocks[0].upper_bound, 50.0);
        assert_eq!(block_list.insert_blocks[1].upper_bound, B);
    }

    #[test]
    fn block_list_pulls() {
        let B = 100.0;
        let mut block_list = BlockList::new(3, B);
        block_list.insert(30, 30.0);
        block_list.insert(10, 10.0);

        // Now prepend some values.
        block_list.batch_prepend(vec![(8, 8.0), (7, 7.0), (9, 9.0)]);
        block_list.insert(50, 50.0);
        block_list.insert(60, 60.0);
        block_list.batch_prepend(vec![(1, 1.0), (3, 3.0), (2, 2.0), (4, 4.0)]);

        // Now prepend some values.
        // Sorts into blocks into:
        // (D0) [1] -> [2, 3, 4] -> [7, 8, 9] D1: [10, 30] -> [50, 60]
        assert_eq!(block_list.len(), 11);

        // Pull.
        let PullResult(elements, upper_bound) = block_list.pull();
        assert_eq!(elements, vec![1, 2, 3]);
        assert_eq!(upper_bound, 4.0);
        assert_eq!(block_list.len(), 8);

        // Pull again
        let PullResult(elements, upper_bound) = block_list.pull();
        assert_eq!(elements, vec![4, 7, 8]);
        assert_eq!(upper_bound, 9.0);
        assert_eq!(block_list.len(), 5);

        // Pull again
        let PullResult(elements, upper_bound) = block_list.pull();
        assert_eq!(elements, vec![9, 10, 30]);
        assert_eq!(upper_bound, 50.0);
        assert_eq!(block_list.len(), 2);

        // Pull again (now we've run out of elements)
        let PullResult(elements, upper_bound) = block_list.pull();
        assert_eq!(elements, vec![50, 60]);
        assert_eq!(upper_bound, B);
        assert_eq!(block_list.is_empty(), true);

        // Pulling from an empty list results in no elements.
        let PullResult(elements, upper_bound) = block_list.pull();
        assert_eq!(elements, vec![]);
        assert_eq!(upper_bound, B);
        assert_eq!(block_list.is_empty(), true);
    }
}


