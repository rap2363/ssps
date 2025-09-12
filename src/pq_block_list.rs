/*
Block data structure proposed in https://arxiv.org/pdf/2504.17033v1, but uses a priority queue to back.

Parameterized by M, and an upper bound B over all values (assuming values are floats) in the block.
Supported operations are Add (Insert), BatchPreprend, and Pull
*/

use orx_priority_queue::*;

pub type NodeId = usize;
pub type Cost = f64;

pub struct PullResult(pub Vec<NodeId>, pub Cost);

#[derive(Debug)]
pub struct BlockList {
    M: usize,
    B: Cost,
    pq: BinaryHeapWithMap<NodeId, Cost>,
}

impl BlockList {
    pub fn new(M: usize, B: Cost) -> Self {
        Self {
            M,
            B,
            pq: BinaryHeapWithMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.pq.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pq.is_empty()
    }

    pub fn insert(&mut self, node_id: NodeId, cost: Cost) {
        // Insert if the current cost is less than what we currently have.
        assert!(cost <= self.B, "inserted cost {} >= B {}", cost, self.B);
        self.pq.decrease_key_or_push(&node_id, cost);
    }

    pub fn batch_prepend(&mut self, nodes_to_prepend: Vec<(NodeId, Cost)>) {
        for (node_id, cost) in nodes_to_prepend.into_iter() {
            self.insert(node_id, cost);
        }
    }

    fn get_minimum_bound(&self) -> Cost {
        self.pq.peek().map(|n| n.1).unwrap_or(self.B)
    }

    pub fn pull(&mut self) -> PullResult {
        // Pull M elements.
        let mut pulled_elements = Vec::new();
        for _ in 0..self.M {
            if let Some((node_id, _)) = self.pq.pop() {
                pulled_elements.push(node_id);
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
        let PullResult(nodes, upper_bound) = block_list.pull();
        assert_eq!(nodes, vec![4, 3]);
        assert_eq!(upper_bound, 7.5);
    }
}


