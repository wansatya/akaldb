//! Adjacency list structure for the AkalDB graph engine.
//!
//! This is the heart of graph traversal performance. The adjacency structure
//! determines how quickly we can answer "what is connected to node X?"
//!
//! ## Design Rationale
//!
//! We maintain TWO adjacency lists per node:
//!
//! 1. **Outgoing edges** (from → to): "what does this node point to?"
//!    Used for forward traversal: Company → Complaints → Evidence
//!
//! 2. **Incoming edges** (to ← from): "what points to this node?"
//!    Used for reverse traversal: Evidence ← Complaint ← Company
//!
//! Both directions are essential for a reasoning database where queries need
//! to traverse relationships in either direction.
//!
//! ## SmallVec Optimization
//!
//! We use `SmallVec<[EdgeId; 8]>` for each adjacency list. This means:
//! - Nodes with ≤8 edges: stored inline, zero heap allocation
//! - Nodes with >8 edges: spills to heap (normal Vec behavior)
//!
//! In real-world knowledge graphs, most nodes have few edges (power-law distribution),
//! so the majority of adjacency lists fit inline. This dramatically reduces
//! allocator pressure and improves cache locality during traversal.
//!
//! ## Why Not CSR (Compressed Sparse Row)?
//!
//! CSR would give even better cache locality for read-only graphs, but it requires
//! rebuilding the entire structure on each insertion. Since AkalDB needs to support
//! live inserts (SPECS: <10ms insert latency), per-node Vec/SmallVec is the right
//! trade-off between traversal speed and mutation cost.

use crate::types::*;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

/// Number of edges stored inline per adjacency list before spilling to heap.
/// 8 was chosen because:
/// - Covers ~90% of nodes in typical knowledge graphs (power-law distribution)
/// - 8 * sizeof(EdgeId) = 8 * 8 = 64 bytes = exactly one cache line
const INLINE_EDGES: usize = 8;

/// Adjacency list type: inline storage for ≤8 edges, heap for more.
pub type AdjList = SmallVec<[EdgeId; INLINE_EDGES]>;

/// Adjacency index that maps each node to its outgoing and incoming edge lists.
///
/// Uses FxHashMap (rustc's fast hash) because:
/// - Node IDs are not sequential when slots are reused with different generations
/// - We need O(1) lookup by NodeId
/// - FxHashMap is ~2x faster than default HashMap for integer-like keys
///
/// Alternative considered: parallel Vec indexed by node.index.
/// Rejected because generation changes mean the same index can map to different
/// logical nodes over time, requiring HashMap for correct keying.
pub struct AdjacencyMap {
    /// node_id → list of outgoing edge IDs
    outgoing: FxHashMap<NodeId, AdjList>,
    /// node_id → list of incoming edge IDs
    incoming: FxHashMap<NodeId, AdjList>,
}

impl AdjacencyMap {
    pub fn new() -> Self {
        Self {
            outgoing: FxHashMap::default(),
            incoming: FxHashMap::default(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            outgoing: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            incoming: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }

    /// Register a new edge in the adjacency structure.
    ///
    /// This adds the edge ID to:
    /// - The outgoing list of the source node (from)
    /// - The incoming list of the target node (to)
    ///
    /// O(1) amortized — SmallVec push is O(1) amortized, HashMap insert is O(1) amortized.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId, edge_id: EdgeId) {
        self.outgoing
            .entry(from)
            .or_insert_with(SmallVec::new)
            .push(edge_id);
        self.incoming
            .entry(to)
            .or_insert_with(SmallVec::new)
            .push(edge_id);
    }

    /// Remove an edge from the adjacency structure.
    ///
    /// Uses `swap_remove` for O(1) removal (order doesn't matter for adjacency lists).
    /// This is preferable to `retain` which is O(n) where n is the list length.
    pub fn remove_edge(&mut self, from: NodeId, to: NodeId, edge_id: EdgeId) {
        if let Some(list) = self.outgoing.get_mut(&from) {
            if let Some(pos) = list.iter().position(|&e| e == edge_id) {
                list.swap_remove(pos);
            }
        }
        if let Some(list) = self.incoming.get_mut(&to) {
            if let Some(pos) = list.iter().position(|&e| e == edge_id) {
                list.swap_remove(pos);
            }
        }
    }

    /// Remove all edges associated with a node (both directions).
    /// Called when a node is deleted from the graph.
    ///
    /// Returns the list of all edge IDs that were connected to this node
    /// so the caller can clean up the EdgeStore.
    pub fn remove_node(&mut self, node_id: NodeId) -> Vec<EdgeId> {
        let mut affected = Vec::new();

        if let Some(out_edges) = self.outgoing.remove(&node_id) {
            affected.extend(out_edges.iter());
        }
        if let Some(in_edges) = self.incoming.remove(&node_id) {
            affected.extend(in_edges.iter());
        }

        affected.sort_unstable();
        affected.dedup();
        affected
    }

    /// Get all outgoing edge IDs from a node.
    /// Returns an empty slice if the node has no outgoing edges.
    ///
    /// This is the primary traversal operation: given a node, find all
    /// its outgoing relationships. O(1) lookup + returning a slice reference.
    #[inline]
    pub fn outgoing(&self, node_id: NodeId) -> &[EdgeId] {
        self.outgoing
            .get(&node_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get all incoming edge IDs to a node.
    #[inline]
    pub fn incoming(&self, node_id: NodeId) -> &[EdgeId] {
        self.incoming
            .get(&node_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Count of outgoing edges from a node. O(1).
    #[inline]
    pub fn out_degree(&self, node_id: NodeId) -> usize {
        self.outgoing
            .get(&node_id)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Count of incoming edges to a node. O(1).
    #[inline]
    pub fn in_degree(&self, node_id: NodeId) -> usize {
        self.incoming
            .get(&node_id)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Total degree (in + out) of a node.
    #[inline]
    pub fn degree(&self, node_id: NodeId) -> usize {
        self.out_degree(node_id) + self.in_degree(node_id)
    }
}

impl Default for AdjacencyMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(i: u32) -> NodeId {
        NodeId { index: i, generation: 0 }
    }

    fn eid(i: u32) -> EdgeId {
        EdgeId { index: i, generation: 0 }
    }

    #[test]
    fn test_add_and_query() {
        let mut adj = AdjacencyMap::new();
        adj.add_edge(nid(0), nid(1), eid(0));
        adj.add_edge(nid(0), nid(2), eid(1));
        adj.add_edge(nid(1), nid(2), eid(2));

        assert_eq!(adj.outgoing(nid(0)).len(), 2);
        assert_eq!(adj.incoming(nid(2)).len(), 2);
        assert_eq!(adj.out_degree(nid(0)), 2);
        assert_eq!(adj.in_degree(nid(2)), 2);
        assert_eq!(adj.degree(nid(0)), 2);  // 2 out, 0 in
    }

    #[test]
    fn test_remove_edge() {
        let mut adj = AdjacencyMap::new();
        adj.add_edge(nid(0), nid(1), eid(0));
        adj.add_edge(nid(0), nid(1), eid(1));

        adj.remove_edge(nid(0), nid(1), eid(0));

        assert_eq!(adj.outgoing(nid(0)).len(), 1);
        assert_eq!(adj.outgoing(nid(0))[0], eid(1));
    }

    #[test]
    fn test_remove_node() {
        let mut adj = AdjacencyMap::new();
        adj.add_edge(nid(0), nid(1), eid(0));
        adj.add_edge(nid(1), nid(2), eid(1));
        adj.add_edge(nid(2), nid(1), eid(2));

        let affected = adj.remove_node(nid(1));
        // Should collect edges 0, 1, 2 (all connected to node 1)
        assert_eq!(affected.len(), 3);
        assert_eq!(adj.outgoing(nid(1)).len(), 0);
        assert_eq!(adj.incoming(nid(1)).len(), 0);
    }

    #[test]
    fn test_empty_adjacency() {
        let adj = AdjacencyMap::new();
        assert_eq!(adj.outgoing(nid(999)).len(), 0);
        assert_eq!(adj.incoming(nid(999)).len(), 0);
        assert_eq!(adj.degree(nid(999)), 0);
    }
}
