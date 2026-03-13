//! In-memory graph storage engine for AkalDB.
//!
//! `MemoryStore` is the unified interface that ties together NodeStore, EdgeStore,
//! and AdjacencyMap into a coherent graph database. All mutations go through this
//! layer to ensure consistency between the three data structures.
//!
//! ## Architecture
//!
//! ```text
//! MemoryStore
//! ├── nodes: NodeStore         (arena-allocated node data)
//! ├── edges: EdgeStore         (arena-allocated edge data)
//! ├── adjacency: AdjacencyMap  (connectivity structure)
//! ├── interner: StringInterner (label/relation deduplication)
//! └── label_index: HashMap<InternedString, Vec<NodeId>>  (label → nodes index)
//! ```
//!
//! ## Consistency Guarantees
//!
//! All operations maintain these invariants:
//! 1. Every edge's `from` and `to` point to live nodes
//! 2. Every EdgeId in AdjacencyMap points to a live edge
//! 3. Deleting a node removes all connected edges
//! 4. label_index is always in sync with NodeStore
//!
//! ## Performance Characteristics
//!
//! | Operation            | Complexity         |
//! |---------------------|--------------------|
//! | Insert node          | O(1) amortized     |
//! | Insert edge          | O(1) amortized     |
//! | Get node by ID       | O(1)               |
//! | Get edge by ID       | O(1)               |
//! | Get neighbors        | O(1)               |
//! | Find by label        | O(1)               |
//! | Delete node          | O(degree) — must remove connected edges |
//! | Delete edge          | O(1)               |
//! | Multi-hop traversal  | O(path_length)     |

use crate::graph::adjacency::AdjacencyMap;
use crate::graph::edge::{EdgeData, EdgeStore};
use crate::graph::node::{NodeData, NodeStore};
use crate::types::*;
use rustc_hash::FxHashMap;

// =============================================================================
// Traversal Results
// =============================================================================

/// A step in a traversal path: the edge taken and the node reached.
#[derive(Debug, Clone)]
pub struct TraversalStep {
    pub edge_id: EdgeId,
    pub relation: InternedString,
    pub target_node: NodeId,
}

/// A complete context path — a sequence of nodes and edges that form
/// a reasoning chain. This is the core output type for CQL PATH queries.
///
/// Example path: Company_X → HAS_COMPLAINT → Complaint_142 → HAS_EVIDENCE → Screenshot_1
#[derive(Debug, Clone)]
pub struct ContextPath {
    pub root: NodeId,
    pub steps: Vec<TraversalStep>,
}

impl ContextPath {
    /// Get all node IDs in this path (root + all targets).
    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids = vec![self.root];
        for step in &self.steps {
            ids.push(step.target_node);
        }
        ids
    }

    /// Path length (number of edges traversed).
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

// =============================================================================
// Memory Store
// =============================================================================

/// The unified in-memory graph storage engine.
///
/// This is the primary API surface for Phase 1. All graph operations go through
/// this struct to maintain consistency between storage and indexing structures.
pub struct MemoryStore {
    /// Node data storage (generational arena)
    nodes: NodeStore,
    /// Edge data storage (generational arena)
    edges: EdgeStore,
    /// Connectivity structure for fast traversal
    adjacency: AdjacencyMap,
    /// String interner for labels and relation types
    interner: StringInterner,
    /// Secondary index: label → list of nodes with that label
    /// Enables fast FIND queries: "FIND Company" → all nodes labeled "Company"
    label_index: FxHashMap<InternedString, Vec<NodeId>>,
}

impl MemoryStore {
    /// Create a new empty graph store.
    pub fn new() -> Self {
        Self {
            nodes: NodeStore::new(),
            edges: EdgeStore::new(),
            adjacency: AdjacencyMap::new(),
            interner: StringInterner::new(),
            label_index: FxHashMap::default(),
        }
    }

    /// Create a graph store with pre-allocated capacity for bulk loading.
    ///
    /// # Arguments
    /// * `node_capacity` — expected number of nodes
    /// * `edge_capacity` — expected number of edges
    pub fn with_capacity(node_capacity: usize, edge_capacity: usize) -> Self {
        Self {
            nodes: NodeStore::with_capacity(node_capacity),
            edges: EdgeStore::with_capacity(edge_capacity),
            adjacency: AdjacencyMap::with_capacity(node_capacity),
            interner: StringInterner::new(),
            label_index: FxHashMap::default(),
        }
    }

    // =========================================================================
    // Node Operations
    // =========================================================================

    /// Add a node to the graph.
    ///
    /// # Arguments
    /// * `label` — the type/category of this node (e.g., "Company", "Person")
    /// * `properties` — arbitrary JSON properties
    ///
    /// # Returns
    /// The NodeId of the newly created node.
    pub fn add_node(&mut self, label: &str, properties: PropertyMap) -> NodeId {
        let label_id = self.interner.intern(label);
        let node = NodeData {
            label: label_id,
            properties,
        };
        let id = self.nodes.insert(node);

        // Update label index for fast FIND queries
        self.label_index
            .entry(label_id)
            .or_insert_with(Vec::new)
            .push(id);

        id
    }

    /// Get a reference to a node's data.
    pub fn get_node(&self, id: NodeId) -> GraphResult<&NodeData> {
        self.nodes.get(id)
    }

    /// Get a mutable reference to a node's data.
    pub fn get_node_mut(&mut self, id: NodeId) -> GraphResult<&mut NodeData> {
        self.nodes.get_mut(id)
    }

    /// Delete a node and all its connected edges.
    ///
    /// This is O(degree) because we must remove all connected edges
    /// to maintain consistency. For each connected edge, we must also
    /// clean up the OTHER endpoint's adjacency list.
    pub fn remove_node(&mut self, id: NodeId) -> GraphResult<NodeData> {
        // First, get the node data to find its label for index cleanup
        let node_data = self.nodes.get(id)?;
        let label = node_data.label;

        // Remove all connected edges from adjacency map.
        // This removes id's own outgoing/incoming entries but NOT
        // the other endpoint's entries. We must do that next.
        let affected_edges = self.adjacency.remove_node(id);

        // For each affected edge, clean up the OTHER endpoint's adjacency entry,
        // then remove from edge store.
        for &edge_id in &affected_edges {
            if let Ok(edge_data) = self.edges.get(edge_id) {
                let from = edge_data.from;
                let to = edge_data.to;

                // Clean up the other endpoint's adjacency list:
                // - If this node is the source (from == id), clean the target's incoming list
                // - If this node is the target (to == id), clean the source's outgoing list
                if from == id {
                    // We already removed id's outgoing; remove from to's incoming
                    self.adjacency.remove_edge(from, to, edge_id);
                }
                if to == id {
                    // We already removed id's incoming; remove from from's outgoing
                    self.adjacency.remove_edge(from, to, edge_id);
                }
            }
            let _ = self.edges.remove(edge_id);
        }

        // Remove from label index
        if let Some(nodes) = self.label_index.get_mut(&label) {
            nodes.retain(|&n| n != id);
            if nodes.is_empty() {
                self.label_index.remove(&label);
            }
        }

        // Remove the node itself
        self.nodes.remove(id)
    }

    /// Find all nodes with a given label.
    /// Returns an empty slice if no nodes have that label.
    ///
    /// This powers the CQL `FIND` clause: `FIND Company` returns all nodes
    /// labeled "Company". O(1) lookup via label_index.
    pub fn find_by_label(&self, label: &str) -> &[NodeId] {
        let label_id = match self.interner_lookup(label) {
            Some(id) => id,
            None => return &[],
        };
        self.label_index
            .get(&label_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    // =========================================================================
    // Edge Operations
    // =========================================================================

    /// Add a directed edge between two nodes.
    ///
    /// # Arguments
    /// * `from` — source node ID
    /// * `to` — target node ID
    /// * `relation` — relationship type (e.g., "HAS_COMPLAINT")
    /// * `properties` — arbitrary JSON properties on the edge
    ///
    /// # Errors
    /// Returns an error if either the source or target node doesn't exist.
    pub fn add_edge(
        &mut self,
        from: NodeId,
        to: NodeId,
        relation: &str,
        properties: PropertyMap,
    ) -> GraphResult<EdgeId> {
        // Validate both endpoints exist before creating the edge
        // This prevents dangling edges in the graph
        if !self.nodes.contains(from) {
            return Err(GraphError::NodeNotFound(from));
        }
        if !self.nodes.contains(to) {
            return Err(GraphError::NodeNotFound(to));
        }

        let relation_id = self.interner.intern(relation);
        let edge = EdgeData {
            from,
            to,
            relation: relation_id,
            timestamp: now_millis(),
            properties,
        };

        let edge_id = self.edges.insert(edge);
        self.adjacency.add_edge(from, to, edge_id);

        Ok(edge_id)
    }

    /// Add a directed edge with a specific timestamp (useful for bulk loading
    /// historical data or importing from external sources).
    pub fn add_edge_with_timestamp(
        &mut self,
        from: NodeId,
        to: NodeId,
        relation: &str,
        properties: PropertyMap,
        timestamp: Timestamp,
    ) -> GraphResult<EdgeId> {
        if !self.nodes.contains(from) {
            return Err(GraphError::NodeNotFound(from));
        }
        if !self.nodes.contains(to) {
            return Err(GraphError::NodeNotFound(to));
        }

        let relation_id = self.interner.intern(relation);
        let edge = EdgeData {
            from,
            to,
            relation: relation_id,
            timestamp,
            properties,
        };

        let edge_id = self.edges.insert(edge);
        self.adjacency.add_edge(from, to, edge_id);

        Ok(edge_id)
    }

    /// Get a reference to an edge's data.
    pub fn get_edge(&self, id: EdgeId) -> GraphResult<&EdgeData> {
        self.edges.get(id)
    }

    /// Delete an edge from the graph.
    pub fn remove_edge(&mut self, id: EdgeId) -> GraphResult<EdgeData> {
        let edge_data = self.edges.get(id)?;
        let from = edge_data.from;
        let to = edge_data.to;

        self.adjacency.remove_edge(from, to, id);
        self.edges.remove(id)
    }

    // =========================================================================
    // Traversal API
    // =========================================================================

    /// Get all outgoing edge IDs from a node.
    /// This is the fundamental traversal primitive — O(1).
    #[inline]
    pub fn outgoing_edges(&self, node_id: NodeId) -> &[EdgeId] {
        self.adjacency.outgoing(node_id)
    }

    /// Get all incoming edge IDs to a node.
    #[inline]
    pub fn incoming_edges(&self, node_id: NodeId) -> &[EdgeId] {
        self.adjacency.incoming(node_id)
    }

    /// Get all outgoing neighbors (target nodes) of a node, with edge data.
    ///
    /// Returns (EdgeId, &EdgeData) pairs for each outgoing edge.
    /// This is the most common traversal operation in path queries.
    pub fn outgoing_neighbors(&self, node_id: NodeId) -> Vec<(EdgeId, &EdgeData)> {
        self.adjacency
            .outgoing(node_id)
            .iter()
            .filter_map(|&eid| self.edges.get(eid).ok().map(|data| (eid, data)))
            .collect()
    }

    /// Get outgoing neighbors filtered by relation type.
    ///
    /// Example: `neighbors_by_relation(company_id, "HAS_COMPLAINT")`
    /// returns only edges of type HAS_COMPLAINT.
    ///
    /// This is critical for targeted traversal in CQL PATH queries.
    pub fn outgoing_by_relation(
        &self,
        node_id: NodeId,
        relation: &str,
    ) -> Vec<(EdgeId, &EdgeData)> {
        let relation_id = match self.interner_lookup(relation) {
            Some(id) => id,
            None => return vec![],
        };
        self.adjacency
            .outgoing(node_id)
            .iter()
            .filter_map(|&eid| {
                let data = self.edges.get(eid).ok()?;
                if data.relation == relation_id {
                    Some((eid, data))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Traverse a multi-hop path from a starting node, following specified relation types.
    ///
    /// This implements the SPECS.md PATH query:
    /// ```text
    /// PATH Company -> Complaint -> Evidence
    /// ```
    ///
    /// # Arguments
    /// * `start` — the starting node ID
    /// * `relations` — sequence of relation types to follow at each hop
    ///
    /// # Returns
    /// All paths from `start` that match the relation sequence.
    /// Returns multiple paths when there are branches at any hop.
    ///
    /// # Performance
    /// O(branching_factor^depth) in the worst case, but typical knowledge graphs
    /// have limited branching per relation type, making this fast in practice.
    pub fn traverse_path(
        &self,
        start: NodeId,
        relations: &[&str],
    ) -> Vec<ContextPath> {
        if relations.is_empty() {
            return vec![ContextPath {
                root: start,
                steps: vec![],
            }];
        }

        // Resolve all relation strings upfront to avoid repeated lookups
        let relation_ids: Vec<Option<InternedString>> = relations
            .iter()
            .map(|r| self.interner_lookup(r))
            .collect();

        // If any relation type doesn't exist in the interner, no paths can match
        if relation_ids.iter().any(|r| r.is_none()) {
            return vec![];
        }
        let relation_ids: Vec<InternedString> = relation_ids.into_iter().flatten().collect();

        let mut results = Vec::new();
        let mut stack: Vec<(NodeId, Vec<TraversalStep>, usize)> = vec![(start, Vec::new(), 0)];

        // Iterative DFS to avoid stack overflow on deep graphs
        while let Some((current_node, path_so_far, depth)) = stack.pop() {
            if depth >= relation_ids.len() {
                // Reached the end of the relation sequence — record this path
                results.push(ContextPath {
                    root: start,
                    steps: path_so_far,
                });
                continue;
            }

            let target_relation = relation_ids[depth];

            for &edge_id in self.adjacency.outgoing(current_node) {
                if let Ok(edge_data) = self.edges.get(edge_id) {
                    if edge_data.relation == target_relation {
                        let mut new_path = path_so_far.clone();
                        new_path.push(TraversalStep {
                            edge_id,
                            relation: target_relation,
                            target_node: edge_data.to,
                        });
                        stack.push((edge_data.to, new_path, depth + 1));
                    }
                }
            }
        }

        results
    }

    /// Breadth-first traversal up to a maximum depth.
    ///
    /// Returns all nodes reachable from `start` within `max_depth` hops,
    /// along with their distance from the start.
    ///
    /// Useful for exploring the neighborhood of a node without specifying
    /// exact relation types.
    pub fn bfs(
        &self,
        start: NodeId,
        max_depth: usize,
    ) -> Vec<(NodeId, usize)> {
        use std::collections::VecDeque;

        let mut visited = FxHashMap::default();
        let mut queue = VecDeque::new();

        visited.insert(start, 0usize);
        queue.push_back((start, 0usize));

        while let Some((node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for &edge_id in self.adjacency.outgoing(node) {
                if let Ok(edge_data) = self.edges.get(edge_id) {
                    let next = edge_data.to;
                    if !visited.contains_key(&next) {
                        visited.insert(next, depth + 1);
                        queue.push_back((next, depth + 1));
                    }
                }
            }
        }

        visited.into_iter().collect()
    }

    // =========================================================================
    // Query Helpers
    // =========================================================================

    /// Resolve an interned string label. Returns the string content.
    pub fn resolve_label(&self, id: InternedString) -> Option<&str> {
        self.interner.resolve(id)
    }

    /// Look up an interned string by its content, without interning it.
    /// Returns None if the string has never been interned.
    fn interner_lookup(&self, s: &str) -> Option<InternedString> {
        // We need to check if the string exists without inserting it.
        // This is a read-only operation on the interner.
        // Since StringInterner doesn't have a lookup-only method,
        // we scan the stored strings. For a small number of unique
        // labels (<1000 typically), this is fast enough.
        // TODO: add a lookup method to StringInterner for O(1) lookup.
        for i in 0..self.interner.len() {
            if self.interner.resolve(InternedString(i as u32)) == Some(s) {
                return Some(InternedString(i as u32));
            }
        }
        None
    }

    /// Get a reference to the string interner (for label resolution).
    pub fn interner(&self) -> &StringInterner {
        &self.interner
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    /// Number of active nodes in the graph.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of active edges in the graph.
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Number of unique labels in the graph.
    #[inline]
    pub fn label_count(&self) -> usize {
        self.label_index.len()
    }

    /// Number of unique interned strings (labels + relation types).
    #[inline]
    pub fn interned_string_count(&self) -> usize {
        self.interner.len()
    }

    /// Iterate over all active nodes.
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeId, &NodeData)> {
        self.nodes.iter()
    }

    /// Iterate over all active edges.
    pub fn iter_edges(&self) -> impl Iterator<Item = (EdgeId, &EdgeData)> {
        self.edges.iter()
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Display — pretty-print the graph for debugging
// =============================================================================

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MemoryStore {{ nodes: {}, edges: {}, labels: {}, interned_strings: {} }}",
            self.node_count(),
            self.edge_count(),
            self.label_count(),
            self.interned_string_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a property map with a single "name" field.
    fn props(name: &str) -> PropertyMap {
        let mut map = PropertyMap::new();
        map.insert("name".to_string(), serde_json::Value::String(name.to_string()));
        map
    }

    #[test]
    fn test_basic_graph_construction() {
        let mut store = MemoryStore::new();

        let company = store.add_node("Company", props("Company X"));
        let complaint = store.add_node("Complaint", props("Ghosting Report #142"));
        let evidence = store.add_node("Evidence", props("Screenshot of ignored emails"));

        store.add_edge(company, complaint, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(complaint, evidence, "HAS_EVIDENCE", empty_properties()).unwrap();

        assert_eq!(store.node_count(), 3);
        assert_eq!(store.edge_count(), 2);
    }

    #[test]
    fn test_find_by_label() {
        let mut store = MemoryStore::new();

        store.add_node("Company", props("Company A"));
        store.add_node("Company", props("Company B"));
        store.add_node("Person", props("Alice"));

        assert_eq!(store.find_by_label("Company").len(), 2);
        assert_eq!(store.find_by_label("Person").len(), 1);
        assert_eq!(store.find_by_label("Unknown").len(), 0);
    }

    #[test]
    fn test_outgoing_neighbors() {
        let mut store = MemoryStore::new();

        let a = store.add_node("A", empty_properties());
        let b = store.add_node("B", empty_properties());
        let c = store.add_node("C", empty_properties());

        store.add_edge(a, b, "REL1", empty_properties()).unwrap();
        store.add_edge(a, c, "REL2", empty_properties()).unwrap();

        let neighbors = store.outgoing_neighbors(a);
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_outgoing_by_relation() {
        let mut store = MemoryStore::new();

        let company = store.add_node("Company", props("X"));
        let complaint1 = store.add_node("Complaint", props("C1"));
        let complaint2 = store.add_node("Complaint", props("C2"));
        let review = store.add_node("Review", props("R1"));

        store.add_edge(company, complaint1, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(company, complaint2, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(company, review, "HAS_REVIEW", empty_properties()).unwrap();

        let complaints = store.outgoing_by_relation(company, "HAS_COMPLAINT");
        assert_eq!(complaints.len(), 2);

        let reviews = store.outgoing_by_relation(company, "HAS_REVIEW");
        assert_eq!(reviews.len(), 1);
    }

    #[test]
    fn test_traverse_path() {
        let mut store = MemoryStore::new();

        let company = store.add_node("Company", props("Company X"));
        let complaint = store.add_node("Complaint", props("C142"));
        let evidence = store.add_node("Evidence", props("Screenshot"));

        store.add_edge(company, complaint, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(complaint, evidence, "HAS_EVIDENCE", empty_properties()).unwrap();

        // PATH Company -> Complaint -> Evidence
        let paths = store.traverse_path(company, &["HAS_COMPLAINT", "HAS_EVIDENCE"]);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].len(), 2);
        assert_eq!(paths[0].steps[0].target_node, complaint);
        assert_eq!(paths[0].steps[1].target_node, evidence);
    }

    #[test]
    fn test_traverse_path_branching() {
        let mut store = MemoryStore::new();

        let company = store.add_node("Company", props("X"));
        let c1 = store.add_node("Complaint", props("C1"));
        let c2 = store.add_node("Complaint", props("C2"));
        let e1 = store.add_node("Evidence", props("E1"));
        let e2 = store.add_node("Evidence", props("E2"));

        store.add_edge(company, c1, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(company, c2, "HAS_COMPLAINT", empty_properties()).unwrap();
        store.add_edge(c1, e1, "HAS_EVIDENCE", empty_properties()).unwrap();
        store.add_edge(c2, e2, "HAS_EVIDENCE", empty_properties()).unwrap();

        let paths = store.traverse_path(company, &["HAS_COMPLAINT", "HAS_EVIDENCE"]);
        assert_eq!(paths.len(), 2, "should find both branches");
    }

    #[test]
    fn test_bfs() {
        let mut store = MemoryStore::new();

        let a = store.add_node("A", empty_properties());
        let b = store.add_node("B", empty_properties());
        let c = store.add_node("C", empty_properties());
        let d = store.add_node("D", empty_properties());

        store.add_edge(a, b, "R", empty_properties()).unwrap();
        store.add_edge(b, c, "R", empty_properties()).unwrap();
        store.add_edge(c, d, "R", empty_properties()).unwrap();

        // Depth 1: should find a, b
        let result = store.bfs(a, 1);
        assert_eq!(result.len(), 2);

        // Depth 3: should find all 4 nodes
        let result = store.bfs(a, 3);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_remove_node_cascades() {
        let mut store = MemoryStore::new();

        let a = store.add_node("A", empty_properties());
        let b = store.add_node("B", empty_properties());
        let c = store.add_node("C", empty_properties());

        store.add_edge(a, b, "R", empty_properties()).unwrap();
        store.add_edge(b, c, "R", empty_properties()).unwrap();

        // Removing B should also remove both edges
        store.remove_node(b).unwrap();

        assert_eq!(store.node_count(), 2);
        assert_eq!(store.edge_count(), 0);
        assert_eq!(store.outgoing_edges(a).len(), 0);
    }

    #[test]
    fn test_edge_to_invalid_node_fails() {
        let mut store = MemoryStore::new();
        let a = store.add_node("A", empty_properties());
        let fake = NodeId { index: 999, generation: 0 };

        let result = store.add_edge(a, fake, "R", empty_properties());
        assert!(result.is_err());
    }

    #[test]
    fn test_string_interning_efficiency() {
        let mut store = MemoryStore::new();

        // Add 1000 nodes all with label "Company"
        for i in 0..1000 {
            store.add_node("Company", props(&format!("Company_{}", i)));
        }

        // Should only have 1 unique interned string for "Company"
        // (plus none for relation types since we haven't added edges)
        assert_eq!(store.interned_string_count(), 1);
    }
}
