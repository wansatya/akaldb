//! Edge storage for the AkalDB graph engine.
//!
//! Edges represent directed relationships between nodes in the knowledge graph
//! (e.g., Company --HAS_COMPLAINT--> Complaint).
//!
//! ## Storage Design
//!
//! Same generational arena pattern as NodeStore for O(1) operations.
//! Edges carry additional metadata:
//! - `relation`: the type of relationship (interned for memory efficiency)
//! - `timestamp`: when the relationship was created/observed
//! - `properties`: arbitrary JSON metadata
//!
//! ## Relationship to Adjacency Lists
//!
//! EdgeStore owns the edge data. The adjacency module (adjacency.rs) maintains
//! the connectivity structure (which nodes connect to which) using EdgeIds as
//! references into this store. This separation allows the adjacency structure
//! to remain compact (just arrays of EdgeIds) while edge data can be fetched
//! on demand during traversal.

use crate::types::*;
use serde::{Deserialize, Serialize};

// =============================================================================
// Edge Data
// =============================================================================

/// The data payload of an edge (relationship) in the knowledge graph.
///
/// Matches the SPECS.md Edge definition:
/// ```text
/// Edge {
///     id: u64
///     from: node_id
///     to: node_id
///     relation: string
///     timestamp: int64
///     properties: json
/// }
/// ```
///
/// We store `from` and `to` as NodeIds (with generation) so we can detect
/// edges pointing to deleted nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeData {
    /// Source node of this directed edge
    pub from: NodeId,
    /// Target node of this directed edge
    pub to: NodeId,
    /// Relationship type (e.g., "HAS_COMPLAINT", "HAS_EVIDENCE")
    /// Interned for memory efficiency — typically <100 unique relation types
    pub relation: InternedString,
    /// When this relationship was created/observed (milliseconds since epoch)
    pub timestamp: Timestamp,
    /// Arbitrary JSON properties attached to this edge
    pub properties: PropertyMap,
}

// =============================================================================
// Edge Slot
// =============================================================================

/// A slot in the edge storage array. Same generational pattern as node slots.
#[derive(Debug, Clone)]
pub(crate) enum Slot {
    Occupied {
        data: EdgeData,
        generation: u32,
    },
    Empty {
        generation: u32,
    },
}

// =============================================================================
// Edge Store
// =============================================================================

/// Arena-style storage for graph edges.
///
/// Identical architecture to NodeStore — see node.rs for detailed rationale
/// on why Vec + free list + generational IDs outperforms HashMap for this workload.
pub struct EdgeStore {
    slots: Vec<Slot>,
    free_list: Vec<u32>,
    count: usize,
}

impl EdgeStore {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_list: Vec::new(),
            count: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free_list: Vec::new(),
            count: 0,
        }
    }

    /// Insert a new edge, returning its ID.
    pub fn insert(&mut self, data: EdgeData) -> EdgeId {
        self.count += 1;

        if let Some(index) = self.free_list.pop() {
            let generation = match &self.slots[index as usize] {
                Slot::Empty { generation } => *generation,
                Slot::Occupied { .. } => unreachable!("free list pointed to occupied slot"),
            };
            self.slots[index as usize] = Slot::Occupied { data, generation };
            EdgeId { index, generation }
        } else {
            let index = self.slots.len() as u32;
            let generation = 0;
            self.slots.push(Slot::Occupied { data, generation });
            EdgeId { index, generation }
        }
    }

    /// Get a reference to edge data by its ID.
    pub fn get(&self, id: EdgeId) -> GraphResult<&EdgeData> {
        match self.slots.get(id.index as usize) {
            Some(Slot::Occupied { data, generation }) if *generation == id.generation => Ok(data),
            Some(Slot::Occupied { .. }) => Err(GraphError::StaleEdgeReference(id)),
            Some(Slot::Empty { .. }) => Err(GraphError::EdgeNotFound(id)),
            None => Err(GraphError::EdgeNotFound(id)),
        }
    }

    /// Get a mutable reference to edge data by its ID.
    pub fn get_mut(&mut self, id: EdgeId) -> GraphResult<&mut EdgeData> {
        match self.slots.get_mut(id.index as usize) {
            Some(Slot::Occupied { data, generation }) if *generation == id.generation => Ok(data),
            Some(Slot::Occupied { .. }) => Err(GraphError::StaleEdgeReference(id)),
            Some(Slot::Empty { .. }) => Err(GraphError::EdgeNotFound(id)),
            None => Err(GraphError::EdgeNotFound(id)),
        }
    }

    /// Delete an edge by its ID, returning the removed data.
    pub fn remove(&mut self, id: EdgeId) -> GraphResult<EdgeData> {
        match self.slots.get(id.index as usize) {
            Some(Slot::Occupied { generation, .. }) if *generation == id.generation => {}
            Some(Slot::Occupied { .. }) => return Err(GraphError::StaleEdgeReference(id)),
            Some(Slot::Empty { .. }) => return Err(GraphError::EdgeNotFound(id)),
            None => return Err(GraphError::EdgeNotFound(id)),
        }

        let old = std::mem::replace(
            &mut self.slots[id.index as usize],
            Slot::Empty {
                generation: id.generation + 1,
            },
        );

        self.free_list.push(id.index);
        self.count -= 1;

        match old {
            Slot::Occupied { data, .. } => Ok(data),
            _ => unreachable!(),
        }
    }

    /// Number of active (non-deleted) edges.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[inline]
    pub fn contains(&self, id: EdgeId) -> bool {
        matches!(
            self.slots.get(id.index as usize),
            Some(Slot::Occupied { generation, .. }) if *generation == id.generation
        )
    }

    /// Iterate over all active edges and their IDs.
    pub fn iter(&self) -> impl Iterator<Item = (EdgeId, &EdgeData)> {
        self.slots.iter().enumerate().filter_map(|(i, slot)| {
            if let Slot::Occupied { data, generation } = slot {
                Some((
                    EdgeId {
                        index: i as u32,
                        generation: *generation,
                    },
                    data,
                ))
            } else {
                None
            }
        })
    }
}

impl Default for EdgeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_edge(from: NodeId, to: NodeId, relation: InternedString) -> EdgeData {
        EdgeData {
            from,
            to,
            relation,
            timestamp: 0,
            properties: empty_properties(),
        }
    }

    fn node_id(index: u32) -> NodeId {
        NodeId { index, generation: 0 }
    }

    #[test]
    fn test_insert_and_get() {
        let mut store = EdgeStore::new();
        let rel = InternedString(0);
        let id = store.insert(make_edge(node_id(0), node_id(1), rel));

        assert_eq!(store.len(), 1);
        let data = store.get(id).unwrap();
        assert_eq!(data.from, node_id(0));
        assert_eq!(data.to, node_id(1));
        assert_eq!(data.relation, rel);
    }

    #[test]
    fn test_remove() {
        let mut store = EdgeStore::new();
        let id = store.insert(make_edge(node_id(0), node_id(1), InternedString(0)));
        let removed = store.remove(id).unwrap();
        assert_eq!(removed.from, node_id(0));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_stale_reference() {
        let mut store = EdgeStore::new();
        let id = store.insert(make_edge(node_id(0), node_id(1), InternedString(0)));
        store.remove(id).unwrap();
        assert!(store.get(id).is_err());
    }
}
