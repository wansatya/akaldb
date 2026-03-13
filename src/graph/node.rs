//! Node storage for the AkalDB graph engine.
//!
//! Nodes represent entities in the knowledge graph (Company, Person, Complaint, etc.).
//!
//! ## Storage Design
//!
//! Nodes are stored in a `Vec<Slot<NodeData>>` where the index IS the NodeId.
//! This gives us O(1) lookup by ID without HashMap overhead.
//!
//! When a node is deleted, its slot is marked as `Empty` with an incremented
//! generation counter, and the slot index is pushed onto a free list for reuse.
//! This prevents unbounded growth of the storage vector.
//!
//! ## Memory Layout
//!
//! ```text
//! NodeStore
//! ├── slots: Vec<Slot<NodeData>>   (contiguous, cache-friendly)
//! ├── free_list: Vec<u32>          (recycled slot indices)
//! └── count: usize                 (active node count)
//!
//! NodeData
//! ├── label: InternedString (4 bytes)  ← vs String (24 bytes)
//! └── properties: PropertyMap          ← JSON object
//! ```

use crate::types::*;
use serde::{Deserialize, Serialize};

// =============================================================================
// Node Data
// =============================================================================

/// The data payload of a node. Kept minimal to reduce per-node memory.
///
/// We use InternedString for labels because in a typical knowledge graph,
/// you might have 50M nodes but only 200 distinct labels. Interning saves
/// ~24 bytes per node = ~1.2 GB at scale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeData {
    /// The type/category of this node (e.g., "Company", "Person")
    /// Stored as an interned string for memory efficiency.
    pub label: InternedString,

    /// Arbitrary JSON properties attached to this node.
    /// Example: { "name": "Company X", "industry": "Tech" }
    pub properties: PropertyMap,
}

// =============================================================================
// Slot-based Storage
// =============================================================================

/// A slot in the node storage array. Can be either occupied or empty.
///
/// The generation counter is the key to safe deletion:
/// - When a slot is allocated, it gets generation G
/// - When deleted, generation becomes G+1
/// - Any NodeId still holding generation G will fail validation
/// - When the slot is reused, it gets generation G+1 (matching new references)
#[derive(Debug, Clone)]
pub(crate) enum Slot {
    /// Slot contains a live node
    Occupied {
        data: NodeData,
        generation: u32,
    },
    /// Slot is available for reuse
    Empty {
        generation: u32,
    },
}

// =============================================================================
// Node Store
// =============================================================================

/// Arena-style storage for graph nodes.
///
/// Why Vec + free list instead of HashMap?
/// - Vec gives O(1) indexed access with perfect cache locality
/// - Free list enables slot reuse without compaction
/// - Generational IDs prevent use-after-free bugs
/// - No hashing overhead on lookups (pure array indexing)
///
/// This pattern is widely used in game engines and ECS frameworks
/// (e.g., Bevy, EnTT) for exactly these performance characteristics.
pub struct NodeStore {
    slots: Vec<Slot>,
    /// Stack of free slot indices for O(1) allocation
    free_list: Vec<u32>,
    /// Number of currently active (non-deleted) nodes
    count: usize,
}

impl NodeStore {
    /// Create a new empty node store.
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_list: Vec::new(),
            count: 0,
        }
    }

    /// Create a node store with pre-allocated capacity.
    /// Use this when you know the approximate graph size upfront
    /// to avoid reallocation during bulk loading.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free_list: Vec::new(),
            count: 0,
        }
    }

    /// Insert a new node, returning its ID.
    ///
    /// Reuses deleted slots when available (O(1) from free list),
    /// otherwise appends to the end of the storage vec (amortized O(1)).
    pub fn insert(&mut self, data: NodeData) -> NodeId {
        self.count += 1;

        if let Some(index) = self.free_list.pop() {
            // Reuse a previously deleted slot
            let generation = match &self.slots[index as usize] {
                Slot::Empty { generation } => *generation,
                Slot::Occupied { .. } => unreachable!("free list pointed to occupied slot"),
            };
            self.slots[index as usize] = Slot::Occupied { data, generation };
            NodeId { index, generation }
        } else {
            // Allocate a new slot at the end
            let index = self.slots.len() as u32;
            let generation = 0;
            self.slots.push(Slot::Occupied { data, generation });
            NodeId { index, generation }
        }
    }

    /// Get a reference to a node's data by its ID.
    /// Returns an error if the node doesn't exist or the generation doesn't match.
    pub fn get(&self, id: NodeId) -> GraphResult<&NodeData> {
        match self.slots.get(id.index as usize) {
            Some(Slot::Occupied { data, generation }) if *generation == id.generation => Ok(data),
            Some(Slot::Occupied { .. }) => Err(GraphError::StaleNodeReference(id)),
            Some(Slot::Empty { .. }) => Err(GraphError::NodeNotFound(id)),
            None => Err(GraphError::NodeNotFound(id)),
        }
    }

    /// Get a mutable reference to a node's data by its ID.
    pub fn get_mut(&mut self, id: NodeId) -> GraphResult<&mut NodeData> {
        match self.slots.get_mut(id.index as usize) {
            Some(Slot::Occupied { data, generation }) if *generation == id.generation => Ok(data),
            Some(Slot::Occupied { .. }) => Err(GraphError::StaleNodeReference(id)),
            Some(Slot::Empty { .. }) => Err(GraphError::NodeNotFound(id)),
            None => Err(GraphError::NodeNotFound(id)),
        }
    }

    /// Delete a node by its ID, returning the removed data.
    ///
    /// The slot is marked empty with an incremented generation and added
    /// to the free list for future reuse. Any existing NodeId references
    /// to this slot will fail generation validation on next access.
    pub fn remove(&mut self, id: NodeId) -> GraphResult<NodeData> {
        match self.slots.get(id.index as usize) {
            Some(Slot::Occupied { generation, .. }) if *generation == id.generation => {}
            Some(Slot::Occupied { .. }) => return Err(GraphError::StaleNodeReference(id)),
            Some(Slot::Empty { .. }) => return Err(GraphError::NodeNotFound(id)),
            None => return Err(GraphError::NodeNotFound(id)),
        }

        // Extract data and mark slot as empty
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

    /// Number of active (non-deleted) nodes.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the store contains no active nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns true if the given NodeId is valid and points to a live node.
    #[inline]
    pub fn contains(&self, id: NodeId) -> bool {
        matches!(
            self.slots.get(id.index as usize),
            Some(Slot::Occupied { generation, .. }) if *generation == id.generation
        )
    }

    /// Iterate over all active nodes and their IDs.
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, &NodeData)> {
        self.slots.iter().enumerate().filter_map(|(i, slot)| {
            if let Slot::Occupied { data, generation } = slot {
                Some((
                    NodeId {
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

impl Default for NodeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(label: InternedString) -> NodeData {
        NodeData {
            label,
            properties: empty_properties(),
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut store = NodeStore::new();
        let label = InternedString(0);
        let id = store.insert(make_node(label));

        assert_eq!(store.len(), 1);
        let data = store.get(id).unwrap();
        assert_eq!(data.label, label);
    }

    #[test]
    fn test_remove_and_reuse() {
        let mut store = NodeStore::new();
        let label = InternedString(0);

        let id1 = store.insert(make_node(label));
        store.remove(id1).unwrap();
        assert_eq!(store.len(), 0);

        // Slot should be reused
        let id2 = store.insert(make_node(InternedString(1)));
        assert_eq!(id2.index, id1.index, "should reuse same slot index");
        assert_eq!(id2.generation, id1.generation + 1, "generation should increment");
    }

    #[test]
    fn test_stale_reference() {
        let mut store = NodeStore::new();
        let id = store.insert(make_node(InternedString(0)));
        store.remove(id).unwrap();

        // Old ID should fail with stale reference
        assert!(matches!(store.get(id), Err(GraphError::NodeNotFound(_))));
    }

    #[test]
    fn test_iter() {
        let mut store = NodeStore::new();
        let _id1 = store.insert(make_node(InternedString(0)));
        let id2 = store.insert(make_node(InternedString(1)));
        let _id3 = store.insert(make_node(InternedString(2)));

        store.remove(id2).unwrap();

        let active: Vec<_> = store.iter().collect();
        assert_eq!(active.len(), 2);
    }
}
