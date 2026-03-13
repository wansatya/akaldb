//! Shared types used across the AkalDB engine.
//!
//! This module defines the foundational types that all other modules depend on.
//! Key design decisions:
//!
//! - **Generational IDs**: NodeId and EdgeId use a generation counter to prevent
//!   stale references after deletion. This is critical for a graph database where
//!   nodes can be deleted while edges still reference them.
//!
//! - **String interning**: Labels and relation types are stored as interned `u32`
//!   IDs rather than heap-allocated Strings. In a typical knowledge graph, the same
//!   labels ("Company", "Person", "HAS_COMPLAINT") repeat millions of times.
//!   Interning reduces memory usage by 60-80% for these fields.
//!
//! - **PropertyMap as serde_json::Map**: Direct use of serde_json's map type avoids
//!   an extra layer of abstraction while giving us full JSON compatibility as
//!   required by the SPECS.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::fmt;

// =============================================================================
// Identifiers
// =============================================================================

/// Unique identifier for a node in the graph.
///
/// Uses a generational index pattern:
/// - `index`: position in the node storage Vec (O(1) lookup)
/// - `generation`: incremented on each reuse of a slot, preventing dangling references
///
/// This approach gives us O(1) access (like array indexing) with safety against
/// use-after-delete bugs (like reference counting), without the overhead of either
/// HashMap lookups or runtime borrow checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub index: u32,
    pub generation: u32,
}

/// Unique identifier for an edge in the graph.
/// Same generational index pattern as NodeId.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId {
    pub index: u32,
    pub generation: u32,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({}:g{})", self.index, self.generation)
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Edge({}:g{})", self.index, self.generation)
    }
}

// =============================================================================
// Property Storage
// =============================================================================

/// Properties attached to nodes and edges.
/// Uses serde_json::Map directly for zero-cost JSON compatibility.
pub type PropertyMap = serde_json::Map<String, serde_json::Value>;

/// Convenience function to create an empty property map.
#[inline]
pub fn empty_properties() -> PropertyMap {
    PropertyMap::new()
}

// =============================================================================
// String Interning
// =============================================================================

/// Interned string identifier. Wraps a u32 index into the interner's string table.
///
/// Using u32 instead of String saves 24 bytes per label/relation field on 64-bit
/// systems (String = ptr + len + cap = 24 bytes vs u32 = 4 bytes).
/// For a graph with 100M edges each carrying a relation type, that's ~2GB saved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InternedString(pub u32);

/// String interner that deduplicates strings and returns lightweight u32 handles.
///
/// Thread-safety note: Phase 1 is single-threaded. For Phase 2+ concurrent access,
/// this will be wrapped in a RwLock (reads vastly outnumber writes in a typical
/// graph workload, so RwLock is preferred over Mutex).
#[derive(Debug, Clone)]
pub struct StringInterner {
    /// Maps string content → interned ID for O(1) dedup lookups
    map: FxHashMap<String, InternedString>,
    /// Maps interned ID → string content for O(1) reverse lookups
    strings: Vec<String>,
}

impl StringInterner {
    pub fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            strings: Vec::new(),
        }
    }

    /// Intern a string, returning its handle. If the string was already interned,
    /// returns the existing handle without allocating.
    pub fn intern(&mut self, s: &str) -> InternedString {
        if let Some(&id) = self.map.get(s) {
            return id;
        }
        let id = InternedString(self.strings.len() as u32);
        self.strings.push(s.to_owned());
        self.map.insert(s.to_owned(), id);
        id
    }

    /// Resolve an interned string back to its content.
    /// Returns None if the ID is invalid (should never happen in correct usage).
    #[inline]
    pub fn resolve(&self, id: InternedString) -> Option<&str> {
        self.strings.get(id.0 as usize).map(|s| s.as_str())
    }

    /// Number of unique strings interned so far.
    #[inline]
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Whether the interner is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Timestamps
// =============================================================================

/// Timestamp type used for edges and reasoning paths.
/// Stored as milliseconds since Unix epoch (i64 to support pre-epoch dates).
pub type Timestamp = i64;

/// Returns the current timestamp in milliseconds since Unix epoch.
pub fn now_millis() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during graph operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphError {
    /// The referenced node ID is invalid or has been deleted
    NodeNotFound(NodeId),
    /// The referenced edge ID is invalid or has been deleted
    EdgeNotFound(EdgeId),
    /// A node with the given external ID already exists
    DuplicateNode(String),
    /// The node ID refers to a deleted slot (generation mismatch)
    StaleNodeReference(NodeId),
    /// The edge ID refers to a deleted slot (generation mismatch)
    StaleEdgeReference(EdgeId),
    /// Storage capacity exceeded
    CapacityExceeded,
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeNotFound(id) => write!(f, "node not found: {}", id),
            Self::EdgeNotFound(id) => write!(f, "edge not found: {}", id),
            Self::DuplicateNode(name) => write!(f, "duplicate node: {}", name),
            Self::StaleNodeReference(id) => write!(f, "stale node reference: {}", id),
            Self::StaleEdgeReference(id) => write!(f, "stale edge reference: {}", id),
            Self::CapacityExceeded => write!(f, "storage capacity exceeded"),
        }
    }
}

impl std::error::Error for GraphError {}

/// Result type alias for graph operations.
pub type GraphResult<T> = Result<T, GraphError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interner_dedup() {
        let mut interner = StringInterner::new();
        let a = interner.intern("Company");
        let b = interner.intern("Company");
        let c = interner.intern("Person");

        assert_eq!(a, b, "same string should return same ID");
        assert_ne!(a, c, "different strings should return different IDs");
        assert_eq!(interner.len(), 2, "should only store 2 unique strings");
    }

    #[test]
    fn test_string_interner_resolve() {
        let mut interner = StringInterner::new();
        let id = interner.intern("HAS_COMPLAINT");
        assert_eq!(interner.resolve(id), Some("HAS_COMPLAINT"));
        assert_eq!(interner.resolve(InternedString(999)), None);
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId { index: 42, generation: 1 };
        assert_eq!(format!("{}", id), "Node(42:g1)");
    }
}
