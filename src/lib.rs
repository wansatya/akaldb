//! # AkalDB — An Installable Reasoning Database
//!
//! AkalDB stores knowledge as a **context graph** rather than
//! flat vectors or tables. It enables AI systems to retrieve **relationships, meaning,
//! and reasoning paths** instead of merely returning semantically similar text.
//!
//! ## Quick Start
//!
//! ```rust
//! use akaldb::prelude::*;
//!
//! let mut db = MemoryStore::new();
//!
//! // Build a knowledge graph
//! let company = db.add_node("Company", props("Company X"));
//! let complaint = db.add_node("Complaint", props("Ghosting Report"));
//! let evidence = db.add_node("Evidence", props("Screenshot"));
//!
//! db.add_edge(company, complaint, "HAS_COMPLAINT", empty_properties()).unwrap();
//! db.add_edge(complaint, evidence, "HAS_EVIDENCE", empty_properties()).unwrap();
//!
//! // Traverse: Company → Complaint → Evidence
//! let paths = db.traverse_path(company, &["HAS_COMPLAINT", "HAS_EVIDENCE"]);
//! assert_eq!(paths.len(), 1);
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │              MemoryStore                 │
//! │  ┌──────────┐ ┌──────────┐ ┌──────────┐│
//! │  │NodeStore  │ │EdgeStore │ │Adjacency ││
//! │  │(arena)   │ │(arena)   │ │Map       ││
//! │  └──────────┘ └──────────┘ └──────────┘│
//! │  ┌──────────────────┐ ┌──────────────┐ │
//! │  │ StringInterner   │ │ LabelIndex   │ │
//! │  └──────────────────┘ └──────────────┘ │
//! └─────────────────────────────────────────┘
//! ```

pub mod graph;
pub mod query;
pub mod storage;
pub mod types;

/// Convenience prelude that re-exports the most commonly used types.
///
/// ```rust
/// use akaldb::prelude::*;
/// ```
pub mod prelude {
    pub use crate::storage::memory_store::{ContextPath, MemoryStore, TraversalStep};
    pub use crate::types::*;

    /// Helper to create a property map with a single "name" field.
    /// Useful for quick prototyping and examples.
    pub fn props(name: &str) -> PropertyMap {
        let mut map = PropertyMap::new();
        map.insert(
            "name".to_string(),
            serde_json::Value::String(name.to_string()),
        );
        map
    }
}
