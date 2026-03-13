//! Graph module — the core data structures for the AkalDB knowledge graph.
//!
//! This module provides:
//! - `node` — Node storage with generational arena allocation
//! - `edge` — Edge storage with generational arena allocation
//! - `adjacency` — Adjacency list structure for fast traversal

pub mod adjacency;
pub mod edge;
pub mod node;
