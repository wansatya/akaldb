//! Storage module — persistence and caching layers for the AkalDB engine.
//!
//! Phase 1 provides in-memory storage only. Future phases will add:
//! - Disk persistence (memory-mapped files)
//! - Write-ahead logging for crash recovery
//! - Snapshot/restore

pub mod memory_store;
