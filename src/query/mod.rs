//! Query module — CQL (Context Query Language) parser and execution engine.
//!
//! This module provides the complete query pipeline from SPECS.md:
//!
//! ```text
//! CQL Query → Lexer → Parser → AST → Executor → Results
//! ```
//!
//! ## Submodules
//!
//! - `ast` — Abstract Syntax Tree types for parsed CQL queries
//! - `lexer` — Tokenizer that converts CQL text into tokens
//! - `parser` — Recursive descent parser that builds AST from tokens
//! - `executor` — Executes parsed queries against the MemoryStore

pub mod ast;
pub mod executor;
pub mod lexer;
pub mod parser;
