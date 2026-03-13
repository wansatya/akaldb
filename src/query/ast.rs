//! Abstract Syntax Tree types for CQL (Context Query Language).
//!
//! These types represent the parsed structure of a CQL query, before execution.
//! The AST is independent of the storage engine, making it testable in isolation
//! and reusable if we ever swap the storage backend.
//!
//! ## Supported Query Types (from SPECS.md)
//!
//! 1. **FIND**: Label-based lookup with optional filters
//!    `FIND Company WHERE complaints.category = Ghosting`
//!
//! 2. **PATH**: Multi-hop traversal through specified relations
//!    `PATH Company -> Complaint -> Evidence WHERE complaint.category = Scam`
//!
//! 3. **TIME**: Temporal ordering of results
//!    `FIND Contract WHERE clause.type = Renewal TIME latest`
//!
//! 4. **COUNT**: Aggregation filter on relationship counts
//!    `FIND Company WHERE complaints.category = Ghosting COUNT complaints > 3`
//!
//! 5. **GROUP BY**: Group results by a property
//!    `FIND Worker WHERE work_hours > 50 GROUP BY industry`

use serde::{Deserialize, Serialize};

// =============================================================================
// Top-level Query
// =============================================================================

/// A parsed CQL query, ready for execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Query {
    /// FIND queries: search for nodes by label with optional filters
    Find(FindQuery),
    /// PATH queries: multi-hop traversal following relation types
    Path(PathQuery),
}

// =============================================================================
// FIND Query
// =============================================================================

/// Represents a FIND query:
/// ```text
/// FIND <label>
/// [WHERE <conditions>]
/// [TIME latest|earliest]
/// [COUNT <property> <op> <value>]
/// [GROUP BY <property>]
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FindQuery {
    /// The node label to search for (e.g., "Company", "Worker")
    pub label: String,
    /// Optional WHERE conditions to filter results
    pub conditions: Vec<Condition>,
    /// Optional temporal ordering
    pub time_order: Option<TimeOrder>,
    /// Optional aggregation filter (COUNT)
    pub count_filter: Option<CountFilter>,
    /// Optional grouping
    pub group_by: Option<String>,
}

// =============================================================================
// PATH Query
// =============================================================================

/// Represents a PATH query:
/// ```text
/// PATH <label> -> <label> -> ... -> <label>
/// [WHERE <conditions>]
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathQuery {
    /// Sequence of node labels defining the traversal path.
    /// Must have at least 2 elements (source and target).
    pub labels: Vec<String>,
    /// Optional WHERE conditions applied to intermediate/final nodes
    pub conditions: Vec<Condition>,
}

// =============================================================================
// Conditions (WHERE clause)
// =============================================================================

/// A single condition in a WHERE clause.
/// Example: `complaints.category = Ghosting`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// The property path to evaluate (e.g., "complaints.category" or "work_hours")
    pub property: String,
    /// The comparison operator
    pub operator: Operator,
    /// The value to compare against
    pub value: Value,
}

/// Comparison operators supported in CQL conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    /// `=` — equality
    Eq,
    /// `!=` — inequality
    NotEq,
    /// `>` — greater than
    Gt,
    /// `<` — less than
    Lt,
    /// `>=` — greater than or equal
    Gte,
    /// `<=` — less than or equal
    Lte,
}

/// Values that can appear in CQL conditions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// String literal (e.g., "Ghosting", "Renewal")
    String(String),
    /// Integer literal (e.g., 50, 3)
    Integer(i64),
    /// Float literal (e.g., 3.14)
    Float(f64),
    /// Boolean literal
    Bool(bool),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::Bool(b) => write!(f, "{}", b),
        }
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::NotEq => write!(f, "!="),
            Operator::Gt => write!(f, ">"),
            Operator::Lt => write!(f, "<"),
            Operator::Gte => write!(f, ">="),
            Operator::Lte => write!(f, "<="),
        }
    }
}

// =============================================================================
// TIME clause
// =============================================================================

/// Temporal ordering for query results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeOrder {
    /// Return the most recent results first
    Latest,
    /// Return the oldest results first
    Earliest,
}

// =============================================================================
// COUNT clause
// =============================================================================

/// Aggregation filter: `COUNT <property> <op> <value>`
/// Example: `COUNT complaints > 3`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CountFilter {
    /// The relation type to count (e.g., "complaints")
    pub relation: String,
    /// The comparison operator
    pub operator: Operator,
    /// The threshold value
    pub value: i64,
}
