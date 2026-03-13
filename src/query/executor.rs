//! Query Executor — runs parsed CQL queries against the MemoryStore.
//!
//! This module implements the SPECS.md Query Execution Pipeline:
//!
//! ```text
//! CQL Query → CQL Parser → Query Planner → Graph Traversal Engine
//!          → Result Nodes → Context Path Builder
//! ```
//!
//! Currently the "query planner" is a direct execution strategy.
//! Future optimization: add cost-based planning for complex queries.
//!
//! ## Execution Strategy
//!
//! **FIND queries:**
//! 1. Find all nodes matching the label (label_index lookup)
//! 2. Filter by WHERE conditions (property matching)
//! 3. Apply COUNT filter (count outgoing edges of matching relation)
//! 4. Apply TIME ordering (sort by edge timestamp)
//! 5. Apply GROUP BY (group results by property value)
//!
//! **PATH queries:**
//! 1. Find all starting nodes matching the first label
//! 2. For each starting node, traverse the relation path
//! 3. Filter results by WHERE conditions
//! 4. Return ContextPaths

use crate::query::ast::*;
use crate::storage::memory_store::{ContextPath, MemoryStore};
use crate::types::*;
use std::collections::BTreeMap;
use std::fmt;

// =============================================================================
// Query Results
// =============================================================================

/// The result of executing a CQL query.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Results from a FIND query: a list of matching nodes
    Nodes(Vec<NodeResult>),
    /// Results from a PATH query: reasoning paths through the graph
    Paths(Vec<ContextPath>),
    /// Results from a GROUP BY query: grouped nodes
    Grouped(BTreeMap<String, Vec<NodeResult>>),
}

/// A single node in query results, with resolved label and properties.
#[derive(Debug, Clone)]
pub struct NodeResult {
    pub id: NodeId,
    pub label: String,
    pub properties: PropertyMap,
}

impl fmt::Display for NodeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self
            .properties
            .get("name")
            .map(|v| v.to_string())
            .unwrap_or_else(|| format!("{}", self.id));
        write!(f, "{}[{}]", self.label, name)
    }
}

// =============================================================================
// Execution Errors
// =============================================================================

#[derive(Debug, Clone)]
pub enum ExecError {
    /// A label referenced in the query doesn't exist in the graph
    LabelNotFound(String),
    /// Property referenced in WHERE clause not found on node
    PropertyNotFound(String),
    /// Type mismatch in comparison
    TypeMismatch(String),
    /// Generic execution error
    Message(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::LabelNotFound(l) => write!(f, "label '{}' not found in graph", l),
            ExecError::PropertyNotFound(p) => write!(f, "property '{}' not found", p),
            ExecError::TypeMismatch(m) => write!(f, "type mismatch: {}", m),
            ExecError::Message(m) => write!(f, "{}", m),
        }
    }
}

impl std::error::Error for ExecError {}

// =============================================================================
// Executor
// =============================================================================

/// Execute a parsed CQL query against a MemoryStore.
///
/// This is the main entry point for query execution.
pub fn execute(store: &MemoryStore, query: &Query) -> Result<QueryResult, ExecError> {
    match query {
        Query::Find(fq) => execute_find(store, fq),
        Query::Path(pq) => execute_path(store, pq),
    }
}

/// Execute a FIND query.
fn execute_find(store: &MemoryStore, query: &FindQuery) -> Result<QueryResult, ExecError> {
    // Step 1: Find all nodes with matching label
    let node_ids = store.find_by_label(&query.label);

    // Step 2: Filter by WHERE conditions
    let mut results: Vec<NodeResult> = Vec::new();
    for &node_id in node_ids {
        let node = match store.get_node(node_id) {
            Ok(n) => n,
            Err(_) => continue,
        };

        // Check all WHERE conditions against node properties
        if conditions_match(&node.properties, &query.conditions) {
            let label = store
                .resolve_label(node.label)
                .unwrap_or("?")
                .to_string();
            results.push(NodeResult {
                id: node_id,
                label,
                properties: node.properties.clone(),
            });
        }
    }

    // Step 3: Apply COUNT filter
    // COUNT complaints > 3 means: only keep nodes that have more than 3
    // outgoing edges with a relation matching "complaints" (case-insensitive contains)
    if let Some(cf) = &query.count_filter {
        results.retain(|nr| {
            let count = count_relations(store, nr.id, &cf.relation);
            compare_i64(count as i64, cf.value, &cf.operator)
        });
    }

    // Step 4: Apply TIME ordering
    // Orders results by the most recent (or earliest) incoming edge timestamp
    if let Some(time_order) = &query.time_order {
        results.sort_by(|a, b| {
            let ts_a = get_latest_edge_timestamp(store, a.id);
            let ts_b = get_latest_edge_timestamp(store, b.id);
            match time_order {
                TimeOrder::Latest => ts_b.cmp(&ts_a),   // Descending
                TimeOrder::Earliest => ts_a.cmp(&ts_b),  // Ascending
            }
        });
    }

    // Step 5: Apply GROUP BY
    if let Some(group_prop) = &query.group_by {
        let mut groups: BTreeMap<String, Vec<NodeResult>> = BTreeMap::new();
        for result in results {
            let key = result
                .properties
                .get(group_prop)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| "(none)".to_string());
            groups.entry(key).or_default().push(result);
        }
        return Ok(QueryResult::Grouped(groups));
    }

    Ok(QueryResult::Nodes(results))
}

/// Execute a PATH query.
///
/// Strategy:
/// 1. Find all nodes matching the first label in the path
/// 2. For each starting node, derive relation types from consecutive label pairs
/// 3. Use traverse_path to find all matching paths
/// 4. Filter by WHERE conditions on intermediate/final nodes
fn execute_path(store: &MemoryStore, query: &PathQuery) -> Result<QueryResult, ExecError> {
    if query.labels.len() < 2 {
        return Err(ExecError::Message("PATH requires at least 2 labels".into()));
    }

    let start_label = &query.labels[0];
    let start_nodes = store.find_by_label(start_label);

    // Build relation types from consecutive label pairs.
    // Convention: the relation between Label_A and Label_B is "HAS_<B_UPPERCASE>"
    // But we also try to find any relation that connects nodes of those labels.
    // For now, we use a flexible approach: traverse ALL outgoing edges and filter
    // by target label.
    let target_labels: Vec<&str> = query.labels[1..].iter().map(|s| s.as_str()).collect();

    let mut all_paths = Vec::new();

    for &start_id in start_nodes {
        // Do a label-aware multi-hop traversal
        let paths = traverse_by_labels(store, start_id, &target_labels);
        all_paths.extend(paths);
    }

    // Filter paths by WHERE conditions
    if !query.conditions.is_empty() {
        all_paths.retain(|path| {
            // Apply conditions to all nodes in the path
            for node_id in path.node_ids() {
                if let Ok(node) = store.get_node(node_id) {
                    if conditions_match(&node.properties, &query.conditions) {
                        return true;
                    }
                }
            }
            false
        });
    }

    Ok(QueryResult::Paths(all_paths))
}

// =============================================================================
// Helpers
// =============================================================================

/// Check if a node's properties satisfy all conditions.
///
/// Supports dotted property paths (e.g., "complaints.category") by
/// treating the last component as the property key. For simple properties
/// like "name", "category", "work_hours", it's a direct lookup.
fn conditions_match(properties: &PropertyMap, conditions: &[Condition]) -> bool {
    conditions.iter().all(|cond| {
        // Get the property name — for dotted paths, use the last component
        let prop_name = cond.property.split('.').last().unwrap_or(&cond.property);

        match properties.get(prop_name) {
            Some(prop_value) => compare_json_value(prop_value, &cond.value, &cond.operator),
            None => false,
        }
    })
}

/// Compare a JSON property value against a CQL value using the given operator.
fn compare_json_value(
    json_val: &serde_json::Value,
    cql_val: &Value,
    operator: &Operator,
) -> bool {
    match (json_val, cql_val) {
        // String comparison
        (serde_json::Value::String(a), Value::String(b)) => {
            compare_str(a, b, operator)
        }
        // Number comparison (JSON number vs CQL integer)
        (serde_json::Value::Number(a), Value::Integer(b)) => {
            if let Some(a_i64) = a.as_i64() {
                compare_i64(a_i64, *b, operator)
            } else if let Some(a_f64) = a.as_f64() {
                compare_f64(a_f64, *b as f64, operator)
            } else {
                false
            }
        }
        // Number comparison (JSON number vs CQL float)
        (serde_json::Value::Number(a), Value::Float(b)) => {
            if let Some(a_f64) = a.as_f64() {
                compare_f64(a_f64, *b, operator)
            } else {
                false
            }
        }
        // Bool comparison
        (serde_json::Value::Bool(a), Value::Bool(b)) => match operator {
            Operator::Eq => a == b,
            Operator::NotEq => a != b,
            _ => false,
        },
        _ => false,
    }
}

fn compare_str(a: &str, b: &str, op: &Operator) -> bool {
    match op {
        Operator::Eq => a.eq_ignore_ascii_case(b),
        Operator::NotEq => !a.eq_ignore_ascii_case(b),
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Gte => a >= b,
        Operator::Lte => a <= b,
    }
}

fn compare_i64(a: i64, b: i64, op: &Operator) -> bool {
    match op {
        Operator::Eq => a == b,
        Operator::NotEq => a != b,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Gte => a >= b,
        Operator::Lte => a <= b,
    }
}

fn compare_f64(a: f64, b: f64, op: &Operator) -> bool {
    match op {
        Operator::Eq => (a - b).abs() < f64::EPSILON,
        Operator::NotEq => (a - b).abs() >= f64::EPSILON,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Gte => a >= b,
        Operator::Lte => a <= b,
    }
}

/// Count the number of outgoing edges with a relation type that matches
/// the given relation name (case-insensitive partial match).
///
/// For COUNT complaints > 3, we look for relations containing "complaint".
fn count_relations(store: &MemoryStore, node_id: NodeId, relation_hint: &str) -> usize {
    let hint_upper = relation_hint.to_uppercase();
    store
        .outgoing_edges(node_id)
        .iter()
        .filter(|&&eid| {
            if let Ok(edge) = store.get_edge(eid) {
                if let Some(rel_str) = store.resolve_label(edge.relation) {
                    rel_str.to_uppercase().contains(&hint_upper)
                } else {
                    false
                }
            } else {
                false
            }
        })
        .count()
}

/// Get the most recent edge timestamp connected to a node (for TIME ordering).
fn get_latest_edge_timestamp(store: &MemoryStore, node_id: NodeId) -> Timestamp {
    let mut latest = 0i64;
    for &eid in store.outgoing_edges(node_id) {
        if let Ok(edge) = store.get_edge(eid) {
            if edge.timestamp > latest {
                latest = edge.timestamp;
            }
        }
    }
    for &eid in store.incoming_edges(node_id) {
        if let Ok(edge) = store.get_edge(eid) {
            if edge.timestamp > latest {
                latest = edge.timestamp;
            }
        }
    }
    latest
}

/// Label-aware multi-hop traversal.
///
/// Unlike `traverse_path` which follows specific relation types, this follows
/// ANY edge whose target node has the expected label. This makes PATH queries
/// work even when the user doesn't know the exact relation names.
///
/// Example: `PATH Company -> Complaint -> Evidence` will follow:
/// - Any edge from a Company node to a node labeled "Complaint"
/// - Then any edge from that Complaint node to a node labeled "Evidence"
fn traverse_by_labels(
    store: &MemoryStore,
    start: NodeId,
    target_labels: &[&str],
) -> Vec<ContextPath> {
    use crate::storage::memory_store::TraversalStep;

    if target_labels.is_empty() {
        return vec![ContextPath {
            root: start,
            steps: vec![],
        }];
    }

    let mut results = Vec::new();
    let mut stack: Vec<(NodeId, Vec<TraversalStep>, usize)> = vec![(start, Vec::new(), 0)];

    while let Some((current_node, path_so_far, depth)) = stack.pop() {
        if depth >= target_labels.len() {
            results.push(ContextPath {
                root: start,
                steps: path_so_far,
            });
            continue;
        }

        let expected_label = target_labels[depth];

        for &edge_id in store.outgoing_edges(current_node) {
            if let Ok(edge_data) = store.get_edge(edge_id) {
                // Check if the target node has the expected label
                if let Ok(target_node) = store.get_node(edge_data.to) {
                    if let Some(label) = store.resolve_label(target_node.label) {
                        if label.eq_ignore_ascii_case(expected_label) {
                            let mut new_path = path_so_far.clone();
                            new_path.push(TraversalStep {
                                edge_id,
                                relation: edge_data.relation,
                                target_node: edge_data.to,
                            });
                            stack.push((edge_data.to, new_path, depth + 1));
                        }
                    }
                }
            }
        }
    }

    results
}

// =============================================================================
// Convenience: parse + execute in one call
// =============================================================================

/// Parse and execute a CQL query string against a MemoryStore.
///
/// This is the highest-level API — takes raw CQL text and returns results.
///
/// # Example
/// ```
/// use akaldb::prelude::*;
/// use akaldb::query::executor::query;
///
/// let mut db = MemoryStore::new();
/// db.add_node("Company", props("Acme Corp"));
///
/// let result = query(&db, "FIND Company").unwrap();
/// ```
pub fn query(store: &MemoryStore, cql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
    let ast = super::parser::parse_cql(cql)?;
    Ok(execute(store, &ast)?)
}

/// Format query results as a human-readable string.
/// Used by the example and future HTTP API.
pub fn format_results(store: &MemoryStore, result: &QueryResult) -> String {
    let mut out = String::new();

    match result {
        QueryResult::Nodes(nodes) => {
            out.push_str(&format!("Found {} result(s):\n", nodes.len()));
            for nr in nodes {
                out.push_str(&format!("  • {}\n", nr));
                for (key, val) in &nr.properties {
                    out.push_str(&format!("      {}: {}\n", key, val));
                }
            }
        }
        QueryResult::Paths(paths) => {
            out.push_str(&format!("Found {} path(s):\n", paths.len()));
            for (i, path) in paths.iter().enumerate() {
                out.push_str(&format!("\n  Path {}:\n", i + 1));
                if let Ok(root) = store.get_node(path.root) {
                    let label = store.resolve_label(root.label).unwrap_or("?");
                    let name = root.properties.get("name").map(|v| v.to_string()).unwrap_or_default();
                    out.push_str(&format!("  ┌ {} [{}]\n", label, name));
                }
                for step in &path.steps {
                    if let Ok(node) = store.get_node(step.target_node) {
                        let label = store.resolve_label(node.label).unwrap_or("?");
                        let name = node.properties.get("name").map(|v| v.to_string()).unwrap_or_default();
                        let rel = store.resolve_label(step.relation).unwrap_or("?");
                        out.push_str(&format!("  └──{}──▶ {} [{}]\n", rel, label, name));
                    }
                }
            }
        }
        QueryResult::Grouped(groups) => {
            out.push_str(&format!("Found {} group(s):\n", groups.len()));
            for (key, nodes) in groups {
                out.push_str(&format!("\n  Group: {} ({} nodes)\n", key, nodes.len()));
                for nr in nodes {
                    out.push_str(&format!("    • {}\n", nr));
                }
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    fn build_test_graph() -> MemoryStore {
        let mut db = MemoryStore::new();

        let cx = db.add_node("Company", {
            let mut p = props("Company X");
            p.insert("industry".into(), "Tech".into());
            p
        });
        let cy = db.add_node("Company", {
            let mut p = props("Company Y");
            p.insert("industry".into(), "Finance".into());
            p
        });

        let c1 = db.add_node("Complaint", {
            let mut p = props("C1");
            p.insert("category".into(), "Ghosting".into());
            p
        });
        let c2 = db.add_node("Complaint", {
            let mut p = props("C2");
            p.insert("category".into(), "Payment".into());
            p
        });
        let c3 = db.add_node("Complaint", {
            let mut p = props("C3");
            p.insert("category".into(), "Ghosting".into());
            p
        });

        let e1 = db.add_node("Evidence", props("Screenshot"));
        let e2 = db.add_node("Evidence", props("Email Log"));

        db.add_edge(cx, c1, "HAS_COMPLAINT", empty_properties()).unwrap();
        db.add_edge(cx, c2, "HAS_COMPLAINT", empty_properties()).unwrap();
        db.add_edge(cy, c3, "HAS_COMPLAINT", empty_properties()).unwrap();
        db.add_edge(c1, e1, "HAS_EVIDENCE", empty_properties()).unwrap();
        db.add_edge(c3, e2, "HAS_EVIDENCE", empty_properties()).unwrap();

        // Workers for GROUP BY tests
        let w1 = db.add_node("Worker", {
            let mut p = props("Alice");
            p.insert("work_hours".into(), serde_json::json!(55));
            p.insert("industry".into(), "Tech".into());
            p
        });
        let w2 = db.add_node("Worker", {
            let mut p = props("Bob");
            p.insert("work_hours".into(), serde_json::json!(45));
            p.insert("industry".into(), "Finance".into());
            p
        });
        let w3 = db.add_node("Worker", {
            let mut p = props("Charlie");
            p.insert("work_hours".into(), serde_json::json!(60));
            p.insert("industry".into(), "Tech".into());
            p
        });

        db.add_edge(cx, w1, "EMPLOYS", empty_properties()).unwrap();
        db.add_edge(cy, w2, "EMPLOYS", empty_properties()).unwrap();
        db.add_edge(cx, w3, "EMPLOYS", empty_properties()).unwrap();

        db
    }

    #[test]
    fn test_find_all_companies() {
        let db = build_test_graph();
        let result = query(&db, "FIND Company").unwrap();
        match result {
            QueryResult::Nodes(nodes) => assert_eq!(nodes.len(), 2),
            _ => panic!("expected Nodes result"),
        }
    }

    #[test]
    fn test_find_with_where() {
        let db = build_test_graph();
        let result = query(&db, "FIND Complaint WHERE category = Ghosting").unwrap();
        match result {
            QueryResult::Nodes(nodes) => {
                assert_eq!(nodes.len(), 2, "should find 2 ghosting complaints");
            }
            _ => panic!("expected Nodes result"),
        }
    }

    #[test]
    fn test_find_with_numeric_where() {
        let db = build_test_graph();
        let result = query(&db, "FIND Worker WHERE work_hours > 50").unwrap();
        match result {
            QueryResult::Nodes(nodes) => {
                assert_eq!(nodes.len(), 2, "should find Alice (55) and Charlie (60)");
            }
            _ => panic!("expected Nodes result"),
        }
    }

    #[test]
    fn test_find_with_count() {
        let db = build_test_graph();
        // Company X has 2 complaints, Company Y has 1
        let result = query(&db, "FIND Company COUNT complaint > 1").unwrap();
        match result {
            QueryResult::Nodes(nodes) => {
                assert_eq!(nodes.len(), 1, "only Company X has >1 complaints");
                assert_eq!(nodes[0].properties.get("name").unwrap(), "Company X");
            }
            _ => panic!("expected Nodes result"),
        }
    }

    #[test]
    fn test_find_with_group_by() {
        let db = build_test_graph();
        let result = query(&db, "FIND Worker WHERE work_hours > 40 GROUP BY industry").unwrap();
        match result {
            QueryResult::Grouped(groups) => {
                assert!(groups.contains_key("Tech"));
                assert!(groups.contains_key("Finance"));
                assert_eq!(groups["Tech"].len(), 2);   // Alice + Charlie
                assert_eq!(groups["Finance"].len(), 1); // Bob
            }
            _ => panic!("expected Grouped result"),
        }
    }

    #[test]
    fn test_path_query() {
        let db = build_test_graph();
        let result = query(&db, "PATH Company -> Complaint -> Evidence").unwrap();
        match result {
            QueryResult::Paths(paths) => {
                assert_eq!(paths.len(), 2, "should find 2 paths (X->C1->E1, Y->C3->E2)");
            }
            _ => panic!("expected Paths result"),
        }
    }

    #[test]
    fn test_path_query_with_where() {
        let db = build_test_graph();
        let result = query(&db, "PATH Company -> Complaint -> Evidence WHERE category = Ghosting").unwrap();
        match result {
            QueryResult::Paths(paths) => {
                // Both paths go through a Ghosting complaint, so both should match
                assert_eq!(paths.len(), 2);
            }
            _ => panic!("expected Paths result"),
        }
    }

    #[test]
    fn test_find_no_results() {
        let db = build_test_graph();
        let result = query(&db, "FIND NonExistent").unwrap();
        match result {
            QueryResult::Nodes(nodes) => assert_eq!(nodes.len(), 0),
            _ => panic!("expected Nodes result"),
        }
    }
}
