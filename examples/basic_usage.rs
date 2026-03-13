//! Example: Basic usage of the AkalDB graph engine.
//!
//! This demonstrates the core functionality from SPECS.md:
//! - Creating entities (nodes) with labels and properties
//! - Creating relationships (edges) between entities
//! - Querying by label
//! - Multi-hop path traversal (the core reasoning operation)
//! - BFS neighborhood exploration
//!
//! Run with: cargo run --example basic_usage

use akaldb::prelude::*;

fn main() {
    println!("╔══════════════════════════════════════════════════╗");
    println!("║            AkalDB — Reasoning Database          ║");
    println!("║          Phase 1: Core Graph Engine              ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    // =========================================================================
    // 1. Create the graph store
    // =========================================================================
    let mut db = MemoryStore::new();

    // =========================================================================
    // 2. Add entities (nodes) to the knowledge graph
    // =========================================================================
    println!("📦 Building knowledge graph...\n");

    // Companies
    let company_x = db.add_node("Company", props("Company X"));
    let company_y = db.add_node("Company", props("Company Y"));

    // Complaints
    let complaint_142 = db.add_node("Complaint", {
        let mut p = props("Ghosting Report #142");
        p.insert("category".into(), "Ghosting".into());
        p.insert("severity".into(), "High".into());
        p
    });
    let complaint_143 = db.add_node("Complaint", {
        let mut p = props("Late Payment #143");
        p.insert("category".into(), "Payment".into());
        p.insert("severity".into(), "Medium".into());
        p
    });
    let complaint_200 = db.add_node("Complaint", {
        let mut p = props("Ghosting Report #200");
        p.insert("category".into(), "Ghosting".into());
        p.insert("severity".into(), "Critical".into());
        p
    });

    // Evidence
    let screenshot_1 = db.add_node("Evidence", {
        let mut p = props("Screenshot of ignored emails");
        p.insert("type".into(), "screenshot".into());
        p
    });
    let email_log = db.add_node("Evidence", {
        let mut p = props("Email delivery log");
        p.insert("type".into(), "log".into());
        p
    });
    let witness = db.add_node("Evidence", {
        let mut p = props("Witness statement");
        p.insert("type".into(), "testimony".into());
        p
    });

    // Workers
    let worker_alice = db.add_node("Worker", {
        let mut p = props("Alice");
        p.insert("role".into(), "Software Engineer".into());
        p.insert("work_hours".into(), serde_json::json!(55));
        p
    });

    println!("   Added {} nodes ({} unique labels)",
        db.node_count(), db.label_count());

    // =========================================================================
    // 3. Create relationships (edges) between entities
    // =========================================================================

    // Company X has complaints
    db.add_edge(company_x, complaint_142, "HAS_COMPLAINT", empty_properties()).unwrap();
    db.add_edge(company_x, complaint_143, "HAS_COMPLAINT", empty_properties()).unwrap();

    // Company Y has a complaint
    db.add_edge(company_y, complaint_200, "HAS_COMPLAINT", empty_properties()).unwrap();

    // Complaints have evidence
    db.add_edge(complaint_142, screenshot_1, "HAS_EVIDENCE", empty_properties()).unwrap();
    db.add_edge(complaint_142, email_log, "HAS_EVIDENCE", empty_properties()).unwrap();
    db.add_edge(complaint_200, witness, "HAS_EVIDENCE", empty_properties()).unwrap();

    // Worker relationship
    db.add_edge(company_x, worker_alice, "EMPLOYS", empty_properties()).unwrap();

    println!("   Added {} edges\n", db.edge_count());

    // =========================================================================
    // 4. Query: FIND Company (label-based lookup)
    // =========================================================================
    println!("─── Query: FIND Company ─────────────────────────");
    let companies = db.find_by_label("Company");
    println!("   Found {} companies:", companies.len());
    for &cid in companies {
        let node = db.get_node(cid).unwrap();
        let name = node.properties.get("name").unwrap();
        println!("   • {} ({})", name, cid);
    }
    println!();

    // =========================================================================
    // 5. Query: Outgoing neighbors by relation
    // =========================================================================
    println!("─── Query: Company X → HAS_COMPLAINT ─────────────");
    let complaints = db.outgoing_by_relation(company_x, "HAS_COMPLAINT");
    println!("   Company X has {} complaints:", complaints.len());
    for (_, edge) in &complaints {
        let target = db.get_node(edge.to).unwrap();
        let name = target.properties.get("name").unwrap();
        let category = target.properties.get("category").unwrap_or(&serde_json::Value::Null);
        println!("   • {} [category: {}]", name, category);
    }
    println!();

    // =========================================================================
    // 6. PATH traversal: Company → Complaint → Evidence
    //    This is the core REASONING operation from SPECS.md
    // =========================================================================
    println!("─── PATH: Company → Complaint → Evidence ─────────");
    println!("   (CQL: PATH Company -> Complaint -> Evidence)\n");

    let paths = db.traverse_path(company_x, &["HAS_COMPLAINT", "HAS_EVIDENCE"]);
    println!("   Found {} reasoning paths from Company X:\n", paths.len());

    for (i, path) in paths.iter().enumerate() {
        let root_node = db.get_node(path.root).unwrap();
        let root_label = db.resolve_label(root_node.label).unwrap_or("?");
        let root_name = root_node.properties.get("name").unwrap();

        println!("   Path {}:", i + 1);
        println!("   ┌ {} [{}] {}",
            root_label, root_name, path.root);

        for step in &path.steps {
            let node = db.get_node(step.target_node).unwrap();
            let label = db.resolve_label(node.label).unwrap_or("?");
            let name = node.properties.get("name").unwrap();
            let rel = db.resolve_label(step.relation).unwrap_or("?");
            println!("   └──{}──▶ {} [{}]", rel, label, name);
        }
        println!();
    }

    // =========================================================================
    // 7. BFS neighborhood exploration
    // =========================================================================
    println!("─── BFS from Company X (depth=2) ─────────────────");
    let bfs_result = db.bfs(company_x, 2);
    println!("   Reachable nodes within 2 hops: {}", bfs_result.len());
    for (node_id, depth) in &bfs_result {
        let node = db.get_node(*node_id).unwrap();
        let label = db.resolve_label(node.label).unwrap_or("?");
        let name = node.properties.get("name").unwrap();
        println!("   {}depth {}: {} [{}]",
            "  ".repeat(*depth), depth, label, name);
    }
    println!();

    // =========================================================================
    // 8. Statistics
    // =========================================================================
    println!("─── Graph Statistics ────────────────────────────");
    println!("   Nodes:            {}", db.node_count());
    println!("   Edges:            {}", db.edge_count());
    println!("   Unique labels:    {}", db.label_count());
    println!("   Interned strings: {}", db.interned_string_count());
    println!();

    println!("✅ Phase 1 complete — core graph engine operational.");
}
