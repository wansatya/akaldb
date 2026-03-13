//! Example: CQL Query Engine Demo
//!
//! Demonstrates the complete CQL query pipeline from SPECS.md:
//! - FIND queries with WHERE, COUNT, TIME, GROUP BY
//! - PATH queries for multi-hop reasoning
//!
//! Run with: cargo run --example cql_demo

use akaldb::prelude::*;
use akaldb::query::executor::{format_results, query, QueryResult};

fn main() {
    println!("╔══════════════════════════════════════════════════╗");
    println!("║            AkalDB — CQL Query Engine            ║");
    println!("║          Phase 2: Context Query Language         ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    // =========================================================================
    // Build knowledge graph
    // =========================================================================
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
    let cz = db.add_node("Company", {
        let mut p = props("Company Z");
        p.insert("industry".into(), "Tech".into());
        p
    });

    let c1 = db.add_node("Complaint", {
        let mut p = props("Ghosting Report #142");
        p.insert("category".into(), "Ghosting".into());
        p
    });
    let c2 = db.add_node("Complaint", {
        let mut p = props("Late Payment #143");
        p.insert("category".into(), "Payment".into());
        p
    });
    let c3 = db.add_node("Complaint", {
        let mut p = props("Ghosting Report #200");
        p.insert("category".into(), "Ghosting".into());
        p
    });
    let c4 = db.add_node("Complaint", {
        let mut p = props("Scam Report #301");
        p.insert("category".into(), "Scam".into());
        p
    });

    let e1 = db.add_node("Evidence", props("Screenshot of ignored emails"));
    let e2 = db.add_node("Evidence", props("Email delivery log"));
    let e3 = db.add_node("Evidence", props("Witness statement"));

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

    // Relationships
    db.add_edge(cx, c1, "HAS_COMPLAINT", empty_properties()).unwrap();
    db.add_edge(cx, c2, "HAS_COMPLAINT", empty_properties()).unwrap();
    db.add_edge(cy, c3, "HAS_COMPLAINT", empty_properties()).unwrap();
    db.add_edge(cz, c4, "HAS_COMPLAINT", empty_properties()).unwrap();
    db.add_edge(c1, e1, "HAS_EVIDENCE", empty_properties()).unwrap();
    db.add_edge(c1, e2, "HAS_EVIDENCE", empty_properties()).unwrap();
    db.add_edge(c3, e3, "HAS_EVIDENCE", empty_properties()).unwrap();
    db.add_edge(cx, w1, "EMPLOYS", empty_properties()).unwrap();
    db.add_edge(cy, w2, "EMPLOYS", empty_properties()).unwrap();
    db.add_edge(cx, w3, "EMPLOYS", empty_properties()).unwrap();

    println!("📦 Knowledge graph loaded: {} nodes, {} edges\n",
        db.node_count(), db.edge_count());

    // =========================================================================
    // CQL Queries
    // =========================================================================
    let queries = [
        // Basic FIND (SPECS.md example)
        "FIND Company",

        // FIND with WHERE (SPECS.md example)
        "FIND Complaint WHERE category = Ghosting",

        // FIND with numeric comparison
        "FIND Worker WHERE work_hours > 50",

        // COUNT aggregation (SPECS.md example)
        "FIND Company COUNT complaint > 1",

        // GROUP BY (SPECS.md example)
        "FIND Worker WHERE work_hours > 40 GROUP BY industry",

        // PATH query — the CORE reasoning operation (SPECS.md example)
        "PATH Company -> Complaint -> Evidence",

        // PATH with WHERE filter
        "PATH Company -> Complaint -> Evidence WHERE category = Ghosting",
    ];

    for cql in &queries {
        println!("═══════════════════════════════════════════════════");
        println!("  CQL: {}", cql);
        println!("═══════════════════════════════════════════════════");

        match query(&db, cql) {
            Ok(result) => {
                let formatted = format_results(&db, &result);
                println!("{}", formatted);

                // Print count summary
                match &result {
                    QueryResult::Nodes(n) => println!("  → {} node(s) returned\n", n.len()),
                    QueryResult::Paths(p) => println!("  → {} reasoning path(s) found\n", p.len()),
                    QueryResult::Grouped(g) => println!("  → {} group(s) returned\n", g.len()),
                }
            }
            Err(e) => println!("  ✗ Error: {}\n", e),
        }
    }

    println!("✅ Phase 2 complete — CQL query engine operational.");
}
