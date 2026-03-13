//! Benchmarks for graph traversal performance.
//!
//! Target from SPECS.md: <5ms traversal latency.
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use akaldb::prelude::*;

/// Build a graph with N companies, each having K complaints, each with M evidence items.
/// Total nodes: N + N*K + N*K*M
/// Total edges: N*K + N*K*M
fn build_graph(companies: usize, complaints_per: usize, evidence_per: usize) -> (MemoryStore, Vec<NodeId>) {
    let total_nodes = companies + companies * complaints_per + companies * complaints_per * evidence_per;
    let total_edges = companies * complaints_per + companies * complaints_per * evidence_per;

    let mut db = MemoryStore::with_capacity(total_nodes, total_edges);
    let mut company_ids = Vec::with_capacity(companies);

    for i in 0..companies {
        let company = db.add_node("Company", props(&format!("Company_{}", i)));
        company_ids.push(company);

        for j in 0..complaints_per {
            let complaint = db.add_node("Complaint", props(&format!("C_{}_{}", i, j)));
            db.add_edge(company, complaint, "HAS_COMPLAINT", empty_properties()).unwrap();

            for k in 0..evidence_per {
                let evidence = db.add_node("Evidence", props(&format!("E_{}_{}_{}", i, j, k)));
                db.add_edge(complaint, evidence, "HAS_EVIDENCE", empty_properties()).unwrap();
            }
        }
    }

    (db, company_ids)
}

fn bench_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("traversal");

    // Test at different scales
    for &(companies, complaints, evidence) in &[
        (100, 5, 3),      // Small:  100 companies, ~2K nodes
        (1000, 10, 5),     // Medium: 1K companies, ~61K nodes
        (10000, 5, 3),     // Large:  10K companies, ~200K nodes
    ] {
        let (db, company_ids) = build_graph(companies, complaints, evidence);
        let total_nodes = db.node_count();

        group.bench_with_input(
            BenchmarkId::new("path_3hop", total_nodes),
            &(db, company_ids),
            |b, (db, ids)| {
                let start = ids[0];
                b.iter(|| {
                    black_box(db.traverse_path(start, &["HAS_COMPLAINT", "HAS_EVIDENCE"]));
                });
            },
        );
    }

    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert_node", |b| {
        let mut db = MemoryStore::new();
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            black_box(db.add_node("Company", props(&format!("C_{}", i))));
        });
    });

    c.bench_function("insert_edge", |b| {
        let mut db = MemoryStore::new();
        // Pre-create nodes
        let nodes: Vec<NodeId> = (0..10000)
            .map(|i| db.add_node("N", props(&format!("{}", i))))
            .collect();
        let mut i = 0usize;
        b.iter(|| {
            let from = nodes[i % nodes.len()];
            let to = nodes[(i + 1) % nodes.len()];
            black_box(db.add_edge(from, to, "REL", empty_properties()).unwrap());
            i += 1;
        });
    });
}

fn bench_bfs(c: &mut Criterion) {
    let (db, company_ids) = build_graph(1000, 5, 3);

    c.bench_function("bfs_depth2", |b| {
        let start = company_ids[0];
        b.iter(|| {
            black_box(db.bfs(start, 2));
        });
    });
}

fn bench_find_by_label(c: &mut Criterion) {
    let (db, _) = build_graph(1000, 5, 3);

    c.bench_function("find_by_label", |b| {
        b.iter(|| {
            black_box(db.find_by_label("Company"));
        });
    });
}

criterion_group!(benches, bench_traversal, bench_insert, bench_bfs, bench_find_by_label);
criterion_main!(benches);
