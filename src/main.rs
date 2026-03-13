//! AkalDB HTTP Server
//!
//! Single-binary deployment as specified in SPECS.md.
//! Default port: 7420
//!
//! ## API Endpoints
//!
//! - `POST /query`     — Execute a CQL query
//! - `POST /nodes`     — Insert a node
//! - `POST /edges`     — Insert an edge
//! - `GET  /nodes`     — List nodes by label
//! - `GET  /health`    — Health check
//! - `GET  /stats`     — Graph statistics
//!
//! ## Usage
//!
//! ```bash
//! ./akaldb start              # Start on default port 7420
//! ./akaldb start --port 8080  # Start on custom port
//! ```

use std::sync::{Arc, RwLock};
use akaldb::prelude::*;
use akaldb::query::executor::{self, QueryResult};
use tiny_http::{Header, Method, Response, Server, StatusCode};

const DEFAULT_PORT: u16 = 7420;
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared application state — the graph database wrapped in a RwLock
/// for concurrent read access from multiple HTTP handler threads.
///
/// RwLock chosen over Mutex because graph queries are overwhelmingly reads.
/// Multiple readers can proceed in parallel; only mutations (insert/delete) take
/// an exclusive write lock.
type AppState = Arc<RwLock<MemoryStore>>;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse port from command line: --port <N>
    let port = args
        .windows(2)
        .find(|w| w[0] == "--port")
        .and_then(|w| w[1].parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);

    let addr = format!("0.0.0.0:{}", port);
    let server = match Server::http(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("✗ Failed to start server on {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    let state: AppState = Arc::new(RwLock::new(MemoryStore::new()));

    // ── ANSI color codes ────────────────────────────────────────────────
    let cyan      = "\x1b[38;5;51m";
    let sky       = "\x1b[38;5;45m";
    let blue      = "\x1b[38;5;39m";
    let indigo    = "\x1b[38;5;63m";
    let purple    = "\x1b[38;5;135m";
    let _magenta  = "\x1b[38;5;171m";
    let bold      = "\x1b[1m";
    let dim       = "\x1b[2m";
    let reset     = "\x1b[0m";
    let white     = "\x1b[97m";
    let green     = "\x1b[38;5;114m";
    let gray      = "\x1b[38;5;242m";

    println!();
    println!("  {cyan}{bold}╔══════════════════════════════════════════════════════════════╗{reset}");
    println!("  {cyan}{bold}║{reset}                                                              {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan} █████╗ {sky}██╗  ██╗{blue} █████╗ {indigo}██╗     {reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan}██╔══██╗{sky}██║ ██╔╝{blue}██╔══██╗{indigo}██║     {reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan}███████║{sky}█████╔╝ {blue}███████║{indigo}██║     {reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan}██╔══██║{sky}██╔═██╗ {blue}██╔══██║{indigo}██║     {reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan}██║  ██║{sky}██║  ██╗{blue}██║  ██║{indigo}███████╗{reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}        {cyan}╚═╝  ╚═╝{sky}╚═╝  ╚═╝{blue}╚═╝  ╚═╝{indigo}╚══════╝{reset}                       {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}                                                              {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}██████╗ {purple}██████╗{reset}                                     {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}██╔══██╗{purple}██╔══██╗{reset}   {dim}Context Graph Database{reset}          {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}██║  ██║{purple}██████╔╝{reset}   {dim}Reasoning as a primitive{reset}        {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}██║  ██║{purple}██╔══██╗{reset}                                     {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}██████╔╝{purple}██████╔╝{reset}   {gray}v{VERSION}{reset}                            {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}       {indigo}{bold}╚═════╝ {purple}╚═════╝{reset}                                      {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}║{reset}                                                              {cyan}{bold}║{reset}");
    println!("  {cyan}{bold}╚══════════════════════════════════════════════════════════════╝{reset}");
    println!();
    println!("  {green}{bold}🚀 Server listening{reset}  →  {white}{bold}http://localhost:{port}{reset}");
    println!();
    println!("  {dim}┌──────────────────────────────────────────────────┐{reset}");
    println!("  {dim}│{reset}  {cyan}POST{reset}  {white}/query{reset}          {dim}Execute CQL query{reset}         {dim}│{reset}");
    println!("  {dim}│{reset}  {cyan}POST{reset}  {white}/nodes{reset}          {dim}Insert node{reset}               {dim}│{reset}");
    println!("  {dim}│{reset}  {cyan}POST{reset}  {white}/edges{reset}          {dim}Insert edge{reset}               {dim}│{reset}");
    println!("  {dim}│{reset}  {green}GET {reset}  {white}/nodes?label=X{reset}  {dim}Find nodes by label{reset}       {dim}│{reset}");
    println!("  {dim}│{reset}  {green}GET {reset}  {white}/health{reset}         {dim}Health check{reset}              {dim}│{reset}");
    println!("  {dim}│{reset}  {green}GET {reset}  {white}/stats{reset}          {dim}Graph statistics{reset}          {dim}│{reset}");
    println!("  {dim}└──────────────────────────────────────────────────┘{reset}");
    println!();

    for mut request in server.incoming_requests() {
        let state = state.clone();

        // Route the request
        let response = match (request.method(), request.url()) {
            // ── Health Check ─────────────────────────────────
            (&Method::Get, "/health") => {
                json_response(200, &serde_json::json!({
                    "status": "ok",
                    "version": VERSION,
                }))
            }

            // ── Statistics ───────────────────────────────────
            (&Method::Get, "/stats") => {
                let db = state.read().unwrap();
                json_response(200, &serde_json::json!({
                    "nodes": db.node_count(),
                    "edges": db.edge_count(),
                    "labels": db.label_count(),
                    "interned_strings": db.interned_string_count(),
                }))
            }

            // ── CQL Query ───────────────────────────────────
            (&Method::Post, "/query") => {
                let body = read_body(&mut request);
                handle_query(&state, &body)
            }

            // ── Insert Node ──────────────────────────────────
            (&Method::Post, "/nodes") => {
                let body = read_body(&mut request);
                handle_insert_node(&state, &body)
            }

            // ── Insert Edge ──────────────────────────────────
            (&Method::Post, "/edges") => {
                let body = read_body(&mut request);
                handle_insert_edge(&state, &body)
            }

            // ── Find Nodes by Label ──────────────────────────
            (&Method::Get, url) if url.starts_with("/nodes") => {
                handle_find_nodes(&state, url)
            }

            // ── CORS Preflight ───────────────────────────────
            (&Method::Options, _) => {
                json_response(200, &serde_json::json!({"status": "ok"}))
            }

            // ── Not Found ────────────────────────────────────
            _ => {
                json_response(404, &serde_json::json!({
                    "error": "not found",
                    "hint": "available endpoints: POST /query, POST /nodes, POST /edges, GET /nodes, GET /health, GET /stats"
                }))
            }
        };

        let _ = request.respond(response);
    }
}

// =============================================================================
// Request Handlers
// =============================================================================

/// Handle POST /query — execute a CQL query.
///
/// Request body: `{"cql": "FIND Company WHERE ..."}`
/// Response: query results as JSON
fn handle_query(state: &AppState, body: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": format!("invalid JSON: {}", e)})),
    };

    let cql = match parsed.get("cql").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return json_response(400, &serde_json::json!({"error": "missing 'cql' field"})),
    };

    let db = state.read().unwrap();

    match executor::query(&db, cql) {
        Ok(result) => {
            let response_json = match &result {
                QueryResult::Nodes(nodes) => {
                    let items: Vec<serde_json::Value> = nodes.iter().map(|n| {
                        serde_json::json!({
                            "id": format!("{}", n.id),
                            "label": n.label,
                            "properties": n.properties,
                        })
                    }).collect();
                    serde_json::json!({
                        "type": "nodes",
                        "count": items.len(),
                        "data": items,
                    })
                }
                QueryResult::Paths(paths) => {
                    let items: Vec<serde_json::Value> = paths.iter().map(|p| {
                        let steps: Vec<serde_json::Value> = p.steps.iter().map(|s| {
                            let node = db.get_node(s.target_node).ok();
                            let label = node.and_then(|n| db.resolve_label(n.label)).unwrap_or("?");
                            let props = node.map(|n| &n.properties);
                            serde_json::json!({
                                "relation": db.resolve_label(s.relation).unwrap_or("?"),
                                "target": {
                                    "id": format!("{}", s.target_node),
                                    "label": label,
                                    "properties": props,
                                }
                            })
                        }).collect();

                        let root = db.get_node(p.root).ok();
                        let root_label = root.and_then(|n| db.resolve_label(n.label)).unwrap_or("?");
                        serde_json::json!({
                            "root": {
                                "id": format!("{}", p.root),
                                "label": root_label,
                                "properties": root.map(|n| &n.properties),
                            },
                            "steps": steps,
                        })
                    }).collect();
                    serde_json::json!({
                        "type": "paths",
                        "count": items.len(),
                        "data": items,
                    })
                }
                QueryResult::Grouped(groups) => {
                    let items: serde_json::Value = groups.iter().map(|(key, nodes)| {
                        let group_nodes: Vec<serde_json::Value> = nodes.iter().map(|n| {
                            serde_json::json!({
                                "id": format!("{}", n.id),
                                "label": n.label,
                                "properties": n.properties,
                            })
                        }).collect();
                        (key.clone(), serde_json::json!(group_nodes))
                    }).collect::<serde_json::Map<String, serde_json::Value>>().into();

                    serde_json::json!({
                        "type": "grouped",
                        "count": groups.len(),
                        "data": items,
                    })
                }
            };
            json_response(200, &response_json)
        }
        Err(e) => json_response(400, &serde_json::json!({"error": format!("{}", e)})),
    }
}

/// Handle POST /nodes — insert a new node.
///
/// Request body: `{"label": "Company", "properties": {"name": "Acme"}}`
fn handle_insert_node(state: &AppState, body: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": format!("invalid JSON: {}", e)})),
    };

    let label = match parsed.get("label").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return json_response(400, &serde_json::json!({"error": "missing 'label' field"})),
    };

    let properties: PropertyMap = parsed
        .get("properties")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut db = state.write().unwrap();
    let id = db.add_node(&label, properties);

    json_response(201, &serde_json::json!({
        "id": format!("{}", id),
        "label": label,
        "index": id.index,
        "generation": id.generation,
    }))
}

/// Handle POST /edges — insert a new edge.
///
/// Request body:
/// ```json
/// {
///   "from": {"index": 0, "generation": 0},
///   "to": {"index": 1, "generation": 0},
///   "relation": "HAS_COMPLAINT",
///   "properties": {}
/// }
/// ```
fn handle_insert_edge(state: &AppState, body: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return json_response(400, &serde_json::json!({"error": format!("invalid JSON: {}", e)})),
    };

    let from = match parse_node_id(parsed.get("from")) {
        Some(id) => id,
        None => return json_response(400, &serde_json::json!({"error": "missing or invalid 'from' field (need {index, generation})"})),
    };

    let to = match parse_node_id(parsed.get("to")) {
        Some(id) => id,
        None => return json_response(400, &serde_json::json!({"error": "missing or invalid 'to' field (need {index, generation})"})),
    };

    let relation = match parsed.get("relation").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return json_response(400, &serde_json::json!({"error": "missing 'relation' field"})),
    };

    let properties: PropertyMap = parsed
        .get("properties")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut db = state.write().unwrap();
    match db.add_edge(from, to, &relation, properties) {
        Ok(edge_id) => json_response(201, &serde_json::json!({
            "id": format!("{}", edge_id),
            "from": format!("{}", from),
            "to": format!("{}", to),
            "relation": relation,
        })),
        Err(e) => json_response(400, &serde_json::json!({"error": format!("{}", e)})),
    }
}

/// Handle GET /nodes?label=Company — find nodes by label.
fn handle_find_nodes(state: &AppState, url: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    let label = url
        .split('?')
        .nth(1)
        .and_then(|qs| {
            qs.split('&')
                .find(|p| p.starts_with("label="))
                .map(|p| p.trim_start_matches("label="))
        });

    let label = match label {
        Some(l) => l,
        None => return json_response(400, &serde_json::json!({"error": "missing 'label' query parameter"})),
    };

    let db = state.read().unwrap();
    let node_ids = db.find_by_label(label);

    let nodes: Vec<serde_json::Value> = node_ids.iter().filter_map(|&id| {
        let node = db.get_node(id).ok()?;
        let resolved_label = db.resolve_label(node.label).unwrap_or("?");
        Some(serde_json::json!({
            "id": format!("{}", id),
            "label": resolved_label,
            "properties": node.properties,
        }))
    }).collect();

    json_response(200, &serde_json::json!({
        "count": nodes.len(),
        "data": nodes,
    }))
}

// =============================================================================
// Helpers
// =============================================================================

/// Read the full request body as a UTF-8 string.
fn read_body(request: &mut tiny_http::Request) -> String {
    let mut body = String::new();
    let _ = request.as_reader().read_to_string(&mut body);
    body
}

/// Parse a NodeId from a JSON value like {"index": 0, "generation": 0}.
fn parse_node_id(val: Option<&serde_json::Value>) -> Option<NodeId> {
    let obj = val?.as_object()?;
    let index = obj.get("index")?.as_u64()? as u32;
    let generation = obj.get("generation")?.as_u64().unwrap_or(0) as u32;
    Some(NodeId { index, generation })
}

/// Create a JSON HTTP response with CORS headers.
fn json_response(status: u16, body: &serde_json::Value) -> Response<std::io::Cursor<Vec<u8>>> {
    let body_bytes = serde_json::to_vec_pretty(body).unwrap_or_default();
    let len = body_bytes.len();

    Response::new(
        StatusCode(status),
        vec![
            Header::from_bytes("Content-Type", "application/json").unwrap(),
            Header::from_bytes("Access-Control-Allow-Origin", "*").unwrap(),
            Header::from_bytes("Access-Control-Allow-Methods", "GET, POST, OPTIONS").unwrap(),
            Header::from_bytes("Access-Control-Allow-Headers", "Content-Type").unwrap(),
        ],
        std::io::Cursor::new(body_bytes),
        Some(len),
        None,
    )
}
