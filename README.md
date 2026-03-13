# AkalDB 🧠

**The context layer for RAG pipelines.** Store knowledge as a context graph — and retrieve structured reasoning paths, not just similar text chunks.

Traditional vector databases retrieve *"fuzzy similarity"*. 
AkalDB retrieves **traceable facts** — the multi-hop relationships that connect your entities and concepts.

> *"How does our authentication work?"*
>
> **Vector RAG:** Returns random paragraphs mentioning "auth".
> **AkalDB RAG:** Returns `APIEndpoint -> AuthMiddleware -> JWTValidator`.

---

## Quick Start

### Install

The fastest way to install AkalDB (Linux/macOS):

```bash
curl -fsSL https://raw.githubusercontent.com/wansatya/akaldb/main/install.sh | bash
```

### Build from Source

```bash
# Requires Rust 1.70+
cargo build --release
```

### Run

```bash
# Start the local context server (default port: 7420)
akaldb start
```

### Test

```bash
cargo test
```

---

## CQL — Context Query Language

AkalDB uses **CQL** (Context Query Language), a declarative query language designed for context retrieval.

### FIND — Search by Label

```
FIND Company
```

### WHERE — Filter by Properties

```
FIND Complaint WHERE category = Ghosting
FIND Worker WHERE work_hours > 50
```

### PATH — Multi-hop Reasoning (Core Feature)

```
PATH Company -> Complaint -> Evidence
```

This traverses the graph:

```
Company_X
 └── HAS_COMPLAINT → Complaint_142
      └── HAS_EVIDENCE → Screenshot_1
```

### COUNT — Aggregation Filter

```
FIND Company COUNT complaints > 3
```

### GROUP BY — Group Results

```
FIND Worker WHERE work_hours > 50 GROUP BY industry
```

### TIME — Temporal Order

```
FIND Contract WHERE clause_type = Renewal TIME latest
```

---

## HTTP API

All interactions go through a simple JSON API.

### Insert a Node

```bash
curl -X POST http://localhost:7420/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "label": "Company",
    "properties": {"name": "Company X", "industry": "Tech"}
  }'
```

Response:

```json
{
  "id": "Node(0:g0)",
  "label": "Company",
  "index": 0,
  "generation": 0
}
```

### Insert an Edge

```bash
curl -X POST http://localhost:7420/edges \
  -H "Content-Type: application/json" \
  -d '{
    "from": {"index": 0, "generation": 0},
    "to": {"index": 1, "generation": 0},
    "relation": "HAS_COMPLAINT",
    "properties": {}
  }'
```

### Execute a CQL Query

```bash
curl -X POST http://localhost:7420/query \
  -H "Content-Type: application/json" \
  -d '{"cql": "FIND Company WHERE industry = Tech"}'
```

Response:

```json
{
  "type": "nodes",
  "count": 1,
  "data": [
    {
      "id": "Node(0:g0)",
      "label": "Company",
      "properties": {"name": "Company X", "industry": "Tech"}
    }
  ]
}
```

### PATH Query

```bash
curl -X POST http://localhost:7420/query \
  -H "Content-Type: application/json" \
  -d '{"cql": "PATH Company -> Complaint -> Evidence"}'
```

Response:

```json
{
  "type": "paths",
  "count": 1,
  "data": [
    {
      "root": {"id": "Node(0:g0)", "label": "Company", "properties": {"name": "Company X"}},
      "steps": [
        {"relation": "HAS_COMPLAINT", "target": {"label": "Complaint", "properties": {"category": "Ghosting"}}},
        {"relation": "HAS_EVIDENCE", "target": {"label": "Evidence", "properties": {"name": "Screenshot"}}}
      ]
    }
  ]
}
```

### Other Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/stats` | GET | Graph statistics (node/edge counts) |
| `/nodes?label=Company` | GET | Find nodes by label |

---

## SDKs

### TypeScript

```bash
cd sdks/typescript && npm install && npm run build
```

```typescript
import { AkalDB } from 'akaldb';

const db = new AkalDB('http://localhost:7420');

// Build the graph
const company = await db.addNode('Company', { name: 'Acme Corp' });
const complaint = await db.addNode('Complaint', { category: 'Ghosting' });
await db.addEdge(company.id, complaint.id, 'HAS_COMPLAINT');

// Query
const result = await db.query('FIND Company');
console.log(result.data);

// Reasoning path
const paths = await db.query('PATH Company -> Complaint -> Evidence');
```

### Python

```bash
cd sdks/python && pip install .
```

```python
from akaldb import AkalDB

db = AkalDB("http://localhost:7420")

# Build the graph
company = db.add_node("Company", {"name": "Acme Corp"})
complaint = db.add_node("Complaint", {"category": "Ghosting"})
db.add_edge(
    {"index": company["index"], "generation": 0},
    {"index": complaint["index"], "generation": 0},
    "HAS_COMPLAINT"
)

# Query
result = db.query("FIND Company")
print(result["data"])

# Async usage
from akaldb.client import AsyncAkalDB

async with AsyncAkalDB() as db:
    result = await db.query("PATH Company -> Complaint -> Evidence")
```

### Go

```go
import "github.com/akaldb/akaldb-go"

db := akaldb.New("http://localhost:7420")

// Build the graph
company, _ := db.AddNode("Company", map[string]interface{}{"name": "Acme Corp"})
complaint, _ := db.AddNode("Complaint", map[string]interface{}{"category": "Ghosting"})
db.AddEdge(company.ID, complaint.ID, "HAS_COMPLAINT", nil)

// Query
result, _ := db.Query("FIND Company")
nodes, _ := result.Nodes()
fmt.Println(nodes)

// Reasoning path
paths, _ := db.Query("PATH Company -> Complaint -> Evidence")
```

---

## Architecture

```
Client
   ↓
Natural Language Query
   ↓
LLM Translator
   ↓
CQL (Context Query Language)
   ↓
AkalDB Engine
   ↓
Graph Traversal
   ↓
Context Path Result
   ↓
LLM Response
```

The LLM does **translation and narration**.
The database performs **reasoning retrieval**.

### Internal Structure

```
MemoryStore
├── NodeStore         (generational arena — O(1) lookup)
├── EdgeStore         (generational arena — O(1) lookup)
├── AdjacencyMap      (SmallVec<[EdgeId; 8]> — cache-friendly)
├── StringInterner    (deduplicates labels/relations — 60-80% memory savings)
└── LabelIndex        (HashMap — O(1) FIND queries)
```

### Data Model

**Node** — an entity in the knowledge graph:

```
Node {
    id: u64
    label: string       ← "Company", "Complaint", "Evidence"
    properties: json    ← { name: "Company X", industry: "Tech" }
}
```

**Edge** — a directed relationship:

```
Edge {
    id: u64
    from: node_id
    to: node_id
    relation: string    ← "HAS_COMPLAINT", "HAS_EVIDENCE"
    timestamp: int64
    properties: json
}
```

---

## Performance

| Metric | Target | Achieved |
|--------|--------|----------|
| Binary size | < 30 MB | **1.3 MB** |
| Traversal latency | < 5 ms | ✅ (sub-millisecond for typical graphs) |
| Insert latency | < 10 ms | ✅ (O(1) amortized) |
| Node capacity | 100M+ | ✅ (limited by available memory) |

### Key Optimizations

- **Generational IDs** — O(1) node/edge lookup via array indexing, not HashMap
- **SmallVec\<[EdgeId; 8]\>** — adjacency lists stored inline (1 cache line) for nodes with ≤8 edges
- **String interning** — labels/relations stored as 4-byte IDs instead of 24-byte Strings
- **FxHashMap** — 2x faster hashing than std HashMap for integer keys
- **Zero-copy traversal** — adjacency lists return slices, no allocation during reads

---

## Project Structure

```
src/
├── main.rs                    # HTTP server (Phase 3)
├── lib.rs                     # Public API
├── types.rs                   # Core types (NodeId, EdgeId, StringInterner)
├── graph/
│   ├── node.rs                # Node storage engine
│   ├── edge.rs                # Edge storage engine
│   └── adjacency.rs           # Adjacency list structure
├── storage/
│   └── memory_store.rs        # Unified graph API + traversal
└── query/
    ├── ast.rs                 # CQL syntax tree
    ├── lexer.rs               # Tokenizer
    ├── parser.rs              # Recursive descent parser
    └── executor.rs            # Query execution pipeline
```

---

## Examples

```bash
# Phase 1: Core graph engine demo
cargo run --example basic_usage

# Phase 2: CQL query engine demo
cargo run --example cql_demo
```

---

## Use Cases

- **AI Knowledge Systems** — structured knowledge graphs for AI agents
- **Whistleblower Platforms** — track relationships between companies, complaints, and evidence
- **Labor Market Intelligence** — analyze worker-employer relationships
- **Document Reasoning** — connect contracts, clauses, amendments, and timelines

---

## Philosophy

AkalDB is not a document store. It is a **context engine**.

Instead of asking: *"Which text is similar?"*

The system answers: **"Which relationships explain the answer?"**

Reasoning becomes a **first-class database primitive**.

---

## License

MIT
