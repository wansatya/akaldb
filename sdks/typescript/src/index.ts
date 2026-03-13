/**
 * AkalDB TypeScript SDK
 *
 * A lightweight client for the AkalDB reasoning database.
 * Uses native fetch (Node 18+) — zero runtime dependencies.
 *
 * @example
 * ```typescript
 * import { AkalDB } from 'akaldb';
 *
 * const db = new AkalDB('http://localhost:7420');
 *
 * // Insert nodes
 * const company = await db.addNode('Company', { name: 'Acme Corp' });
 * const complaint = await db.addNode('Complaint', { category: 'Ghosting' });
 *
 * // Insert edge
 * await db.addEdge(company.id, complaint.id, 'HAS_COMPLAINT');
 *
 * // Query with CQL
 * const result = await db.query('FIND Company WHERE name = "Acme Corp"');
 * console.log(result.data);
 *
 * // Path query for reasoning
 * const paths = await db.query('PATH Company -> Complaint -> Evidence');
 * console.log(paths.data);
 * ```
 */

// =============================================================================
// Types
// =============================================================================

/** A node identifier returned by the server. */
export interface NodeId {
  index: number;
  generation: number;
}

/** A node as returned by insert operations. */
export interface NodeRef {
  id: NodeId;
  label: string;
}

/** A node in query results. */
export interface NodeResult {
  id: string;
  label: string;
  properties: Record<string, unknown>;
}

/** A step in a reasoning path. */
export interface PathStep {
  relation: string;
  target: NodeResult;
}

/** A complete reasoning path. */
export interface PathResult {
  root: NodeResult;
  steps: PathStep[];
}

/** Result from a FIND query. */
export interface NodesQueryResult {
  type: 'nodes';
  count: number;
  data: NodeResult[];
}

/** Result from a PATH query. */
export interface PathsQueryResult {
  type: 'paths';
  count: number;
  data: PathResult[];
}

/** Result from a GROUP BY query. */
export interface GroupedQueryResult {
  type: 'grouped';
  count: number;
  data: Record<string, NodeResult[]>;
}

/** Any query result type. */
export type QueryResult = NodesQueryResult | PathsQueryResult | GroupedQueryResult;

/** Edge insert response. */
export interface EdgeRef {
  id: string;
  from: string;
  to: string;
  relation: string;
}

/** Graph statistics. */
export interface Stats {
  nodes: number;
  edges: number;
  labels: number;
  interned_strings: number;
}

/** Health check response. */
export interface Health {
  status: string;
  version: string;
}

/** Error response from the server. */
export class AkalDBError extends Error {
  public readonly statusCode: number;
  public readonly body: unknown;

  constructor(message: string, statusCode: number, body?: unknown) {
    super(message);
    this.name = 'AkalDBError';
    this.statusCode = statusCode;
    this.body = body;
  }
}

// =============================================================================
// Client
// =============================================================================

/**
 * AkalDB client for interacting with the reasoning database.
 *
 * All methods are async and use native fetch (Node 18+, all modern browsers).
 * No external dependencies required.
 */
export class AkalDB {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;

  /**
   * Create a new AkalDB client.
   *
   * @param url - Base URL of the AkalDB server (default: http://localhost:7420)
   * @param options - Optional configuration
   */
  constructor(
    url: string = 'http://localhost:7420',
    options: { headers?: Record<string, string> } = {}
  ) {
    // Remove trailing slash
    this.baseUrl = url.replace(/\/+$/, '');
    this.headers = {
      'Content-Type': 'application/json',
      ...options.headers,
    };
  }

  // ===========================================================================
  // Query API — the core interface from SPECS.md
  // ===========================================================================

  /**
   * Execute a CQL query against the database.
   *
   * This is the primary API — all SPECS.md query types are supported:
   * - `FIND Company`
   * - `FIND Complaint WHERE category = Ghosting`
   * - `FIND Company COUNT complaints > 3`
   * - `FIND Worker WHERE work_hours > 50 GROUP BY industry`
   * - `PATH Company -> Complaint -> Evidence`
   *
   * @param cql - The CQL query string
   * @returns Query results (nodes, paths, or groups)
   *
   * @example
   * ```typescript
   * const result = await db.query('FIND Company WHERE industry = Tech');
   * if (result.type === 'nodes') {
   *   for (const node of result.data) {
   *     console.log(node.label, node.properties);
   *   }
   * }
   * ```
   */
  async query(cql: string): Promise<QueryResult> {
    return this.post<QueryResult>('/query', { cql });
  }

  // ===========================================================================
  // Node Operations
  // ===========================================================================

  /**
   * Add a node to the knowledge graph.
   *
   * @param label - Node type/category (e.g., "Company", "Person")
   * @param properties - Arbitrary JSON properties
   * @returns Reference to the created node
   *
   * @example
   * ```typescript
   * const node = await db.addNode('Company', { name: 'Acme', industry: 'Tech' });
   * console.log(node.id); // { index: 0, generation: 0 }
   * ```
   */
  async addNode(label: string, properties: Record<string, unknown> = {}): Promise<NodeRef> {
    const res = await this.post<{ index: number; generation: number; label: string }>('/nodes', {
      label,
      properties,
    });
    return {
      id: { index: res.index, generation: res.generation },
      label: res.label,
    };
  }

  /**
   * Find all nodes with a given label.
   *
   * @param label - The node label to search for
   * @returns Array of matching nodes
   */
  async findByLabel(label: string): Promise<{ count: number; data: NodeResult[] }> {
    return this.get(`/nodes?label=${encodeURIComponent(label)}`);
  }

  // ===========================================================================
  // Edge Operations
  // ===========================================================================

  /**
   * Add a directed edge between two nodes.
   *
   * @param from - Source node ID
   * @param to - Target node ID
   * @param relation - Relationship type (e.g., "HAS_COMPLAINT")
   * @param properties - Optional edge properties
   * @returns Reference to the created edge
   *
   * @example
   * ```typescript
   * const company = await db.addNode('Company', { name: 'Acme' });
   * const complaint = await db.addNode('Complaint', { category: 'Ghosting' });
   * await db.addEdge(company.id, complaint.id, 'HAS_COMPLAINT');
   * ```
   */
  async addEdge(
    from: NodeId,
    to: NodeId,
    relation: string,
    properties: Record<string, unknown> = {}
  ): Promise<EdgeRef> {
    return this.post<EdgeRef>('/edges', {
      from: { index: from.index, generation: from.generation },
      to: { index: to.index, generation: to.generation },
      relation,
      properties,
    });
  }

  // ===========================================================================
  // System
  // ===========================================================================

  /** Get graph statistics (node count, edge count, etc.). */
  async stats(): Promise<Stats> {
    return this.get<Stats>('/stats');
  }

  /** Health check — returns server status and version. */
  async health(): Promise<Health> {
    return this.get<Health>('/health');
  }

  // ===========================================================================
  // HTTP helpers
  // ===========================================================================

  private async get<T>(path: string): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: 'GET',
      headers: this.headers,
    });
    return this.handleResponse<T>(res);
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: this.headers,
      body: JSON.stringify(body),
    });
    return this.handleResponse<T>(res);
  }

  private async handleResponse<T>(res: globalThis.Response): Promise<T> {
    const body = await res.json();
    if (!res.ok) {
      throw new AkalDBError(
        body.error || `HTTP ${res.status}`,
        res.status,
        body
      );
    }
    return body as T;
  }
}

// Default export for convenience
export default AkalDB;
