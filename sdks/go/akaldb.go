// Package akaldb provides a Go client for the AkalDB reasoning database.
//
// AkalDB is an installable reasoning database that stores knowledge as a context
// graph rather than flat vectors or tables. This SDK provides a complete client for
// the AkalDB HTTP API.
//
// Usage:
//
//	db := akaldb.New("http://localhost:7420")
//
//	// Insert nodes
//	company, _ := db.AddNode("Company", map[string]interface{}{"name": "Acme Corp"})
//	complaint, _ := db.AddNode("Complaint", map[string]interface{}{"category": "Ghosting"})
//
//	// Insert edge
//	db.AddEdge(company.ID, complaint.ID, "HAS_COMPLAINT", nil)
//
//	// CQL query
//	result, _ := db.Query(`FIND Company WHERE name = "Acme Corp"`)
//	fmt.Println(result.Data)
//
//	// Path query — the core reasoning operation
//	paths, _ := db.Query("PATH Company -> Complaint -> Evidence")
//	fmt.Println(paths.Data)
package akaldb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// =============================================================================
// Types
// =============================================================================

// NodeID identifies a node in the AkalDB graph.
// Uses generational indexing for safe references.
type NodeID struct {
	Index      uint32 `json:"index"`
	Generation uint32 `json:"generation"`
}

// NodeRef is returned when a node is created.
type NodeRef struct {
	ID         NodeID `json:"-"`
	IDStr      string `json:"id"`
	Label      string `json:"label"`
	Index      uint32 `json:"index"`
	Generation uint32 `json:"generation"`
}

// NodeResult is a node returned in query results.
type NodeResult struct {
	ID         string                 `json:"id"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
}

// PathStep is a single step in a reasoning path.
type PathStep struct {
	Relation string     `json:"relation"`
	Target   NodeResult `json:"target"`
}

// PathResult is a complete reasoning path.
type PathResult struct {
	Root  NodeResult `json:"root"`
	Steps []PathStep `json:"steps"`
}

// QueryResult holds the results of a CQL query.
// The Data field type depends on the Type field:
//   - "nodes":   Data is []NodeResult
//   - "paths":   Data is []PathResult
//   - "grouped": Data is map[string][]NodeResult
type QueryResult struct {
	Type  string          `json:"type"`
	Count int             `json:"count"`
	Data  json.RawMessage `json:"data"`
}

// Nodes extracts node results from a "nodes" type query result.
func (qr *QueryResult) Nodes() ([]NodeResult, error) {
	if qr.Type != "nodes" {
		return nil, fmt.Errorf("query result type is '%s', not 'nodes'", qr.Type)
	}
	var nodes []NodeResult
	err := json.Unmarshal(qr.Data, &nodes)
	return nodes, err
}

// Paths extracts path results from a "paths" type query result.
func (qr *QueryResult) Paths() ([]PathResult, error) {
	if qr.Type != "paths" {
		return nil, fmt.Errorf("query result type is '%s', not 'paths'", qr.Type)
	}
	var paths []PathResult
	err := json.Unmarshal(qr.Data, &paths)
	return paths, err
}

// Groups extracts grouped results from a "grouped" type query result.
func (qr *QueryResult) Groups() (map[string][]NodeResult, error) {
	if qr.Type != "grouped" {
		return nil, fmt.Errorf("query result type is '%s', not 'grouped'", qr.Type)
	}
	var groups map[string][]NodeResult
	err := json.Unmarshal(qr.Data, &groups)
	return groups, err
}

// EdgeRef is returned when an edge is created.
type EdgeRef struct {
	ID       string `json:"id"`
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
}

// Stats holds graph statistics.
type Stats struct {
	Nodes           int `json:"nodes"`
	Edges           int `json:"edges"`
	Labels          int `json:"labels"`
	InternedStrings int `json:"interned_strings"`
}

// Health holds health check response.
type Health struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// FindResult holds the result of a find-by-label query.
type FindResult struct {
	Count int          `json:"count"`
	Data  []NodeResult `json:"data"`
}

// Error is returned when the server responds with an error.
type Error struct {
	StatusCode int
	Message    string
	Body       json.RawMessage
}

func (e *Error) Error() string {
	return fmt.Sprintf("akaldb: %s (HTTP %d)", e.Message, e.StatusCode)
}

// =============================================================================
// Client
// =============================================================================

// Client is the AkalDB SDK client.
type Client struct {
	baseURL    string
	httpClient *http.Client
	headers    map[string]string
}

// Option configures the Client.
type Option func(*Client)

// WithTimeout sets the HTTP client timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		c.httpClient.Timeout = d
	}
}

// WithHeaders sets additional HTTP headers.
func WithHeaders(h map[string]string) Option {
	return func(c *Client) {
		for k, v := range h {
			c.headers[k] = v
		}
	}
}

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// New creates a new AkalDB client.
//
// Default URL: http://localhost:7420
// Default timeout: 30 seconds
func New(url string, opts ...Option) *Client {
	url = strings.TrimRight(url, "/")
	c := &Client{
		baseURL:    url,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		headers:    map[string]string{"Content-Type": "application/json"},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// =============================================================================
// Query API — the core interface from SPECS.md
// =============================================================================

// Query executes a CQL query against the database.
//
// This is the primary API. All SPECS.md query types are supported:
//   - FIND Company
//   - FIND Complaint WHERE category = Ghosting
//   - FIND Company COUNT complaints > 3
//   - FIND Worker WHERE work_hours > 50 GROUP BY industry
//   - PATH Company -> Complaint -> Evidence
func (c *Client) Query(cql string) (*QueryResult, error) {
	var result QueryResult
	err := c.post("/query", map[string]string{"cql": cql}, &result)
	return &result, err
}

// =============================================================================
// Node Operations
// =============================================================================

// AddNode adds a node to the knowledge graph.
func (c *Client) AddNode(label string, properties map[string]interface{}) (*NodeRef, error) {
	if properties == nil {
		properties = map[string]interface{}{}
	}
	body := map[string]interface{}{
		"label":      label,
		"properties": properties,
	}
	var ref NodeRef
	err := c.post("/nodes", body, &ref)
	if err == nil {
		ref.ID = NodeID{Index: ref.Index, Generation: ref.Generation}
	}
	return &ref, err
}

// FindByLabel finds all nodes with a given label.
func (c *Client) FindByLabel(label string) (*FindResult, error) {
	var result FindResult
	err := c.get(fmt.Sprintf("/nodes?label=%s", url.QueryEscape(label)), &result)
	return &result, err
}

// =============================================================================
// Edge Operations
// =============================================================================

// AddEdge adds a directed edge between two nodes.
func (c *Client) AddEdge(from, to NodeID, relation string, properties map[string]interface{}) (*EdgeRef, error) {
	if properties == nil {
		properties = map[string]interface{}{}
	}
	body := map[string]interface{}{
		"from":       map[string]interface{}{"index": from.Index, "generation": from.Generation},
		"to":         map[string]interface{}{"index": to.Index, "generation": to.Generation},
		"relation":   relation,
		"properties": properties,
	}
	var ref EdgeRef
	err := c.post("/edges", body, &ref)
	return &ref, err
}

// =============================================================================
// System
// =============================================================================

// Stats returns graph statistics.
func (c *Client) Stats() (*Stats, error) {
	var stats Stats
	err := c.get("/stats", &stats)
	return &stats, err
}

// Health performs a health check.
func (c *Client) Health() (*Health, error) {
	var health Health
	err := c.get("/health", &health)
	return &health, err
}

// =============================================================================
// HTTP helpers
// =============================================================================

func (c *Client) get(path string, target interface{}) error {
	req, err := http.NewRequest("GET", c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("akaldb: failed to create request: %w", err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return c.doRequest(req, target)
}

func (c *Client) post(path string, body interface{}, target interface{}) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("akaldb: failed to marshal body: %w", err)
	}
	req, err := http.NewRequest("POST", c.baseURL+path, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("akaldb: failed to create request: %w", err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return c.doRequest(req, target)
}

func (c *Client) doRequest(req *http.Request, target interface{}) error {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("akaldb: request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("akaldb: failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errBody struct {
			ErrorMsg string `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &errBody)
		msg := errBody.ErrorMsg
		if msg == "" {
			msg = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return &Error{
			StatusCode: resp.StatusCode,
			Message:    msg,
			Body:       json.RawMessage(bodyBytes),
		}
	}

	if target != nil {
		if err := json.Unmarshal(bodyBytes, target); err != nil {
			return fmt.Errorf("akaldb: failed to unmarshal response: %w", err)
		}
	}
	return nil
}
