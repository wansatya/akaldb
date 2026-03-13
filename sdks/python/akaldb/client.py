"""
AkalDB Python SDK

A client library for the AkalDB reasoning database.
Supports both synchronous and asynchronous usage via httpx.

Usage:
    from akaldb import AkalDB

    db = AkalDB("http://localhost:7420")

    # Insert nodes
    company = db.add_node("Company", {"name": "Acme Corp"})
    complaint = db.add_node("Complaint", {"category": "Ghosting"})

    # Insert edge
    db.add_edge(company["id"], complaint["id"], "HAS_COMPLAINT")

    # CQL query
    result = db.query('FIND Company WHERE name = "Acme Corp"')
    print(result["data"])

    # Path query — the core reasoning operation
    paths = db.query("PATH Company -> Complaint -> Evidence")
    for path in paths["data"]:
        print(path["root"]["label"], "->", [s["target"]["label"] for s in path["steps"]])
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import httpx


# =============================================================================
# Types
# =============================================================================


@dataclass
class NodeId:
    """Identifier for a node in the AkalDB graph."""

    index: int
    generation: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {"index": self.index, "generation": self.generation}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeId":
        return cls(index=data["index"], generation=data.get("generation", 0))


class AkalDBError(Exception):
    """Error returned by the AkalDB server."""

    def __init__(self, message: str, status_code: int, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# =============================================================================
# Client
# =============================================================================


class AkalDB:
    """
    Client for the AkalDB reasoning database.

    Provides both synchronous and asynchronous APIs. The sync API uses
    httpx.Client internally. For async usage, use AkalDB.async_client().

    Args:
        url: Base URL of the AkalDB server (default: http://localhost:7420)
        timeout: Request timeout in seconds (default: 30)
        headers: Additional HTTP headers to send with each request
    """

    def __init__(
        self,
        url: str = "http://localhost:7420",
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers={"Content-Type": "application/json", **(headers or {})},
        )

    def close(self):
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # =========================================================================
    # Query API — the core interface from SPECS.md
    # =========================================================================

    def query(self, cql: str) -> Dict[str, Any]:
        """
        Execute a CQL query against the database.

        This is the primary API. All SPECS.md query types are supported:
        - FIND Company
        - FIND Complaint WHERE category = Ghosting
        - FIND Company COUNT complaints > 3
        - FIND Worker WHERE work_hours > 50 GROUP BY industry
        - PATH Company -> Complaint -> Evidence

        Args:
            cql: The CQL query string

        Returns:
            Dict with keys: type, count, data

        Example:
            >>> result = db.query("FIND Company WHERE industry = Tech")
            >>> for node in result["data"]:
            ...     print(node["label"], node["properties"])
        """
        return self._post("/query", {"cql": cql})

    # =========================================================================
    # Node Operations
    # =========================================================================

    def add_node(
        self,
        label: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Add a node to the knowledge graph.

        Args:
            label: Node type/category (e.g., "Company", "Person")
            properties: Arbitrary JSON properties

        Returns:
            Dict with id, label, index, generation

        Example:
            >>> node = db.add_node("Company", {"name": "Acme", "industry": "Tech"})
            >>> print(node["index"])  # 0
        """
        return self._post("/nodes", {
            "label": label,
            "properties": properties or {},
        })

    def find_by_label(self, label: str) -> Dict[str, Any]:
        """
        Find all nodes with a given label.

        Args:
            label: The node label to search for

        Returns:
            Dict with count and data (list of nodes)
        """
        return self._get(f"/nodes?label={label}")

    # =========================================================================
    # Edge Operations
    # =========================================================================

    def add_edge(
        self,
        from_id: Union[NodeId, Dict[str, int]],
        to_id: Union[NodeId, Dict[str, int]],
        relation: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Add a directed edge between two nodes.

        Args:
            from_id: Source node ID (NodeId or dict with index/generation)
            to_id: Target node ID (NodeId or dict with index/generation)
            relation: Relationship type (e.g., "HAS_COMPLAINT")
            properties: Optional edge properties

        Returns:
            Dict with id, from, to, relation

        Example:
            >>> company = db.add_node("Company", {"name": "Acme"})
            >>> complaint = db.add_node("Complaint", {"category": "Ghosting"})
            >>> db.add_edge(
            ...     {"index": company["index"], "generation": company["generation"]},
            ...     {"index": complaint["index"], "generation": complaint["generation"]},
            ...     "HAS_COMPLAINT"
            ... )
        """
        from_dict = from_id.to_dict() if isinstance(from_id, NodeId) else from_id
        to_dict = to_id.to_dict() if isinstance(to_id, NodeId) else to_id

        return self._post("/edges", {
            "from": from_dict,
            "to": to_dict,
            "relation": relation,
            "properties": properties or {},
        })

    # =========================================================================
    # System
    # =========================================================================

    def stats(self) -> Dict[str, Any]:
        """Get graph statistics (node count, edge count, etc.)."""
        return self._get("/stats")

    def health(self) -> Dict[str, Any]:
        """Health check — returns server status and version."""
        return self._get("/health")

    # =========================================================================
    # HTTP helpers
    # =========================================================================

    def _get(self, path: str) -> Dict[str, Any]:
        response = self._client.get(path)
        return self._handle_response(response)

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        response = self._client.post(path, json=body)
        return self._handle_response(response)

    def _handle_response(self, response: httpx.Response) -> Dict[str, Any]:
        data = response.json()
        if response.status_code >= 400:
            error_msg = data.get("error", f"HTTP {response.status_code}")
            raise AkalDBError(error_msg, response.status_code, data)
        return data


# =============================================================================
# Async Client
# =============================================================================


class AsyncAkalDB:
    """
    Async client for the AkalDB reasoning database.

    Uses httpx.AsyncClient for non-blocking I/O.

    Usage:
        async with AsyncAkalDB("http://localhost:7420") as db:
            result = await db.query("FIND Company")
    """

    def __init__(
        self,
        url: str = "http://localhost:7420",
        timeout: float = 30.0,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers={"Content-Type": "application/json", **(headers or {})},
        )

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def query(self, cql: str) -> Dict[str, Any]:
        """Execute a CQL query (async)."""
        return await self._post("/query", {"cql": cql})

    async def add_node(self, label: str, properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add a node (async)."""
        return await self._post("/nodes", {"label": label, "properties": properties or {}})

    async def add_edge(
        self,
        from_id: Union[NodeId, Dict[str, int]],
        to_id: Union[NodeId, Dict[str, int]],
        relation: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Add an edge (async)."""
        from_dict = from_id.to_dict() if isinstance(from_id, NodeId) else from_id
        to_dict = to_id.to_dict() if isinstance(to_id, NodeId) else to_id
        return await self._post("/edges", {
            "from": from_dict, "to": to_dict, "relation": relation, "properties": properties or {},
        })

    async def find_by_label(self, label: str) -> Dict[str, Any]:
        """Find nodes by label (async)."""
        return await self._get(f"/nodes?label={label}")

    async def stats(self) -> Dict[str, Any]:
        """Get graph statistics (async)."""
        return await self._get("/stats")

    async def health(self) -> Dict[str, Any]:
        """Health check (async)."""
        return await self._get("/health")

    async def _get(self, path: str) -> Dict[str, Any]:
        response = await self._client.get(path)
        return self._handle_response(response)

    async def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        response = await self._client.post(path, json=body)
        return self._handle_response(response)

    def _handle_response(self, response: httpx.Response) -> Dict[str, Any]:
        data = response.json()
        if response.status_code >= 400:
            raise AkalDBError(data.get("error", f"HTTP {response.status_code}"), response.status_code, data)
        return data
