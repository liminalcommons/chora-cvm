"""
CVM HTTP API: The nervous system for the Ghost.

Exposes the CVM over HTTP so external clients (HUD, CLI, other agents)
can invoke protocols, query entities, and emit signals.

Run with: uvicorn chora_cvm.api:app --port 8000
"""

from __future__ import annotations

import os
import re
import sqlite3
import json
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .kernel.engine import CvmEngine
from .kernel.store import EventStore
from .std import create_focus, emit_signal, entities_query, fts_search, manifest_entity

# --- Configuration ---

DEFAULT_DB_PATH = os.environ.get("CHORA_DB", "chora-cvm.db")

# --- Pydantic Models ---


class InvokeRequest(BaseModel):
    """Request body for protocol invocation."""

    inputs: Dict[str, Any] = {}
    persona_id: Optional[str] = None


class CreateFocusRequest(BaseModel):
    """Request body for creating a Focus entity."""

    title: str
    description: Optional[str] = None
    signal_id: Optional[str] = None
    persona_id: Optional[str] = None


class EmitSignalRequest(BaseModel):
    """Request body for emitting a Signal entity."""

    title: str
    source_id: Optional[str] = None
    signal_type: str = "attention"
    urgency: str = "normal"
    description: Optional[str] = None


class OrientResponse(BaseModel):
    """Response from the orient endpoint."""

    entity_counts: Dict[str, int]
    total_entities: int
    active_focuses: List[Dict[str, Any]]
    recent_signals: List[Dict[str, Any]]
    recent_learnings: List[Dict[str, Any]]


class EntityResponse(BaseModel):
    """Response containing a single entity."""

    id: str
    type: str
    status: str
    data: Dict[str, Any]


class EntityListResponse(BaseModel):
    """Response containing a list of entities."""

    entities: List[Dict[str, Any]]
    count: int


class ToolSummary(BaseModel):
    """Summary of a tool entity for Command Palette integration."""

    id: str
    title: str
    handler: Optional[str] = None
    description: Optional[str] = None
    group: str = "CVM Tools"
    shortcut: Optional[str] = None


class ToolListResponse(BaseModel):
    """Response containing a list of tool summaries."""

    tools: List[ToolSummary]
    count: int


class ProtocolSummary(BaseModel):
    """Summary of a protocol entity for Command Palette integration."""

    id: str
    title: str
    description: Optional[str] = None
    group: str = "CVM Protocols"
    requires_inputs: bool = False


class ProtocolListResponse(BaseModel):
    """Response containing a list of protocol summaries."""

    protocols: List[ProtocolSummary]
    count: int


class CapabilitySummary(BaseModel):
    """Summary of a capability (protocol or primitive) for discovery."""

    id: str
    kind: str  # "protocol" or "primitive"
    description: str
    interface: Dict[str, Any] = {}


class CapabilityListResponse(BaseModel):
    """Response containing all available capabilities."""

    capabilities: List[CapabilitySummary]
    count: int


class CreateEntityRequest(BaseModel):
    """Request body for creating a new entity."""

    type: str
    id: Optional[str] = None  # If not provided, will be generated from title
    data: Dict[str, Any]


class PanelConfig(BaseModel):
    """Configuration for HUD panels."""

    context: bool = True
    events: bool = True
    signals: bool = False
    artifacts: bool = True
    workflows: bool = True


class LayoutResponse(BaseModel):
    """Response for layout configuration."""

    mode: str = "split"
    panels: PanelConfig


class LayoutUpdateRequest(BaseModel):
    """Request body for updating layout."""

    mode: Optional[str] = None
    panels: Optional[Dict[str, bool]] = None


# --- Voice Interpretation Models (Bicameral Mind) ---


class VoiceContext(BaseModel):
    """Context from the HUD for voice interpretation."""

    current_focus_id: Optional[str] = None
    active_panel: Optional[str] = None
    recent_actions: List[str] = []


class VoiceInterpretRequest(BaseModel):
    """Request body for voice interpretation."""

    transcript: str
    context: Optional[VoiceContext] = None
    session_id: Optional[str] = None


class VoiceAction(BaseModel):
    """An action to be taken by the HUD."""

    type: str  # "invoke_tool", "open_panel", "create_entity", "navigate", "none"
    target: Optional[str] = None
    data: Dict[str, Any] = {}


class VoiceInterpretResponse(BaseModel):
    """
    Response from voice interpretation.

    The Ghost's thinking made visible to the Shell.
    """

    intent: str  # "action_dispatch", "query", "unknown"
    confidence: float  # 0.0 to 1.0
    resolution_level: str  # "solid", "liquid", "gas", "plasma"
    thoughts: List[str]  # The Ghost's reasoning trace
    action: Optional[VoiceAction] = None
    speech: Optional[str] = None  # What the Ghost says back
    matched_tool_id: Optional[str] = None


# --- Constants ---

LAYOUT_ENTITY_ID = "pattern-hud-layout-default"
DEFAULT_LAYOUT = {
    "mode": "split",
    "panels": {
        "context": True,
        "events": True,
        "signals": False,
        "artifacts": True,
        "workflows": True,
    }
}


# --- Helper Functions ---


def slugify(text: str) -> str:
    """Convert text to a URL-friendly slug."""
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text)
    text = text.strip("-")
    return text


# --- FastAPI App ---

app = FastAPI(
    title="Chora CVM API",
    description="HTTP interface to the Chora Core Virtual Machine",
    version="0.1.0",
)

# CORS for HUD (localhost:3001)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001", "http://127.0.0.1:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_path() -> str:
    """Get the database path from environment or default."""
    return DEFAULT_DB_PATH


# --- Engine Singleton ---

_engine: Optional[CvmEngine] = None


def get_engine() -> CvmEngine:
    """Get or create the CvmEngine singleton."""
    global _engine
    if _engine is None:
        _engine = CvmEngine(get_db_path())
    return _engine


@app.on_event("shutdown")
async def shutdown_engine():
    """Clean up engine resources on shutdown."""
    global _engine
    if _engine:
        _engine.close()
        _engine = None


# --- Endpoints ---


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "chora-cvm"}


@app.get("/capabilities", response_model=CapabilityListResponse)
async def list_capabilities():
    """
    List all available capabilities (protocols + primitives).

    This is the discovery endpoint for Command Palettes and Agents.
    Returns everything the CVM can do, enabling dynamic tool discovery.
    """
    engine = get_engine()
    caps = engine.list_capabilities()

    return CapabilityListResponse(
        capabilities=[
            CapabilitySummary(
                id=c.id,
                kind=c.kind.value,
                description=c.description,
                interface=c.interface,
            )
            for c in caps
        ],
        count=len(caps),
    )


@app.get("/orient", response_model=OrientResponse)
async def orient():
    """
    Get system orientation — entity counts, active focuses, recent signals.

    This is the primary status endpoint for the HUD.
    """
    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Entity counts by type
        cur = conn.execute(
            "SELECT type, COUNT(*) as count FROM entities GROUP BY type ORDER BY count DESC"
        )
        entity_counts = {row["type"]: row["count"] for row in cur.fetchall()}
        total = sum(entity_counts.values())

        # Active focuses (status in data_json != resolved)
        cur = conn.execute("""
            SELECT id, data_json
            FROM entities
            WHERE type = 'focus'
              AND json_extract(data_json, '$.status') != 'resolved'
            ORDER BY id DESC
            LIMIT 10
        """)
        active_focuses = []
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            active_focuses.append({
                "id": row["id"],
                "title": data.get("title", ""),
                "status": data.get("status", "active"),
            })

        # Recent signals
        cur = conn.execute("""
            SELECT id, data_json
            FROM entities
            WHERE type = 'signal'
            ORDER BY id DESC
            LIMIT 5
        """)
        recent_signals = []
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            recent_signals.append({
                "id": row["id"],
                "title": data.get("title", ""),
                "urgency": data.get("urgency", "normal"),
            })

        # Recent learnings
        cur = conn.execute("""
            SELECT id, data_json
            FROM entities
            WHERE type = 'learning'
            ORDER BY id DESC
            LIMIT 5
        """)
        recent_learnings = []
        for row in cur.fetchall():
            data = json.loads(row["data_json"])
            recent_learnings.append({
                "id": row["id"],
                "title": data.get("title", ""),
            })

        conn.close()

        return OrientResponse(
            entity_counts=entity_counts,
            total_entities=total,
            active_focuses=active_focuses,
            recent_signals=recent_signals,
            recent_learnings=recent_learnings,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/entities/{entity_id}")
async def get_entity(entity_id: str):
    """Get a single entity by ID."""
    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT id, type, data_json FROM entities WHERE id = ?",
            (entity_id,),
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail=f"Entity not found: {entity_id}")

        data = json.loads(row["data_json"])
        return {
            "id": row["id"],
            "type": row["type"],
            "status": data.get("status", "active"),
            "data": data,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/entities")
async def list_entities(
    type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(50, description="Maximum entities to return"),
):
    """List entities, optionally filtered by type."""
    db_path = get_db_path()

    result = entities_query(db_path, entity_type=type, limit=limit)

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return {"entities": result.get("entities", []), "count": len(result.get("entities", []))}


@app.post("/entities", response_model=EntityResponse)
async def create_entity(request: CreateEntityRequest):
    """
    Create a new entity in the CVM.

    If no ID is provided, one will be generated from the title in the data.
    The entity type is prefixed to the ID (e.g., "tool-quick-note").
    """
    db_path = get_db_path()

    try:
        # Generate ID if not provided
        entity_id = request.id
        if not entity_id:
            title = request.data.get("title")
            if not title:
                raise HTTPException(
                    status_code=400,
                    detail="Either 'id' must be provided or 'data.title' must be set for ID generation"
                )
            entity_id = f"{request.type}-{slugify(title)}"

        # Create the entity
        result = manifest_entity(
            db_path=db_path,
            entity_type=request.type,
            entity_id=entity_id,
            data=request.data,
        )

        return EntityResponse(
            id=result["id"],
            type=result["type"],
            status="active",
            data=request.data,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/invoke/{intent}")
async def invoke(intent: str, request: InvokeRequest):
    """
    Execute a protocol or primitive by intent.

    Intent resolution (via CvmEngine):
    - "protocol-horizon" → exact match
    - "horizon" → resolves to "protocol-horizon"
    - "primitive-entity-get" → exact match
    - "entity_get" → resolves to "primitive-entity-get"

    This is the Unified Membrane — same engine as CLI.
    """
    engine = get_engine()

    result = engine.dispatch(
        intent=intent,
        inputs=request.inputs,
        persona_id=request.persona_id,
    )

    if not result.ok:
        raise HTTPException(
            status_code=400,
            detail={
                "error_kind": result.error_kind,
                "error_message": result.error_message,
            },
        )

    return result.to_dict()


@app.post("/focus")
async def create_focus_endpoint(request: CreateFocusRequest):
    """Create a Focus entity — declare what is being attended to."""
    db_path = get_db_path()

    result = create_focus(
        db_path=db_path,
        title=request.title,
        description=request.description,
        signal_id=request.signal_id,
        persona_id=request.persona_id,
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@app.post("/signal")
async def emit_signal_endpoint(request: EmitSignalRequest):
    """Emit a Signal entity — something demands attention."""
    db_path = get_db_path()

    result = emit_signal(
        db_path=db_path,
        title=request.title,
        source_id=request.source_id,
        signal_type=request.signal_type,
        urgency=request.urgency,
        description=request.description,
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


@app.get("/search")
async def search_entities(
    q: str = Query(..., description="Search query"),
    type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(20, description="Maximum results to return"),
):
    """Full-text search across entities."""
    db_path = get_db_path()

    result = fts_search(db_path, query=q, entity_type=type, limit=limit)

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return {"results": result.get("results", []), "count": len(result.get("results", []))}


@app.get("/tools", response_model=ToolListResponse)
async def list_tools(
    active_only: bool = Query(True, description="Only return active tools"),
    limit: int = Query(50, description="Maximum tools to return"),
):
    """
    List tool entities for Command Palette integration.

    Returns tools with their handler, description, and group for dynamic menu generation.
    Filters out internal tools unless specifically requested.
    """
    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Build query - filter active tools and exclude internal tools
        query = "SELECT id, type, data_json FROM entities WHERE type = 'tool'"
        params: List[Any] = []

        if active_only:
            # Tool is active if status is 'active' or status is not set
            query += " AND (json_extract(data_json, '$.status') = 'active' OR json_extract(data_json, '$.status') IS NULL)"

        # Exclude internal tools (those marked with internal: true)
        query += " AND (json_extract(data_json, '$.internal') IS NULL OR json_extract(data_json, '$.internal') != 1)"

        query += " ORDER BY id ASC LIMIT ?"
        params.append(limit)

        cur = conn.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        tools = []
        for row in rows:
            data = json.loads(row["data_json"])

            # Build description from fallback chain:
            # phenomenology → description → cognition.ready_at_hand
            description = data.get("phenomenology") or data.get("description")
            if not description and "cognition" in data:
                description = data["cognition"].get("ready_at_hand")

            tools.append(
                ToolSummary(
                    id=row["id"],
                    title=data.get("title", row["id"]),
                    handler=data.get("handler"),
                    description=description,
                    group=data.get("group", "CVM Tools"),
                    shortcut=data.get("shortcut"),
                )
            )

        return ToolListResponse(tools=tools, count=len(tools))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/protocols", response_model=ProtocolListResponse)
async def list_protocols(
    limit: int = Query(50, description="Maximum protocols to return"),
):
    """
    List protocol entities for Command Palette integration.

    Returns protocols with their title, description, and group for dynamic menu generation.
    Filters out internal protocols (marked with internal: true).
    Includes requires_inputs flag based on inputs_schema.
    """
    db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Build query - get protocol entities, exclude internal ones
        query = """
            SELECT id, type, data_json
            FROM entities
            WHERE type = 'protocol'
              AND (json_extract(data_json, '$.internal') IS NULL
                   OR json_extract(data_json, '$.internal') != 1)
            ORDER BY id ASC
            LIMIT ?
        """

        cur = conn.execute(query, (limit,))
        rows = cur.fetchall()
        conn.close()

        protocols = []
        for row in rows:
            data = json.loads(row["data_json"])

            # Build description from data
            description = data.get("description")

            # Get title, fall back to entity ID if None or missing
            title = data.get("title")
            if not title:
                title = row["id"]

            # Determine if protocol requires inputs
            # Check if inputs_schema has required fields
            requires_inputs = False
            inputs_schema = data.get("inputs_schema")
            if inputs_schema and isinstance(inputs_schema, dict):
                required = inputs_schema.get("required", [])
                if required and len(required) > 0:
                    requires_inputs = True

            # Get group, fall back to default
            group = data.get("group")
            if not group:
                group = "CVM Protocols"

            protocols.append(
                ProtocolSummary(
                    id=row["id"],
                    title=title,
                    description=description,
                    group=group,
                    requires_inputs=requires_inputs,
                )
            )

        return ProtocolListResponse(protocols=protocols, count=len(protocols))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Layout Endpoints (Stigmergic Layout) ---


def get_layout_data(db_path: str) -> dict:
    """Get current layout from database, or return default."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT data_json FROM entities WHERE id = ?",
        (LAYOUT_ENTITY_ID,),
    )
    row = cur.fetchone()
    conn.close()

    if row:
        return json.loads(row["data_json"])
    return DEFAULT_LAYOUT.copy()


def update_layout_data(db_path: str, updates: dict) -> dict:
    """Update layout entity with new values, creating if needed."""
    current = get_layout_data(db_path)

    # Apply updates
    if "mode" in updates and updates["mode"]:
        current["mode"] = updates["mode"]
    if "panels" in updates and updates["panels"]:
        for key, value in updates["panels"].items():
            if "panels" not in current:
                current["panels"] = DEFAULT_LAYOUT["panels"].copy()
            current["panels"][key] = value

    # Save to database
    manifest_entity(
        db_path=db_path,
        entity_type="pattern",
        entity_id=LAYOUT_ENTITY_ID,
        data=current,
    )
    return current


@app.get("/layout", response_model=LayoutResponse)
async def get_layout():
    """
    Get current HUD layout configuration.

    Returns the layout entity as the source of truth for panel states.
    The HUD can poll this endpoint to stay in sync with system state.
    """
    db_path = get_db_path()

    try:
        layout = get_layout_data(db_path)

        # Ensure panels dict exists
        panels = layout.get("panels", DEFAULT_LAYOUT["panels"])

        return LayoutResponse(
            mode=layout.get("mode", "split"),
            panels=PanelConfig(**panels),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/layout", response_model=LayoutResponse)
async def update_layout(request: LayoutUpdateRequest):
    """
    Update HUD layout configuration.

    Allows partial updates to mode and/or panel visibility.
    """
    db_path = get_db_path()

    try:
        updates = {}
        if request.mode:
            updates["mode"] = request.mode
        if request.panels:
            updates["panels"] = request.panels

        layout = update_layout_data(db_path, updates)
        panels = layout.get("panels", DEFAULT_LAYOUT["panels"])

        return LayoutResponse(
            mode=layout.get("mode", "split"),
            panels=PanelConfig(**panels),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Main entry point for direct execution ---

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
