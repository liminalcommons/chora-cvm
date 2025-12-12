# Chora CVM Agent Instructions

You have access to a Chora CVM instance via HTTP API. This document is your self-contained manual for interacting with the system.

---

## API Reference

### Base URL

```
http://localhost:8000
```

### Discovery

**GET /capabilities**

Returns all available capabilities (protocols + primitives). Use this to discover what the system can do.

```bash
curl http://localhost:8000/capabilities
```

Response:
```json
{
  "capabilities": [
    {
      "id": "protocol-orient",
      "kind": "protocol",
      "description": "Get system orientation",
      "interface": {}
    },
    {
      "id": "primitive-manifest-entity",
      "kind": "primitive",
      "description": "Create an entity",
      "interface": {"required": ["entity_type", "entity_id", "data"]}
    }
  ],
  "count": 42
}
```

### Invocation

**POST /invoke/{intent}**

Execute a protocol or primitive by intent. The intent can be:
- Full ID: `"protocol-orient"` or `"primitive-manifest-entity"`
- Short name: `"orient"` resolves to `"protocol-orient"`
- Underscore variant: `"manifest_entity"` resolves to `"primitive-manifest-entity"`

```bash
curl -X POST http://localhost:8000/invoke/orient \
  -H "Content-Type: application/json" \
  -d '{"inputs": {}, "persona_id": null}'
```

Response on success:
```json
{
  "ok": true,
  "data": { ... }
}
```

Response on error:
```json
{
  "detail": {
    "error_kind": "intent_not_found",
    "error_message": "Could not resolve intent: unknown-thing"
  }
}
```

### Convenience Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/orient` | GET | System orientation (entity counts, focuses, signals) |
| `/capabilities` | GET | All available protocols + primitives |
| `/entities` | GET | Query entities (optional `?type=` filter) |
| `/entities/{id}` | GET | Get single entity by ID |
| `/entities` | POST | Create new entity |
| `/search?q=` | GET | Full-text search |
| `/focus` | POST | Create a Focus entity |
| `/signal` | POST | Emit a Signal entity |

---

## The Physics

The CVM implements a graph-based computation model. Understanding the physics helps you work with the system effectively.

### The Decemvirate (10 Entity Types)

| Type | Physics | Role | Question |
|------|---------|------|----------|
| **inquiry** | gas | potential | What is it like when...? |
| **learning** | radiation | feedback | What happened? |
| **principle** | crystal | truth | What is always true? |
| **pattern** | lattice | blueprint | How is it shaped? |
| **story** | clarity | desire | What do we want? |
| **behavior** | solid | expectation | What should happen? |
| **tool** | field | affordance | What can be done? |
| **signal** | impulse | interrupt | What demands attention? |
| **focus** | plasma | energy | What now? |
| **relationship** | force | connection | What connects? |

### Entity IDs

Entity IDs follow the pattern: `{type}-{slug}`

Examples:
- `learning-discovered-pattern-x`
- `behavior-user-can-login`
- `tool-quick-note`
- `focus-current-task`

### Bonds (12 Forces)

Entities connect through typed bonds:

| Verb | From → To | Meaning |
|------|-----------|---------|
| **yields** | inquiry → learning | Exploration produces insight |
| **surfaces** | learning → principle | Insight reveals truth |
| **induces** | learning → pattern | Insight suggests shape |
| **governs** | principle → pattern | Truth constrains form |
| **clarifies** | principle → story | Truth informs desire |
| **structures** | pattern → behavior | Form shapes expectation |
| **specifies** | story → behavior | Desire defines expectation |
| **implements** | behavior → tool | Expectation guides capability |
| **verifies** | tool → behavior | Capability proves expectation |
| **addresses** | focus → signal | Attention responds to interrupt |
| **relates** | relationship → * | Connection binds entities |

### The Generative Chain

Work flows through bonds:

```
Inquiry ──yields──> Learning ──surfaces──> Principle
   │                    │                     │
   │                induces              clarifies
   ▼                    ▼                     │
 (Gas)              Pattern <──governs────────┤
                        │                     │
                   structures                 │
                        ▼                     ▼
Tool <──implements── Behavior <──specifies── Story
   │                    ↑
   └────verifies────────┘  (The Tension Loop)
```

**A Story is stable ONLY if the Behaviors it specifies are verified by Tools.**

---

## Common Protocols

| Intent | Purpose | Inputs |
|--------|---------|--------|
| `orient` | Get system orientation | none |
| `digest` | Transform entity into learning | `entity_id` |
| `induce` | Propose pattern from learnings | `learning_ids` |
| `horizon` | View upcoming work | `days` (optional) |

---

## Workflow Examples

### Creating a Learning

```bash
# Create the entity
curl -X POST http://localhost:8000/entities \
  -H "Content-Type: application/json" \
  -d '{
    "type": "learning",
    "data": {
      "title": "Discovered that X improves Y",
      "insight": "When doing X, Y became more efficient"
    }
  }'

# Response includes the generated ID
# {"id": "learning-discovered-that-x-improves-y", ...}
```

### Checking System State

```bash
# Get orientation
curl http://localhost:8000/orient

# Response:
# {
#   "entity_counts": {"learning": 42, "behavior": 15, ...},
#   "total_entities": 127,
#   "active_focuses": [...],
#   "recent_signals": [...],
#   "recent_learnings": [...]
# }
```

### Invoking a Protocol

```bash
# Run the orient protocol
curl -X POST http://localhost:8000/invoke/orient \
  -H "Content-Type: application/json" \
  -d '{"inputs": {}}'
```

---

## Error Handling

Errors return HTTP 400 with structured detail:

```json
{
  "detail": {
    "error_kind": "intent_not_found",
    "error_message": "Could not resolve intent: foo"
  }
}
```

Common error kinds:
- `intent_not_found` - The intent could not be resolved to a protocol or primitive
- `primitive_not_found` - The primitive is not registered
- `execution_error` - The protocol/primitive failed during execution

---

## The Parable of the Mute Graph

The CVM's primitives (like `manifest_entity`, `manage_bond`, `fts_search`) are not Python ceremony — they are vocabulary for a graph that cannot speak Python.

The Protocol VM cannot call `len()` or `sum()`; it needs reified verbs. When you invoke `primitive-list-sum`, you're speaking the graph's language.

We accept the ceremony to gain the sovereignty.

---

*This document is the "SDK" for LLM agents interacting with Chora CVM.*
