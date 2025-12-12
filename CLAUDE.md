# Chora Core Virtual Machine (CVM)

**Package**: chora-cvm
**Type**: Event-Sourced Graph VM with Self-Teaching Capabilities
**Last Updated**: 2025-12-10

---

## Quick Start for Claude

The CVM is the living kernel of the Chora system. It executes protocols, stores entities, and can teach about its own usage. This is where computation meets meaning.

**Key directories**:
- `src/chora_cvm/` - Core VM implementation
- `tests/features/` - BDD feature files
- `tests/step_defs/` - pytest-bdd step definitions
- `scripts/` - Tooling (audit, coverage, etc.)

**Databases** (in workspace root):
- `chora-cvm-manifest.db` - The Loom: entity store with Decemvirate physics
- `chora-cvm.db` - Genesis db for testing

---

## Core Patterns (Source of Truth)

| Concept | Source of Truth | Notes |
|---------|----------------|-------|
| **Entity Schema** | `schema.py` | Pydantic models for all entity types |
| **Event Store** | `store.py` | SQLite + JSON1 + FTS layer |
| **VM Execution** | `vm.py` | Protocol graph interpreter |
| **Primitives** | `lib/*.py` + `std.py` | 39 primitives across 8 domains (Strangler Fig migration) |
| **CLI Interface** | `cli.py` | Click-based command interface |
| **BDD Tests** | `tests/features/*.feature` | Gherkin scenarios |
| **Step Definitions** | `tests/step_defs/test_*.py` | pytest-bdd implementations |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         CLI Layer                           │
│  cli.py, cloud_cli.py                                       │
├─────────────────────────────────────────────────────────────┤
│                      Semantic Layer                         │
│  semantic.py (embeddings, search, suggestions)              │
├─────────────────────────────────────────────────────────────┤
│                      Standard Library                       │
│  std.py (primitives: manifest, bond, focus, signal, etc.)   │
├─────────────────────────────────────────────────────────────┤
│                         VM Layer                            │
│  vm.py (protocol graph execution)                           │
├─────────────────────────────────────────────────────────────┤
│                       Store Layer                           │
│  store.py (EventStore: SQLite + JSON1 + FTS)                │
├─────────────────────────────────────────────────────────────┤
│                      Schema Layer                           │
│  schema.py (Pydantic entity models)                         │
└─────────────────────────────────────────────────────────────┘
```

### The Crystal Palace (Primitive Library)

Primitives live in `src/chora_cvm/lib/` organized by domain:

| Domain | Purpose | Example Primitives |
|--------|---------|-------------------|
| attention | Focus, signal management | create_focus, emit_signal |
| build | Construction, assembly | build_protocol |
| chronos | Time, rhythm, seasons | get_kairotic_moment |
| cognition | Thinking, reasoning | surface_by_context |
| graph | Entity, bond operations | get_constellation |
| io | Input/output, storage | sqlite_query, fts_search |
| logic | Computation, comparison | list_sum, math_add |
| sys | System utilities | log, get_env |

**The Parable of the Mute Graph**: These primitives are not Python ceremony — they are vocabulary for a graph that cannot speak Python. The Protocol VM cannot call `len()` or `sum()`; it needs reified verbs. We accept the ceremony to gain the sovereignty.

---

## The Decemvirate (10 Entity Types)

The CVM implements physics for 10 noun types arranged in complementary pairs:

| Pair | Noun A | Noun B | Physics |
|------|--------|--------|---------|
| **SOURCE** | inquiry | signal | gas / impulse |
| **WISDOM** | learning | principle | radiation / crystal |
| **FORM** | pattern | story | lattice / clarity |
| **REALITY** | behavior | tool | solid / field |
| **BINDING** | focus | relationship | plasma / force |

---

## Key Functions

Functions are migrating from `std.py` to `lib/` (Strangler Fig pattern). For the latest primitive inventory, see `lib/*.py`.

### Entity Lifecycle
- `manifest_entity()` - Create entity in store
- `manifest_entities()` - Batch create
- `entities_query()` - Query with filters

### Bonding (12 Forces)
- `manage_bond()` - Create/update bonds
- `get_constellation()` - Tension network around entity
- `get_provenance_chain()` - Upstream lineage

### Attention Layer
- `create_focus()` - Declare what matters now
- `resolve_focus()` - Close attention loop
- `emit_signal()` - Demand attention
- `list_active_focuses()` - Current attention

### Self-Teaching
- `teach_scan_usage()` - Analyze usage patterns
- `surface_by_context()` - Find relevant entities
- `teach_format()` - Render explanations

### Infrastructure
- `sqlite_query()` - Raw SQL access
- `fts_search()` - Full-text search
- `fts_index_entity()` - Index for search

---

## Testing Discipline

BDD-first development with pytest-bdd:

```bash
# Run all BDD tests
pytest tests/step_defs/ -v

# Run specific feature
pytest tests/step_defs/test_focus.py -v

# Check integrity (behaviors -> tests)
just integrity
```

### Feature File Pattern

```gherkin
# tests/features/example.feature
@behavior:example-behavior-id
Scenario: Description of expected behavior
  Given precondition
  When action occurs
  Then expected outcome
```

### Step Definition Pattern

```python
# tests/step_defs/test_example.py
from pytest_bdd import given, when, then, scenarios, parsers
scenarios("../features/example.feature")

@given("precondition")
def setup_precondition(db_path, test_context):
    # setup code
    pass

@when("action occurs")
def perform_action(db_path, test_context):
    # action code
    pass

@then("expected outcome")
def verify_outcome(db_path, test_context):
    # assertion code
    pass
```

---

## Invitation & Crypto Layer

For secure circle collaboration:

- `invitation.py` - Zero-friction invitation via GitHub SSH keys
- `keyring.py` - Local keyring management
- `sync_router.py` - Multi-backend sync routing

```bash
# Create invitation
just invite <circle> <github-username>

# Accept invitation
just accept <circle>
```

---

## Audit Commands

```bash
# Code coverage (behaviors -> tools, primitives -> entities)
just audit

# Documentation health
just audit-docs

# Full audit
just audit-all
```

---

## Genesis (Bootstrapping)

To bootstrap a fresh CVM database:

```bash
python genesis.py [db_path]
```

This creates foundational primitives and protocols:
- `primitive-sys-log` - Logging
- `primitive-manifest-entity` - Entity creation
- `protocol-hello-world` - Example protocol
- `protocol-manifest-entity` - Manifestation protocol

---

## Integration Points

### With chora-inference (optional)
- Semantic embeddings for entity similarity
- Bond suggestions based on meaning
- Graceful degradation when unavailable

### With justfile (workspace root)
- All `just` commands route through CVM primitives
- Entity queries, bonding, orientation
