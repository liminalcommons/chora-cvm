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

