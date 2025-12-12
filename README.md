Chora Core Virtual Machine (chora-cvm)
======================================

This package implements the vNext Chora Core Virtual Machine as specified by the
Chora vNext DNA (Schema Specification v3.0):

- Event-sourced kernel backed by CR-SQLite
- Graph-interpreted Protocol execution
- Primitive registry bridged to Python functions

The kernel is intentionally minimal and defers all domain semantics to the
entity graph stored in the database.

