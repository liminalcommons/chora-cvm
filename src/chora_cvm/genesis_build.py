"""
Genesis Build: Wave 4 Build Integrity Protocols.

This module defines primitives and protocols for build integrity checking.
These are "allow-listed" primitives - specific tools (ruff, mypy, pytest)
rather than generic shell execution, following security guidelines.

Primitives defined:
- primitive-get-packages: List workspace packages
- primitive-run-lint: Run ruff linter
- primitive-run-typecheck: Run mypy
- primitive-run-tests: Run pytest
- primitive-check-build-integrity: Full build check with signal emission

Protocol defined:
- protocol-check-build-integrity: Graph-defined build integrity check
"""

from __future__ import annotations

from chora_cvm.schema import (
    PrimitiveData,
    PrimitiveEntity,
    ProtocolData,
    ProtocolEntity,
    ProtocolGraph,
    ProtocolInterface,
    ProtocolNode,
    ProtocolNodeKind,
    ProtocolEdge,
)
from chora_cvm.store import EventStore


def bootstrap_build_primitives(store: EventStore) -> list[str]:
    """
    Register build primitives needed for build integrity checking.

    Returns list of created primitive IDs.
    """
    created = []

    # primitive-get-packages
    prim_get_packages = PrimitiveEntity(
        id="primitive-get-packages",
        data=PrimitiveData(
            python_ref="chora_cvm.std.get_packages",
            description="List available packages in the workspace",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "workspace_path": {"type": "string"},
                    },
                    "required": [],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "packages": {"type": "array"},
                        "count": {"type": "integer"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim_get_packages)
    created.append(prim_get_packages.id)

    # primitive-run-lint (allow-listed: ruff only)
    prim_run_lint = PrimitiveEntity(
        id="primitive-run-lint",
        data=PrimitiveData(
            python_ref="chora_cvm.std.run_lint",
            description="Run ruff linter on a Python package (allow-listed)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                        "fix": {"type": "boolean"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim_run_lint)
    created.append(prim_run_lint.id)

    # primitive-run-typecheck (allow-listed: mypy only)
    prim_run_typecheck = PrimitiveEntity(
        id="primitive-run-typecheck",
        data=PrimitiveData(
            python_ref="chora_cvm.std.run_typecheck",
            description="Run mypy type checker on a Python package (allow-listed)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim_run_typecheck)
    created.append(prim_run_typecheck.id)

    # primitive-run-tests (allow-listed: pytest only)
    prim_run_tests = PrimitiveEntity(
        id="primitive-run-tests",
        data=PrimitiveData(
            python_ref="chora_cvm.std.run_tests",
            description="Run pytest on a Python package (allow-listed)",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "package_path": {"type": "string"},
                        "coverage": {"type": "boolean"},
                        "coverage_threshold": {"type": "integer"},
                    },
                    "required": ["package_path"],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean"},
                        "exit_code": {"type": "integer"},
                        "stdout": {"type": "string"},
                        "stderr": {"type": "string"},
                        "tool": {"type": "string"},
                        "package": {"type": "string"},
                        "coverage_met": {"type": "boolean"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim_run_tests)
    created.append(prim_run_tests.id)

    # primitive-check-build-integrity (composite)
    prim_check_build = PrimitiveEntity(
        id="primitive-check-build-integrity",
        data=PrimitiveData(
            python_ref="chora_cvm.std.check_build_integrity",
            description="Check build integrity across all packages with signal emission",
            interface={
                "inputs": {
                    "type": "object",
                    "properties": {
                        "workspace_path": {"type": "string"},
                        "db_path": {"type": "string"},
                        "emit_signals": {"type": "boolean"},
                    },
                    "required": [],
                },
                "outputs": {
                    "type": "object",
                    "properties": {
                        "healthy": {"type": "boolean"},
                        "packages_checked": {"type": "integer"},
                        "results": {"type": "object"},
                        "signals_emitted": {"type": "array"},
                    },
                },
            },
        ),
    )
    store.save_entity(prim_check_build)
    created.append(prim_check_build.id)

    return created


def bootstrap_protocol_check_build_integrity(store: EventStore) -> str:
    """
    Bootstrap protocol-check-build-integrity.

    This protocol runs the full build integrity check as a graph-defined workflow.
    """
    protocol = ProtocolEntity(
        id="protocol-check-build-integrity",
        data=ProtocolData(
            title="Check Build Integrity",
            description="Graph-defined protocol for checking build integrity across all packages",
            interface=ProtocolInterface(
                inputs={
                    "type": "object",
                    "properties": {
                        "workspace_path": {"type": "string"},
                        "db_path": {"type": "string"},
                        "emit_signals": {"type": "boolean"},
                    },
                    "required": [],
                },
                outputs={
                    "type": "object",
                    "properties": {
                        "healthy": {"type": "boolean"},
                        "packages_checked": {"type": "integer"},
                        "results": {"type": "object"},
                        "signals_emitted": {"type": "array"},
                    },
                },
            ),
            graph=ProtocolGraph(
                start="check_build",
                nodes={
                    "check_build": ProtocolNode(
                        kind=ProtocolNodeKind.CALL,
                        ref="primitive-check-build-integrity",
                        inputs={
                            "workspace_path": "$input.workspace_path",
                            "db_path": "$input.db_path",
                            "emit_signals": "$input.emit_signals",
                        },
                    ),
                    "end": ProtocolNode(
                        kind=ProtocolNodeKind.RETURN,
                        outputs={
                            "healthy": "$check_build.healthy",
                            "packages_checked": "$check_build.packages_checked",
                            "results": "$check_build.results",
                            "signals_emitted": "$check_build.signals_emitted",
                        },
                    ),
                },
                edges=[
                    ProtocolEdge(**{"from": "check_build", "to": "end"}),
                ],
            ),
        ),
    )
    store.save_entity(protocol)
    return protocol.id


def bootstrap_build_governance(store: EventStore, verbose: bool = True) -> dict:
    """
    Bootstrap all build governance primitives and protocols.

    Args:
        store: EventStore instance
        verbose: Print progress

    Returns:
        Summary of created entities
    """
    if verbose:
        print("[*] Bootstrapping Wave 4 Build Governance...")

    # Create primitives
    primitives = bootstrap_build_primitives(store)
    if verbose:
        for p in primitives:
            print(f"    + Primitive: {p}")

    # Create protocol
    protocol_id = bootstrap_protocol_check_build_integrity(store)
    if verbose:
        print(f"    + Protocol: {protocol_id}")

    if verbose:
        print("[*] Wave 4 Build Governance complete.")

    return {
        "primitives": primitives,
        "protocols": [protocol_id],
    }
