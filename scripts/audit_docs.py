#!/usr/bin/env python3
"""
Documentation Health Audit for Chora Workspace

Checks:
1. CLAUDE.md presence in packages (submodules)
2. AGENTS.md consistency with root
3. Research docs that might want integration
4. Stale references (paths that don't exist)
5. Documentation coverage across the hierarchy

Usage:
    python3 packages/chora-cvm/scripts/audit_docs.py [--verbose]
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Any


def find_packages(root: Path) -> List[Path]:
    """Find all packages (direct children of packages/)."""
    packages_dir = root / "packages"
    if not packages_dir.exists():
        return []

    packages = []
    for item in packages_dir.iterdir():
        if item.is_dir() and not item.name.startswith('.'):
            # Skip old/archived workspaces for primary audit
            if "old" not in item.name.lower():
                packages.append(item)
    return sorted(packages)


def check_claude_md(package: Path) -> Dict[str, Any]:
    """Check CLAUDE.md status for a package."""
    claude_path = package / "CLAUDE.md"
    agents_path = package / "AGENTS.md"

    result = {
        "name": package.name,
        "has_claude_md": claude_path.exists(),
        "has_agents_md": agents_path.exists(),
        "claude_size": 0,
        "agents_size": 0,
        "stale_refs": [],
    }

    if claude_path.exists():
        result["claude_size"] = claude_path.stat().st_size
        result["stale_refs"] = check_stale_refs(claude_path, package)

    if agents_path.exists():
        result["agents_size"] = agents_path.stat().st_size

    return result


def check_stale_refs(doc_path: Path, package: Path) -> List[str]:
    """Find references to paths that don't exist."""
    stale = []
    try:
        content = doc_path.read_text()

        # Find path-like references (backticks or quotes around paths)
        path_patterns = [
            r'`([a-zA-Z0-9_/.-]+\.(py|md|yaml|json|ts|js))`',
            r'`(packages/[a-zA-Z0-9_/-]+)`',
            r'`(src/[a-zA-Z0-9_/-]+)`',
        ]

        for pattern in path_patterns:
            for match in re.finditer(pattern, content):
                ref_path = match.group(1)
                # Check relative to package or workspace root
                full_path = package / ref_path
                root_path = package.parent.parent / ref_path

                if not full_path.exists() and not root_path.exists():
                    stale.append(ref_path)
    except Exception:
        pass

    return stale


def find_research_docs(root: Path) -> List[Dict[str, Any]]:
    """Find research documents that might want integration."""
    research_dir = root / "docs" / "research"
    if not research_dir.exists():
        return []

    docs = []
    for item in research_dir.rglob("*"):
        if item.is_file() and item.suffix in ('.md', '.txt', '.yaml'):
            rel_path = item.relative_to(root)
            size = item.stat().st_size

            # Detect research type from path/name
            research_type = "general"
            path_str = str(rel_path).lower()
            if "inquiry" in path_str:
                research_type = "inquiry"
            elif "brief" in path_str:
                research_type = "brief"
            elif "ar-" in path_str:
                research_type = "architecture-research"
            elif "synthesis" in path_str or "response" in path_str:
                research_type = "synthesis"

            docs.append({
                "path": str(rel_path),
                "name": item.name,
                "size": size,
                "type": item.suffix,
                "research_type": research_type,
            })

    return sorted(docs, key=lambda x: x["size"], reverse=True)


def detect_evolution_signals(root: Path) -> List[Dict[str, Any]]:
    """Detect signals that documentation might want to evolve."""
    signals = []

    # Read main docs for reference
    claude_content = ""
    agents_content = ""
    claude_path = root / "CLAUDE.md"
    agents_path = root / "AGENTS.md"

    if claude_path.exists():
        claude_content = claude_path.read_text()
    if agents_path.exists():
        agents_content = agents_path.read_text()

    # 1. Check for outdated noun counts anywhere
    stale_noun_patterns = [
        ("7 Nouns", "seven nouns"),
        ("8 Nouns", "eight nouns"),  # Also outdated
    ]

    for exact, lower in stale_noun_patterns:
        if agents_content:
            if exact in agents_content or lower in agents_content.lower():
                signals.append({
                    "file": "AGENTS.md",
                    "signal": f"References '{exact}' but system evolved to Decemvirate (10)",
                    "suggestion": "Verify noun count reflects current physics",
                })

    # 2. Check for unsurfaced inquiries in research
    research_dir = root / "docs" / "research"
    if research_dir.exists():
        for inq in research_dir.glob("inquiry-*.md"):
            inq_name = inq.stem.replace("inquiry-", "")
            # Check if mentioned anywhere in main docs
            if inq_name not in claude_content.lower() and inq_name not in agents_content.lower():
                signals.append({
                    "file": str(inq.relative_to(root)),
                    "signal": f"Inquiry '{inq_name}' not surfaced in main docs",
                    "suggestion": "Consider if this inquiry informs current work",
                })
            # Also check for stale physics references within inquiry
            try:
                content = inq.read_text()
                if "7 Nouns" in content or "(7 Nouns)" in content:
                    signals.append({
                        "file": str(inq.relative_to(root)),
                        "signal": "Contains stale '7 Nouns' reference (now Decemvirate: 10)",
                        "suggestion": "Update physics references if inquiry is still active",
                    })
            except Exception:
                pass

    # 3. Check for briefs with README that might represent completed research
    briefs_dir = research_dir / "briefs" if research_dir.exists() else None
    if briefs_dir and briefs_dir.exists():
        for brief_dir in briefs_dir.iterdir():
            if brief_dir.is_dir():
                readme = brief_dir / "README.md"
                if readme.exists():
                    content = readme.read_text()
                    if "outcome" in content.lower() or "decision" in content.lower():
                        signals.append({
                            "file": str(readme.relative_to(root)),
                            "signal": f"Brief '{brief_dir.name}' has outcome - may want integration",
                            "suggestion": "Review if decisions should update main docs",
                        })

    return signals


def check_root_docs(root: Path) -> Dict[str, Any]:
    """Check root-level documentation."""
    return {
        "claude_md": (root / "CLAUDE.md").exists(),
        "claude_md_size": (root / "CLAUDE.md").stat().st_size if (root / "CLAUDE.md").exists() else 0,
        "agents_md": (root / "AGENTS.md").exists(),
        "agents_md_size": (root / "AGENTS.md").stat().st_size if (root / "AGENTS.md").exists() else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Audit documentation health")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument("--check", action="store_true", help="Exit 1 if issues found")
    args = parser.parse_args()

    root = Path(__file__).parent.parent.parent.parent

    print("Documentation Health Audit")
    print("=" * 60)
    print()

    # Root docs
    root_docs = check_root_docs(root)
    print("Root Documentation")
    print("-" * 40)
    print(f"  CLAUDE.md: {'✓' if root_docs['claude_md'] else '✗'} ({root_docs['claude_md_size']:,} bytes)")
    print(f"  AGENTS.md: {'✓' if root_docs['agents_md'] else '✗'} ({root_docs['agents_md_size']:,} bytes)")
    print()

    # Package docs
    packages = find_packages(root)
    print(f"Package Documentation ({len(packages)} packages)")
    print("-" * 40)

    with_claude = 0
    without_claude = []
    stale_total = 0

    for pkg in packages:
        status = check_claude_md(pkg)
        if status["has_claude_md"]:
            with_claude += 1
            marker = "✓"
            if status["stale_refs"]:
                marker = "⚠"
                stale_total += len(status["stale_refs"])
        else:
            marker = "✗"
            without_claude.append(status["name"])

        if args.verbose or not status["has_claude_md"]:
            size_str = f"({status['claude_size']:,} bytes)" if status["has_claude_md"] else ""
            print(f"  {marker} {status['name']:<30} {size_str}")
            if args.verbose and status["stale_refs"]:
                for ref in status["stale_refs"][:3]:
                    print(f"      └─ stale: {ref}")

    print()
    print(f"  Coverage: {with_claude}/{len(packages)} ({100*with_claude//len(packages)}%)")

    if without_claude:
        print(f"\n  Missing CLAUDE.md:")
        for name in without_claude:
            print(f"    - {name}")

    if stale_total > 0:
        print(f"\n  Stale references found: {stale_total}")

    # Research docs
    research = find_research_docs(root)
    if research:
        print()
        print(f"Research Documents ({len(research)} files)")
        print("-" * 40)

        total_size = sum(d["size"] for d in research)
        print(f"  Total: {total_size:,} bytes across {len(research)} files")

        if args.verbose:
            print("\n  Largest documents (potential integration candidates):")
            for doc in research[:10]:
                print(f"    {doc['size']:>8,} bytes  {doc['path']}")
        else:
            # Show just top 5
            print("\n  Top research documents:")
            for doc in research[:5]:
                print(f"    {doc['size']:>8,} bytes  {doc['name']}")

    # Evolution Signals
    signals = detect_evolution_signals(root)
    if signals:
        print()
        print(f"Evolution Signals ({len(signals)} detected)")
        print("-" * 40)
        for sig in signals:
            print(f"  ⚡ {sig['file']}")
            print(f"     Signal: {sig['signal']}")
            print(f"     Suggestion: {sig['suggestion']}")
            print()

    # Summary
    print()
    print("Summary")
    print("-" * 40)

    issues = []
    if without_claude:
        issues.append(f"{len(without_claude)} packages missing CLAUDE.md")
    if stale_total > 0:
        issues.append(f"{stale_total} stale path references")
    if signals:
        issues.append(f"{len(signals)} evolution signals")

    if issues:
        print("  Issues found:")
        for issue in issues:
            print(f"    ⚠ {issue}")
    else:
        print("  ✓ No documentation issues found")

    if args.check and issues:
        sys.exit(1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
