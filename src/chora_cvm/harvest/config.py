"""
Repository configurations for legacy content harvesting.

Each RepoConfig defines:
- name: Identifier for the repository
- path: Relative path from workspace root
- priority: Higher = prefer for deduplication (10 = highest)
- description: Human-readable description
- extensions: File types to index
- excludes: Patterns to skip
- include_patterns: Optional whitelist (if set, only these paths are indexed)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class RepoConfig:
    """Configuration for a repository to harvest."""

    name: str
    path: str  # Relative to workspace root
    priority: int
    description: str
    extensions: list[str] = field(default_factory=lambda: [".md", ".yaml", ".yml"])
    excludes: list[str] = field(default_factory=list)
    include_patterns: Optional[list[str]] = None

    def get_absolute_path(self, workspace_root: str) -> Path:
        """Get absolute path to this repository."""
        return Path(workspace_root) / self.path


# Default exclusion patterns (applied to all repos)
DEFAULT_EXCLUDES = [
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "*.pyc",
    ".DS_Store",
    ".coverage",
    "htmlcov",
    "dist",
    "build",
    "*.egg-info",
]


def get_archive_repo_configs() -> list[RepoConfig]:
    """Get configurations for archive repositories (v3, v4, v5 implementations)."""
    return [
        RepoConfig(
            name="archive-v4-store",
            path="archive/v4/chora-store",
            priority=6,  # Lower than active packages
            description="v4 chora-store implementation (physics engine, 3827 lines)",
            extensions=[".md", ".py", ".feature"],
            excludes=[
                *DEFAULT_EXCLUDES,
            ],
            include_patterns=[
                "src/**/*.py",  # Core implementation
                "tests/features/*.feature",  # BDD specs
                "docs/**/*.md",  # Documentation
                "CLAUDE.md",
                "AGENTS.md",
                "README.md",
            ],
        ),
        RepoConfig(
            name="archive-v4-starship",
            path="archive/v4/chora-starship",
            priority=6,
            description="v4 orchestration layer (17 modules)",
            extensions=[".md", ".py", ".feature"],
            excludes=[*DEFAULT_EXCLUDES],
            include_patterns=[
                "src/**/*.py",
                "tests/features/*.feature",
                "*.md",
            ],
        ),
        RepoConfig(
            name="archive-v5",
            path="archive/v5",
            priority=5,  # Transition code, lower priority
            description="v5 transition architecture",
            extensions=[".md", ".py", ".feature"],
            excludes=[*DEFAULT_EXCLUDES],
        ),
    ]


def get_legacy_repo_configs() -> list[RepoConfig]:
    """Get configurations for all legacy repositories."""
    return [
        RepoConfig(
            name="chora-base",
            path="packages/chora-base",
            priority=10,
            description="SAP framework and patterns (49 SAPs, 245 navigation files)",
            extensions=[".md", ".yaml", ".yml", ".py"],
            excludes=[
                *DEFAULT_EXCLUDES,
                "templates/",  # Template files with placeholders
                "static-template/",  # Jinja templates
                "examples/",  # Example projects
            ],
            include_patterns=[
                "docs/skilled-awareness/**/*",  # SAP artifacts
                "docs/**/*.md",  # Other docs
                "AGENTS.md",
                "CLAUDE.md",
                "README.md",
                "scripts/**/*.py",  # Python automation
                "src/**/*.py",  # Python source
            ],
        ),
        RepoConfig(
            name="chora-workspace",
            path="packages/chora-workspace",
            priority=8,
            description="Active development workspace (50+ capabilities, 49 features)",
            extensions=[".md", ".yaml", ".yml", ".py"],
            excludes=[
                *DEFAULT_EXCLUDES,
                "k8s/",  # K8s configs (too noisy)
                "packages/",  # Submodules handled separately
                # Skip large pre-made exports
                "CHORA_COMPOSE_EXPORT.md",
                "REPOSITORY_EXPORT.md",
                "repomix-*.md",
            ],
            include_patterns=[
                ".chora/capabilities/**/*",
                ".chora/features/**/*",
                ".chora/standards/**/*",
                "docs/research/**/*",
                "docs/**/*.md",
                "scripts/**/*.py",
                "AGENTS.md",
                "CLAUDE.md",
                "CAPABILITIES.md",
                "README.md",
            ],
        ),
        RepoConfig(
            name="chora-workspace-old2",
            path="packages/chora-workspace-old2",
            priority=10,  # Higher than old - prefer refactored content
            description="Refactored workspace (80% complexity reduction)",
            extensions=[".md", ".yaml", ".yml", ".py"],
            excludes=[
                *DEFAULT_EXCLUDES,
                ".beads.backup*",
                "packages/",  # Nested package references
            ],
            # No include_patterns - index everything (it's already clean)
        ),
        RepoConfig(
            name="chora-workspace-old",
            path="packages/chora-workspace-old",
            priority=5,  # Lower - only for unique historical content
            description="Historical workspace (archive, unique research)",
            extensions=[".md", ".yaml", ".yml"],  # No Python - too much noise
            excludes=[
                *DEFAULT_EXCLUDES,
                "site/",  # Duplicate of docs/
                "test-integration/",
                "test-integration-all/",
                "test-integration-final/",
                "test-integration-local/",
                ".beads.backup*",
                "packages/",  # Submodules
                "chora-workspace/",  # Nested workspace
            ],
            include_patterns=[
                # Only unique content not in old2
                "docs/research/khora/**/*",
                "docs/research/graphiti/**/*",
                "docs/wardley/**/*",
                ".chora/intention-graph/**/*",
                # Core docs (in case unique versions)
                "CLAUDE.md",
                "AGENTS.md",
                "README.md",
            ],
        ),
    ]


def should_include_file(
    filepath: Path, repo_config: RepoConfig, repo_root: Path
) -> bool:
    """Check if a file should be included based on repo config."""
    relative_path = filepath.relative_to(repo_root)
    relative_str = str(relative_path)

    # Check exclusions first
    for exclude in repo_config.excludes:
        if exclude.endswith("/"):
            # Directory exclusion
            if relative_str.startswith(exclude) or f"/{exclude}" in f"/{relative_str}":
                return False
        elif "*" in exclude:
            # Glob pattern
            import fnmatch

            if fnmatch.fnmatch(filepath.name, exclude):
                return False
            if fnmatch.fnmatch(relative_str, exclude):
                return False
        else:
            # Exact match or prefix
            if relative_str == exclude or relative_str.startswith(exclude):
                return False
            if filepath.name == exclude:
                return False

    # Check include patterns if specified
    if repo_config.include_patterns:
        import fnmatch

        for pattern in repo_config.include_patterns:
            if fnmatch.fnmatch(relative_str, pattern):
                return True
            # Handle ** glob
            if "**" in pattern:
                pattern_parts = pattern.split("**")
                if len(pattern_parts) == 2:
                    prefix, suffix = pattern_parts
                    prefix = prefix.rstrip("/")
                    suffix = suffix.lstrip("/")
                    if relative_str.startswith(prefix) and (
                        not suffix or relative_str.endswith(suffix) or fnmatch.fnmatch(filepath.name, suffix.lstrip("*"))
                    ):
                        return True
        return False

    return True
