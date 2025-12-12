"""
Legacy content harvester: Orchestrates discovery, parsing, and indexing.

Main entry point for harvesting content from legacy repositories.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Optional

from .config import RepoConfig, should_include_file, DEFAULT_EXCLUDES
from .dedup import DeduplicationEngine
from .parsers.markdown import MarkdownParser
from .parsers.yaml_parser import YAMLParser
from .parsers.python_parser import PythonParser
from .schema import init_legacy_db, get_db_stats


@dataclass
class HarvestResult:
    """Result of harvesting a single file."""

    doc_id: str
    filename: str
    title: str
    chunks: int
    lines: int
    content_type: str
    is_duplicate: bool = False


@dataclass
class RepoReport:
    """Report for harvesting a single repository."""

    name: str
    files_processed: int
    files_skipped: int
    chunks_created: int
    duplicates_found: int
    errors: list[str] = field(default_factory=list)


@dataclass
class HarvestReport:
    """Complete harvest report."""

    repos: list[RepoReport]
    total_files: int
    total_chunks: int
    total_duplicates: int
    duration_seconds: float


class LegacyHarvester:
    """Harvest content from legacy repositories into a searchable database."""

    def __init__(self, db_path: str, configs: list[RepoConfig], workspace_root: str):
        """
        Initialize the harvester.

        Args:
            db_path: Path to the SQLite database
            configs: List of repository configurations
            workspace_root: Root directory of the workspace
        """
        self.db_path = db_path
        self.configs = configs
        self.workspace_root = workspace_root
        self.conn: Optional[sqlite3.Connection] = None

        # Parsers
        self.markdown_parser = MarkdownParser()
        self.yaml_parser = YAMLParser()
        self.python_parser = PythonParser()

        # Deduplication engine
        self.dedup = DeduplicationEngine(
            {config.name: config.priority for config in configs}
        )

    def harvest_all(self) -> HarvestReport:
        """Harvest all configured repositories."""
        start_time = datetime.now()

        # Initialize database
        self.conn = init_legacy_db(self.db_path)

        repo_reports = []
        total_files = 0
        total_chunks = 0
        total_duplicates = 0

        for config in self.configs:
            print(f"\n[*] Harvesting {config.name}...")
            print(f"    Path: {config.path}")
            print(f"    Priority: {config.priority}")

            report = self.harvest_repo(config)
            repo_reports.append(report)

            total_files += report.files_processed
            total_chunks += report.chunks_created
            total_duplicates += report.duplicates_found

            print(f"    Files: {report.files_processed} ({report.files_skipped} skipped)")
            print(f"    Chunks: {report.chunks_created}")
            print(f"    Duplicates: {report.duplicates_found}")
            if report.errors:
                print(f"    Errors: {len(report.errors)}")

        self.conn.close()
        self.conn = None

        duration = (datetime.now() - start_time).total_seconds()

        return HarvestReport(
            repos=repo_reports,
            total_files=total_files,
            total_chunks=total_chunks,
            total_duplicates=total_duplicates,
            duration_seconds=duration,
        )

    def harvest_repo(self, config: RepoConfig) -> RepoReport:
        """Harvest a single repository."""
        repo_root = config.get_absolute_path(self.workspace_root)

        if not repo_root.exists():
            return RepoReport(
                name=config.name,
                files_processed=0,
                files_skipped=0,
                chunks_created=0,
                duplicates_found=0,
                errors=[f"Repository path not found: {repo_root}"],
            )

        # Register repository
        repo_id = f"repo-{config.name}"
        self._register_repo(repo_id, config, repo_root)

        # Discover files
        files_to_process = []
        files_skipped = 0

        for ext in config.extensions:
            for filepath in repo_root.rglob(f"*{ext}"):
                if not filepath.is_file():
                    continue

                # Check exclusions and includes
                if not should_include_file(filepath, config, repo_root):
                    files_skipped += 1
                    continue

                # Skip very large files
                try:
                    if filepath.stat().st_size > 5 * 1024 * 1024:  # 5MB
                        files_skipped += 1
                        continue
                except OSError:
                    files_skipped += 1
                    continue

                files_to_process.append(filepath)

        # Process files
        chunks_created = 0
        duplicates_found = 0
        errors = []

        for filepath in sorted(files_to_process):
            try:
                result = self._harvest_file(filepath, repo_id, config, repo_root)
                chunks_created += result.chunks
                if result.is_duplicate:
                    duplicates_found += 1
            except Exception as e:
                errors.append(f"{filepath.name}: {str(e)}")

        # Update repository stats
        self._update_repo_stats(repo_id, len(files_to_process))

        return RepoReport(
            name=config.name,
            files_processed=len(files_to_process),
            files_skipped=files_skipped,
            chunks_created=chunks_created,
            duplicates_found=duplicates_found,
            errors=errors,
        )

    def _register_repo(self, repo_id: str, config: RepoConfig, repo_root: Path) -> None:
        """Register a repository in the database."""
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO repositories (id, name, path, description, priority, last_harvested_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                description=excluded.description,
                priority=excluded.priority,
                last_harvested_at=excluded.last_harvested_at
            """,
            (
                repo_id,
                config.name,
                str(repo_root),
                config.description,
                config.priority,
                datetime.now().isoformat(),
            ),
        )
        self.conn.commit()

    def _update_repo_stats(self, repo_id: str, file_count: int) -> None:
        """Update repository statistics."""
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE repositories
            SET total_files = ?
            WHERE id = ?
            """,
            (file_count, repo_id),
        )
        self.conn.commit()

    def _harvest_file(
        self, filepath: Path, repo_id: str, config: RepoConfig, repo_root: Path
    ) -> HarvestResult:
        """Harvest a single file."""
        relative_path = filepath.relative_to(repo_root)
        doc_id = f"doc-{sha256((repo_id + str(relative_path)).encode()).hexdigest()[:12]}"

        # Read content
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        lines = content.split("\n")
        line_count = len(lines)
        size_bytes = len(content.encode("utf-8"))

        # Determine content type and parse
        content_type = self._get_content_type(filepath)
        chunks = self._parse_content(content, filepath, content_type)

        # Get title
        title = self._extract_title(content, filepath.name, content_type)

        # Check for duplicates
        content_hash, is_duplicate_of = self.dedup.process_document(
            doc_id, repo_id, content
        )

        # Insert document
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO documents (
                id, repository_id, filename, relative_path, absolute_path,
                content_type, title, size_bytes, line_count, content_hash,
                is_duplicate_of, harvested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                size_bytes=excluded.size_bytes,
                line_count=excluded.line_count,
                content_hash=excluded.content_hash,
                is_duplicate_of=excluded.is_duplicate_of,
                harvested_at=excluded.harvested_at
            """,
            (
                doc_id,
                repo_id,
                filepath.name,
                str(relative_path),
                str(filepath),
                content_type,
                title,
                size_bytes,
                line_count,
                content_hash,
                is_duplicate_of,
                datetime.now().isoformat(),
            ),
        )

        # Delete existing chunks (for re-harvest)
        cur.execute("DELETE FROM chunks WHERE document_id = ?", (doc_id,))
        try:
            cur.execute("DELETE FROM chunks_fts WHERE document_id = ?", (doc_id,))
        except sqlite3.OperationalError:
            pass

        # Insert chunks (only if not duplicate)
        chunk_count = 0
        if not is_duplicate_of:
            for i, chunk in enumerate(chunks):
                chunk_id = f"{doc_id}-chunk-{i:03d}"

                cur.execute(
                    """
                    INSERT INTO chunks (
                        id, document_id, section_title, content,
                        line_start, line_end, chunk_type, heading_level, metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        doc_id,
                        chunk.title,
                        chunk.content,
                        chunk.line_start,
                        chunk.line_end,
                        chunk.chunk_type,
                        chunk.heading_level,
                        json.dumps(chunk.metadata) if chunk.metadata else None,
                    ),
                )

                # Index in FTS
                try:
                    cur.execute(
                        """
                        INSERT INTO chunks_fts (id, document_id, section_title, content, chunk_type)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (chunk_id, doc_id, chunk.title, chunk.content, chunk.chunk_type),
                    )
                except sqlite3.OperationalError:
                    pass

                # Insert tags
                tags = chunk.metadata.get("tags", {})
                for tag_type, tag_values in tags.items():
                    for tag_value in tag_values:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO tags (chunk_id, tag, value)
                            VALUES (?, ?, ?)
                            """,
                            (chunk_id, tag_type, tag_value),
                        )

                chunk_count += 1

        self.conn.commit()

        return HarvestResult(
            doc_id=doc_id,
            filename=filepath.name,
            title=title,
            chunks=chunk_count,
            lines=line_count,
            content_type=content_type,
            is_duplicate=bool(is_duplicate_of),
        )

    def _get_content_type(self, filepath: Path) -> str:
        """Determine content type from file extension."""
        ext = filepath.suffix.lower()
        if ext in (".md", ".markdown"):
            return "markdown"
        elif ext in (".yaml", ".yml"):
            return "yaml"
        elif ext == ".py":
            return "python"
        else:
            return "unknown"

    def _parse_content(self, content: str, filepath: Path, content_type: str) -> list:
        """Parse content using appropriate parser."""
        if content_type == "markdown":
            return self.markdown_parser.parse(content, filepath)
        elif content_type == "yaml":
            return self.yaml_parser.parse(content, filepath)
        elif content_type == "python":
            return self.python_parser.parse(content, filepath)
        else:
            return []

    def _extract_title(self, content: str, filename: str, content_type: str) -> str:
        """Extract document title."""
        if content_type == "markdown":
            return self.markdown_parser.extract_title(content, filename)
        elif content_type == "yaml":
            return self.yaml_parser.extract_title(content, filename)
        elif content_type == "python":
            return self.python_parser.extract_title(content, filename)
        else:
            return filename


def search_legacy(db_path: str, query: str, limit: int = 20) -> list[dict]:
    """
    Search the legacy content database.

    Args:
        db_path: Path to the database
        query: FTS5 search query
        limit: Maximum results to return

    Returns:
        List of search results with document and chunk info
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                c.id as chunk_id,
                c.section_title,
                c.chunk_type,
                substr(c.content, 1, 300) as snippet,
                d.id as doc_id,
                d.filename,
                d.relative_path,
                d.title as doc_title,
                r.name as repo_name
            FROM chunks_fts
            JOIN chunks c ON chunks_fts.id = c.id
            JOIN documents d ON c.document_id = d.id
            JOIN repositories r ON d.repository_id = r.id
            WHERE chunks_fts MATCH ?
            AND d.is_duplicate_of IS NULL
            ORDER BY rank
            LIMIT ?
            """,
            (query, limit),
        )

        results = []
        for row in cur.fetchall():
            results.append(dict(row))

        return results

    finally:
        conn.close()
