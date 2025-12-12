"""
YAML parser: Extract structured content from YAML files.

Handles:
- Single-document YAML files
- Multi-document YAML (separated by ---)
- Capability files (.chora/capabilities/)
- Feature files (.chora/features/)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class Chunk:
    """A chunk of content extracted from a document."""

    title: str
    content: str
    line_start: int
    line_end: int
    chunk_type: str = "yaml-document"
    heading_level: Optional[int] = None
    metadata: dict = field(default_factory=dict)


class YAMLParser:
    """Parse YAML files into chunks."""

    def parse(self, content: str, filepath: Path) -> list[Chunk]:
        """Parse YAML content into chunks."""
        chunks = []

        # Detect file type from path
        file_type = self._detect_file_type(filepath)

        # Split multi-document YAML
        documents = self._split_documents(content)

        for i, (doc_content, start_line, end_line) in enumerate(documents):
            try:
                data = yaml.safe_load(doc_content)
                if data is None:
                    continue

                # Create chunk based on file type
                if file_type == "capability":
                    chunk = self._parse_capability(data, doc_content, start_line, end_line)
                elif file_type == "feature":
                    chunk = self._parse_feature(data, doc_content, start_line, end_line)
                else:
                    chunk = self._parse_generic(data, doc_content, start_line, end_line, i)

                if chunk:
                    chunks.append(chunk)

            except yaml.YAMLError:
                # If YAML parsing fails, create a raw chunk
                chunks.append(
                    Chunk(
                        title=f"Document {i + 1}",
                        content=doc_content,
                        line_start=start_line,
                        line_end=end_line,
                        chunk_type="yaml-raw",
                    )
                )

        return chunks

    def _detect_file_type(self, filepath: Path) -> str:
        """Detect the type of YAML file from its path."""
        path_str = str(filepath)

        if "/capabilities/" in path_str or "capability" in filepath.stem:
            return "capability"
        elif "/features/" in path_str or "feature" in filepath.stem:
            return "feature"
        elif "/standards/" in path_str:
            return "standard"
        else:
            return "generic"

    def _split_documents(self, content: str) -> list[tuple[str, int, int]]:
        """Split multi-document YAML into individual documents."""
        documents = []
        lines = content.split("\n")
        current_doc_lines = []
        current_start = 1

        for i, line in enumerate(lines, 1):
            if line.strip() == "---" and current_doc_lines:
                # End current document
                doc_content = "\n".join(current_doc_lines).strip()
                if doc_content:
                    documents.append((doc_content, current_start, i - 1))
                current_doc_lines = []
                current_start = i + 1
            else:
                current_doc_lines.append(line)

        # Don't forget last document
        if current_doc_lines:
            doc_content = "\n".join(current_doc_lines).strip()
            if doc_content:
                documents.append((doc_content, current_start, len(lines)))

        return documents if documents else [(content, 1, len(lines))]

    def _parse_capability(
        self, data: dict, raw_content: str, start_line: int, end_line: int
    ) -> Optional[Chunk]:
        """Parse a capability YAML file."""
        if not isinstance(data, dict):
            return None

        # Extract key fields
        cap_id = data.get("id", data.get("name", "unknown"))
        name = data.get("name", cap_id)
        description = data.get("description", "")
        ontology = data.get("ontology", {})

        # Build searchable content
        content_parts = [
            f"Capability: {name}",
            f"ID: {cap_id}",
        ]

        if description:
            content_parts.append(f"Description: {description}")

        if ontology:
            content_parts.append(f"Ontology: {yaml.dump(ontology, default_flow_style=False)}")

        # Include handlers if present
        handlers = data.get("handlers", [])
        if handlers:
            content_parts.append(f"Handlers: {', '.join(str(h) for h in handlers)}")

        return Chunk(
            title=f"Capability: {name}",
            content="\n".join(content_parts),
            line_start=start_line,
            line_end=end_line,
            chunk_type="yaml-capability",
            metadata={
                "capability_id": cap_id,
                "name": name,
                "ontology": ontology,
                "raw": data,
            },
        )

    def _parse_feature(
        self, data: dict, raw_content: str, start_line: int, end_line: int
    ) -> Optional[Chunk]:
        """Parse a feature manifest YAML file."""
        if not isinstance(data, dict):
            return None

        # Extract key fields
        feature_id = data.get("id", data.get("name", "unknown"))
        name = data.get("name", feature_id)
        status = data.get("status", "unknown")
        description = data.get("description", "")

        # Build searchable content
        content_parts = [
            f"Feature: {name}",
            f"ID: {feature_id}",
            f"Status: {status}",
        ]

        if description:
            content_parts.append(f"Description: {description}")

        # Include requirements if present
        requirements = data.get("requirements", [])
        if requirements:
            content_parts.append("Requirements:")
            for req in requirements[:10]:  # Limit to first 10
                if isinstance(req, dict):
                    content_parts.append(f"  - {req.get('id', 'unknown')}: {req.get('description', '')}")
                else:
                    content_parts.append(f"  - {req}")

        # Include acceptance criteria if present
        acceptance = data.get("acceptance_criteria", data.get("acceptance", []))
        if acceptance:
            content_parts.append("Acceptance Criteria:")
            for criterion in acceptance[:10]:
                content_parts.append(f"  - {criterion}")

        return Chunk(
            title=f"Feature: {name}",
            content="\n".join(content_parts),
            line_start=start_line,
            line_end=end_line,
            chunk_type="yaml-feature",
            metadata={
                "feature_id": feature_id,
                "name": name,
                "status": status,
                "raw": data,
            },
        )

    def _parse_generic(
        self, data: dict, raw_content: str, start_line: int, end_line: int, index: int
    ) -> Chunk:
        """Parse a generic YAML document."""
        # Try to extract a title
        title = "YAML Document"
        if isinstance(data, dict):
            title = data.get("name", data.get("id", data.get("title", f"Document {index + 1}")))

        # Create readable content
        content = yaml.dump(data, default_flow_style=False, allow_unicode=True)

        return Chunk(
            title=str(title),
            content=content,
            line_start=start_line,
            line_end=end_line,
            chunk_type="yaml-document",
            metadata={"raw": data} if isinstance(data, dict) else {},
        )

    def extract_title(self, content: str, filename: str) -> str:
        """Extract document title from YAML or filename."""
        try:
            data = yaml.safe_load(content)
            if isinstance(data, dict):
                return data.get("name", data.get("id", data.get("title", filename)))
        except yaml.YAMLError:
            pass
        return filename
