"""
Markdown parser: Split by ATX headers with frontmatter and tag extraction.

Chunk types:
- section: Content under a markdown header
- frontmatter: YAML frontmatter at start of file
"""

from __future__ import annotations

import re
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
    chunk_type: str = "section"
    heading_level: Optional[int] = None
    metadata: dict = field(default_factory=dict)


class MarkdownParser:
    """Parse markdown files into sections by ATX headers."""

    # Patterns for tag extraction
    SAP_PATTERN = re.compile(r"\bSAP-(\d{3})\b")
    FEATURE_PATTERN = re.compile(r"\bfeature-[\w-]+\b")
    PATTERN_PATTERN = re.compile(r"\bpattern-[\w-]+\b")
    PRINCIPLE_PATTERN = re.compile(r"\bprinciple-[\w-]+\b")

    def parse(self, content: str, filepath: Path) -> list[Chunk]:
        """Parse markdown content into chunks."""
        chunks = []

        # Extract frontmatter if present
        frontmatter, content_start = self._extract_frontmatter(content)
        if frontmatter:
            chunks.append(
                Chunk(
                    title="Frontmatter",
                    content=yaml.dump(frontmatter, default_flow_style=False),
                    line_start=1,
                    line_end=content_start - 1,
                    chunk_type="frontmatter",
                    metadata=frontmatter,
                )
            )

        # Parse sections
        sections = self._parse_sections(content)
        chunks.extend(sections)

        # Extract tags for all chunks
        for chunk in chunks:
            chunk.metadata["tags"] = self._extract_tags(chunk.content)

        return chunks

    def _extract_frontmatter(self, content: str) -> tuple[Optional[dict], int]:
        """Extract YAML frontmatter from start of content."""
        if not content.startswith("---"):
            return None, 1

        lines = content.split("\n")
        end_idx = None

        for i, line in enumerate(lines[1:], 1):
            if line.strip() == "---":
                end_idx = i
                break

        if end_idx is None:
            return None, 1

        try:
            frontmatter_text = "\n".join(lines[1:end_idx])
            frontmatter = yaml.safe_load(frontmatter_text)
            return frontmatter, end_idx + 2
        except yaml.YAMLError:
            return None, 1

    def _parse_sections(self, content: str) -> list[Chunk]:
        """Split content into sections by ATX headers."""
        lines = content.split("\n")
        sections = []
        current_section = {
            "title": "Preamble",
            "lines": [],
            "line_start": 1,
            "level": 0,
        }

        for i, line in enumerate(lines, 1):
            header_match = re.match(r"^(#{1,6})\s+(.+)$", line)
            if header_match:
                # Save current section if it has content
                if current_section["lines"]:
                    content_text = "\n".join(current_section["lines"]).strip()
                    if content_text:
                        sections.append(
                            Chunk(
                                title=current_section["title"],
                                content=content_text,
                                line_start=current_section["line_start"],
                                line_end=i - 1,
                                chunk_type="section",
                                heading_level=current_section["level"],
                            )
                        )

                # Start new section
                current_section = {
                    "title": header_match.group(2).strip(),
                    "level": len(header_match.group(1)),
                    "lines": [],
                    "line_start": i,
                }
            else:
                current_section["lines"].append(line)

        # Don't forget the last section
        if current_section["lines"]:
            content_text = "\n".join(current_section["lines"]).strip()
            if content_text:
                sections.append(
                    Chunk(
                        title=current_section["title"],
                        content=content_text,
                        line_start=current_section["line_start"],
                        line_end=len(lines),
                        chunk_type="section",
                        heading_level=current_section["level"],
                    )
                )

        return sections

    def _extract_tags(self, content: str) -> dict[str, list[str]]:
        """Extract tags (SAP references, feature IDs, etc.) from content."""
        tags = {}

        # SAP references
        sap_matches = self.SAP_PATTERN.findall(content)
        if sap_matches:
            tags["sap"] = [f"SAP-{m}" for m in set(sap_matches)]

        # Feature references
        feature_matches = self.FEATURE_PATTERN.findall(content)
        if feature_matches:
            tags["feature"] = list(set(feature_matches))

        # Pattern references
        pattern_matches = self.PATTERN_PATTERN.findall(content)
        if pattern_matches:
            tags["pattern"] = list(set(pattern_matches))

        # Principle references
        principle_matches = self.PRINCIPLE_PATTERN.findall(content)
        if principle_matches:
            tags["principle"] = list(set(principle_matches))

        return tags

    def extract_title(self, content: str, filename: str) -> str:
        """Extract document title from first H1 or filename."""
        title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if title_match:
            return title_match.group(1).strip()
        return filename
