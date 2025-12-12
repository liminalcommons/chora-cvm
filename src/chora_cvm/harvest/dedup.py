"""
Deduplication engine: Detect and mark duplicate content across repositories.

Strategy:
- Content-hash based: SHA256 of file content
- Priority-based canonical selection: Higher priority repos win
- Duplicate documents are marked with is_duplicate_of pointing to canonical
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Optional


@dataclass
class DocumentInfo:
    """Information about a document for deduplication."""

    id: str
    repository_id: str
    content_hash: str
    priority: int
    is_duplicate_of: Optional[str] = None


class DeduplicationEngine:
    """Detect and track duplicate content across repositories."""

    def __init__(self, repo_priorities: dict[str, int]):
        """
        Initialize with repository priorities.

        Args:
            repo_priorities: Map of repo_id -> priority (higher = prefer)
        """
        self.priorities = repo_priorities
        self.hash_index: dict[str, DocumentInfo] = {}  # content_hash -> canonical DocumentInfo
        self.duplicates_found = 0

    def compute_hash(self, content: str) -> str:
        """Compute SHA256 hash of content."""
        return sha256(content.encode("utf-8")).hexdigest()

    def process_document(
        self, doc_id: str, repository_id: str, content: str
    ) -> tuple[str, Optional[str]]:
        """
        Process a document and determine if it's a duplicate.

        Args:
            doc_id: Unique document ID
            repository_id: Repository this document belongs to
            content: Document content

        Returns:
            Tuple of (content_hash, is_duplicate_of)
            - is_duplicate_of is None if this is the canonical version
            - is_duplicate_of is the canonical doc_id if this is a duplicate
        """
        content_hash = self.compute_hash(content)
        doc_priority = self.priorities.get(repository_id, 0)

        doc_info = DocumentInfo(
            id=doc_id,
            repository_id=repository_id,
            content_hash=content_hash,
            priority=doc_priority,
        )

        if content_hash not in self.hash_index:
            # First time seeing this content - it's canonical
            self.hash_index[content_hash] = doc_info
            return content_hash, None

        # We've seen this content before
        canonical = self.hash_index[content_hash]

        if doc_priority > canonical.priority:
            # New doc is higher priority - it becomes canonical
            self.hash_index[content_hash] = doc_info

            # Mark old canonical as duplicate of new
            canonical.is_duplicate_of = doc_id
            self.duplicates_found += 1

            return content_hash, None
        else:
            # New doc is lower or equal priority - it's the duplicate
            doc_info.is_duplicate_of = canonical.id
            self.duplicates_found += 1

            return content_hash, canonical.id

    def get_canonical(self, content_hash: str) -> Optional[str]:
        """Get the canonical document ID for a content hash."""
        info = self.hash_index.get(content_hash)
        return info.id if info else None

    def get_stats(self) -> dict:
        """Get deduplication statistics."""
        return {
            "unique_content": len(self.hash_index),
            "duplicates_found": self.duplicates_found,
        }

    def reset(self) -> None:
        """Reset the deduplication state."""
        self.hash_index.clear()
        self.duplicates_found = 0
