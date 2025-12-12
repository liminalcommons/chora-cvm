"""
Harvest module: Extract and index content from legacy repositories.

Creates a searchable SQLite database (chora-legacy.db) with FTS5 support
for markdown, YAML, and Python source files across multiple repositories.
"""

from .harvester import LegacyHarvester
from .schema import init_legacy_db

__all__ = ["LegacyHarvester", "init_legacy_db"]
