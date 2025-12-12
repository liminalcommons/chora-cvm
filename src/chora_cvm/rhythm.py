"""
Kairotic Rhythm Sensing — Detecting System Phases.

The system can sense its current phase in the kairotic cycle:
- Orange/Lit side (active, visible): Pioneer → Cultivator → Regulator
- Purple/Shadow side (preparatory, receptive): Steward → Curator → Scout

Based on the Cycle of Kairotic Flow by Kylie Stedman Gomes,
inspired by Simon Wardley's Pioneers/Settlers/Town Planners.

Core Principle: "Power only gets corrupted when it accumulates and stagnates."
The system senses its rhythm to know when to build, tend, or wait.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .store import EventStore


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class KairoticState:
    """
    Current position in the kairotic cycle.

    The six archetypes arranged in circular flow:
    - Orange/Lit side: Pioneer → Cultivator → Regulator (active, visible)
    - Purple/Shadow side: Steward → Curator → Scout (preparatory, receptive)
    """

    pioneer_weight: float = 0.0     # Explore new territory (genesis)
    cultivator_weight: float = 0.0  # Growth, bonding, learning capture
    regulator_weight: float = 0.0   # Stabilization, verification closing
    steward_weight: float = 0.0     # Maintenance, tending, high integrity
    curator_weight: float = 0.0     # Digestion, discernment
    scout_weight: float = 0.0       # Void detection, sensing, preparation

    @property
    def dominant_phase(self) -> str:
        """The phase with highest weight."""
        phases = {
            "pioneer": self.pioneer_weight,
            "cultivator": self.cultivator_weight,
            "regulator": self.regulator_weight,
            "steward": self.steward_weight,
            "curator": self.curator_weight,
            "scout": self.scout_weight,
        }
        return max(phases, key=phases.get)

    @property
    def side(self) -> str:
        """Orange (active) or Purple (preparatory)."""
        orange = self.pioneer_weight + self.cultivator_weight + self.regulator_weight
        purple = self.steward_weight + self.curator_weight + self.scout_weight
        return "orange" if orange > purple else "purple"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict with computed properties."""
        result = asdict(self)
        result["dominant_phase"] = self.dominant_phase
        result["side"] = self.side
        return result


@dataclass
class TemporalHealth:
    """
    Metrics computed over a rolling window.

    Point-in-time metrics tell *where* we are.
    Rate of change tells *which way we're moving*.
    """

    window_days: int = 7

    # Creation rates (anabolic)
    entities_created: int = 0
    bonds_created: int = 0
    learnings_captured: int = 0

    # Decomposition rates (catabolic)
    entities_composted: int = 0
    entities_digested: int = 0

    # Stability rates
    verifies_added: int = 0
    verifies_broken: int = 0

    @property
    def growth_rate(self) -> float:
        """Net entity creation rate per day."""
        created = self.entities_created
        removed = self.entities_composted + self.entities_digested
        return (created - removed) / self.window_days if self.window_days > 0 else 0.0

    @property
    def metabolic_balance(self) -> float:
        """
        Ratio of anabolic to catabolic activity.

        1.0 = balanced
        > 1.0 = more growth than decay
        < 1.0 = more decay than growth
        """
        anabolic = self.entities_created + self.bonds_created
        catabolic = self.entities_composted + self.entities_digested
        if catabolic == 0:
            return float("inf") if anabolic > 0 else 1.0
        return anabolic / catabolic

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict with computed properties."""
        result = asdict(self)
        result["growth_rate"] = self.growth_rate
        result["metabolic_balance"] = self.metabolic_balance
        return result


# =============================================================================
# sense_kairotic_state — Detect current phase in the cycle
# =============================================================================


def sense_kairotic_state(db_path: str) -> dict[str, Any]:
    """
    Sense the system's current kairotic state.

    Analyzes entity creation patterns, bond health, and activity levels
    to determine which phase(s) the system is currently in.

    Returns:
        Dict containing KairoticState with phase weights, dominant_phase, and side.
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Get temporal data
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    # Count recent inquiries (Pioneer signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'inquiry'
        AND json_extract(data_json, '$.status') = 'active'
        AND (json_extract(data_json, '$.created_at') > ? OR json_extract(data_json, '$.created_at') IS NULL)
        """,
        (week_ago,),
    )
    recent_inquiries = cur.fetchone()[0]

    # Count recent learnings (Cultivator signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'learning'
        AND (json_extract(data_json, '$.created_at') > ? OR json_extract(data_json, '$.created_at') IS NULL)
        """,
        (week_ago,),
    )
    recent_learnings = cur.fetchone()[0]

    # Count recent bonds (Cultivator signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM bonds
        WHERE json_extract(data_json, '$.created_at') > ?
        """,
        (week_ago,),
    )
    recent_bonds = cur.fetchone()[0]

    # Verification rate (Regulator signal)
    cur.execute("SELECT COUNT(*) FROM entities WHERE type = 'behavior'")
    total_behaviors = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM bonds WHERE type = 'verifies'")
    total_verifies = cur.fetchone()[0]

    verification_rate = total_verifies / total_behaviors if total_behaviors > 0 else 0.0

    # Integrity score approximation (Steward signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'behavior'
        AND json_extract(data_json, '$.status') = 'verified'
        """
    )
    verified_behaviors = cur.fetchone()[0]
    integrity_score = verified_behaviors / total_behaviors if total_behaviors > 0 else 1.0

    # Change rate (Steward signal - low change = steward mode)
    cur.execute("SELECT COUNT(*) FROM entities")
    total_entities = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE json_extract(data_json, '$.created_at') > ?
        """,
        (week_ago,),
    )
    recent_entities = cur.fetchone()[0]

    change_rate = recent_entities / 7.0 if total_entities > 0 else 0.0

    # Active signals (Scout signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'signal'
        AND json_extract(data_json, '$.status') = 'active'
        """
    )
    active_signals = cur.fetchone()[0]

    # Orphan count (Curator signal)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities e
        LEFT JOIN bonds b1 ON e.id = b1.from_id
        LEFT JOIN bonds b2 ON e.id = b2.to_id
        WHERE b1.id IS NULL AND b2.id IS NULL
        AND e.type != 'relationship'
        """
    )
    orphan_count = cur.fetchone()[0]

    store.close()

    # Compute phase weights
    state = KairoticState()

    # Pioneer: High inquiry creation, low verification
    pioneer_inquiry_signal = min(recent_inquiries / 3.0, 1.0) if recent_inquiries > 0 else 0.0
    pioneer_low_verify = max(0.0, 1.0 - verification_rate) if verification_rate < 0.5 else 0.0
    state.pioneer_weight = (pioneer_inquiry_signal + pioneer_low_verify) / 2.0

    # Cultivator: High bonding activity, learning capture
    cultivator_bonds = min(recent_bonds / 10.0, 1.0) if recent_bonds > 0 else 0.0
    cultivator_learnings = min(recent_learnings / 5.0, 1.0) if recent_learnings > 0 else 0.0
    state.cultivator_weight = (cultivator_bonds + cultivator_learnings) / 2.0

    # Regulator: Stabilization, verification closing
    regulator_verify = verification_rate if verification_rate > 0.5 else 0.0
    regulator_recent_verify = min(total_verifies / 5.0, 1.0) if total_verifies > 0 else 0.0
    state.regulator_weight = (regulator_verify + regulator_recent_verify) / 2.0

    # Steward: Maintenance, low change, high integrity
    steward_integrity = integrity_score if integrity_score > 0.8 else 0.0
    steward_low_change = max(0.0, 1.0 - min(change_rate, 1.0))
    state.steward_weight = (steward_integrity + steward_low_change) / 2.0

    # Curator: Digestion activity, orphan detection
    curator_orphans = min(orphan_count / 5.0, 1.0) if orphan_count > 0 else 0.0
    state.curator_weight = curator_orphans

    # Scout: Void detection, signal emission
    scout_signals = min(active_signals / 3.0, 1.0) if active_signals > 0 else 0.0
    state.scout_weight = scout_signals

    # Normalize weights so they sum to 1.0
    total = (
        state.pioneer_weight
        + state.cultivator_weight
        + state.regulator_weight
        + state.steward_weight
        + state.curator_weight
        + state.scout_weight
    )
    if total > 0:
        state.pioneer_weight /= total
        state.cultivator_weight /= total
        state.regulator_weight /= total
        state.steward_weight /= total
        state.curator_weight /= total
        state.scout_weight /= total

    return state.to_dict()


# =============================================================================
# temporal_health — Rolling window metrics
# =============================================================================


def temporal_health(db_path: str, window_days: int = 7) -> dict[str, Any]:
    """
    Compute temporal health metrics over a rolling window.

    Args:
        db_path: Path to the database
        window_days: Number of days to look back (default 7)

    Returns:
        Dict containing TemporalHealth metrics.
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()

    # Count entities created in window
    # Exclude 'relationship' type since those are bond projections already counted in bonds_created
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE json_extract(data_json, '$.created_at') > ?
        AND type != 'relationship'
        """,
        (cutoff,),
    )
    entities_created = cur.fetchone()[0]

    # Count bonds created in window
    cur.execute(
        """
        SELECT COUNT(*) FROM bonds
        WHERE json_extract(data_json, '$.created_at') > ?
        """,
        (cutoff,),
    )
    bonds_created = cur.fetchone()[0]

    # Count learnings created in window
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'learning'
        AND json_extract(data_json, '$.created_at') > ?
        """,
        (cutoff,),
    )
    learnings_captured = cur.fetchone()[0]

    # Count composted entities (from archive table)
    cur.execute(
        """
        SELECT COUNT(*) FROM archive
        WHERE archived_at > ?
        AND reason = 'composted'
        """,
        (cutoff,),
    )
    entities_composted = cur.fetchone()[0]

    # Count digested entities
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE json_extract(data_json, '$.status') = 'digested'
        AND json_extract(data_json, '$.digested_at') > ?
        """,
        (cutoff,),
    )
    entities_digested = cur.fetchone()[0]

    # Count verifies bonds added
    cur.execute(
        """
        SELECT COUNT(*) FROM bonds
        WHERE type = 'verifies'
        AND json_extract(data_json, '$.created_at') > ?
        """,
        (cutoff,),
    )
    verifies_added = cur.fetchone()[0]

    store.close()

    health = TemporalHealth(
        window_days=window_days,
        entities_created=entities_created,
        bonds_created=bonds_created,
        learnings_captured=learnings_captured,
        entities_composted=entities_composted,
        entities_digested=entities_digested,
        verifies_added=verifies_added,
        verifies_broken=0,  # Would need event log to track broken bonds
    )

    return health.to_dict()


# =============================================================================
# compute_satiation — The question of enough
# =============================================================================


def compute_satiation(db_path: str) -> float:
    """
    Compute satiation score (0.0 = hungry, 1.0 = satiated).

    Satiation = high integrity × low entropy × low growth pressure

    Interpretation:
    - 0.0 - 0.3: Hungry (active work needed — pioneer/cultivator)
    - 0.3 - 0.6: Digesting (metabolic work — curator/regulator)
    - 0.6 - 0.8: Content (maintenance mode — steward)
    - 0.8 - 1.0: Satiated (receptive mode — scout, await invitation)

    Returns:
        Float between 0.0 and 1.0.
    """
    store = EventStore(db_path)
    cur = store._conn.cursor()

    # Compute integrity (verified behaviors / total behaviors)
    cur.execute("SELECT COUNT(*) FROM entities WHERE type = 'behavior'")
    total_behaviors = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM bonds WHERE type = 'verifies'")
    total_verifies = cur.fetchone()[0]

    integrity = total_verifies / total_behaviors if total_behaviors > 0 else 1.0
    integrity = min(integrity, 1.0)  # Cap at 1.0

    # Compute entropy (orphans + stale signals + deprecated)
    cur.execute(
        """
        SELECT COUNT(*) FROM entities e
        LEFT JOIN bonds b1 ON e.id = b1.from_id
        LEFT JOIN bonds b2 ON e.id = b2.to_id
        WHERE b1.id IS NULL AND b2.id IS NULL
        AND e.type != 'relationship'
        """
    )
    orphan_count = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'signal'
        AND json_extract(data_json, '$.status') = 'active'
        """
    )
    active_signals = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE json_extract(data_json, '$.status') = 'deprecated'
        """
    )
    deprecated_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM entities")
    total_entities = cur.fetchone()[0]

    # Entropy score: ratio of "problematic" entities to total
    entropy_items = orphan_count + active_signals + deprecated_count
    entropy_score = entropy_items / total_entities if total_entities > 0 else 0.0
    entropy_score = min(entropy_score, 1.0)

    # Growth pressure: active inquiries + unresolved signals
    cur.execute(
        """
        SELECT COUNT(*) FROM entities
        WHERE type = 'inquiry'
        AND json_extract(data_json, '$.status') = 'active'
        """
    )
    active_inquiries = cur.fetchone()[0]

    # Diminishing growth pressure factor
    growth_pressure = 1.0 / (1 + active_inquiries + active_signals)

    store.close()

    # Satiation = integrity × (1 - entropy) × growth_pressure
    low_entropy = 1.0 - entropy_score
    satiation = integrity * low_entropy * growth_pressure

    return min(max(satiation, 0.0), 1.0)


# =============================================================================
# get_rhythm_summary — Human-readable summary for orient
# =============================================================================


def get_rhythm_summary(db_path: str) -> str:
    """
    Get rhythm summary for display in orient output.

    Returns:
        Human-readable multi-line string summarizing kairotic state.
    """
    state = sense_kairotic_state(db_path)
    satiation = compute_satiation(db_path)
    temporal = temporal_health(db_path)

    # Satiation interpretation
    if satiation >= 0.8:
        satiation_label = "satiated"
    elif satiation >= 0.6:
        satiation_label = "content"
    elif satiation >= 0.3:
        satiation_label = "digesting"
    else:
        satiation_label = "hungry"

    lines = [
        f"System Phase: {state['dominant_phase'].upper()} ({state['side']} side)",
        f"  Pioneer: {state['pioneer_weight']:.2f} | Cultivator: {state['cultivator_weight']:.2f} | Regulator: {state['regulator_weight']:.2f}",
        f"  Steward: {state['steward_weight']:.2f} | Curator: {state['curator_weight']:.2f} | Scout: {state['scout_weight']:.2f}",
        "",
        f"Satiation: {satiation:.2f} ({satiation_label})",
        f"Growth Rate: {temporal['growth_rate']:+.1f} entities/week",
        f"Metabolic Balance: {temporal['metabolic_balance']:.1f} (anabolic / catabolic)",
    ]

    return "\n".join(lines)
