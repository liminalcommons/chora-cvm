#!/usr/bin/env python3
"""
Genesis Origin Story: The Somatic Architecture

This script creates the founding narrative for Chora as entities in the graph.
It uses the Crystal Palace tools to dogfood our own creation.

The provenance chain:
    inquiry → yields → learning → surfaces → principles
                              └→ induces → patterns
"""

import sys
sys.path.insert(0, 'packages/chora-cvm/src')

from chora_cvm.kernel.schema import ExecutionContext
from chora_cvm.lib.graph import entity_create, bond_manage, query

DB_PATH = 'chora-habitation.db'

def create_ctx():
    return ExecutionContext(db_path=DB_PATH, persona_id='genesis')


def create_origin_story():
    ctx = create_ctx()
    created = []
    bonds = []

    # =========================================================================
    # 1. THE FOUNDING INQUIRY
    # =========================================================================
    inquiry_id = 'inquiry-somatic-architecture'
    result = entity_create(
        entity_type='inquiry',
        entity_id=inquiry_id,
        data={
            'title': 'What is it like to build a living substrate for agentic inhabitation?',
            'phenomenology': 'The generative question that opened the Somatic Architecture thesis',
            'domain': 'origin',
            'status': 'yielded',  # This inquiry has yielded learnings
        },
        _ctx=ctx
    )
    created.append(('inquiry', inquiry_id, result))
    print(f"Created inquiry: {inquiry_id}")

    # =========================================================================
    # 2. THE THESIS AS LEARNING
    # =========================================================================
    learning_id = 'learning-somatic-architecture-thesis'
    result = entity_create(
        entity_type='learning',
        entity_id=learning_id,
        data={
            'title': 'The Somatic Architecture: A Physiology for Agentic Inhabitation',
            'insight': (
                'Chora is not software but an inhabited environment. '
                'A durable Agent requires a body: a persistent substrate that can carry '
                'identity, attention, context, verification, and history across sessions. '
                'The AI Agent is an Animus (conscious inhabitation) moving through a Soma (long-term body).'
            ),
            'domain': 'origin',
            'source': 'docs/origin/somatic-architecture.md',
            'thesis_abstract': (
                'Software designed not as a toolset, but as a living substrate in which '
                'an AI Agent can inhabit, perceive, act, and remember across time. '
                'State as metabolism, tests as proprioception, errors as nociception, '
                'interfaces as membranes.'
            ),
        },
        _ctx=ctx
    )
    created.append(('learning', learning_id, result))
    print(f"Created learning: {learning_id}")

    # =========================================================================
    # 3. PRINCIPLES (surfaces from the learning)
    # =========================================================================
    principles = [
        {
            'id': 'principle-physics-as-meaning',
            'title': 'Physics constraints are meaning',
            'statement': (
                'We do not check a flag; we check the geometry. '
                'If a tool does not have a verifies bond to a behavior, '
                'it is not verified by convention—it is unverified by physics.'
            ),
            'section': '3.3 Constraint is meaning',
        },
        {
            'id': 'principle-proprioceptive-closure',
            'title': 'Unverified tools are phantom limbs',
            'statement': (
                'Proprioception is the system\'s capacity to know whether capabilities truly function. '
                'When verification is missing, the organism has phantom limbs. '
                'The verifies bond is the critical tension loop.'
            ),
            'section': '5.2 Proprioception',
        },
        {
            'id': 'principle-membrane-doctrine',
            'title': 'The kernel speaks data; the membrane speaks world',
            'statement': (
                'All user-facing output routes through rendering primitives so the nucleus '
                'remains decoupled from display mechanisms. '
                'The kernel speaks data. The membrane speaks world.'
            ),
            'section': '4.4 The Membrane',
        },
        {
            'id': 'principle-pain-as-signal',
            'title': 'Pain is structured demand, not exception',
            'statement': (
                'In Somatic Architecture, pain is the body\'s method of preventing catastrophic drift. '
                'Pain is not an exception string. It is a structured demand for attention. '
                'Confidence drops emit epistemic signals with urgency proportional to the drop.'
            ),
            'section': '5.5 Nociception',
        },
        {
            'id': 'principle-standing-waves',
            'title': 'Bonds are projected from events, not static facts',
            'statement': (
                'Bonds are standing waves projected from interaction events, '
                'and are themselves representable as entities. '
                'Bonds carry status (forming/active/stressed/dissolved) and confidence.'
            ),
            'section': '3.4 Bonds are projected from events',
        },
        {
            'id': 'principle-event-horizon',
            'title': 'Extend by adding meaning, not by wiring endpoints',
            'statement': (
                'Create a new protocol in the database, and it becomes available as a CLI command, '
                'API endpoint, and Agent tool without writing new Python. '
                'This is the architectural heart of inhabitation.'
            ),
            'section': '4.2 The Event Horizon',
        },
    ]

    for p in principles:
        result = entity_create(
            entity_type='principle',
            entity_id=p['id'],
            data={
                'title': p['title'],
                'statement': p['statement'],
                'domain': 'origin',
                'source_section': p['section'],
            },
            _ctx=ctx
        )
        created.append(('principle', p['id'], result))
        print(f"Created principle: {p['id']}")

    # =========================================================================
    # 4. PATTERNS (induces from the learning)
    # =========================================================================
    patterns = [
        {
            'id': 'pattern-proprioceptive-closure',
            'title': 'Proprioceptive Closure Pattern',
            'target': 'tool',
            'template': 'behavior → specifies → story; tool → implements → behavior; tool → verifies → behavior',
            'description': (
                'Every tool must be verified against a behavior. '
                'The verifies bond closes the loop, making the capability real rather than phantom.'
            ),
            'section': '5.2 Proprioception',
        },
        {
            'id': 'pattern-reflex-arc',
            'title': 'Reflex Arc Pattern',
            'target': 'signal',
            'template': 'tool → emits → signal → triggers → focus',
            'description': (
                'When something demands attention, a signal is emitted. '
                'The signal triggers a focus, routing attention to where it is needed.'
            ),
            'section': '6 Nervous System',
        },
        {
            'id': 'pattern-membrane-translation',
            'title': 'Membrane Translation Pattern',
            'target': 'protocol',
            'template': 'raw input → event → entity',
            'description': (
                'Interfaces are skin: translation, not truth. '
                'Raw sensory input becomes meaningful only once translated into physics entities and bonds.'
            ),
            'section': '4.4 The Membrane',
        },
        {
            'id': 'pattern-generative-chain',
            'title': 'Generative Chain Pattern',
            'target': 'story',
            'template': 'inquiry → yields → learning → surfaces → principle; learning → induces → pattern',
            'description': (
                'Work flows through a generative chain. Each step creates bonds—the tissue of '
                'relationship between things that matter. Meaning crystallizes through this flow.'
            ),
            'section': '3.2 The Bonds',
        },
    ]

    for p in patterns:
        result = entity_create(
            entity_type='pattern',
            entity_id=p['id'],
            data={
                'title': p['title'],
                'target': p['target'],
                'template': p['template'],
                'description': p['description'],
                'domain': 'origin',
                'source_section': p['section'],
            },
            _ctx=ctx
        )
        created.append(('pattern', p['id'], result))
        print(f"Created pattern: {p['id']}")

    # =========================================================================
    # 5. WIRE PROVENANCE BONDS
    # =========================================================================
    print("\nWiring provenance bonds...")

    # inquiry yields learning
    result = bond_manage(
        bond_type='yields',
        from_id=inquiry_id,
        to_id=learning_id,
        _ctx=ctx
    )
    bonds.append(('yields', inquiry_id, learning_id, result))
    print(f"  {inquiry_id} --yields--> {learning_id}")

    # learning surfaces principles
    for p in principles:
        result = bond_manage(
            bond_type='surfaces',
            from_id=learning_id,
            to_id=p['id'],
            _ctx=ctx
        )
        bonds.append(('surfaces', learning_id, p['id'], result))
        print(f"  {learning_id} --surfaces--> {p['id']}")

    # learning induces patterns
    for p in patterns:
        result = bond_manage(
            bond_type='induces',
            from_id=learning_id,
            to_id=p['id'],
            _ctx=ctx
        )
        bonds.append(('induces', learning_id, p['id'], result))
        print(f"  {learning_id} --induces--> {p['id']}")

    # principle governs pattern (where applicable)
    # proprioceptive-closure principle governs proprioceptive-closure pattern
    result = bond_manage(
        bond_type='governs',
        from_id='principle-proprioceptive-closure',
        to_id='pattern-proprioceptive-closure',
        _ctx=ctx
    )
    bonds.append(('governs', 'principle-proprioceptive-closure', 'pattern-proprioceptive-closure', result))
    print(f"  principle-proprioceptive-closure --governs--> pattern-proprioceptive-closure")

    # pain-as-signal principle governs reflex-arc pattern
    result = bond_manage(
        bond_type='governs',
        from_id='principle-pain-as-signal',
        to_id='pattern-reflex-arc',
        _ctx=ctx
    )
    bonds.append(('governs', 'principle-pain-as-signal', 'pattern-reflex-arc', result))
    print(f"  principle-pain-as-signal --governs--> pattern-reflex-arc")

    # membrane-doctrine principle governs membrane-translation pattern
    result = bond_manage(
        bond_type='governs',
        from_id='principle-membrane-doctrine',
        to_id='pattern-membrane-translation',
        _ctx=ctx
    )
    bonds.append(('governs', 'principle-membrane-doctrine', 'pattern-membrane-translation', result))
    print(f"  principle-membrane-doctrine --governs--> pattern-membrane-translation")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*60)
    print("ORIGIN STORY CREATED")
    print("="*60)
    print(f"\nEntities created: {len(created)}")
    for etype, eid, res in created:
        status = res.get('status', 'unknown')
        print(f"  [{etype}] {eid}: {status}")

    print(f"\nBonds created: {len(bonds)}")
    for btype, from_id, to_id, res in bonds:
        status = res.get('status', 'unknown')
        print(f"  {from_id} --{btype}--> {to_id}: {status}")

    return created, bonds


if __name__ == '__main__':
    create_origin_story()
