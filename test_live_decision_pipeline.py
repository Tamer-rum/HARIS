import asyncio

from config import get_settings
from nokia_clients import LiveNokiaClient
from agents import HarisAgentSystem


async def main():
    print("=" * 60)
    print("HARIS - LIVE DECISION PIPELINE TEST")
    print("=" * 60)

    settings = get_settings()

    if not settings.is_live:
        raise RuntimeError(
            f"NAC_MODE must be a live mode, got {settings.nac_mode!r}"
        )

    client = LiveNokiaClient(settings)

    system = HarisAgentSystem(
        client,
        settings=settings,
    )

    print("\nHARIS Agent System: CREATED")

    

    state = {
        "cycle_id": "decision-test",
        "dust_advisory": True,
        "trace": [],
    }

    # ---------------------------------------------------------
    # 1. SENTINEL
    # ---------------------------------------------------------
    print("\n[1] SENTINEL")

    state = await system._sentinel(state)

    print("Incident:")
    print(state["incident"])

    print("\nCongestion evidence:")
    for item in state["congestion"]:
        print(item)

    # ---------------------------------------------------------
    # 2. CARTOGRAPHER
    # ---------------------------------------------------------
    print("\n[2] CARTOGRAPHER")

    state = await system._cartographer(state)

    print("Locations:")
    for item in state.get("locations", []):
        print(item)

    # ---------------------------------------------------------
    # 3. TRIAGE
    # ---------------------------------------------------------
    print("\n[3] TRIAGE")

    state = await system._triage(state)

    print("Plan:")
    print(state["plan"])

    # ---------------------------------------------------------
    # 4. WARDEN
    #
    # Live WARDEN must fail closed if the required verification
    # context is not available.
    # ---------------------------------------------------------
    print("\n[4] WARDEN")

    state = await system._warden(state)

    print("Warden:")
    print(state["warden"])

    valid_levels = {"None", "Low", "Medium", "High"}
    assert all(
        item["congestion_level"] in valid_levels
        for item in state["congestion"]
    ), "Live congestion evidence must remain categorical Nokia evidence"

    actionable_cells = {
        item["cell_id"]
        for item in state["congestion"]
        if item["congestion_level"] in {"Medium", "High"}
    }
    tier_one_cells = {
        item["cell_id"]
        for item in state["devices"]
        if item["tier"] == 1
    }
    actions = state["plan"]["actions"]
    if state["dust_advisory"] and actionable_cells & tier_one_cells:
        assert actions, "Storm Shield must retain actions for actionable Tier-1 cells"
    if actions:
        assert len(actions) <= settings.guardrails.max_devices_reconfigured_per_cycle
        assert "action_safety_ok" in state["warden"]["safety_checks"]
    else:
        assert not state["warden"]["verified"]

    # ---------------------------------------------------------
# 5. NETWORK SAFETY CHECK
#
# WARDEN validates the network remediation plan before
# allowing ACTUATOR to perform any network mutation.
# ---------------------------------------------------------
    print("\n[5] NETWORK SAFETY CHECK")

    plan = state.get("plan", {})
    warden = state.get("warden", {})
    actions = plan.get("actions", [])

    warden_verified = bool(
        warden.get("verified", False)
    )

    safety_checks = warden.get(
        "safety_checks",
        {}
    )

    if not actions:
        print(
            "INFO: No network actions were proposed "
            "in this cycle."
        )

    elif warden_verified:
        print(
            "PASS: WARDEN approved the network remediation plan."
        )

        print(
            f"  Confidence: {warden.get('confidence', 0):.2f}"
        )

        print(
            f"  Blast radius: "
            f"{warden.get('blast_radius', 0):.2f}"
        )

        print(
            f"  Expected cost: "
            f"${warden.get('expected_cost_usd', 0):.2f}"
        )

        print(
            f"  Actions: {len(actions)}"
        )

        print(
            f"  Safety checks: {safety_checks}"
        )

    else:
        print(
            "PASS: WARDEN rejected the network remediation plan."
        )

        print(
            f"  Reason: "
            f"{warden.get('reason', 'unknown')}"
        )

        print(
            f"  Safety checks: {safety_checks}"
        )
    

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("DECISION PIPELINE SUMMARY")
    print("=" * 60)

    incident = state.get("incident", {})
    plan = state.get("plan", {})

    print(f"Severity: {incident.get('severity')}")
    print(f"Peak congestion: {incident.get('peak_congestion_level')}")
    print(f"Peak confidence: {incident.get('peak_confidence_level')}")
    print(f"Affected cells: {incident.get('affected_cells')}")
    print(f"Affected devices: {incident.get('affected_devices')}")
    print(f"Planned actions: {len(plan.get('actions', []))}")
    print(f"Approval required: {plan.get('approval_required')}")
    print(f"Plan confidence: {plan.get('confidence')}")
    print(
        f"Expected cost: "
        f"${plan.get('expected_cost_usd', 0):.2f}"
    )
    print(f"Blast radius: {plan.get('blast_radius')}")
    print(f"Warden verified: {warden.get('verified')}")

    print("\nTRACE:")

    for entry in state.get("trace", []):
        print(entry)

    print("\n" + "=" * 60)
    print("DECISION PIPELINE TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
