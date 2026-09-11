import copy
import time
import unittest
from unittest.mock import patch

import run_api
from config import AppSettings
from durable_core import DurablePlatformCore, InMemoryRepositoryBundle, IncidentState, RecoveryState, VersionConflict
from platform_lifecycle import reconstruct_platform_state
from recovery_consistency import (
    overlay_authoritative_recovery, repair_incident_recovery_projection,
    save_recovery_and_repair,
)

def incident(recovery="PENDING"):
    now=time.time()
    return {"incident_id":"inc-r","correlation_key":"cell:T03","primary_entity":"T03","affected_entities":["T03"],"affected_devices":[],"trigger_event_id":"evt-r","trigger_provenance":"NOKIA_LIVE","trigger_source_timestamp":now,"opened_at":now,"updated_at":now,"severity":"critical","priority":"P1","state":IncidentState.DETECTED.value,"plan_version":0,"warden_decision":None,"verification_state":"UNCHANGED","recovery_state":recovery,"outcome":"REAL_PARTIAL","closed_at":None,"version":0,"trace_id":"trace-r"}

def recovery(state, version=None):
    row={"recovery_id":"recovery-inc-r","incident_id":"inc-r","resource_keys":["device:test"],"state":state,"started_at":1.0,"completed_at":2.0 if state in {"COMPLETE","FAILED"} else None,"failure_reason":None}
    if version is not None: row["version"]=version
    return row

class Phase8DRecoveryConsistencyTests(unittest.TestCase):
    def setUp(self):
        self.bundle=InMemoryRepositoryBundle();self.bundle.incidents.create_or_get_active(incident())

    def test_forward_matching_idempotent_and_projection_repair(self):
        first=save_recovery_and_repair(self.bundle,recovery("RELEASING"))
        self.assertEqual(first["version"],0)
        same=save_recovery_and_repair(self.bundle,recovery("RELEASING"))
        self.assertEqual(same["version"],0)
        self.assertEqual(self.bundle.incidents.get("inc-r")["recovery_state"],"RELEASING")
        partial=save_recovery_and_repair(self.bundle,recovery("PARTIAL"))
        self.assertEqual(partial["version"],1)
        self.assertEqual(self.bundle.incidents.get("inc-r")["recovery_state"],"PARTIAL")

    def test_stale_writer_and_terminal_regression_are_rejected(self):
        self.bundle.recovery.save(recovery("RELEASING"),-1)
        self.bundle.recovery.save(recovery("COMPLETE"),0)
        with self.assertRaises(VersionConflict): self.bundle.recovery.save(recovery("PARTIAL"),0)
        with self.assertRaises(VersionConflict): save_recovery_and_repair(self.bundle,recovery("RELEASING"))
        self.assertEqual(self.bundle.recovery.for_incident("inc-r")["state"],"COMPLETE")

    def test_partial_can_only_advance_to_complete(self):
        save_recovery_and_repair(self.bundle,recovery("PARTIAL"))
        with self.assertRaises(VersionConflict): save_recovery_and_repair(self.bundle,recovery("FAILED"))
        completed=save_recovery_and_repair(self.bundle,recovery("COMPLETE"))
        self.assertEqual(completed["state"],"COMPLETE")

    def test_projection_conflict_retries_and_is_idempotent(self):
        original=self.bundle.incidents.update; calls=[]
        def conflict_once(row,expected_version):
            calls.append(1)
            if len(calls)==1: raise VersionConflict("race")
            return original(row,expected_version)
        self.bundle.incidents.update=conflict_once
        save_recovery_and_repair(self.bundle,recovery("COMPLETE"),retries=2)
        self.assertEqual(len(calls),2)
        self.assertTrue(repair_incident_recovery_projection(self.bundle,"inc-r"))
        self.assertEqual(len(calls),2)

    def test_projection_failure_preserves_authoritative_recovery(self):
        self.bundle.incidents.update=lambda *_args,**_kwargs: (_ for _ in ()).throw(VersionConflict("race"))
        stored=save_recovery_and_repair(self.bundle,recovery("PARTIAL"),retries=2)
        self.assertEqual(stored["state"],"PARTIAL")
        self.assertEqual(self.bundle.recovery.for_incident("inc-r")["state"],"PARTIAL")
        self.assertEqual(overlay_authoritative_recovery(self.bundle,self.bundle.incidents.get("inc-r"))["recovery_state"],"PARTIAL")

    def test_reconstruction_repairs_and_preserves_real_partial_truth(self):
        self.bundle.recovery.save(recovery("COMPLETE"),-1)
        result=reconstruct_platform_state(self.bundle)
        restored=result.active_incidents[0]
        self.assertEqual(restored["recovery_state"],"COMPLETE")
        self.assertEqual(restored["recovery_authority"],"RECOVERY_REPOSITORY")
        self.assertEqual(restored["verification_state"],"UNCHANGED")
        self.assertEqual(restored["outcome"],"REAL_PARTIAL")

    def test_noc_snapshot_overlays_authoritative_recovery_truth(self):
        self.bundle.recovery.save(recovery("COMPLETE"),-1)
        core=DurablePlatformCore(
            events=self.bundle.events,network=self.bundle.network_state,
            incidents=self.bundle.incidents,actions=self.bundle.actions,
            ownership=self.bundle.resource_ownership,verifications=self.bundle.verification,
            recoveries=self.bundle.recovery,outbox=self.bundle.outbox,inbox=self.bundle.inbox,
            checkpoints=self.bundle.checkpoints,cost_ledger=self.bundle.cost_ledger,
        )
        core.readiness="READY"
        configured=AppSettings(nac_mode="fixture",fixture_dir="fixtures")
        previous=run_api._repository_bundle
        run_api._repository_bundle=self.bundle
        try:
            with patch.object(run_api,"get_durable_core",return_value=core), patch.object(run_api,"get_settings",return_value=configured):
                row=run_api._authoritative_snapshot()["active_incidents"][0]
        finally:
            run_api._repository_bundle=previous
        self.assertEqual(row["recovery_state"],"COMPLETE")
        self.assertEqual(row["recovery_authority"],"RECOVERY_REPOSITORY")
        self.assertEqual(row["verification_state"],"UNCHANGED")
        self.assertEqual(row["outcome"],"REAL_PARTIAL")

if __name__ == "__main__": unittest.main()
