import asyncio
import unittest
from copy import deepcopy
from pathlib import Path
import re

from config import AppSettings
from durable_core import ActionCommand, ActionState, DuplicateEvent, IncidentState, InMemoryRepositoryBundle, InvalidTransition, ProjectionIntegrityError, RepositoryUnavailable, ResourceAlreadyOwned, UnknownActionOutcome, VersionConflict
from platform_events import EventType, HarisEvent, Provenance
from postgres_persistence import MockPostgresTransport, PostgresRepositoryBundle, build_repository_bundle


def canonical_event():
    return HarisEvent(event_id="evt-db-1",event_type=EventType.NETWORK_CONGESTION_CHANGED,source="test",source_mode="fixture",source_event_id="source-1",source_timestamp=1.0,received_at=2.0,created_at=3.0,entity_type="HARIS_CONFIGURED_LOGICAL_CELL",entity_id="T03",correlation_key="cell:T03",provenance=Provenance.FIXTURE_SIMULATED,payload={"congestion_level":"High"},trace_id="trace-1")


def action_row(state="PENDING",version=0):
    command=ActionCommand(incident_id="inc-1",command_type="QOD_CREATE",resource_key="device:a",device_id="a",plan_version=1,requested_at=1.0,state=ActionState(state),version=version)
    row=deepcopy(vars(command));row["state"]=state;row["idempotency_key"]=command.idempotency_key;return row


class PostgresPersistenceContractTests(unittest.TestCase):
    def test_network_write_sql_signature_and_python_payload_are_identical(self):
        sql=Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_text(encoding="utf-8")
        signatures=re.findall(
            r"create\s+or\s+replace\s+function\s+public\.haris_write_network_state\s*\(([^)]*)\)\s*returns\s+([a-z0-9_]+)",
            sql,re.I|re.S,
        )
        self.assertEqual(len(signatures),1)
        sql_arguments={part.strip().split()[0]:part.strip().split()[1].lower() for part in signatures[0][0].split(",")}
        self.assertEqual(sql_arguments,{"p_record":"jsonb","p_expected_version":"bigint"})
        self.assertEqual(signatures[0][1].lower(),"jsonb")

        timestamp_fields=(
            "raw_congestion_observed_at","reachability_observed_at",
            "location_observed_at","last_source_change_at",
            "last_operational_change_at","updated_at",
        )
        record={
            "entity_id":"PERSISTENCE-TEST-CELL-static", "entity_type":"HARIS_CONFIGURED_LOGICAL_CELL",
            "mapping_source":"PERSISTENCE_INTEGRATION_TEST", "provenance":"HARIS_DERIVED",
            "raw_congestion":"High", "raw_congestion_observed_at":1.0,
            "reachability_summary":{}, "reachability_observed_at":2.0,
            "location_summary":{}, "location_observed_at":3.0, "freshness":"FRESH",
            "haris_operational_state":"INCIDENT_OPEN", "active_incident_ids":[],
            "last_source_change_at":4.0, "last_operational_change_at":5.0,
            "version":0, "updated_at":6.0,
        }
        transport=MockPostgresTransport({"haris_write_network_state":lambda args:{**args["p_record"],"version":1}})
        saved=PostgresRepositoryBundle(transport).network_state.save(record,0)
        rpc_name,arguments=transport.calls[0]
        self.assertEqual(rpc_name,"haris_write_network_state")
        self.assertEqual(set(arguments),set(sql_arguments))
        self.assertEqual(set(arguments["p_record"]),set(record))
        self.assertIsInstance(arguments["p_record"],dict);self.assertIsInstance(arguments["p_expected_version"],int)
        for field in timestamp_fields:
            self.assertIsInstance(arguments["p_record"][field],str)
            self.assertTrue(arguments["p_record"][field].endswith("Z"))
            self.assertEqual(saved[field],record[field])

    def test_incident_create_sql_signature_and_nested_payload_are_identical(self):
        sql=Path("supabase/migrations/002_haris_durable_event_incident_core.sql").read_text(encoding="utf-8")
        signatures=re.findall(
            r"create\s+or\s+replace\s+function\s+public\.haris_create_incident\s*\(([^)]*)\)\s*returns\s+([a-z0-9_]+)",
            sql,re.I|re.S,
        )
        self.assertEqual(signatures,[("p_record jsonb","jsonb")])
        table_match=re.search(
            r"create\s+table\s+if\s+not\s+exists\s+public\.haris_incidents\s*\((.*?)\);",
            sql,re.I|re.S,
        )
        self.assertIsNotNone(table_match)
        table_columns={line.strip().split()[0] for line in table_match.group(1).splitlines() if line.strip()}
        incident={
            "incident_id":"PERSISTENCE-TEST-INC-static", "schema_version":1,
            "correlation_key":"persistence-integration:static", "primary_entity":"PERSISTENCE-TEST-CELL-static",
            "affected_entities":["PERSISTENCE-TEST-CELL-static"], "affected_devices":[],
            "trigger_event_id":"PERSISTENCE-TEST-EVT-static", "trigger_provenance":"PERSISTENCE_INTEGRATION_TEST",
            "trigger_source_timestamp":1.0, "opened_at":2.0, "updated_at":3.0,
            "severity":"test", "priority":"PERSISTENCE_TEST", "state":IncidentState.DETECTED.value,
            "plan_version":1, "warden_decision":None, "verification_state":"PENDING",
            "recovery_state":"PENDING", "outcome":None, "closed_at":None,
            "version":0, "trace_id":"PERSISTENCE-TEST-TRACE-static",
        }
        self.assertEqual(set(incident),table_columns)
        self.assertIn("where state in ('DETECTED','EVALUATING','PLANNED','WARDEN_REVIEW','APPROVED','MITIGATING','VERIFYING','RECOVERING','ESCALATED')",sql)
        transport=MockPostgresTransport({"haris_create_incident":lambda args:{**args["p_record"],"_created":True}})
        stored,created=PostgresRepositoryBundle(transport).incidents.create_or_get_active(incident)
        rpc_name,arguments=transport.calls[0]
        self.assertEqual(rpc_name,"haris_create_incident")
        self.assertEqual(set(arguments),{"p_record"})
        self.assertEqual(set(arguments["p_record"]),table_columns)
        self.assertIsInstance(arguments["p_record"],dict)
        for field in ("trigger_source_timestamp","opened_at","updated_at"):
            self.assertIsInstance(arguments["p_record"][field],str)
            self.assertTrue(arguments["p_record"][field].endswith("Z"))
            self.assertEqual(stored[field],incident[field])
        for field in ("warden_decision","outcome","closed_at"):
            self.assertIsNone(arguments["p_record"][field])
        self.assertIsInstance(arguments["p_record"]["schema_version"],int)
        self.assertIsInstance(arguments["p_record"]["plan_version"],int)
        self.assertIsInstance(arguments["p_record"]["version"],int)
        self.assertIsInstance(arguments["p_record"]["affected_entities"],list)
        self.assertIsInstance(arguments["p_record"]["affected_devices"],list)
        self.assertEqual(stored["state"],IncidentState.DETECTED.value)
        self.assertTrue(created);self.assertNotIn("_created",stored)

    def test_event_round_trip_duplicate_and_bounded_replay(self):
        event=canonical_event(); row={**event.model_dump(mode="json"),"sequence":7,"idempotency_key":event.idempotency_key}
        transport=MockPostgresTransport({"haris_append_domain_event":[{"inserted":True},{"inserted":False}],"haris_read_domain":lambda args:[row],"haris_event_sequence":{"sequence":7}})
        repo=PostgresRepositoryBundle(transport).events
        self.assertTrue(repo.append(event));self.assertFalse(repo.append(event))
        self.assertEqual(repo.get(event.event_id).event_id,event.event_id)
        self.assertEqual(repo.events_after(6)[0][0],7);self.assertEqual(repo.sequence(),7)
        self.assertLessEqual(transport.calls[-2][1]["p_limit"],1000)

    def test_network_round_trip_and_version_conflict_mapping(self):
        record={"entity_id":"T03","version":2,"provenance":"FIXTURE_SIMULATED","updated_at":2.0}
        transport=MockPostgresTransport({"haris_write_network_state":record,"haris_read_domain":lambda args:[record]})
        bundle=PostgresRepositoryBundle(transport);repo=bundle.network_state
        self.assertEqual(repo.save(record,1)["version"],2);self.assertEqual(repo.get("T03")["entity_id"],"T03")
        transport.responses["haris_write_network_state"]=RuntimeError("haris_version_conflict")
        with self.assertRaises(VersionConflict):repo.save(record,1)
        self.assertEqual(bundle.metrics.version_conflicts,1);self.assertGreaterEqual(bundle.metrics.repository_operation_total,3)

    def test_network_snapshot_and_utc_serialization(self):
        record={"entity_id":"T03","version":1,"provenance":"FIXTURE_SIMULATED","updated_at":"1970-01-01T00:00:02Z"}
        checkpoint={"snapshot_version":1,"last_event_sequence":9,"projection":{"T03":record},"created_at":"1970-01-01T00:00:03Z"}
        transport=MockPostgresTransport({"haris_read_domain":lambda args:[record],"haris_write_network_state":lambda args:{**args["p_record"],"version":1},"haris_save_checkpoint":checkpoint,"haris_latest_checkpoint":checkpoint})
        repo=PostgresRepositoryBundle(transport).network_state
        saved=repo.save({**record,"updated_at":2.0},0)
        self.assertEqual(saved["updated_at"],2.0)
        self.assertTrue(transport.calls[0][1]["p_record"]["updated_at"].endswith("Z"))
        self.assertEqual(repo.load_all()["T03"]["updated_at"],2.0)
        repo.save_snapshot({"T03":record},9,3.0)
        self.assertEqual(repo.latest_snapshot()["created_at"],3.0)

    def test_incident_transition_validates_before_transport(self):
        incident={"incident_id":"inc-1","correlation_key":"cell:T03","primary_entity":"T03","state":"DETECTED","version":0}
        transport=MockPostgresTransport({"haris_create_incident":incident,"haris_read_domain":lambda args:[incident],"haris_transition_incident":{**incident,"state":"EVALUATING","version":1}})
        repo=PostgresRepositoryBundle(transport).incidents
        self.assertTrue(repo.create_or_get_active(incident)[1]);self.assertEqual(repo.find_active("cell:T03")["incident_id"],"inc-1")
        changed=repo.transition("inc-1",__import__("durable_core").IncidentState.EVALUATING,actor="SENTINEL",reason_code="HIGH",trace_id="trace",at=2.0)
        self.assertEqual(changed["state"],"EVALUATING")
        before=len(transport.calls)
        with self.assertRaises(InvalidTransition):repo.transition("inc-1",__import__("durable_core").IncidentState.RESOLVED,actor="x",reason_code="x",trace_id="x",at=3.0)
        self.assertEqual(len(transport.calls),before+1)  # only the safe current-state read

    def test_incident_update_and_transition_history(self):
        incident={"incident_id":"inc-1","correlation_key":"cell:T03","primary_entity":"T03","state":"DETECTED","version":0,"updated_at":1.0}
        transition={"incident_id":"inc-1","from_state":"DETECTED","to_state":"EVALUATING","occurred_at":"1970-01-01T00:00:02Z"}
        def read(args): return [transition] if args["p_kind"]=="transition" else [incident]
        transport=MockPostgresTransport({"haris_read_domain":read,"haris_update_incident":{**incident,"version":1,"updated_at":"1970-01-01T00:00:03Z"}})
        repo=PostgresRepositoryBundle(transport).incidents
        self.assertEqual(repo.update(incident,0)["version"],1)
        self.assertEqual(repo.transitions("inc-1")[0]["occurred_at"],2.0)
        with self.assertRaises(InvalidTransition):repo.update({**incident,"state":"RESOLVED"},0)

    def test_action_idempotency_and_sent_becomes_unknown(self):
        sent=action_row("SENT",2)
        def read(args): return [sent] if args["p_kind"]=="action" else []
        def save(args):
            row=deepcopy(args["p_record"]);row["version"]=args["p_expected_version"]+1;return row
        transport=MockPostgresTransport({"haris_read_domain":read,"haris_save_action":save})
        repo=PostgresRepositoryBundle(transport).actions
        changed=repo.mark_restart_unknown()
        self.assertEqual(changed[0].state,ActionState.OUTCOME_UNKNOWN);self.assertNotEqual(changed[0].state,ActionState.SUCCESS)

    def test_action_create_get_idempotency_and_expected_version_update(self):
        command=ActionCommand(incident_id="inc-1",command_type="QOD_CREATE",resource_key="device:a",device_id="a",plan_version=1,requested_at=1.0)
        stored={}
        def save(args):
            incoming=deepcopy(args["p_record"]);current=stored.get(incoming["command_id"])
            if current is None: incoming["version"]=0
            else: incoming["version"]=args["p_expected_version"]+1
            stored[incoming["command_id"]]=incoming;return incoming
        def read(args): return list(stored.values())
        transport=MockPostgresTransport({"haris_save_action":save,"haris_read_domain":read})
        repo=PostgresRepositoryBundle(transport).actions
        created,is_new=repo.create_or_get(command)
        self.assertTrue(is_new);self.assertEqual(created.command_id,command.command_id)
        self.assertEqual(transport.calls[0][1]["p_expected_version"],-1)
        self.assertEqual(repo.get(command.command_id).requested_at,1.0)
        self.assertEqual(repo.get_by_idempotency_key(command.idempotency_key).command_id,command.command_id)
        created.state=ActionState.READY
        self.assertEqual(repo.update(created,0).version,1)

    def test_action_and_incident_created_markers_make_retries_unambiguous(self):
        command=ActionCommand(incident_id="inc-1",command_type="QOD_CREATE",resource_key="device:a",device_id="a",plan_version=1,requested_at=1.0)
        action={**action_row(),"command_id":command.command_id,"idempotency_key":command.idempotency_key,"_created":False}
        incident={"incident_id":"inc-1","correlation_key":"cell:T03","primary_entity":"T03","state":"DETECTED","version":0,"_created":False}
        bundle=PostgresRepositoryBundle(MockPostgresTransport({"haris_save_action":action,"haris_create_incident":incident}))
        stored,created=bundle.actions.create_or_get(command)
        self.assertFalse(created);self.assertEqual(stored.command_id,command.command_id)
        stored_incident,created=bundle.incidents.create_or_get_active(incident)
        self.assertFalse(created);self.assertNotIn("_created",stored_incident)

    def test_ownership_conflict_and_wrong_owner_fail_closed(self):
        owned={"resource_key":"device:a","owner_incident_id":"inc-a","ownership_state":"OWNED","version":1}
        transport=MockPostgresTransport({"haris_read_domain":lambda args:[owned],"haris_acquire_resource":RuntimeError("haris_resource_already_owned")})
        repo=PostgresRepositoryBundle(transport).resource_ownership
        with self.assertRaises(ResourceAlreadyOwned):repo.acquire(owned)
        with self.assertRaises(ResourceAlreadyOwned):repo.release("device:a","inc-b",2.0,1)

    def test_ownership_acquire_read_and_release_correct_owner(self):
        owned={"resource_key":"device:a","resource_type":"QOD","owner_incident_id":"inc-a","ownership_state":"OWNED","version":0,"acquired_at":"1970-01-01T00:00:01Z"}
        released={**owned,"ownership_state":"RELEASED","version":1,"released_at":"1970-01-01T00:00:02Z"}
        transport=MockPostgresTransport({"haris_acquire_resource":owned,"haris_read_domain":lambda args:[owned],"haris_release_resource":released})
        repo=PostgresRepositoryBundle(transport).resource_ownership
        self.assertEqual(repo.acquire(owned)["acquired_at"],1.0)
        self.assertEqual(repo.get_active("device:a")["owner_incident_id"],"inc-a")
        self.assertEqual(repo.owned_by("inc-a")[0]["resource_key"],"device:a")
        self.assertEqual(repo.release("device:a","inc-a",2.0,0)["ownership_state"],"RELEASED")

    def test_verification_recovery_inbox_outbox_checkpoint_and_cost(self):
        responses={
            "haris_save_verification":{"verification_id":"v1","incident_id":"i1","state":"PENDING"},
            "haris_save_recovery":{"recovery_id":"r1","incident_id":"i1","state":"RELEASING"},
            "haris_claim_inbox":True,"haris_claim_outbox":[{"outbox_id":"o1","state":"CLAIMED","claim_owner":"w","attempt_count":1}],
            "haris_append_outbox":{"outbox_id":"o2","event_id":"e2","event_type":"X","state":"PENDING","created_at":"1970-01-01T00:00:01Z"},
            "haris_pending_outbox":lambda args:[{"outbox_id":"o2","state":"PENDING","created_at":"1970-01-01T00:00:01Z"}],
            "haris_ack_outbox":{},"haris_fail_outbox":{},"haris_save_checkpoint":{"snapshot_version":1,"last_event_sequence":9},
            "haris_latest_checkpoint":{"snapshot_version":1,"last_event_sequence":9},
            "haris_append_policy_cost":{"ledger_id":"c1","cost_basis":"HARIS_POLICY_COST_MODEL"},
            "haris_read_policy_cost":lambda args:[{"ledger_id":"c1","incident_id":"i1","estimated_policy_cost":1.25}],
            "haris_policy_cost_totals":{"incident_total":1.25,"day_total":2.5},
            "haris_read_domain":lambda args:({"verification":[{"verification_id":"v1","incident_id":"i1","state":"PENDING"}],"recovery":[{"recovery_id":"r1","incident_id":"i1","state":"RELEASING"}]}).get(args["p_kind"],[]),
        }
        bundle=PostgresRepositoryBundle(MockPostgresTransport(responses))
        self.assertEqual(bundle.verification.save({"verification_id":"v1","incident_id":"i1","state":"PENDING"})["state"],"PENDING")
        self.assertEqual(bundle.verification.get("v1")["verification_id"],"v1");self.assertEqual(len(bundle.verification.pending()),1)
        self.assertEqual(bundle.recovery.save({"recovery_id":"r1","incident_id":"i1","state":"RELEASING"})["state"],"RELEASING")
        self.assertEqual(bundle.recovery.get("r1")["recovery_id"],"r1");self.assertEqual(len(bundle.recovery.pending()),1)
        self.assertTrue(bundle.inbox.claim("key","evt",1.0));bundle.inbox.transport.responses["haris_claim_inbox"]=False;self.assertFalse(bundle.inbox.claim("key","evt",1.0))
        self.assertEqual(bundle.outbox.append({"outbox_id":"o2","event_id":"e2","event_type":"X","created_at":1.0})["created_at"],1.0)
        self.assertEqual(bundle.outbox.unsent()[0]["outbox_id"],"o2");self.assertEqual(bundle.outbox.claim("w")[0]["claim_owner"],"w")
        bundle.outbox.ack("o1","w");bundle.outbox.fail("o1","w","safe failure",True)
        self.assertEqual(bundle.checkpoints.save({},9,1.0)["last_event_sequence"],9);self.assertEqual(bundle.checkpoints.latest()["last_event_sequence"],9)
        self.assertEqual(bundle.cost_ledger.append({"ledger_id":"c1","incident_id":"i1","estimated_policy_cost":1.25})["cost_basis"],"HARIS_POLICY_COST_MODEL")
        self.assertEqual(bundle.cost_ledger.for_incident("i1")[0]["ledger_id"],"c1")
        self.assertEqual(bundle.cost_ledger.incident_total("i1"),1.25);self.assertEqual(bundle.cost_ledger.day_total("2026-01-01"),2.5)

    def test_secret_is_rejected_before_transport(self):
        transport=MockPostgresTransport();repo=PostgresRepositoryBundle(transport).network_state
        for unsafe in ({"access_token":"x"},{"nested":{"client_secret":"x"}},{"authorization_url":"https://x.invalid/?state=x"},{"phone_number":"+99999991000"}):
            with self.assertRaises(ValueError):repo.save({"entity_id":"T03",**unsafe},0)
        self.assertEqual(transport.calls,[])

    def test_bundle_parity_and_test_composition(self):
        memory=InMemoryRepositoryBundle();postgres=PostgresRepositoryBundle(MockPostgresTransport())
        expected={"events":("append","get","lookup","events_after","sequence"),"network_state":("load_all","get","save","save_snapshot","latest_snapshot"),"incidents":("create_or_get_active","get","find_active","active","update","transition","transitions"),"actions":("create_or_get","get","get_by_idempotency_key","update","pending_or_unknown","reconciliation_required","mark_restart_unknown","for_incident"),"resource_ownership":("acquire","get_active","active","release","owned_by"),"verification":("save","get","for_incident","pending"),"recovery":("save","get","for_incident","pending"),"inbox":("claim",),"inbound":("process",),"outbox":("append","unsent","mark_sent","claim","ack","fail"),"checkpoints":("save","latest"),"cost_ledger":("append","for_incident","incident_total","day_total")}
        for family,methods in expected.items():
            for method in methods:self.assertTrue(hasattr(getattr(memory,family),method));self.assertTrue(hasattr(getattr(postgres,family),method))
        built=build_repository_bundle(AppSettings(nac_mode="fixture",fixture_dir="fixtures",haris_persistence_mode="memory"))
        self.assertIsInstance(built,InMemoryRepositoryBundle)
        with self.assertRaises(RepositoryUnavailable):postgres.resource_locks.acquire("worker",["resource:a"])

    def test_postgres_missing_configuration_fails_without_transport(self):
        from postgres_persistence import PersistenceNotConfigured
        settings=AppSettings(nac_mode="fixture",fixture_dir="fixtures",haris_persistence_mode="postgres",supabase_url=None,supabase_key=None)
        with self.assertRaises(PersistenceNotConfigured):build_repository_bundle(settings,runtime=__import__("runtime").RuntimeEnvironment.PRODUCTION)

    def test_platform_health_is_not_ready_when_postgres_is_unconfigured(self):
        import run_api
        from unittest.mock import patch
        from postgres_persistence import PersistenceNotConfigured
        from platform_lifecycle import PlatformLifecycle
        settings=AppSettings(nac_mode="fixture",fixture_dir="fixtures",haris_persistence_mode="postgres")
        old=(run_api._durable_core,run_api._repository_bundle,run_api._persistence_error,run_api._platform_lifecycle)
        try:
            run_api._durable_core=None;run_api._repository_bundle=None;run_api._persistence_error=None;run_api._platform_lifecycle=PlatformLifecycle()
            with patch.object(run_api,"get_settings",return_value=settings),patch.object(run_api,"build_repository_bundle",side_effect=PersistenceNotConfigured("PERSISTENCE_NOT_CONFIGURED")):
                run_api.initialize_platform(settings=settings,runtime=__import__("runtime").RuntimeEnvironment.PRODUCTION)
                from fastapi import Response
                result=asyncio.run(run_api.platform_health(Response()))
        finally:
            run_api._durable_core,run_api._repository_bundle,run_api._persistence_error,run_api._platform_lifecycle=old
        self.assertEqual(result["status"],"NOT_READY");self.assertFalse(result["persistence"]["connected"])

    def test_malformed_response_fails_closed(self):
        repo=PostgresRepositoryBundle(MockPostgresTransport({"haris_read_domain":"not-json"})).network_state
        with self.assertRaises(RepositoryUnavailable):repo.load_all()

    def test_empty_required_results_and_contract_errors_fail_closed(self):
        event=canonical_event()
        with self.assertRaises(RepositoryUnavailable):PostgresRepositoryBundle(MockPostgresTransport({"haris_append_domain_event":[]})).events.append(event)
        with self.assertRaises(RepositoryUnavailable):PostgresRepositoryBundle(MockPostgresTransport({"haris_event_sequence":{}})).events.sequence()
        cases=((RuntimeError("haris_duplicate"),DuplicateEvent),(RuntimeError("haris_unknown_action_outcome"),UnknownActionOutcome),(RuntimeError("haris_projection_integrity"),ProjectionIntegrityError),(TimeoutError("timeout"),RepositoryUnavailable))
        for error,expected in cases:
            repo=PostgresRepositoryBundle(MockPostgresTransport({"haris_append_domain_event":error})).events
            with self.assertRaises(expected):repo.append(event)

    def test_outbox_rejects_secret_bearing_error_before_transport(self):
        transport=MockPostgresTransport();repo=PostgresRepositoryBundle(transport).outbox
        with self.assertRaises(ValueError):repo.fail("o1","worker","access_token=do-not-store",True)
        self.assertEqual(transport.calls,[])

if __name__=="__main__":unittest.main()
