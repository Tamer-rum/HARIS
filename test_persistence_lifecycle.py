import asyncio
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from fastapi import Response

import external.validate_persistence_restart as persistence_validator
from config import AppSettings
from durable_core import (
    ActionCommand, ActionState, DurablePlatformCore, InMemoryRepositoryBundle,
    ProjectionIntegrityError, RepositoryUnavailable,
)
from external.validate_persistence_restart import (
    integration_gate, main as persistence_validator_main,
    run_offline_orchestration, run_read_only_preflight,
    safe_preflight_diagnostic, sanitized_child_environment,
)
from platform_lifecycle import (
    PlatformLifecycle, PlatformLifecycleState, reconstruct_platform_state,
)
from postgres_persistence import (
    MockPostgresTransport,
    PersistenceAuthenticationFailed, PersistenceHostNotAllowed,
    PersistenceNetworkBlocked, PersistenceNotConfigured,
    PersistenceResponseInvalid, PersistenceRpcContractFailed,
    PersistenceSchemaNotReady, PersistenceTransportUnavailable,
    PostgresRepositoryBundle, build_persistence_transport,
    build_repository_bundle,
)
from runtime import RuntimeEnvironment, external_access_policy, provider_access_count


def settings(mode="memory", *, url=None, key=None, observations=False, loop=False):
    return AppSettings(
        nac_mode="fixture", fixture_dir="fixtures", haris_persistence_mode=mode,
        supabase_url=url, supabase_key=key,
        nokia_observation_enabled=observations, enable_continuous_loop=loop,
        haris_operational_api_token="test-operational-token",
        gemini_api_key=None, groq_api_key=None,
    )


class Rpc:
    def __init__(self): self.calls=[]
    def rpc(self, name, arguments): self.calls.append((name, arguments)); return []


class StatefulIntegrationTransport:
    """Offline RPC contract double; separate clients share only durable rows."""
    def __init__(self, state): self.state=state;self.calls=[];self.closed=False
    def close(self): self.closed=True
    def rpc(self,name,arguments):
        self.calls.append((name,copy.deepcopy(arguments)))
        s=self.state
        if name=="haris_append_domain_event":
            record=copy.deepcopy(arguments["p_event"]);key=record["event_id"]
            if key in s["events"]:return {"inserted":False,"event_id":key}
            s["sequence"]+=1;record["sequence"]=s["sequence"];s["events"][key]=record
            return {"inserted":True,"event_id":key}
        if name=="haris_write_network_state":
            record=copy.deepcopy(arguments["p_record"]);record["version"]=1;s["network"][record["entity_id"]]=record;return record
        if name=="haris_create_incident":
            record=copy.deepcopy(arguments["p_record"]);key=record["incident_id"]
            if key in s["incidents"]:return {**copy.deepcopy(s["incidents"][key]),"_created":False}
            s["incidents"][key]=record;return {**copy.deepcopy(record),"_created":True}
        if name=="haris_append_incident_transition":
            record=copy.deepcopy(arguments["p_record"]);record["transition_id"]=len(s["transitions"])+1;s["transitions"].append(record);return record
        if name=="haris_acquire_resource":
            record=copy.deepcopy(arguments["p_record"]);record["ownership_id"]=len(s["ownership"])+1;record["version"]=0;s["ownership"][record["resource_key"]]=record;return record
        if name=="haris_save_action":
            record=copy.deepcopy(arguments["p_record"]);key=record["command_id"]
            if key in s["actions"]:return {**copy.deepcopy(s["actions"][key]),"_created":False}
            record["version"]=0;s["actions"][key]=record;return {**copy.deepcopy(record),"_created":True}
        if name=="haris_save_verification":
            record=copy.deepcopy(arguments["p_record"]);s["verifications"][record["verification_id"]]=record;return record
        if name=="haris_event_sequence":return {"sequence":s["sequence"]}
        if name=="haris_save_checkpoint":
            record=copy.deepcopy(arguments["p_record"]);record["snapshot_id"]=len(s["checkpoints"])+1;s["checkpoints"].append(record);return record
        if name=="haris_latest_checkpoint":return copy.deepcopy(s["checkpoints"][-1]) if s["checkpoints"] else {}
        if name=="haris_read_domain":
            kind=arguments["p_kind"];key=arguments.get("p_key")
            mapping={"event":s["events"],"network":s["network"],"incident":s["incidents"],"ownership":s["ownership"],"action":s["actions"],"verification":s["verifications"]}
            if kind=="transition":rows=[r for r in s["transitions"] if r["incident_id"]==key]
            else:
                values=mapping[kind]
                rows=[values[key]] if key in values else [] if key else list(values.values())
            return copy.deepcopy(rows[:arguments.get("p_limit",200)])
        raise AssertionError(f"unexpected offline RPC: {name}")


def integration_state():
    return {"events":{},"network":{},"incidents":{},"transitions":[],"ownership":{},"actions":{},"verifications":{},"checkpoints":[],"sequence":0}


class PersistenceCompositionTests(unittest.TestCase):
    def test_single_composition_root_modes_and_exact_hostname(self):
        calls=[]
        factory=lambda _settings,hostname: calls.append(hostname) or Rpc()
        self.assertIsInstance(build_repository_bundle(settings("postgres",url="https://project.supabase.co",key="secret-placeholder"),runtime=RuntimeEnvironment.TEST,transport_factory=factory),InMemoryRepositoryBundle)
        self.assertEqual(calls,[])
        self.assertIsInstance(build_repository_bundle(settings(),runtime=RuntimeEnvironment.DEVELOPMENT,transport_factory=factory),InMemoryRepositoryBundle)
        production=build_repository_bundle(settings("postgres",url="https://project.supabase.co",key="secret-placeholder"),runtime=RuntimeEnvironment.PRODUCTION,transport_factory=factory)
        integration=build_repository_bundle(settings("postgres",url="https://project.supabase.co",key="secret-placeholder"),runtime=RuntimeEnvironment.PERSISTENCE_INTEGRATION,transport_factory=factory)
        self.assertIsInstance(production,PostgresRepositoryBundle);self.assertIsInstance(integration,PostgresRepositoryBundle)
        self.assertEqual(calls,["project.supabase.co","project.supabase.co"])

    def test_postgres_configuration_and_url_fail_before_factory(self):
        for candidate in (
            settings("postgres"), settings("postgres",url="https://project.supabase.co"),
            settings("postgres",key="secret-placeholder"),
            settings("postgres",url="http://project.supabase.co",key="secret-placeholder"),
            settings("postgres",url="https://user:pass@project.supabase.co",key="secret-placeholder"),
            settings("unknown"),
        ):
            calls=[]
            with self.assertRaises(PersistenceNotConfigured):
                build_repository_bundle(candidate,runtime=RuntimeEnvironment.PRODUCTION,transport_factory=lambda *_:calls.append(True))
            self.assertEqual(calls,[])

    def test_real_transport_is_lazy_and_factory_failure_never_falls_back(self):
        configured=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")
        transport=build_persistence_transport(configured,RuntimeEnvironment.PRODUCTION)
        self.addCleanup(transport.close)
        self.assertEqual(transport.hostname,"project.supabase.co")
        self.assertIsNone(transport._client)
        with self.assertRaises(RepositoryUnavailable):
            build_repository_bundle(configured,runtime=RuntimeEnvironment.PRODUCTION,transport_factory=lambda *_:(_ for _ in ()).throw(RepositoryUnavailable("down")))


class ReconstructionTests(unittest.TestCase):
    def bundle(self):
        bundle=InMemoryRepositoryBundle()
        incident={"incident_id":"inc-1","correlation_key":"cell:T03","primary_entity":"T03","state":"DETECTED","version":0,"opened_at":1.0}
        bundle.incidents.create_or_get_active(incident)
        bundle.network_state.save({"entity_id":"T03","version":0,"provenance":"FIXTURE_SIMULATED"},0)
        bundle.resource_ownership.acquire({"resource_key":"device:a","owner_incident_id":"inc-1","ownership_state":"OWNED","version":0})
        bundle.actions.create_or_get(ActionCommand(incident_id="inc-1",command_type="QOD_CREATE",resource_key="device:a",device_id="a",plan_version=1,requested_at=1.0,state=ActionState.SENT))
        bundle.verification.save({"verification_id":"v1","incident_id":"inc-1","state":"PENDING"})
        bundle.recovery.save({"recovery_id":"r1","incident_id":"inc-1","state":"PENDING"})
        bundle.outbox.append({"outbox_id":"o1","event_id":"e1","event_type":"X","created_at":1.0})
        return bundle

    def test_bounded_reconstruction_order_and_sent_unknown(self):
        bundle=self.bundle();order=[]
        def wrap(obj,name,label):
            original=getattr(obj,name)
            def called(*args,**kwargs):order.append(label);return original(*args,**kwargs)
            setattr(obj,name,called)
        for obj,name,label in (
            (bundle.checkpoints,"latest","checkpoint"),(bundle.network_state,"load_all","network"),
            (bundle.incidents,"active","incidents"),(bundle.resource_ownership,"active","ownership"),
            (bundle.actions,"pending_or_unknown","actions"),(bundle.actions,"mark_restart_unknown","mark_unknown"),
            (bundle.actions,"reconciliation_required","reconciliation"),(bundle.verification,"pending","verification"),
            (bundle.recovery,"pending","recovery"),(bundle.outbox,"unsent","outbox"),(bundle.events,"sequence","sequence"),
        ):wrap(obj,name,label)
        result=reconstruct_platform_state(bundle)
        self.assertEqual(order[:6],["checkpoint","network","incidents","ownership","actions","mark_unknown"])
        self.assertEqual(result.pending_actions[0].state,ActionState.OUTCOME_UNKNOWN)
        self.assertEqual(result.reconciliation_required[0].state,ActionState.OUTCOME_UNKNOWN)
        self.assertEqual(bundle.actions.get(result.pending_actions[0].command_id).state,ActionState.OUTCOME_UNKNOWN)

    def test_integrity_conflicts_fail_closed(self):
        bundle=self.bundle()
        bundle.incidents._items["inc-2"]={"incident_id":"inc-2","correlation_key":"cell:T03","primary_entity":"T04","state":"DETECTED","version":0}
        with self.assertRaises(ProjectionIntegrityError):reconstruct_platform_state(bundle)
        bundle=self.bundle();bundle.actions._by_id[next(iter(bundle.actions._by_id))].incident_id="missing"
        with self.assertRaises(ProjectionIntegrityError):reconstruct_platform_state(bundle)

    def test_checkpoint_projection_integrity_and_version_regression(self):
        bundle=self.bundle();bundle.checkpoints.save({"T03":{"entity_id":"T03","version":99}},0,1.0)
        with self.assertRaises(ProjectionIntegrityError):reconstruct_platform_state(bundle)
        bundle=self.bundle();bundle.network_state._items["T03"]["version"]=-1
        with self.assertRaises(ProjectionIntegrityError):reconstruct_platform_state(bundle)

    def test_persistence_integration_namespace_is_not_in_noc_views(self):
        bundle=InMemoryRepositoryBundle()
        bundle.network_state._items["PERSISTENCE-TEST-CELL-run"]={"entity_id":"PERSISTENCE-TEST-CELL-run","version":0,"source_mode":"PERSISTENCE_INTEGRATION_TEST"}
        bundle.incidents._items["PERSISTENCE-TEST-INC-run"]={"incident_id":"PERSISTENCE-TEST-INC-run","correlation_key":"persistence:run","primary_entity":"PERSISTENCE-TEST-CELL-run","state":"DETECTED","version":0}
        bundle.incidents._transitions["PERSISTENCE-TEST-INC-run"]=[]
        result=reconstruct_platform_state(bundle)
        self.assertEqual(result.network_state,{});self.assertEqual(result.active_incidents,[])
        core=DurablePlatformCore(events=bundle.events,network=bundle.network_state,incidents=bundle.incidents,actions=bundle.actions,ownership=bundle.resource_ownership,verifications=bundle.verification,recoveries=bundle.recovery,outbox=bundle.outbox,inbox=bundle.inbox,checkpoints=bundle.checkpoints,cost_ledger=bundle.cost_ledger)
        core.restore_reconstruction(result);snapshot=core.snapshot()
        self.assertEqual(snapshot["network_state"],{});self.assertEqual(snapshot["active_incidents"],[])


class ApiLifecycleTests(unittest.TestCase):
    def setUp(self):
        import run_api
        self.run_api=run_api
        names=("_durable_core","_repository_bundle","_persistence_error","_platform_lifecycle","_network_state","_incident_manager","_system","_scheduler","_scheduler_task","_observation_task","_observations","_runtime_ingestor","_runtime_consumer","_runtime_task","_runtime_metrics","_durable_decision_service")
        self.names=names;self.previous={name:getattr(run_api,name) for name in names}
        run_api._durable_core=None;run_api._repository_bundle=None;run_api._persistence_error=None
        run_api._platform_lifecycle=PlatformLifecycle();run_api._network_state=None;run_api._incident_manager=None;run_api._system=None;run_api._scheduler=None;run_api._scheduler_task=None;run_api._observation_task=None;run_api._observations=None
        self.settings_patch=patch.object(run_api,"get_settings",return_value=settings())
        self.settings_patch.start()

    def tearDown(self):
        self.settings_patch.stop()
        for name,value in self.previous.items():setattr(self.run_api,name,value)

    def test_memory_ready_rebuilds_views_and_health_snapshot(self):
        self.assertTrue(self.run_api.initialize_platform(settings=settings(),runtime=RuntimeEnvironment.DEVELOPMENT))
        health=asyncio.run(self.run_api.platform_health(Response()));snapshot=asyncio.run(self.run_api.noc_snapshot())
        self.assertEqual(health["status"],"READY");self.assertTrue(health["persistence"]["repository_ready"])
        self.assertEqual(snapshot["platform_status"],"READY");self.assertIsNotNone(self.run_api._network_state);self.assertIsNotNone(self.run_api._incident_manager)

    def test_postgres_not_configured_is_not_ready_without_memory_fallback(self):
        self.assertFalse(self.run_api.initialize_platform(settings=settings("postgres"),runtime=RuntimeEnvironment.PRODUCTION))
        health=asyncio.run(self.run_api.platform_health(Response()));snapshot=asyncio.run(self.run_api.noc_snapshot())
        self.assertEqual(health["status"],"NOT_READY");self.assertEqual(health["persistence"]["reason"],"PERSISTENCE_NOT_CONFIGURED")
        self.assertIsNone(self.run_api._repository_bundle);self.assertIsNone(self.run_api._durable_core)
        self.assertNotIn("network_state",snapshot)

    def test_postgres_mock_reconstructs_ready_and_transport_failure_is_not_ready(self):
        configured=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")
        transport=MockPostgresTransport({"haris_latest_checkpoint":{},"haris_read_domain":[],"haris_pending_outbox":[],"haris_event_sequence":{"sequence":0}})
        self.assertTrue(self.run_api.initialize_platform(settings=configured,runtime=RuntimeEnvironment.PRODUCTION,transport_factory=lambda *_:transport))
        health=asyncio.run(self.run_api.platform_health(Response()))
        self.assertEqual(health["status"],"READY");self.assertEqual(health["persistence"]["mode"],"postgres");self.assertTrue(health["persistence"]["connected"])
        self.assertFalse(self.run_api.initialize_platform(settings=configured,runtime=RuntimeEnvironment.PRODUCTION,transport_factory=lambda *_:(_ for _ in ()).throw(RepositoryUnavailable("private transport error"))))
        health=asyncio.run(self.run_api.platform_health(Response()))
        self.assertEqual(health["persistence"]["reason"],"PERSISTENCE_UNAVAILABLE");self.assertFalse(health["persistence"]["repository_ready"]);self.assertIsNone(self.run_api._durable_core)

    def test_real_transport_404_during_reconstruction_is_schema_not_ready(self):
        import httpx
        from supabase_transport import SupabaseRpcTransport
        configured=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")
        client=httpx.Client(transport=httpx.MockTransport(lambda _request:httpx.Response(404,json={"message":"private"})))
        self.addCleanup(client.close)
        transport=SupabaseRpcTransport(
            "https://project.supabase.co","secret-placeholder",
            expected_hostname="project.supabase.co",client=client,
            network_authorizer=lambda _hostname:None,
        )
        self.assertFalse(self.run_api.initialize_platform(
            settings=configured,runtime=RuntimeEnvironment.PRODUCTION,
            transport_factory=lambda *_:transport,
        ))
        health=asyncio.run(self.run_api.platform_health(Response()))
        self.assertEqual(health["persistence"]["reason"],"PERSISTENCE_SCHEMA_NOT_READY")
        self.assertFalse(health["persistence"]["repository_ready"])
        self.assertIsNone(self.run_api._durable_core)
        self.assertNotIn("private",str(health).lower())

    def test_reconstruction_failure_and_integrity_failure_are_safe(self):
        configured=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")
        bundle=InMemoryRepositoryBundle()
        with patch.object(bundle.network_state,"load_all",side_effect=RepositoryUnavailable("raw private failure")),patch.object(self.run_api,"build_repository_bundle",return_value=bundle):
            self.assertFalse(self.run_api.initialize_platform(settings=configured,runtime=RuntimeEnvironment.PRODUCTION))
        self.assertEqual(asyncio.run(self.run_api.platform_health(Response()))["persistence"]["reason"],"RECONSTRUCTION_FAILED")
        bundle=InMemoryRepositoryBundle();bundle.network_state._items["bad"]={"entity_id":"different","version":0}
        with patch.object(self.run_api,"build_repository_bundle",return_value=bundle):
            self.assertFalse(self.run_api.initialize_platform(settings=configured,runtime=RuntimeEnvironment.PRODUCTION))
        health=asyncio.run(self.run_api.platform_health(Response()))
        self.assertEqual(health["persistence"]["reason"],"PROJECTION_INTEGRITY_FAILED");self.assertNotIn("private",str(health).lower())

    def test_auth_and_schema_failures_have_explicit_public_reasons(self):
        configured=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")
        for failure,reason in (
            (PersistenceAuthenticationFailed("private auth detail"),"PERSISTENCE_AUTH_FAILED"),
            (PersistenceSchemaNotReady("private schema detail"),"PERSISTENCE_SCHEMA_NOT_READY"),
        ):
            with self.subTest(reason=reason):
                self.assertFalse(self.run_api.initialize_platform(
                    settings=configured,runtime=RuntimeEnvironment.PRODUCTION,
                    transport_factory=lambda *_args,error=failure:(_ for _ in ()).throw(error),
                ))
                public=asyncio.run(self.run_api.platform_health(Response()))["persistence"]
                self.assertEqual(public["reason"],reason)
                self.assertNotIn("private",str(public).lower())
                snapshot=asyncio.run(self.run_api.noc_snapshot())
                self.assertEqual(snapshot["persistence"]["reason"],reason)
                self.assertNotIn("secret-placeholder",str((public,snapshot)))
                class Socket:
                    def __init__(self):
                        self.sent=[];self.headers={"authorization":"Bearer test-operational-token"};self.client=type("Client",(),{"host":"127.0.0.1"})()
                    async def accept(self):pass
                    async def send_json(self,value):self.sent.append(value)
                    async def receive_text(self):
                        from fastapi import WebSocketDisconnect
                        raise WebSocketDisconnect()
                socket=Socket();asyncio.run(self.run_api.noc_websocket(socket))
                self.assertEqual(socket.sent[0]["type"],"platform_state")
                self.assertNotIn("secret-placeholder",str(socket.sent))

    def test_workers_are_created_only_after_ready_and_never_on_failure(self):
        observed=[]
        class Task:
            def cancel(self):pass
            def done(self):return True
        def create(coroutine,**_kwargs):
            observed.append(self.run_api._platform_lifecycle.state.value);coroutine.close();return Task()
        with patch.object(self.run_api.asyncio,"create_task",side_effect=create),patch.object(self.run_api,"get_settings",return_value=settings(observations=True,loop=True)):
            asyncio.run(self.run_api.start_haris_scheduler())
        self.assertEqual(observed,["READY","READY"])
        self.assertEqual(self.run_api._platform_lifecycle.history,["STARTING","PERSISTENCE_CONFIGURING","PERSISTENCE_CONNECTING","RECONSTRUCTING","READY"])
        observed.clear()
        with patch.object(self.run_api.asyncio,"create_task",side_effect=create),patch.object(self.run_api,"get_settings",return_value=settings("postgres")):
            asyncio.run(self.run_api.start_haris_scheduler())
        self.assertEqual(observed,[])

    def test_websocket_first_message_ready_and_not_ready(self):
        class Socket:
            def __init__(self):
                self.sent=[];self.headers={"authorization":"Bearer test-operational-token"};self.client=type("Client",(),{"host":"127.0.0.1"})()
            async def accept(self):pass
            async def send_json(self,value):self.sent.append(value)
            async def receive_text(self):
                from fastapi import WebSocketDisconnect
                raise WebSocketDisconnect()
        socket=Socket();asyncio.run(self.run_api.noc_websocket(socket));self.assertEqual(socket.sent[0]["type"],"platform_state");self.assertNotIn("network_state",socket.sent[0]["data"])
        self.assertTrue(self.run_api.initialize_platform(settings=settings(),runtime=RuntimeEnvironment.DEVELOPMENT))
        socket=Socket();asyncio.run(self.run_api.noc_websocket(socket));self.assertEqual(socket.sent[0]["type"],"snapshot")

    def test_reconstructing_api_and_websocket_never_fabricate_state(self):
        self.run_api._platform_lifecycle.mode="postgres";self.run_api._platform_lifecycle.state=PlatformLifecycleState.RECONSTRUCTING;self.run_api._platform_lifecycle.reconstruction="IN_PROGRESS";self.run_api._platform_lifecycle.configured=True;self.run_api._platform_lifecycle.connected=True
        snapshot=asyncio.run(self.run_api.noc_snapshot());self.assertEqual(snapshot["lifecycle_state"],"RECONSTRUCTING");self.assertNotIn("network_state",snapshot)
        class Socket:
            def __init__(self):
                self.sent=[];self.headers={"authorization":"Bearer test-operational-token"};self.client=type("Client",(),{"host":"127.0.0.1"})()
            async def accept(self):pass
            async def send_json(self,value):self.sent.append(value)
            async def receive_text(self):
                from fastapi import WebSocketDisconnect
                raise WebSocketDisconnect()
        socket=Socket();asyncio.run(self.run_api.noc_websocket(socket));self.assertEqual(socket.sent[0]["type"],"platform_state");self.assertEqual(socket.sent[0]["data"]["lifecycle_state"],"RECONSTRUCTING")

    def test_repository_failure_after_ready_degrades_all_read_surfaces(self):
        self.assertTrue(self.run_api.initialize_platform(settings=settings(),runtime=RuntimeEnvironment.DEVELOPMENT))
        class Socket:
            def __init__(self):
                self.sent=[];self.headers={"authorization":"Bearer test-operational-token"};self.client=type("Client",(),{"host":"127.0.0.1"})()
            async def accept(self):pass
            async def send_json(self,value):self.sent.append(value)
            async def receive_text(self):
                from fastapi import WebSocketDisconnect
                raise WebSocketDisconnect()
        with patch.object(self.run_api._durable_core.events,"sequence",side_effect=RepositoryUnavailable("private")):
            health=asyncio.run(self.run_api.platform_health(Response()));snapshot=asyncio.run(self.run_api.noc_snapshot());socket=Socket();asyncio.run(self.run_api.noc_websocket(socket))
        self.assertEqual(health["status"],"NOT_READY");self.assertEqual(snapshot["platform_status"],"NOT_READY");self.assertEqual(socket.sent[0]["type"],"platform_state")
        self.assertNotIn("private",str((health,snapshot,socket.sent)).lower())


class RestartHarnessTests(unittest.TestCase):
    def test_integration_identifiers_are_unique_and_run_scoped(self):
        first=persistence_validator._integration_ids("deadbeef12345678")
        second=persistence_validator._integration_ids("feedface87654321")
        self.assertEqual(first["correlation_key"],"persistence-integration:deadbeef12345678")
        self.assertEqual(second["correlation_key"],"persistence-integration:feedface87654321")
        for key in ("event_id","entity_id","incident_id","correlation_key","resource_key","action_id","verification_id"):
            self.assertNotEqual(first[key],second[key])

    def integration_environment(self):
        return {
            "HARIS_RUNTIME_ENV":"persistence_integration",
            "HARIS_ALLOW_PERSISTENCE_INTEGRATION":"true",
            "HARIS_PERSISTENCE_MODE":"postgres",
            "SUPABASE_URL":"https://project.supabase.co",
            "SUPABASE_KEY":"test-only-placeholder-key",
        }

    def test_real_mode_children_build_fresh_bundles_and_reconstruct_shared_rows(self):
        state=integration_state();transports=[]
        def fresh_bundle():
            transport=StatefulIntegrationTransport(state);transports.append(transport)
            return PostgresRepositoryBundle(transport),transport
        stdout_a,stdout_b=io.StringIO(),io.StringIO()
        with tempfile.TemporaryDirectory(prefix="haris-real-mode-offline-") as directory:
            state_file=Path(directory)/"identifiers.json"
            with patch.dict(os.environ,self.integration_environment(),clear=True),patch.object(persistence_validator,"_bundle",side_effect=fresh_bundle):
                with contextlib.redirect_stdout(stdout_a):
                    self.assertEqual(persistence_validator._integration_child_a("deadbeef12345678",state_file),0)
                identifiers=json.loads(state_file.read_text(encoding="utf-8"))
                with contextlib.redirect_stdout(stdout_b):
                    self.assertEqual(persistence_validator._integration_child_b("deadbeef12345678",state_file),0)
        self.assertEqual(len(transports),2);self.assertIsNot(transports[0],transports[1])
        self.assertTrue(all(transport.closed for transport in transports))
        self.assertEqual(json.loads(stdout_a.getvalue())["status"],"PROCESS_A_OK")
        process_b=json.loads(stdout_b.getvalue())
        self.assertEqual(process_b["status"],"PROCESS_B_OK")
        self.assertEqual(process_b["stored_action_state"],"SENT")
        self.assertEqual(process_b["runtime_action_state"],"OUTCOME_UNKNOWN")
        self.assertTrue(process_b["reconciliation_required"]);self.assertEqual(process_b["duplicate_incidents"],0)
        self.assertEqual(state["actions"][identifiers["action_id"]]["state"],"SENT")
        self.assertEqual(set(identifiers),{"status","run_id","event_id","entity_id","incident_id","correlation_key","resource_key","action_id","verification_id","checkpoint_id"})
        self.assertTrue(all(record["source_mode"]=="PERSISTENCE_INTEGRATION_TEST" for record in state["events"].values()))
        self.assertFalse(any(name.startswith("nokia") for transport in transports for name,_args in transport.calls))

    def test_integration_bundle_uses_proven_transport_then_postgres_bundle(self):
        transport=StatefulIntegrationTransport(integration_state())
        configured=settings("postgres",url="https://project.supabase.co",key="test-only-placeholder-key")
        with patch.dict(os.environ,self.integration_environment(),clear=True),patch("config.get_settings",return_value=configured),patch("postgres_persistence.build_persistence_transport",return_value=transport) as build:
            bundle,returned=persistence_validator._bundle()
        self.assertIsInstance(bundle,PostgresRepositoryBundle);self.assertIs(returned,transport)
        build.assert_called_once_with(configured,runtime=RuntimeEnvironment.PERSISTENCE_INTEGRATION)

    def test_integration_controller_preflights_spawns_a_then_b_and_compares_ids(self):
        def child(stage,run_id,_state_file,*,integration,fail=False):
            self.assertTrue(integration);self.assertFalse(fail)
            ids=persistence_validator._integration_ids(run_id)
            common={"status":f"PROCESS_{stage}_OK",**ids,"checkpoint_id":7}
            if stage=="B":common.update(stored_action_state="SENT",runtime_action_state="OUTCOME_UNKNOWN",reconciliation_required=True,duplicate_incidents=0)
            return common
        with patch.dict(os.environ,self.integration_environment(),clear=True),patch.object(persistence_validator,"run_read_only_preflight",return_value={"status":"PERSISTENCE_PREFLIGHT_OK"}),patch.object(persistence_validator,"_run_child",side_effect=child) as spawned:
            result=persistence_validator.run_integration_controller()
        self.assertEqual([call.args[0] for call in spawned.call_args_list],["A","B"])
        self.assertEqual(result["status"],"PERSISTENCE_RESTART_VALIDATED")
        self.assertEqual(result["sent_action_outcome_unknown"],"PASS")

    def test_missing_integration_gate_fails_before_preflight_or_child(self):
        with patch.dict(os.environ,{},clear=True),patch.object(persistence_validator,"run_read_only_preflight",side_effect=AssertionError("preflight called")),patch.object(persistence_validator,"_run_child",side_effect=AssertionError("child called")):
            with self.assertRaises(PersistenceNetworkBlocked):
                persistence_validator.run_integration_controller()

    def test_real_mode_failures_emit_only_safe_machine_reason(self):
        sensitive="private-key-and-upstream-body"
        stdout=io.StringIO()
        with patch.dict(os.environ,self.integration_environment(),clear=True),patch.object(persistence_validator,"_bundle",side_effect=RuntimeError(sensitive)),contextlib.redirect_stdout(stdout):
            with tempfile.TemporaryDirectory(prefix="haris-real-mode-failure-") as directory:
                code=persistence_validator._integration_child_a("deadbeef12345678",Path(directory)/"identifiers.json")
        self.assertEqual(code,3)
        self.assertEqual(json.loads(stdout.getvalue()),{"process":"A","safe_reason":"PERSISTENCE_UNAVAILABLE","status":"PROCESS_A_FAILED"})
        self.assertNotIn(sensitive,stdout.getvalue())

    def test_contract_failure_reports_only_allowlisted_stage_rpc_and_shapes(self):
        from postgres_persistence import PersistenceRpcContractFailed
        state=integration_state()
        class ContractFailureTransport(StatefulIntegrationTransport):
            def rpc(self,name,arguments):
                if name=="haris_write_network_state":
                    raise PersistenceRpcContractFailed(
                        "private upstream row and key",http_status=400,rpc_name=name,
                        expected_shape="object",actual_shape_type="unknown",
                    )
                return super().rpc(name,arguments)
        transport=ContractFailureTransport(state);stdout=io.StringIO()
        with tempfile.TemporaryDirectory(prefix="haris-contract-diagnostic-") as directory:
            with patch.dict(os.environ,self.integration_environment(),clear=True),patch.object(persistence_validator,"_bundle",return_value=(PostgresRepositoryBundle(transport),transport)),contextlib.redirect_stdout(stdout):
                code=persistence_validator._integration_child_a("deadbeef12345678",Path(directory)/"identifiers.json")
        self.assertEqual(code,3)
        self.assertEqual(json.loads(stdout.getvalue()),{
            "status":"PROCESS_A_FAILED","safe_reason":"PERSISTENCE_RPC_CONTRACT_FAILED",
            "process":"A","stage":"WRITE_NETWORK_STATE",
            "rpc_name":"haris_write_network_state","expected_shape":"object",
            "actual_shape_type":"unknown","http_status":400,
        })
        self.assertNotIn("private",stdout.getvalue())

    def test_child_contract_context_survives_parent_without_untrusted_metadata(self):
        safe={"status":"PROCESS_B_FAILED","safe_reason":"PERSISTENCE_RPC_CONTRACT_FAILED","process":"B","stage":"READ_ACTION","rpc_name":"haris_read_domain","expected_shape":"array","actual_shape_type":"object"}
        with self.assertRaises(persistence_validator.PersistenceValidationFailure) as raised:
            persistence_validator._parse_output(subprocess.CompletedProcess(["validator"],3,json.dumps(safe),""))
        self.assertEqual(raised.exception.process,"B");self.assertEqual(raised.exception.stage,"READ_ACTION")
        self.assertEqual(raised.exception.rpc_name,"haris_read_domain");self.assertEqual(raised.exception.actual_shape_type,"object")
        unsafe={**safe,"rpc_name":"private_rpc_name","stage":"private stage","actual_shape_type":"private value"}
        with self.assertRaises(persistence_validator.PersistenceValidationFailure) as raised:
            persistence_validator._parse_output(subprocess.CompletedProcess(["validator"],3,json.dumps(unsafe),""))
        self.assertIsNone(raised.exception.rpc_name);self.assertIsNone(raised.exception.stage)
        self.assertEqual(raised.exception.actual_shape_type,"unknown")

    def test_direct_validator_resolves_project_root_from_any_working_directory(self):
        project_root=Path(__file__).resolve().parent
        validator=project_root/"external"/"validate_persistence_restart.py"
        environment=sanitized_child_environment(dict(os.environ),integration=False)
        # The validator must bootstrap itself; do not let PYTHONPATH mask a
        # regression in direct-script import resolution.
        environment.pop("PYTHONPATH",None)
        with tempfile.TemporaryDirectory(prefix="haris-validator-cwd-") as other_directory:
            for working_directory in (project_root,Path(other_directory)):
                with self.subTest(cwd=str(working_directory)):
                    completed=subprocess.run(
                        [sys.executable,str(validator),"--preflight"],
                        cwd=str(working_directory),env=environment,
                        capture_output=True,text=True,timeout=15,check=False,
                    )
                    self.assertEqual(completed.returncode,3)
                    self.assertEqual(completed.stderr,"")
                    diagnostic=json.loads(completed.stdout)
                    self.assertEqual(diagnostic,{"process":"PARENT","safe_reason":"PERSISTENCE_NETWORK_BLOCKED","status":"PERSISTENCE_PREFLIGHT_FAILED"})
                    self.assertNotIn("ModuleNotFoundError",completed.stdout+completed.stderr)

            completed=subprocess.run(
                [sys.executable,str(validator),"--offline-orchestration"],
                cwd=other_directory,env=environment,
                capture_output=True,text=True,timeout=30,check=False,
            )
            self.assertEqual(completed.returncode,0)
            self.assertEqual(completed.stderr,"")
            self.assertEqual(json.loads(completed.stdout)["status"],"OFFLINE_HARNESS_ORCHESTRATION=PASS")

    def test_gates_and_exact_hostname_allowlist_contract(self):
        base={"HARIS_RUNTIME_ENV":"persistence_integration","HARIS_ALLOW_PERSISTENCE_INTEGRATION":"true","HARIS_PERSISTENCE_MODE":"postgres","SUPABASE_URL":"https://project.supabase.co","SUPABASE_KEY":"secret-placeholder"}
        self.assertEqual(integration_gate(base),(True,"AUTHORIZED","project.supabase.co"))
        for change in ({"HARIS_ALLOW_PERSISTENCE_INTEGRATION":"false"},{"SUPABASE_KEY":""},{"SUPABASE_URL":"http://project.supabase.co"},{"SUPABASE_URL":"https://project.supabase.co?x=1"}):
            candidate={**base,**change};self.assertFalse(integration_gate(candidate)[0])

    def test_persistence_integration_policy_denies_generic_external_http(self):
        environment={"HARIS_RUNTIME_ENV":"persistence_integration","HARIS_ALLOW_PERSISTENCE_INTEGRATION":"true"}
        with patch.dict(os.environ,environment,clear=True),patch.object(sys,"argv",["validator"]):
            policy=external_access_policy()
        self.assertTrue(policy.allow_remote_database);self.assertTrue(policy.allow_persistence_http)
        self.assertFalse(policy.allow_external_http);self.assertFalse(policy.allow_nokia_read)
        self.assertFalse(policy.allow_nokia_write);self.assertFalse(policy.allow_llm);self.assertFalse(policy.allow_oauth)

    def test_child_environment_is_allowlisted_and_never_printed(self):
        source={"PATH":os.environ.get("PATH",""),"HARIS_RUNTIME_ENV":"persistence_integration","HARIS_ALLOW_PERSISTENCE_INTEGRATION":"true","HARIS_PERSISTENCE_MODE":"postgres","SUPABASE_URL":"https://project.supabase.co","SUPABASE_KEY":"secret-placeholder","NAC_API_TOKEN":"blocked","GEMINI_API_KEY":"blocked","GROQ_API_KEY":"blocked","REDIS_URL":"blocked","WEATHER_API_KEY":"blocked"}
        integration=sanitized_child_environment(source,integration=True)
        self.assertEqual(integration["SUPABASE_KEY"],"secret-placeholder")
        for key in ("NAC_API_TOKEN","GEMINI_API_KEY","GROQ_API_KEY","REDIS_URL","WEATHER_API_KEY"):self.assertNotIn(key,integration)
        offline=sanitized_child_environment(source,integration=False);self.assertNotIn("SUPABASE_KEY",offline);self.assertEqual(offline["HARIS_RUNTIME_ENV"],"test")

    def test_offline_two_process_orchestration_and_error_propagation(self):
        result=run_offline_orchestration()
        self.assertEqual(result["status"],"OFFLINE_HARNESS_ORCHESTRATION=PASS")
        self.assertEqual(result["process_a"]["incident_id"],result["process_b"]["incident_id"])
        self.assertEqual(result["process_b"]["action_state"],"OUTCOME_UNKNOWN")
        with self.assertRaises(RuntimeError):run_offline_orchestration(fail_stage="B")

    def test_read_only_preflight_contract_uses_transport_without_provider(self):
        class Transport:
            def __init__(self):self.preflight_calls=0;self.closed=False
            def preflight(self):self.preflight_calls+=1;return []
            def close(self):self.closed=True
        transport=Transport()
        with patch("external.validate_persistence_restart.integration_gate",return_value=(True,"AUTHORIZED","project.supabase.co")),patch("config.get_settings",return_value=settings("postgres",url="https://project.supabase.co",key="secret-placeholder")),patch("postgres_persistence.build_persistence_transport",return_value=transport):
            result=run_read_only_preflight()
        self.assertEqual(result,{"status":"PERSISTENCE_PREFLIGHT_OK","safe_reason":"PERSISTENCE_PREFLIGHT_OK"})
        self.assertEqual(transport.preflight_calls,1);self.assertTrue(transport.closed)

    def test_preflight_gate_failures_are_typed_without_network(self):
        with patch.dict(os.environ,{},clear=True):
            with self.assertRaises(PersistenceNetworkBlocked):run_read_only_preflight()
        invalid={"HARIS_RUNTIME_ENV":"persistence_integration","HARIS_ALLOW_PERSISTENCE_INTEGRATION":"true","HARIS_PERSISTENCE_MODE":"postgres","SUPABASE_URL":"https://not-supabase.invalid/path?secret=value","SUPABASE_KEY":"test-placeholder"}
        with patch.dict(os.environ,invalid,clear=True):
            with self.assertRaises(PersistenceHostNotAllowed):run_read_only_preflight()

    def test_preflight_cli_emits_only_allowlisted_machine_readable_diagnostics(self):
        sensitive="do-not-emit-sensitive-material"
        cases=(
            (PersistenceAuthenticationFailed(sensitive,http_status=401),"PERSISTENCE_AUTH_FAILED",401),
            (PersistenceSchemaNotReady(sensitive,http_status=404),"PERSISTENCE_SCHEMA_NOT_READY",404),
            (PersistenceTransportUnavailable(sensitive),"PERSISTENCE_UNAVAILABLE",None),
            (PersistenceRpcContractFailed(sensitive),"PERSISTENCE_RPC_CONTRACT_FAILED",None),
            (PersistenceResponseInvalid(sensitive),"PERSISTENCE_RESPONSE_INVALID",None),
            (PersistenceNetworkBlocked(sensitive),"PERSISTENCE_NETWORK_BLOCKED",None),
            (PersistenceHostNotAllowed(sensitive),"PERSISTENCE_HOST_NOT_ALLOWED",None),
            (RuntimeError(sensitive+" Authorization=secret https://private.invalid"),"PERSISTENCE_UNAVAILABLE",None),
        )
        for error,reason,status in cases:
            stdout,stderr=io.StringIO(),io.StringIO()
            with self.subTest(reason=reason),patch("external.validate_persistence_restart.run_read_only_preflight",side_effect=error),patch("external.validate_persistence_restart.run_integration_controller",side_effect=AssertionError("write path called")),contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
                code=persistence_validator_main(["--preflight"])
            self.assertEqual(code,3);self.assertEqual(stderr.getvalue(),"")
            diagnostic=json.loads(stdout.getvalue())
            self.assertEqual(diagnostic["status"],"PERSISTENCE_PREFLIGHT_FAILED")
            self.assertEqual(diagnostic["safe_reason"],reason)
            self.assertEqual(diagnostic.get("http_status"),status)
            combined=stdout.getvalue()+stderr.getvalue()+json.dumps(diagnostic)
            self.assertNotIn(sensitive,combined)
            self.assertNotIn("Authorization=secret",combined)
            self.assertNotIn("private.invalid",combined)
            if not isinstance(error,RuntimeError) or isinstance(error,RepositoryUnavailable):
                self.assertNotIn(sensitive,str(error))

    def test_preflight_cli_success_is_single_safe_json_result(self):
        result={"status":"PERSISTENCE_PREFLIGHT_OK","safe_reason":"PERSISTENCE_PREFLIGHT_OK"}
        stdout,stderr=io.StringIO(),io.StringIO()
        with patch("external.validate_persistence_restart.run_read_only_preflight",return_value=result),patch("external.validate_persistence_restart.run_integration_controller",side_effect=AssertionError("write path called")),contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):
            code=persistence_validator_main(["--preflight"])
        self.assertEqual(code,0);self.assertEqual(stderr.getvalue(),"")
        self.assertEqual(json.loads(stdout.getvalue()),result)

    def test_imports_have_no_provider_side_effect(self):
        import durable_core, postgres_persistence, platform_lifecycle, run_api, runtime
        self.assertTrue(all((durable_core,postgres_persistence,platform_lifecycle,run_api,runtime)))
        self.assertEqual(provider_access_count(),0)


if __name__ == "__main__":
    unittest.main()
