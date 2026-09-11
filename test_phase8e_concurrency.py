import asyncio
import time
import unittest
from pathlib import Path

from durable_core import InMemoryOutboxRepository, InMemoryResourceOwnershipRepository, ResourceAlreadyOwned
from event_bus import InMemoryEventBus
from observations import ObservationStore
from runtime_events import DurableOutboxWakeupConsumer


class LeaseConcurrencyTests(unittest.TestCase):
    def lease(self, owner, start, expires):
        return {"resource_key": "device:test", "resource_type": "DEVICE", "owner_incident_id": owner,
                "acquired_at": start, "lease_started_at": start, "lease_expires_at": expires,
                "renewable": True, "adopted_by_incident": False, "provider_resource_id": None}

    def test_valid_lease_cannot_be_stolen_and_current_owner_can_renew_release(self):
        repo = InMemoryResourceOwnershipRepository(); now = time.time()
        owned = repo.acquire(self.lease("inc-a", now, now + 30))
        with self.assertRaises(ResourceAlreadyOwned): repo.acquire(self.lease("inc-b", now, now + 30))
        renewed = repo.renew("device:test", "inc-a", owned["version"], 30)
        with self.assertRaises(ResourceAlreadyOwned):
            repo.release("device:test", "inc-a", now, owned["version"])
        released = repo.release("device:test", "inc-a", now, renewed["version"])
        self.assertEqual(released["ownership_state"], "RELEASED")

    def test_expired_lease_reclaim_invalidates_stale_owner(self):
        repo = InMemoryResourceOwnershipRepository(); now = time.time()
        old = repo.acquire(self.lease("inc-a", now - 10, now - 1))
        new = repo.acquire(self.lease("inc-b", now, now + 30))
        self.assertGreater(new["version"], old["version"])
        with self.assertRaises(ResourceAlreadyOwned): repo.renew("device:test", "inc-a", old["version"], 30)
        with self.assertRaises(ResourceAlreadyOwned): repo.release("device:test", "inc-a", now, old["version"])


class OutboxGenerationTests(unittest.TestCase):
    def item(self): return {"outbox_id":"out-1","event_id":"evt-1","event_type":"X","trace_id":"t","state":"PENDING","created_at":time.time()}

    def test_reclaim_generation_blocks_old_ack_fail_and_renew(self):
        repo=InMemoryOutboxRepository(); repo.append(self.item())
        a=repo.claim("worker-a",lease_seconds=1)[0]
        repo._items["out-1"]["claim_expires_at"]=time.time()-1
        b=repo.claim("worker-b",lease_seconds=30)[0]
        self.assertGreater(b["claim_generation"],a["claim_generation"])
        for operation in (
            lambda: repo.renew("out-1","worker-a",a["claim_generation"],30),
            lambda: repo.ack("out-1","worker-a",a["claim_generation"]),
            lambda: repo.fail("out-1","worker-a","safe",True,a["claim_generation"]),
        ):
            with self.assertRaises(ResourceAlreadyOwned): operation()
        repo.renew("out-1","worker-b",b["claim_generation"],30)
        repo.ack("out-1","worker-b",b["claim_generation"])
        self.assertEqual(repo._items["out-1"]["state"],"SENT")

    def test_bounded_heartbeat_keeps_long_handler_owned_and_stops(self):
        class Events:
            def get(self, _): return type("E",(),{"event_id":"evt-1"})()
        class Bundle: pass
        bundle=Bundle(); bundle.outbox=InMemoryOutboxRepository(); bundle.events=Events(); bundle.outbox.append(self.item())
        class Bus:
            async def publish(self, _): await asyncio.sleep(.12)
        consumer=DurableOutboxWakeupConsumer(bundle=bundle,event_bus=Bus(),is_ready=lambda:True,owner="worker",lease_seconds=0.09)
        self.assertEqual(asyncio.run(consumer.process_once()),1)
        self.assertEqual(bundle.outbox._items["out-1"]["state"],"SENT")


class Migration007StaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql=Path("supabase/migrations/007_haris_lease_outbox_concurrency.sql").read_text(encoding="utf-8").lower()
        cls.verify=Path("supabase/verification/verify_007.sql").read_text(encoding="utf-8").lower()

    def test_scope_and_contract(self):
        for token in ("claim_generation","haris_renew_resource","haris_renew_outbox_claim","p_expected_version","p_claim_generation","pt409","security definer","service_role"):
            self.assertIn(token,self.sql)
        self.assertNotIn("grant execute on function public.haris_renew_outbox_claim(text,text,bigint,integer) to anon",self.sql)
        self.assertIn("create or replace function public.haris_ack_outbox",self.sql)
        self.assertIn("create or replace function public.haris_fail_outbox",self.sql)

    def test_verifier_is_read_only_single_result(self):
        self.assertIn("begin read only",self.verify); self.assertIn("rollback",self.verify)
        self.assertEqual(self.verify.count("select * from result"),1)

    def test_prior_migrations_unchanged_by_scope(self):
        self.assertNotIn("haris_recoveries",self.sql)
        self.assertNotIn("haris_action_commands",self.sql)


if __name__ == "__main__": unittest.main()
