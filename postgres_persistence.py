"""Server-only Postgres repositories over an injected, network-free-by-default RPC transport."""
from __future__ import annotations
import copy, time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Protocol
from urllib.parse import urlparse
from durable_core import (ACTIVE_INCIDENT_STATES,_TRANSITIONS,ActionCommand,ActionState,DuplicateEvent,IncidentState,InMemoryRepositoryBundle,InvalidTransition,ProjectionIntegrityError,RepositoryUnavailable,ResourceAlreadyOwned,UnknownActionOutcome,VersionConflict,_require_safe)
from platform_events import HarisEvent

class PersistenceNotConfigured(RepositoryUnavailable):pass
class PersistenceTransportUnavailable(RepositoryUnavailable):
    safe_reason='PERSISTENCE_UNAVAILABLE'
    def __init__(self,*_ignored,http_status=None):super().__init__(self.safe_reason);self.http_status=http_status
class PersistenceAuthenticationFailed(PersistenceTransportUnavailable):safe_reason='PERSISTENCE_AUTH_FAILED'
class PersistenceSchemaNotReady(PersistenceTransportUnavailable):safe_reason='PERSISTENCE_SCHEMA_NOT_READY'
class PersistenceNetworkBlocked(PersistenceTransportUnavailable):safe_reason='PERSISTENCE_NETWORK_BLOCKED'
class PersistenceHostNotAllowed(PersistenceTransportUnavailable):safe_reason='PERSISTENCE_HOST_NOT_ALLOWED'
class PersistenceRpcContractFailed(RepositoryUnavailable):
    safe_reason='PERSISTENCE_RPC_CONTRACT_FAILED'
    def __init__(self,*_ignored,http_status=None,rpc_name=None,expected_shape=None,actual_shape_type=None):
        super().__init__(self.safe_reason);self.http_status=http_status;self.rpc_name=rpc_name;self.expected_shape=expected_shape;self.actual_shape_type=actual_shape_type
class PersistenceResponseInvalid(RepositoryUnavailable):
    safe_reason='PERSISTENCE_RESPONSE_INVALID'
    def __init__(self,*_ignored,http_status=None,rpc_name=None,expected_shape=None,actual_shape_type=None):
        super().__init__(self.safe_reason);self.http_status=http_status;self.rpc_name=rpc_name;self.expected_shape=expected_shape;self.actual_shape_type=actual_shape_type

class PostgresTransport(Protocol):
    def rpc(self,name:str,arguments:dict[str,Any])->Any: ...

class MockPostgresTransport:
    def __init__(self,responses=None): self.responses=responses or {}; self.calls=[]
    def rpc(self,name,arguments):
        self.calls.append((name,copy.deepcopy(arguments))); value=self.responses.get(name,[])
        if isinstance(value,list) and value and isinstance(value[0],(list,dict,Exception)): value=value.pop(0)
        if isinstance(value,Exception): raise value
        return value(arguments) if callable(value) else copy.deepcopy(value)

class PersistenceMetrics:
    def __init__(self): self.repository_operation_total=0; self.repository_error_total=0; self.repository_latency_ms=0.0; self.version_conflicts=0; self.resource_conflicts=0; self.outbox_pending=0
    def record(self,started,error=False): self.repository_operation_total+=1; self.repository_error_total+=int(error); self.repository_latency_ms+=(time.monotonic()-started)*1000

_TIMESTAMP_FIELDS = {
    'source_timestamp', 'received_at', 'created_at', 'updated_at', 'occurred_at',
    'requested_at', 'last_attempt_at', 'completed_at', 'acquired_at', 'released_at',
    'lease_started_at', 'lease_expires_at', 'started_at', 'claimed_at', 'sent_at',
    'committed_at', 'claim_expires_at', 'p_occurred_at',
    'raw_congestion_observed_at', 'reachability_observed_at',
    'location_observed_at', 'last_source_change_at',
    'last_operational_change_at', 'trigger_source_timestamp',
    'opened_at', 'closed_at',
}

def _canonical_timestamp(v):
    if v is None or isinstance(v, str): return v
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), timezone.utc).isoformat().replace('+00:00', 'Z')
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    raise RepositoryUnavailable('invalid_repository_timestamp')

def _json(v):
    if isinstance(v,Enum): return v.value
    if isinstance(v,datetime): return _canonical_timestamp(v)
    if isinstance(v,dict):
        return {str(k):_canonical_timestamp(x) if str(k) in _TIMESTAMP_FIELDS else _json(x) for k,x in v.items()}
    if isinstance(v,(list,tuple)): return [_json(x) for x in v]
    return v
def _rows(v):
    if hasattr(v,'data'): v=v.data
    if isinstance(v,dict) and 'data' in v: v=v['data']
    if v is None:return []
    if isinstance(v,dict):return [v]
    if isinstance(v,list) and all(isinstance(x,dict) for x in v):return v
    raise RepositoryUnavailable('malformed_repository_response')
def _one(v):
    rows = _rows(v)
    if len(rows) != 1 or not rows[0]: raise RepositoryUnavailable('missing_repository_result')
    return rows[0]
def _record(v, *required):
    row = _one(v)
    if any(key not in row or row[key] is None for key in required):
        raise RepositoryUnavailable('malformed_repository_response')
    return row
def _domain_record(row):
    def convert(value):
        if isinstance(value,dict):
            result={}
            for key,nested in value.items():
                try: result[key]=_ts(nested) if key in _TIMESTAMP_FIELDS-{'p_occurred_at'} and nested is not None else convert(nested)
                except Exception as exc: raise RepositoryUnavailable('malformed_repository_timestamp') from exc
            return result
        if isinstance(value,list):return [convert(item) for item in value]
        return copy.deepcopy(value)
    return convert(row)
def _ts(v):
    if v is None or isinstance(v,(int,float)): return v
    parsed = datetime.fromisoformat(str(v).replace('Z','+00:00'))
    if parsed.tzinfo is None: parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()

class _Repo:
    def __init__(self,t,m): self.transport=t; self.metrics=m
    def rpc(self,name,**args):
        _require_safe(args); started=time.monotonic()
        try: result=self.transport.rpc(name,_json(args)); self.metrics.record(started); return result
        except (VersionConflict,DuplicateEvent,InvalidTransition,ResourceAlreadyOwned,UnknownActionOutcome,ProjectionIntegrityError,RepositoryUnavailable): self.metrics.record(started,True); raise
        except Exception as exc:
            msg=str(exc).lower(); self.metrics.record(started,True)
            if 'version_conflict' in msg:self.metrics.version_conflicts+=1;raise VersionConflict('repository_version_conflict') from exc
            if 'resource_already_owned' in msg or 'resource_conflict' in msg:self.metrics.resource_conflicts+=1;raise ResourceAlreadyOwned('repository_resource_conflict') from exc
            if 'invalid_recovery_transition' in msg:raise VersionConflict('recovery_transition_conflict') from exc
            if 'invalid_transition' in msg:raise InvalidTransition('repository_invalid_transition') from exc
            if 'duplicate' in msg:raise DuplicateEvent('repository_duplicate') from exc
            if 'unknown_action_outcome' in msg or 'outcome_unknown' in msg:raise UnknownActionOutcome('repository_action_outcome_unknown') from exc
            if 'projection_integrity' in msg:raise ProjectionIntegrityError('repository_projection_integrity') from exc
            raise RepositoryUnavailable('persistence_operation_failed') from exc
    def read(self,kind,key=None,after=0,limit=200): return _rows(self.rpc('haris_read_domain',p_kind=kind,p_key=key,p_after=after,p_limit=limit))

class PostgresEventRepository(_Repo):
    def append(self,e):
        p=e.model_dump(mode='json');p['idempotency_key']=e.idempotency_key
        return bool(_record(self.rpc('haris_append_domain_event',p_event=p),'inserted')['inserted'])
    def _event(self,r):
        try:
            v={k:r[k] for k in HarisEvent.model_fields if k in r}
            for k in ('source_timestamp','received_at','created_at'):v[k]=_ts(v.get(k))
            return HarisEvent(**v)
        except Exception as exc:
            raise RepositoryUnavailable('malformed_event_record') from exc
    def get(self,k):
        r=self.read('event',k,0,1);return self._event(r[0]) if r else None
    def lookup(self,k):return next((self._event(r) for r in self.read('event',None,0,1000) if r.get('idempotency_key')==k),None)
    def events_after(self,s=0):return [(int(r['sequence']),self._event(r)) for r in self.read('event',None,s,1000)]
    def sequence(self):
        return int(_record(self.rpc('haris_event_sequence'),'sequence')['sequence'])

class PostgresNetworkStateRepository(_Repo):
    def load_all(self):return {r['entity_id']:_domain_record(r) for r in self.read('network',None,0,1000) if r.get('source_mode')!='PERSISTENCE_INTEGRATION_TEST' and not str(r.get('entity_id','')).startswith('PERSISTENCE-TEST-')}
    def get(self,k):
        r=self.read('network',k,0,1);return _domain_record(r[0]) if r else None
    def save(self,e,expected_version):return _domain_record(_one(self.rpc('haris_write_network_state',p_record=e,p_expected_version=expected_version)))
    def save_snapshot(self,p,s,c):self.rpc('haris_save_checkpoint',p_record={'snapshot_version':1,'last_event_sequence':s,'projection':p,'created_at':c})
    def latest_snapshot(self):
        r=_rows(self.rpc('haris_latest_checkpoint'));return _domain_record(r[0]) if r and r[0] else None

class PostgresIncidentRepository(_Repo):
    def create_or_get_active(self,i):
        r=_domain_record(_one(self.rpc('haris_create_incident',p_record=i)));created=bool(r.pop('_created',r.get('incident_id')==i.get('incident_id')));return r,created
    def get(self,k):
        r=self.read('incident',k,0,1);return _domain_record(r[0]) if r else None
    def active(self):return [_domain_record(r) for r in self.read('incident',None,0,500) if IncidentState(r['state']) in ACTIVE_INCIDENT_STATES and r.get('source_mode')!='PERSISTENCE_INTEGRATION_TEST' and not str(r.get('primary_entity','')).startswith('PERSISTENCE-TEST-')]
    def recent(self,limit=50):
        rows=[_domain_record(r) for r in self.read('incident',None,0,500) if r.get('source_mode')!='PERSISTENCE_INTEGRATION_TEST' and not str(r.get('primary_entity','')).startswith('PERSISTENCE-TEST-')]
        return sorted(rows,key=lambda r:float(r.get('updated_at') or r.get('opened_at') or 0),reverse=True)[:max(0,int(limit))]
    def find_active(self,k):return next((r for r in self.active() if r.get('correlation_key')==k),None)
    def update(self,incident,expected_version):
        current=self.get(incident['incident_id'])
        if not current or int(current.get('version',0))!=expected_version:raise VersionConflict('repository_version_conflict')
        if IncidentState(incident['state'])!=IncidentState(current['state']):raise InvalidTransition('incident state changes require transition()')
        return _domain_record(_one(self.rpc('haris_update_incident',p_record=incident,p_expected_version=expected_version)))
    def transition(self,i,to_state,*,actor,reason_code,trace_id,at):
        current=self.get(i)
        if not current:raise RepositoryUnavailable('incident_not_found')
        source=IncidentState(current['state'])
        if to_state not in _TRANSITIONS.get(source,set()):raise InvalidTransition(f'{source.value} -> {to_state.value}')
        return _domain_record(_one(self.rpc('haris_transition_incident',p_incident_id=i,p_from_state=source.value,p_to_state=to_state.value,p_expected_version=int(current.get('version',0)),p_actor=actor,p_reason_code=reason_code,p_trace_id=trace_id,p_occurred_at=at)))
    def transitions(self,i):return [_domain_record(r) for r in self.read('transition',i,0,1000)]

def _cmd(r):
    try:
        v={k:r.get(k) for k in ActionCommand.__dataclass_fields__};v['requested_at']=_ts(v['requested_at']);v['last_attempt_at']=_ts(v['last_attempt_at']);v['completed_at']=_ts(v['completed_at']);v['state']=ActionState(v['state']);return ActionCommand(**v)
    except Exception as exc:
        raise RepositoryUnavailable('malformed_action_record') from exc
class PostgresActionRepository(_Repo):
    def create_or_get(self,c):
        p=_json(vars(c));p['idempotency_key']=c.idempotency_key;r=_one(self.rpc('haris_save_action',p_record=p,p_expected_version=-1));return _cmd(r),bool(r.get('_created',r.get('command_id')==c.command_id))
    def get(self,k):
        r=self.read('action',k,0,1);return _cmd(r[0]) if r else None
    def get_by_idempotency_key(self,k):return next((_cmd(r) for r in self.read('action',None,0,1000) if r.get('idempotency_key')==k),None)
    def update(self,c,expected_version):return _cmd(_one(self.rpc('haris_save_action',p_record={**_json(vars(c)),'idempotency_key':c.idempotency_key},p_expected_version=expected_version)))
    def pending_or_unknown(self):return [_cmd(r) for r in self.read('action',None,0,500) if r.get('state') in {'PENDING','READY','SENT','OUTCOME_UNKNOWN','RECONCILIATION_REQUIRED'}]
    def reconciliation_required(self):return [c for c in self.pending_or_unknown() if c.state in {ActionState.SENT,ActionState.OUTCOME_UNKNOWN,ActionState.RECONCILIATION_REQUIRED}]
    def for_incident(self,k):return [_cmd(r) for r in self.read('action',None,0,1000) if r.get('incident_id')==k]
    def mark_restart_unknown(self):
        out=[]
        for c in self.reconciliation_required():
            if c.state==ActionState.SENT:
                p=_json(vars(c));p.update(state='OUTCOME_UNKNOWN',failure_reason='process_restart_before_provider_outcome');out.append(_cmd(_one(self.rpc('haris_save_action',p_record=p,p_expected_version=c.version))))
        return out

class PostgresResourceOwnershipRepository(_Repo):
    def acquire(self,r):return _domain_record(_one(self.rpc('haris_acquire_resource',p_record=r)))
    def active(self):return [_domain_record(r) for r in self.read('ownership',None,0,1000) if r.get('ownership_state')=='OWNED']
    def get_active(self,k):return next((_domain_record(r) for r in self.read('ownership',k,0,10) if r.get('ownership_state')=='OWNED'),None)
    def owned_by(self,i):return [r for r in self.active() if r.get('owner_incident_id')==i]
    def renew(self,k,i,expected_version,lease_seconds):return _domain_record(_one(self.rpc('haris_renew_resource',p_resource_key=k,p_incident_id=i,p_expected_version=expected_version,p_lease_seconds=lease_seconds)))
    def release(self,k,i,at,expected_version):
        del at;current=self.get_active(k)
        if not current or current.get('owner_incident_id')!=i:raise ResourceAlreadyOwned('resource_owner_mismatch')
        return _domain_record(_one(self.rpc('haris_release_resource',p_resource_key=k,p_incident_id=i,p_expected_version=expected_version)))

class PostgresVerificationRepository(_Repo):
    def save(self,r):return _domain_record(_one(self.rpc('haris_save_verification',p_record=r)))
    def get(self,k):
        r=self.read('verification',k,0,1);return _domain_record(r[0]) if r else None
    def for_incident(self,k):return [_domain_record(r) for r in self.read('verification',None,0,500) if r.get('incident_id')==k]
    def pending(self):return [_domain_record(r) for r in self.read('verification',None,0,500) if r.get('state')=='PENDING']
class PostgresRecoveryRepository(_Repo):
    def save(self,r,expected_version=None):
        if expected_version is None:
            current=self.for_incident(r['incident_id']);expected_version=int(current.get('version',0)) if current else -1
        return _domain_record(_one(self.rpc('haris_save_recovery',p_record=r,p_expected_version=expected_version)))
    def get(self,k):
        r=self.read('recovery',k,0,1);return _domain_record(r[0]) if r else None
    def for_incident(self,k):return next((_domain_record(r) for r in self.read('recovery',None,0,500) if r.get('incident_id')==k),None)
    def pending(self):return [_domain_record(r) for r in self.read('recovery',None,0,500) if r.get('state') in {'PENDING','RELEASING','VERIFYING_RELEASE'}]
class PostgresInboxRepository(_Repo):
    def claim(self,k,e,claimed_at):del claimed_at;return bool(self.rpc('haris_claim_inbox',p_idempotency_key=k,p_event_id=e))
class PostgresInboundEventRepository(_Repo):
    """Production atomic inbox/event/projection/incident/outbox boundary."""
    def process(self,event,*,projection=None,incident=None,transition=None,outbox=None):
        payload=event.model_dump(mode='json');payload['idempotency_key']=event.idempotency_key
        result=_record(self.rpc(
            'haris_process_inbound_event',p_event=payload,p_projection=projection,
            p_incident=incident,p_transition=transition,p_outbox=outbox,
        ),'status')
        if result['status'] not in {'accepted','duplicate'}:
            raise RepositoryUnavailable('malformed_inbound_event_result')
        return _domain_record(result)
class PostgresOutboxRepository(_Repo):
    def append(self,e):
        record=copy.deepcopy(e)
        if not record.get('event_id'):raise RepositoryUnavailable('outbox_event_id_required')
        record.setdefault('outbox_id',f"out-{record['event_id']}")
        record.setdefault('payload',{})
        return _domain_record(_one(self.rpc('haris_append_outbox',p_record=record)))
    def unsent(self):
        r=[_domain_record(x) for x in _rows(self.rpc('haris_pending_outbox',p_limit=500))];self.metrics.outbox_pending=len(r);return r
    def mark_sent(self,i,at):del at;self.rpc('haris_ack_outbox',p_outbox_id=i,p_owner='haris-core')
    def claim(self,o,limit=25,lease_seconds=30):return [_domain_record(r) for r in _rows(self.rpc('haris_claim_outbox',p_owner=o,p_limit=limit,p_lease_seconds=lease_seconds))]
    def renew(self,i,o,generation,lease_seconds):return _domain_record(_one(self.rpc('haris_renew_outbox_claim',p_outbox_id=i,p_owner=o,p_claim_generation=generation,p_lease_seconds=lease_seconds)))
    def ack(self,i,o,claim_generation=None):self.rpc('haris_ack_outbox',p_outbox_id=i,p_owner=o,p_claim_generation=claim_generation)
    def fail(self,i,o,error_safe,retry,claim_generation=None):
        _require_safe(error_safe)
        self.rpc('haris_fail_outbox',p_outbox_id=i,p_owner=o,p_error_safe=error_safe[:256],p_retry=retry,p_claim_generation=claim_generation)
class PostgresCheckpointRepository(_Repo):
    def save(self,p,s,c):return _domain_record(_one(self.rpc('haris_save_checkpoint',p_record={'snapshot_version':1,'last_event_sequence':s,'projection':p,'created_at':c})))
    def latest(self):
        r=_rows(self.rpc('haris_latest_checkpoint'));return _domain_record(r[0]) if r and r[0] else None
class PostgresCostLedgerRepository(_Repo):
    def append(self,r):return _domain_record(_one(self.rpc('haris_append_policy_cost',p_record={**r,'cost_basis':'HARIS_POLICY_COST_MODEL'})))
    def for_incident(self,k):return [_domain_record(r) for r in _rows(self.rpc('haris_read_policy_cost',p_incident_id=k,p_limit=500))]
    def incident_total(self,k):return float(_record(self.rpc('haris_policy_cost_totals',p_incident_id=k,p_day_bucket=None),'incident_total')['incident_total'])
    def day_total(self,d):return float(_record(self.rpc('haris_policy_cost_totals',p_incident_id=None,p_day_bucket=d),'day_total')['day_total'])
class TransactionScopedResourceLocks:
    def acquire(self,owner,keys):
        del owner,keys
        raise RepositoryUnavailable('transaction_scoped_lock_requires_atomic_rpc')
    def release(self,owner,keys):del owner,keys
class PostgresRepositoryBundle:
    def __init__(self,t,metrics=None):
        self.metrics=metrics or PersistenceMetrics();a=(t,self.metrics);self.events=PostgresEventRepository(*a);self.network_state=PostgresNetworkStateRepository(*a);self.incidents=PostgresIncidentRepository(*a);self.actions=PostgresActionRepository(*a);self.resource_ownership=PostgresResourceOwnershipRepository(*a);self.verification=PostgresVerificationRepository(*a);self.recovery=PostgresRecoveryRepository(*a);self.inbox=PostgresInboxRepository(*a);self.inbound=PostgresInboundEventRepository(*a);self.outbox=PostgresOutboxRepository(*a);self.checkpoints=PostgresCheckpointRepository(*a);self.cost_ledger=PostgresCostLedgerRepository(*a);self.resource_locks=TransactionScopedResourceLocks()
_transport_factory:Callable[[Any,str],PostgresTransport]|None=None
def register_postgres_transport_factory(f):global _transport_factory;_transport_factory=f
def _validated_postgres_hostname(settings):
    if not getattr(settings,'supabase_url',None) or not getattr(settings,'supabase_key',None):
        raise PersistenceNotConfigured('PERSISTENCE_NOT_CONFIGURED')
    parsed=urlparse(str(settings.supabase_url))
    hostname=(parsed.hostname or '').lower()
    if (parsed.scheme.lower()!='https' or not hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in {'','/'}
            or not hostname.endswith('.supabase.co') or hostname=='supabase.co'):
        raise PersistenceNotConfigured('PERSISTENCE_NOT_CONFIGURED')
    return hostname
def build_persistence_transport(settings,runtime=None,transport_factory=None):
    from runtime import RuntimeEnvironment,runtime_environment
    runtime=runtime or runtime_environment();mode=str(getattr(settings,'haris_persistence_mode','memory')).lower().strip()
    if mode=='memory':return None
    if mode!='postgres':raise PersistenceNotConfigured('PERSISTENCE_NOT_CONFIGURED')
    if runtime not in {
        RuntimeEnvironment.PRODUCTION,
        RuntimeEnvironment.PERSISTENCE_INTEGRATION,
        RuntimeEnvironment.REAL_QOD_VALIDATION,
    }:
        raise PersistenceTransportUnavailable('PERSISTENCE_UNAVAILABLE')
    hostname=_validated_postgres_hostname(settings)
    factory=transport_factory or _transport_factory
    if factory is None:
        # Importing the repository layer remains network- and credential-free.
        # The concrete server transport is selected only at composition time.
        from supabase_transport import build_supabase_rpc_transport
        factory=build_supabase_rpc_transport
    try:
        transport=factory(settings,hostname)
        if transport is None or not callable(getattr(transport,'rpc',None)):
            raise PersistenceTransportUnavailable('PERSISTENCE_UNAVAILABLE')
        return transport
    except RepositoryUnavailable:raise
    except Exception as exc:raise PersistenceTransportUnavailable('PERSISTENCE_UNAVAILABLE') from exc
def build_repository_bundle(settings,runtime=None,transport_factory=None):
    from runtime import RuntimeEnvironment,runtime_environment
    runtime=runtime or runtime_environment();mode=str(getattr(settings,'haris_persistence_mode','memory')).lower().strip()
    if runtime is RuntimeEnvironment.TEST:return InMemoryRepositoryBundle()
    if mode=='memory':return InMemoryRepositoryBundle()
    transport=build_persistence_transport(settings,runtime,transport_factory)
    return PostgresRepositoryBundle(transport)
