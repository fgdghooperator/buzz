PRAGMA foreign_keys=ON;

CREATE TABLE objectives (
  objective_id TEXT PRIMARY KEY NOT NULL,
  client_scope TEXT NOT NULL CHECK(client_scope IN ('REMH','TCC','FGD')),
  lane TEXT NOT NULL CHECK(lane IN ('OPERATIONS_CONTROL','OI_PLUGIN','INFRASTRUCTURE_WIRING')),
  owner_agent TEXT NOT NULL CHECK(length(owner_agent) BETWEEN 1 AND 256),
  state TEXT NOT NULL CHECK(state IN ('ACTIVE','AWAITING_OWNER','OWNER_PAUSED','NEEDS_SUPERVISOR','OBJECTIVE_COMPLETE')),
  generation INTEGER NOT NULL CHECK(generation >= 1),
  last_verified_progress_at INTEGER NOT NULL,
  next_review_at INTEGER,
  last_intervention_at INTEGER,
  recovery_policy TEXT NOT NULL CHECK(length(recovery_policy) BETWEEN 1 AND 256),
  next_action TEXT NOT NULL CHECK(length(next_action) BETWEEN 1 AND 1024),
  updated_at INTEGER NOT NULL,
  CHECK((state = 'ACTIVE' AND next_review_at IS NOT NULL) OR (state != 'ACTIVE' AND next_review_at IS NULL))
) STRICT;

CREATE INDEX objectives_due
  ON objectives(state, next_review_at)
  WHERE state = 'ACTIVE';

CREATE TABLE proof_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  source_generation INTEGER NOT NULL CHECK(source_generation >= 1),
  resulting_generation INTEGER NOT NULL CHECK(resulting_generation >= source_generation),
  proof_type TEXT NOT NULL CHECK(proof_type IN ('SOURCE_CHANGE','TEST_STARTED','TEST_RESULT','BLOCKER_DIAGNOSIS','COMMIT','PACKAGE','REVIEW_RESULT','DEPLOYMENT_PREFLIGHT','OWNER_TEST_PACKET')),
  occurred_at INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  classification TEXT NOT NULL CHECK(classification IN ('ADVANCING','NON_ADVANCING','FINAL')),
  advances_generation INTEGER NOT NULL CHECK(advances_generation IN (0,1)),
  reference TEXT NOT NULL CHECK(length(reference) BETWEEN 1 AND 512),
  new_event_identity TEXT CHECK(new_event_identity IS NULL OR length(new_event_identity) BETWEEN 1 AND 512),
  digest TEXT CHECK(digest IS NULL OR length(digest) <= 128),
  result TEXT CHECK(result IS NULL OR length(result) <= 512),
  created_at INTEGER NOT NULL,
  CHECK((advances_generation = 1 AND resulting_generation = source_generation + 1) OR (advances_generation = 0 AND resulting_generation = source_generation))
) STRICT;

CREATE UNIQUE INDEX proof_events_new_event_identity
  ON proof_events(objective_id, new_event_identity)
  WHERE new_event_identity IS NOT NULL;

CREATE TRIGGER proof_events_append_only_update
BEFORE UPDATE ON proof_events
BEGIN
  SELECT RAISE(ABORT, 'proof ledger is append-only');
END;

CREATE TRIGGER proof_events_append_only_delete
BEFORE DELETE ON proof_events
BEGIN
  SELECT RAISE(ABORT, 'proof ledger is append-only');
END;

CREATE TABLE dispatch_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  objective_generation INTEGER NOT NULL CHECK(objective_generation >= 1),
  attempt INTEGER NOT NULL DEFAULT 1 CHECK(attempt >= 1),
  state TEXT NOT NULL CHECK(state IN ('PENDING','CLAIMED','WAKE_INTENT','SENT','ACKED','VERIFY_BEFORE_RETRY','FAILED')),
  target_agent_key TEXT NOT NULL CHECK(length(target_agent_key) BETWEEN 1 AND 256),
  target_pubkey TEXT CHECK(target_pubkey IS NULL OR length(target_pubkey) = 64),
  channel_id TEXT CHECK(channel_id IS NULL OR length(channel_id) BETWEEN 1 AND 256),
  thread_root_id TEXT CHECK(thread_root_id IS NULL OR length(thread_root_id) = 64),
  idempotency_key TEXT UNIQUE CHECK(idempotency_key IS NULL OR length(idempotency_key) BETWEEN 1 AND 256),
  ack_marker TEXT CHECK(ack_marker IS NULL OR length(ack_marker) BETWEEN 1 AND 512),
  wake_payload TEXT CHECK(wake_payload IS NULL OR length(wake_payload) BETWEEN 1 AND 4096),
  lease_holder TEXT CHECK(lease_holder IS NULL OR length(lease_holder) BETWEEN 1 AND 256),
  claimed_at INTEGER,
  claim_expires_at INTEGER,
  wake_intent_at INTEGER,
  sent_at INTEGER,
  ack_event_id TEXT UNIQUE CHECK(ack_event_id IS NULL OR length(ack_event_id) BETWEEN 1 AND 128),
  acknowledged_at INTEGER,
  recovery_state TEXT CHECK(recovery_state IS NULL OR length(recovery_state) BETWEEN 1 AND 256),
  error_code TEXT CHECK(error_code IS NULL OR length(error_code) BETWEEN 1 AND 128),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(objective_id, objective_generation),
  CHECK(state != 'CLAIMED' OR (lease_holder IS NOT NULL AND claimed_at IS NOT NULL AND claim_expires_at IS NOT NULL)),
  CHECK(state NOT IN ('WAKE_INTENT','SENT','ACKED') OR (target_pubkey IS NOT NULL AND channel_id IS NOT NULL AND thread_root_id IS NOT NULL AND idempotency_key IS NOT NULL AND ack_marker IS NOT NULL AND wake_payload IS NOT NULL AND wake_intent_at IS NOT NULL)),
  CHECK(state != 'SENT' OR sent_at IS NOT NULL),
  CHECK(state != 'ACKED' OR (ack_event_id IS NOT NULL AND acknowledged_at IS NOT NULL))
) STRICT;

CREATE INDEX dispatch_outbox_state
  ON dispatch_outbox(state, claim_expires_at, updated_at);

CREATE TABLE dispatch_exceptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  dispatch_id INTEGER NOT NULL REFERENCES dispatch_outbox(id),
  code TEXT NOT NULL CHECK(length(code) BETWEEN 1 AND 128),
  occurred_at INTEGER NOT NULL
) STRICT;

CREATE TABLE provider_circuits (
  client_scope TEXT PRIMARY KEY NOT NULL CHECK(client_scope = 'REMH'),
  failure_count INTEGER NOT NULL CHECK(failure_count >= 1),
  last_failure_at INTEGER NOT NULL,
  open_until INTEGER NOT NULL,
  last_error_code TEXT NOT NULL CHECK(length(last_error_code) BETWEEN 1 AND 128)
) STRICT;

CREATE TABLE leases (
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL,
  holder TEXT NOT NULL CHECK(length(holder) BETWEEN 1 AND 256),
  expires_at INTEGER NOT NULL,
  PRIMARY KEY(objective_id, generation)
) STRICT;

CREATE TABLE event_cursors (
  source TEXT PRIMARY KEY NOT NULL CHECK(length(source) BETWEEN 1 AND 256),
  cursor TEXT NOT NULL CHECK(length(cursor) BETWEEN 1 AND 512),
  updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE human_authority_admissions (
  admission_reference TEXT PRIMARY KEY NOT NULL CHECK(length(admission_reference) BETWEEN 1 AND 512),
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL CHECK(generation >= 1),
  operation TEXT NOT NULL CHECK(length(operation) BETWEEN 1 AND 128),
  actor TEXT NOT NULL CHECK(length(actor) BETWEEN 1 AND 128),
  admitted_at INTEGER NOT NULL CHECK(admitted_at >= 0),
  consumed_at INTEGER CHECK(consumed_at IS NULL OR consumed_at >= admitted_at)
) STRICT;

CREATE TRIGGER human_authority_admissions_immutable_update
BEFORE UPDATE ON human_authority_admissions
WHEN NOT (
  OLD.admission_reference = NEW.admission_reference
  AND OLD.objective_id = NEW.objective_id
  AND OLD.generation = NEW.generation
  AND OLD.operation = NEW.operation
  AND OLD.actor = NEW.actor
  AND OLD.admitted_at = NEW.admitted_at
  AND OLD.consumed_at IS NULL
  AND NEW.consumed_at IS NOT NULL
)
BEGIN
  SELECT RAISE(ABORT, 'human authority admission is immutable and single-use');
END;

CREATE TRIGGER human_authority_admissions_immutable_delete
BEFORE DELETE ON human_authority_admissions
BEGIN
  SELECT RAISE(ABORT, 'human authority admission ledger is append-only');
END;

CREATE TABLE blocker_routes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL CHECK(generation >= 1),
  route TEXT NOT NULL CHECK(route IN ('SPECIALIST','SUPERVISOR','SCRUM','DEPENDENCY_RETRY','HUMAN_OWNER')),
  reference TEXT NOT NULL CHECK(length(reference) BETWEEN 1 AND 512),
  authority_admission_reference TEXT REFERENCES human_authority_admissions(admission_reference),
  diagnosed_at INTEGER NOT NULL,
  UNIQUE(objective_id, generation, reference),
  CHECK((route = 'HUMAN_OWNER' AND authority_admission_reference IS NOT NULL) OR (route != 'HUMAN_OWNER' AND authority_admission_reference IS NULL))
) STRICT;

CREATE TABLE objective_resources (
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL CHECK(generation >= 1),
  resource_key TEXT NOT NULL CHECK(length(resource_key) BETWEEN 1 AND 256),
  required INTEGER NOT NULL DEFAULT 1 CHECK(required = 1),
  registered_at INTEGER NOT NULL,
  reconciliation_reference TEXT CHECK(reconciliation_reference IS NULL OR length(reconciliation_reference) BETWEEN 1 AND 512),
  reconciled_at INTEGER,
  PRIMARY KEY(objective_id, generation, resource_key),
  CHECK((reconciliation_reference IS NULL AND reconciled_at IS NULL) OR (reconciliation_reference IS NOT NULL AND reconciled_at IS NOT NULL))
) STRICT;

CREATE INDEX objective_resources_resolution
  ON objective_resources(objective_id, generation, reconciled_at);

CREATE TABLE smoke_dispatch_outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT UNIQUE NOT NULL CHECK(length(objective_id) BETWEEN 1 AND 256),
  objective_generation INTEGER NOT NULL DEFAULT 1 CHECK(objective_generation = 1),
  client_scope TEXT NOT NULL DEFAULT 'FGD_TEST' CHECK(client_scope = 'FGD_TEST'),
  lane TEXT NOT NULL DEFAULT 'CONTROL_PLANE_SMOKE' CHECK(lane = 'CONTROL_PLANE_SMOKE'),
  state TEXT NOT NULL CHECK(state IN ('PENDING','CLAIMED','WAKE_INTENT','SENT','ACKED','VERIFY_BEFORE_RETRY')),
  target_agent_key TEXT NOT NULL CHECK(length(target_agent_key) BETWEEN 1 AND 256),
  target_pubkey TEXT NOT NULL CHECK(length(target_pubkey) = 64),
  channel_id TEXT NOT NULL CHECK(length(channel_id) BETWEEN 1 AND 256),
  thread_root_id TEXT NOT NULL CHECK(length(thread_root_id) = 64),
  idempotency_key TEXT UNIQUE NOT NULL CHECK(length(idempotency_key) BETWEEN 1 AND 256),
  ack_marker TEXT UNIQUE NOT NULL CHECK(length(ack_marker) BETWEEN 1 AND 512),
  wake_payload TEXT NOT NULL CHECK(length(wake_payload) BETWEEN 1 AND 4096),
  wake_intent_at INTEGER,
  sent_at INTEGER,
  ack_event_id TEXT UNIQUE CHECK(ack_event_id IS NULL OR length(ack_event_id) BETWEEN 1 AND 128),
  acknowledged_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  CHECK(state NOT IN ('WAKE_INTENT','SENT','ACKED','VERIFY_BEFORE_RETRY') OR wake_intent_at IS NOT NULL),
  CHECK(state != 'SENT' OR sent_at IS NOT NULL),
  CHECK(state != 'ACKED' OR (ack_event_id IS NOT NULL AND acknowledged_at IS NOT NULL))
) STRICT;

CREATE INDEX smoke_dispatch_outbox_state
  ON smoke_dispatch_outbox(state, updated_at);
