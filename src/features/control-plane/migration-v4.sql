DROP TRIGGER IF EXISTS objectives_state_deadline_insert;
DROP TRIGGER IF EXISTS objectives_state_deadline_update;
DROP TRIGGER IF EXISTS objectives_state_deadline_insert_v2;
DROP TRIGGER IF EXISTS objectives_state_deadline_update_v2;
DROP TRIGGER IF EXISTS objectives_constraints_insert;
DROP TRIGGER IF EXISTS objectives_constraints_update;

CREATE TRIGGER objectives_constraints_insert
BEFORE INSERT ON objectives
WHEN NOT (
  NEW.client_scope IN ('REMH','TCC','FGD')
  AND NEW.lane IN ('OPERATIONS_CONTROL','OI_PLUGIN','INFRASTRUCTURE_WIRING')
  AND length(NEW.owner_agent) BETWEEN 1 AND 256
  AND NEW.state IN ('ACTIVE','AWAITING_OWNER','OWNER_PAUSED','NEEDS_SUPERVISOR','OBJECTIVE_COMPLETE')
  AND NEW.generation >= 1
  AND length(NEW.recovery_policy) BETWEEN 1 AND 256
  AND length(NEW.next_action) BETWEEN 1 AND 1024
  AND (
    (NEW.state = 'ACTIVE' AND NEW.next_review_at IS NOT NULL)
    OR (NEW.state != 'ACTIVE' AND NEW.next_review_at IS NULL)
  )
)
BEGIN
  SELECT RAISE(ABORT, 'objective constraints violated');
END;

CREATE TRIGGER objectives_constraints_update
BEFORE UPDATE ON objectives
WHEN NOT (
  NEW.client_scope IN ('REMH','TCC','FGD')
  AND NEW.lane IN ('OPERATIONS_CONTROL','OI_PLUGIN','INFRASTRUCTURE_WIRING')
  AND length(NEW.owner_agent) BETWEEN 1 AND 256
  AND NEW.state IN ('ACTIVE','AWAITING_OWNER','OWNER_PAUSED','NEEDS_SUPERVISOR','OBJECTIVE_COMPLETE')
  AND NEW.generation >= 1
  AND length(NEW.recovery_policy) BETWEEN 1 AND 256
  AND length(NEW.next_action) BETWEEN 1 AND 1024
  AND (
    (NEW.state = 'ACTIVE' AND NEW.next_review_at IS NOT NULL)
    OR (NEW.state != 'ACTIVE' AND NEW.next_review_at IS NULL)
  )
)
BEGIN
  SELECT RAISE(ABORT, 'objective constraints violated');
END;

CREATE INDEX IF NOT EXISTS objectives_due
  ON objectives(state, next_review_at)
  WHERE state = 'ACTIVE';

DROP TRIGGER IF EXISTS proof_events_append_only_update;
DROP TRIGGER IF EXISTS proof_events_append_only_delete;
ALTER TABLE proof_events RENAME TO legacy_proof_events_v3;

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

INSERT INTO proof_events(
  id, objective_id, source_generation, resulting_generation, proof_type,
  occurred_at, observed_at, classification, advances_generation,
  reference, new_event_identity, digest, result, created_at
)
SELECT
  id, objective_id, generation - 1, generation, proof_type,
  occurred_at, created_at, 'ADVANCING', 1,
  reference, NULL, digest, result, created_at
FROM legacy_proof_events_v3;

DROP TABLE legacy_proof_events_v3;

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

CREATE TABLE IF NOT EXISTS provider_circuits (
  client_scope TEXT PRIMARY KEY NOT NULL CHECK(client_scope = 'REMH'),
  failure_count INTEGER NOT NULL CHECK(failure_count >= 1),
  last_failure_at INTEGER NOT NULL,
  open_until INTEGER NOT NULL,
  last_error_code TEXT NOT NULL CHECK(length(last_error_code) BETWEEN 1 AND 128)
) STRICT;

CREATE TABLE IF NOT EXISTS leases (
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL,
  holder TEXT NOT NULL CHECK(length(holder) BETWEEN 1 AND 256),
  expires_at INTEGER NOT NULL,
  PRIMARY KEY(objective_id, generation)
) STRICT;

CREATE TABLE IF NOT EXISTS event_cursors (
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
