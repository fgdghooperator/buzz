ALTER TABLE interventions RENAME TO legacy_interventions_v1;
ALTER TABLE dispatch_exceptions RENAME TO legacy_dispatch_exceptions_v1;

CREATE TABLE IF NOT EXISTS proof_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  generation INTEGER NOT NULL CHECK(generation >= 2),
  proof_type TEXT NOT NULL CHECK(proof_type IN ('SOURCE_CHANGE','TEST_STARTED','TEST_RESULT','BLOCKER_DIAGNOSIS','COMMIT','PACKAGE','REVIEW_RESULT','DEPLOYMENT_PREFLIGHT','OWNER_TEST_PACKET')),
  occurred_at INTEGER NOT NULL,
  reference TEXT NOT NULL CHECK(length(reference) BETWEEN 1 AND 512),
  digest TEXT CHECK(digest IS NULL OR length(digest) <= 128),
  result TEXT CHECK(result IS NULL OR length(result) <= 512),
  created_at INTEGER NOT NULL,
  UNIQUE(objective_id, generation)
) STRICT;

UPDATE objectives
SET next_review_at = CASE
  WHEN state = 'ACTIVE' THEN COALESCE(next_review_at, updated_at + 900)
  ELSE NULL
END;

CREATE TRIGGER objectives_state_deadline_insert_v2
BEFORE INSERT ON objectives
WHEN NOT ((NEW.state = 'ACTIVE' AND NEW.next_review_at IS NOT NULL) OR (NEW.state != 'ACTIVE' AND NEW.next_review_at IS NULL))
BEGIN
  SELECT RAISE(ABORT, 'objective state and review deadline disagree');
END;

CREATE TRIGGER objectives_state_deadline_update_v2
BEFORE UPDATE OF state,next_review_at ON objectives
WHEN NOT ((NEW.state = 'ACTIVE' AND NEW.next_review_at IS NOT NULL) OR (NEW.state != 'ACTIVE' AND NEW.next_review_at IS NULL))
BEGIN
  SELECT RAISE(ABORT, 'objective state and review deadline disagree');
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

INSERT INTO dispatch_outbox(
  id, objective_id, objective_generation, attempt, state, target_agent_key,
  idempotency_key, ack_marker, recovery_state, error_code, created_at, updated_at
)
SELECT
  id, objective_id, generation, 1, 'VERIFY_BEFORE_RETRY', agent_key,
  'legacy-v1:' || id,
  'FGD_ACK:legacy-v1:' || id,
  'LEGACY_V1_REQUIRES_RECONCILIATION', error_code, created_at,
  COALESCE(completed_at, created_at)
FROM legacy_interventions_v1;

CREATE TABLE dispatch_exceptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective_id TEXT NOT NULL REFERENCES objectives(objective_id),
  dispatch_id INTEGER NOT NULL REFERENCES dispatch_outbox(id),
  code TEXT NOT NULL CHECK(length(code) BETWEEN 1 AND 128),
  occurred_at INTEGER NOT NULL
) STRICT;

INSERT INTO dispatch_exceptions(id, objective_id, dispatch_id, code, occurred_at)
SELECT id, objective_id, intervention_id, code, occurred_at
FROM legacy_dispatch_exceptions_v1;

DROP TABLE legacy_dispatch_exceptions_v1;
DROP TABLE legacy_interventions_v1;
