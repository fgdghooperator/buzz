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
