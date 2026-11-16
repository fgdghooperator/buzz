use super::coordinator::{
    Coordination, Coordinator, DispatchFailure, WakeAdapter, WakeAdmission, WakeTarget,
};
use super::store::{
    AckOutcome, BlockerInput, BlockerRoute, DispatchState, Evaluation,
    HumanAuthorityAdmissionInput, NewObjective, ObjectiveState, ProofInput, ProofKind,
    SignedAckEvent, SilentReason, SmokePrepareInput, Store, WakeIntentInput,
};
use rusqlite::types::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn unix_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

fn active<'a>(id: &'a str, owner: &'a str, now: i64, review_at: i64) -> NewObjective<'a> {
    let recovery_policy = match review_at - now {
        100 => "review-100",
        86_400 => "review-86400",
        _ => "wake-owner-once-per-generation",
    };
    NewObjective {
        objective_id: id,
        client_scope: "REMH",
        lane: "OPERATIONS_CONTROL",
        owner_agent: owner,
        last_verified_progress_at: now,
        recovery_policy,
        next_action: "execute the next concrete action",
        updated_at: now,
    }
}

fn proof<'a>(objective_id: &'a str, at: i64, _next_review_at: i64) -> ProofInput<'a> {
    ProofInput {
        objective_id,
        expected_generation: 1,
        kind: ProofKind::TestResult,
        occurred_at: at,
        observed_at: at,
        reference: "native-store:test-result",
        digest: Some("sha256:0123456789abcdef"),
        result: Some("PASS"),
    }
}

fn admit_owner_decision(
    store: &mut Store,
    objective_id: &str,
    generation: i64,
    reference: &str,
    admitted_at: i64,
) {
    store
        .admit_human_authority(HumanAuthorityAdmissionInput {
            objective_id,
            generation,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: reference,
            admitted_at,
        })
        .unwrap();
}

const TARGET_PUBKEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const OTHER_PUBKEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const THREAD_ROOT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_ROOT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CHANNEL: &str = "fgd-control-plane-smoke";
const SMOKE_OBJECTIVE: &str = "FGD-SMOKE-PIP-NATIVE-1";
const SMOKE_PUBKEY: &str = "ec40342bf26a9c31fe647b0122de9defe94656cae804383e49b191541b77ce6b";
const SMOKE_CHANNEL: &str = "51d885db-c687-44a6-8f2c-01bd679e7bb0";
const SMOKE_ROOT: &str = "a5b8732af2784b1525f39f4fc5d1472835ced60cc8aa1ea43968bd6e92b32e3b";

#[test]
fn first_slice_is_not_registered_or_exposed_to_the_main_webview() {
    let build = include_str!("../../build.rs");
    let native_lib = include_str!("../lib.rs");
    let capability: serde_json::Value =
        serde_json::from_str(include_str!("../../capabilities/default.json")).unwrap();
    let permissions = capability["permissions"].as_array().unwrap();
    let has_permission = |permission: &str| {
        permissions
            .iter()
            .any(|entry| entry.as_str() == Some(permission))
    };

    for command in [
        "control_plane_observe_ack",
        "control_plane_smoke_prepare",
        "control_plane_smoke_snapshot",
        "control_plane_smoke_begin_release",
        "control_plane_smoke_record_sent",
        "control_plane_smoke_reconcile",
    ] {
        assert!(!build.contains(&format!("\"{command}\"")));
        assert!(!native_lib.contains(&format!("control_plane::{command}")));
        assert!(!has_permission(&format!(
            "allow-{}",
            command.replace('_', "-")
        )));
    }
}

fn reserve(store: &mut Store, objective_id: &str, generation: i64, now: i64) -> i64 {
    match store
        .evaluate(objective_id, generation, now, "fgd-smoke-agent")
        .unwrap()
    {
        Evaluation::Dispatch(dispatch) => dispatch.id,
        other => panic!("expected dispatch, got {other:?}"),
    }
}

fn freeze(store: &mut Store, dispatch_id: i64, now: i64, key: &str) {
    store
        .claim_dispatch(dispatch_id, "native-evaluator", now, now + 60)
        .unwrap();
    store
        .record_wake_intent(
            dispatch_id,
            "native-evaluator",
            WakeIntentInput {
                target_agent_key: "fgd-smoke-agent",
                target_pubkey: TARGET_PUBKEY,
                channel_id: CHANNEL,
                thread_root_id: THREAD_ROOT,
                idempotency_key: key,
                instruction: "Perform the isolated native wake smoke.",
            },
            now + 1,
        )
        .unwrap();
}

fn ack<'a>(event_id: &'a str, marker: &'a str, at: i64) -> SignedAckEvent<'a> {
    SignedAckEvent {
        event_id,
        author_pubkey: TARGET_PUBKEY,
        channel_id: CHANNEL,
        thread_root_id: THREAD_ROOT,
        content: marker,
        created_at: at,
    }
}

fn query_values(
    connection: &rusqlite::Connection,
    sql: &str,
    column_count: usize,
) -> Vec<Vec<Value>> {
    let mut statement = connection.prepare(sql).unwrap();
    statement
        .query_map([], |row| {
            (0..column_count)
                .map(|column| row.get(column))
                .collect::<rusqlite::Result<Vec<Value>>>()
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

fn control_plane_tables(connection: &rusqlite::Connection) -> Vec<String> {
    connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

fn assert_v4_migration_parity(db: &rusqlite::Connection) {
    let indexes: Vec<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert!(indexes.iter().any(|name| name == "objectives_due"));
    assert!(indexes
        .iter()
        .any(|name| name == "proof_events_new_event_identity"));

    let triggers: Vec<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert!(!triggers.iter().any(|name| name.ends_with("_v2")));
    assert!(!triggers
        .iter()
        .any(|name| name.starts_with("objectives_state_deadline_")));

    assert!(db
        .execute(
            "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES ('REMH-INVALID-STATE','REMH','OPERATIONS_CONTROL','kenny','INVALID',1,1,NULL,NULL,'bounded','none',1)",
            [],
        )
        .is_err());
    db.execute(
        "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES ('REMH-PARITY-CHECK','REMH','OPERATIONS_CONTROL','kenny','ACTIVE',1,1,2,NULL,'bounded','none',1)",
        [],
    )
    .unwrap();
    assert!(db
        .execute(
            "UPDATE objectives SET next_review_at=NULL WHERE objective_id='REMH-PARITY-CHECK'",
            [],
        )
        .is_err());
    assert!(db
        .execute(
            "UPDATE objectives SET state='AWAITING_OWNER' WHERE objective_id='REMH-PARITY-CHECK'",
            [],
        )
        .is_err());
    assert!(db
        .execute(
            "UPDATE objectives SET state='INVALID' WHERE objective_id='REMH-PARITY-CHECK'",
            [],
        )
        .is_err());
    db.execute(
        "DELETE FROM objectives WHERE objective_id='REMH-PARITY-CHECK'",
        [],
    )
    .unwrap();
}

fn assert_migrated_objective_constraint_triggers(db: &rusqlite::Connection) {
    let triggers: Vec<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert!(triggers
        .iter()
        .any(|name| name == "objectives_constraints_insert"));
    assert!(triggers
        .iter()
        .any(|name| name == "objectives_constraints_update"));
    assert!(!triggers
        .iter()
        .any(|name| name.starts_with("objectives_state_deadline_")));
}

fn assert_objective_constraint_parity(db: &rusqlite::Connection) {
    for (objective_id, client_scope, lane) in [
        ("PARITY-VALID-REMH", "REMH", "OPERATIONS_CONTROL"),
        ("PARITY-VALID-TCC", "TCC", "OI_PLUGIN"),
        ("PARITY-VALID-FGD", "FGD", "INFRASTRUCTURE_WIRING"),
    ] {
        db.execute(
            "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES (?1,?2,?3,'kenny','ACTIVE',1,1,2,NULL,'bounded','continue',1)",
            rusqlite::params![objective_id, client_scope, lane],
        )
        .unwrap();
    }

    let invalid_inserts = [
        (
            "PARITY-INVALID-SCOPE",
            "'OTHER'",
            "'OPERATIONS_CONTROL'",
            "'kenny'",
            "1",
            "'bounded'",
            "'continue'",
        ),
        (
            "PARITY-INVALID-LANE",
            "'REMH'",
            "'OTHER'",
            "'kenny'",
            "1",
            "'bounded'",
            "'continue'",
        ),
        (
            "PARITY-INVALID-GENERATION",
            "'REMH'",
            "'OPERATIONS_CONTROL'",
            "'kenny'",
            "0",
            "'bounded'",
            "'continue'",
        ),
        (
            "PARITY-INVALID-OWNER",
            "'REMH'",
            "'OPERATIONS_CONTROL'",
            "''",
            "1",
            "'bounded'",
            "'continue'",
        ),
        (
            "PARITY-INVALID-POLICY",
            "'REMH'",
            "'OPERATIONS_CONTROL'",
            "'kenny'",
            "1",
            "''",
            "'continue'",
        ),
        (
            "PARITY-INVALID-ACTION",
            "'REMH'",
            "'OPERATIONS_CONTROL'",
            "'kenny'",
            "1",
            "'bounded'",
            "''",
        ),
    ];
    for (objective_id, client_scope, lane, owner, generation, policy, action) in invalid_inserts {
        let sql = format!(
            "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES ('{objective_id}',{client_scope},{lane},{owner},'ACTIVE',{generation},1,2,NULL,{policy},{action},1)"
        );
        assert!(
            db.execute(&sql, []).is_err(),
            "accepted invalid insert: {objective_id}"
        );
    }

    db.execute(
        "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES ('PARITY-UPDATE','REMH','OPERATIONS_CONTROL','kenny','ACTIVE',1,1,2,NULL,'bounded','continue',1)",
        [],
    )
    .unwrap();
    for sql in [
        "UPDATE objectives SET client_scope='OTHER' WHERE objective_id='PARITY-UPDATE'",
        "UPDATE objectives SET lane='OTHER' WHERE objective_id='PARITY-UPDATE'",
        "UPDATE objectives SET generation=0 WHERE objective_id='PARITY-UPDATE'",
        "UPDATE objectives SET owner_agent='' WHERE objective_id='PARITY-UPDATE'",
        "UPDATE objectives SET recovery_policy='' WHERE objective_id='PARITY-UPDATE'",
        "UPDATE objectives SET next_action='' WHERE objective_id='PARITY-UPDATE'",
    ] {
        assert!(
            db.execute(sql, []).is_err(),
            "accepted invalid update: {sql}"
        );
    }
}

fn create_legacy_duplicate_proof_fixture(path: &std::path::Path, version: i64) {
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE objectives(objective_id TEXT PRIMARY KEY, client_scope TEXT NOT NULL, lane TEXT NOT NULL, owner_agent TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL, last_verified_progress_at INTEGER NOT NULL, next_review_at INTEGER, last_intervention_at INTEGER, recovery_policy TEXT NOT NULL, next_action TEXT NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE proof_events(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), generation INTEGER NOT NULL, proof_type TEXT NOT NULL, occurred_at INTEGER NOT NULL, reference TEXT NOT NULL, digest TEXT, result TEXT, created_at INTEGER NOT NULL, UNIQUE(objective_id,generation)) STRICT;
         INSERT INTO objectives VALUES ('REMH-LEGACY-DUP','REMH','OPERATIONS_CONTROL','kenny','ACTIVE',3,300,1200,250,'wake-owner-once-per-generation','continue',300);
         INSERT INTO objectives VALUES ('REMH-LEGACY-WAITING','REMH','OPERATIONS_CONTROL','kenny','AWAITING_OWNER',2,200,NULL,150,'wake-owner-once-per-generation','await owner',200);
         INSERT INTO proof_events(objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at) VALUES ('REMH-LEGACY-DUP',2,'TEST_RESULT',200,'legacy:repeated','sha256:first','PASS',201);
         INSERT INTO proof_events(objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at) VALUES ('REMH-LEGACY-DUP',3,'TEST_RESULT',300,'legacy:repeated','sha256:second','PASS',301);",
    )
    .unwrap();
    db.pragma_update(None, "user_version", version).unwrap();
}

fn create_v1_objective_constraint_fixture(path: &std::path::Path) {
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE objectives(objective_id TEXT PRIMARY KEY, client_scope TEXT NOT NULL, lane TEXT NOT NULL, owner_agent TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL, last_verified_progress_at INTEGER NOT NULL, next_review_at INTEGER, last_intervention_at INTEGER, recovery_policy TEXT NOT NULL, next_action TEXT NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE interventions(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), generation INTEGER NOT NULL, agent_key TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, completed_at INTEGER, error_code TEXT, UNIQUE(objective_id,generation)) STRICT;
         CREATE TABLE dispatch_exceptions(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), intervention_id INTEGER NOT NULL REFERENCES interventions(id), code TEXT NOT NULL, occurred_at INTEGER NOT NULL) STRICT;
         INSERT INTO objectives VALUES ('REMH-V1-PARITY','REMH','OPERATIONS_CONTROL','kenny','ACTIVE',1,100,200,NULL,'bounded','continue',100);
         PRAGMA user_version=1;",
    )
    .unwrap();
}

#[test]
fn migrated_v1_objectives_enforce_fresh_insert_and_update_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    create_v1_objective_constraint_fixture(&path);
    drop(Store::open(path.clone()).unwrap());
    let db = rusqlite::Connection::open(path).unwrap();
    assert_migrated_objective_constraint_triggers(&db);
    assert_objective_constraint_parity(&db);
}

#[test]
fn migrated_v2_objectives_enforce_fresh_insert_and_update_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    create_legacy_duplicate_proof_fixture(&path, 2);
    drop(Store::open(path.clone()).unwrap());
    let db = rusqlite::Connection::open(path).unwrap();
    assert_migrated_objective_constraint_triggers(&db);
    assert_objective_constraint_parity(&db);
}

#[test]
fn migrated_v3_objectives_enforce_fresh_insert_and_update_constraints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    create_legacy_duplicate_proof_fixture(&path, 3);
    drop(Store::open(path.clone()).unwrap());
    let db = rusqlite::Connection::open(path).unwrap();
    assert_migrated_objective_constraint_triggers(&db);
    assert_objective_constraint_parity(&db);
}

#[test]
fn version_one_intervention_migrates_to_non_replayable_outbox_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE objectives(objective_id TEXT PRIMARY KEY, client_scope TEXT NOT NULL, lane TEXT NOT NULL, owner_agent TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL, last_verified_progress_at INTEGER NOT NULL, next_review_at INTEGER, last_intervention_at INTEGER, recovery_policy TEXT NOT NULL, next_action TEXT NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE interventions(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), generation INTEGER NOT NULL, agent_key TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, completed_at INTEGER, error_code TEXT, UNIQUE(objective_id,generation)) STRICT;
         CREATE TABLE dispatch_exceptions(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), intervention_id INTEGER NOT NULL REFERENCES interventions(id), code TEXT NOT NULL, occurred_at INTEGER NOT NULL) STRICT;
         INSERT INTO objectives VALUES ('REMH-V1','REMH','OPERATIONS_CONTROL','fgd-smoke-agent','ACTIVE',1,100,200,300,'bounded','continue',300);
         INSERT INTO interventions(objective_id,generation,agent_key,status,created_at) VALUES ('REMH-V1',1,'fgd-smoke-agent','RESERVED',300);
         INSERT INTO dispatch_exceptions(objective_id,intervention_id,code,occurred_at) VALUES ('REMH-V1',1,'legacy_failure',301);
         PRAGMA user_version=1;",
    )
    .unwrap();
    let objective_before = query_values(&db, "SELECT * FROM objectives", 12);
    drop(db);

    let mut store = Store::open(path.clone()).unwrap();
    let migrated = store.dispatches("REMH-V1").unwrap();
    assert_eq!(migrated.len(), 1);
    assert_eq!(migrated[0].state, DispatchState::VerifyBeforeRetry);
    assert_eq!(migrated[0].idempotency_key.as_deref(), Some("legacy-v1:1"));
    assert_eq!(store.exceptions("REMH-V1").unwrap().len(), 1);
    assert_eq!(
        store
            .evaluate("REMH-V1", 1, 400, "fgd-smoke-agent")
            .unwrap(),
        Evaluation::Silent(SilentReason::AlreadyIntervened)
    );
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    let violations: i64 = db
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(violations, 0);
    assert_eq!(
        query_values(&db, "SELECT * FROM objectives", 12),
        objective_before
    );
    assert_v4_migration_parity(&db);

    let fresh_dir = tempfile::tempdir().unwrap();
    let fresh_path = fresh_dir.path().join("control-plane.sqlite3");
    drop(Store::open(fresh_path.clone()).unwrap());
    let fresh_db = rusqlite::Connection::open(fresh_path).unwrap();
    assert_eq!(control_plane_tables(&db), control_plane_tables(&fresh_db));
    assert!(control_plane_tables(&db)
        .iter()
        .any(|table| table == "provider_circuits"));
}

#[test]
fn verified_proof_is_append_only_advances_generation_and_derives_the_review_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-2", "kenny-live-key", 100, 1_000))
        .unwrap();

    let objective = store
        .record_verified_progress(proof("REMH-2", 200, 7_200))
        .unwrap();
    assert_eq!(objective.generation, 2);
    assert_eq!(objective.last_verified_progress_at, 200);
    assert_eq!(objective.next_review_at, Some(1_100));
    assert_eq!(store.proofs("REMH-2").unwrap().len(), 1);

    drop(store);
    let db = rusqlite::Connection::open(path).unwrap();
    assert!(db
        .execute("UPDATE proof_events SET result='changed'", [])
        .is_err());
    assert!(db.execute("DELETE FROM proof_events", []).is_err());
}

#[test]
fn every_qualifying_remh_proof_kind_advances_supervision() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-PROOFS", "kenny-live-key", 100, 1_000))
        .unwrap();
    let kinds = [
        ProofKind::SourceChange,
        ProofKind::TestResult,
        ProofKind::Commit,
        ProofKind::Package,
        ProofKind::ReviewResult,
        ProofKind::DeploymentPreflight,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let occurred_at = 200 + index as i64;
        let reference = format!("bounded-reference-{index}");
        let digest = format!("sha256:bounded-{index}");
        let needs_result = matches!(
            kind,
            ProofKind::TestResult | ProofKind::ReviewResult | ProofKind::DeploymentPreflight
        );
        store
            .record_verified_progress(ProofInput {
                objective_id: "REMH-PROOFS",
                expected_generation: 1 + index as i64,
                kind,
                occurred_at,
                observed_at: occurred_at,
                reference: &reference,
                digest: (!needs_result).then_some(digest.as_str()),
                result: needs_result.then_some("PASS"),
            })
            .unwrap();
    }
    assert_eq!(store.proofs("REMH-PROOFS").unwrap().len(), 6);
    assert_eq!(
        store.objective("REMH-PROOFS").unwrap().unwrap().generation,
        7
    );
}

#[test]
fn a_bounded_long_running_review_deadline_is_not_replaced_by_a_universal_window() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-3", "kenny-live-key", 100, 86_500))
        .unwrap();
    assert_eq!(
        store.objective("REMH-3").unwrap().unwrap().next_review_at,
        Some(86_500)
    );
    assert_eq!(
        store
            .evaluate("REMH-3", 1, 1_000, "kenny-live-key")
            .unwrap(),
        Evaluation::Silent(SilentReason::NotDue)
    );
}

#[test]
fn stale_generation_is_silent_and_current_generation_creates_one_pending_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-4", "kenny-live-key", 100, 200))
        .unwrap();
    store
        .record_verified_progress(proof("REMH-4", 150, 250))
        .unwrap();

    assert_eq!(
        store.evaluate("REMH-4", 1, 300, "kenny-live-key").unwrap(),
        Evaluation::Silent(SilentReason::StaleGeneration)
    );
    assert!(matches!(
        store.evaluate("REMH-4", 2, 300, "kenny-live-key").unwrap(),
        Evaluation::Dispatch(_)
    ));
    assert_eq!(
        store.evaluate("REMH-4", 2, 300, "kenny-live-key").unwrap(),
        Evaluation::Silent(SilentReason::AlreadyIntervened)
    );
    let dispatches = store.dispatches("REMH-4").unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].state, DispatchState::Pending);
}

#[test]
fn working_only_status_does_not_refresh_verified_progress_or_generation() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-5", "kenny-live-key", 100, 200))
        .unwrap();
    let before = store.objective("REMH-5").unwrap().unwrap();
    let after = store.record_status("REMH-5", "WORKING", 150).unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(
        after.last_verified_progress_at,
        before.last_verified_progress_at
    );
    assert_eq!(after.next_review_at, before.next_review_at);
}

#[test]
fn ready_for_owner_test_becomes_dormant_and_owner_instruction_reactivates_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-6", "kenny-live-key", 100, 200))
        .unwrap();
    let packet = || ProofInput {
        objective_id: "REMH-6",
        expected_generation: 1,
        kind: ProofKind::OwnerTestPacket,
        occurred_at: 180,
        observed_at: 180,
        reference: "packet:REMH-6",
        digest: None,
        result: Some("READY FOR OWNER TEST"),
    };
    assert!(store
        .ready_for_owner_test(packet(), 180, "missing-admission")
        .is_err());
    admit_owner_decision(&mut store, "REMH-6", 1, "authority:REMH-6:1", 170);
    let waiting = store
        .ready_for_owner_test(packet(), 180, "authority:REMH-6:1")
        .unwrap();
    assert_eq!(waiting.state, ObjectiveState::AwaitingOwner);
    assert_eq!(waiting.sentinel(), "AWAITING OWNER");
    assert_eq!(
        store
            .evaluate(
                "REMH-6",
                waiting.generation,
                180 + 48 * 3_600,
                "kenny-live-key"
            )
            .unwrap(),
        Evaluation::Silent(SilentReason::Dormant)
    );
    assert!(store.dispatches("REMH-6").unwrap().is_empty());

    let active = store
        .reactivate(
            "REMH-6",
            "owner supplied the requested acceptance result",
            180 + 48 * 3_600,
        )
        .unwrap();
    assert_eq!(active.state, ObjectiveState::Active);
    assert_eq!(active.generation, waiting.generation + 1);
    assert_eq!(active.next_review_at, Some(180 + 48 * 3_600 + 100));
}

#[test]
fn native_authority_admission_rejects_malformed_duplicate_and_non_current_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-AUTHORITY", "kenny", 100, 1_000))
        .unwrap();

    for input in [
        HumanAuthorityAdmissionInput {
            objective_id: "",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "authority:empty-objective",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 0,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "authority:bad-generation",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 1,
            operation: "",
            actor: "kenny",
            admission_reference: "authority:empty-operation",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "",
            admission_reference: "authority:empty-actor",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-MISSING",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "authority:missing-objective",
            admitted_at: 150,
        },
        HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 2,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "authority:future-generation",
            admitted_at: 150,
        },
    ] {
        assert!(store.admit_human_authority(input).is_err());
    }

    admit_owner_decision(&mut store, "REMH-AUTHORITY", 1, "authority:accepted", 150);
    assert!(store
        .admit_human_authority(HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTHORITY",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "another-human",
            admission_reference: "authority:accepted",
            admitted_at: 151,
        })
        .is_err());
}

#[test]
fn owner_gate_consumes_only_matching_current_exact_operation_authority_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    for id in ["REMH-AUTH-TARGET", "REMH-AUTH-OTHER"] {
        store
            .create_active(active(id, "kenny", 100, 1_000))
            .unwrap();
    }
    store
        .admit_human_authority(HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTH-OTHER",
            generation: 1,
            operation: "REQUEST_OWNER_DECISION",
            actor: "kenny",
            admission_reference: "authority:wrong-objective",
            admitted_at: 150,
        })
        .unwrap();
    store
        .admit_human_authority(HumanAuthorityAdmissionInput {
            objective_id: "REMH-AUTH-TARGET",
            generation: 1,
            operation: "PAUSE_OWNER",
            actor: "kenny",
            admission_reference: "authority:wrong-operation",
            admitted_at: 150,
        })
        .unwrap();
    admit_owner_decision(&mut store, "REMH-AUTH-TARGET", 1, "authority:valid", 190);

    let before = store.objective("REMH-AUTH-TARGET").unwrap().unwrap();
    for reference in [
        None,
        Some("authority:forged"),
        Some("authority:wrong-objective"),
        Some("authority:wrong-operation"),
    ] {
        assert!(store
            .route_blocker(BlockerInput {
                objective_id: "REMH-AUTH-TARGET",
                expected_generation: 1,
                route: BlockerRoute::HumanOwner,
                diagnosed_at: 200,
                reference: "gate:rejected-authority",
                authority_admission_reference: reference,
            })
            .is_err());
        assert_eq!(
            store.objective("REMH-AUTH-TARGET").unwrap().unwrap(),
            before
        );
        assert!(store.proofs("REMH-AUTH-TARGET").unwrap().is_empty());
        assert_eq!(
            rusqlite::Connection::open(&path)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM blocker_routes WHERE objective_id='REMH-AUTH-TARGET'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }

    let waiting = store
        .route_blocker(BlockerInput {
            objective_id: "REMH-AUTH-TARGET",
            expected_generation: 1,
            route: BlockerRoute::HumanOwner,
            diagnosed_at: 200,
            reference: "gate:accepted-authority",
            authority_admission_reference: Some("authority:valid"),
        })
        .unwrap();
    assert_eq!(waiting.state, ObjectiveState::AwaitingOwner);
    assert!(store
        .route_blocker(BlockerInput {
            objective_id: "REMH-AUTH-OTHER",
            expected_generation: 1,
            route: BlockerRoute::HumanOwner,
            diagnosed_at: 201,
            reference: "gate:replay",
            authority_admission_reference: Some("authority:valid"),
        })
        .is_err());
    let consumed_at: i64 = rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT consumed_at FROM human_authority_admissions WHERE admission_reference='authority:valid'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(consumed_at, 200);
}

#[test]
fn owner_test_packet_rejections_leave_objective_proof_and_blocker_ledgers_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-OWNER-PACKET", "kenny", 100, 1_000))
        .unwrap();
    store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-OWNER-PACKET",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 200,
            observed_at: 201,
            reference: "packet:duplicate",
            digest: Some("sha256:baseline"),
            result: Some("PASS"),
        })
        .unwrap();
    store
        .route_blocker(BlockerInput {
            objective_id: "REMH-OWNER-PACKET",
            expected_generation: 2,
            route: BlockerRoute::Specialist,
            diagnosed_at: 202,
            reference: "blocker:baseline",
            authority_admission_reference: None,
        })
        .unwrap();

    admit_owner_decision(
        &mut store,
        "REMH-OWNER-PACKET",
        2,
        "authority:owner-packet",
        202,
    );

    let before_objective = store.objective("REMH-OWNER-PACKET").unwrap().unwrap();
    let before_proofs = store.proofs("REMH-OWNER-PACKET").unwrap();
    let before_blockers: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM blocker_routes WHERE objective_id='REMH-OWNER-PACKET'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    for (reference, occurred_at, observed_at, result) in [
        ("packet:stale", 200, 203, Some("READY FOR OWNER TEST")),
        ("packet:future", 10_000, 203, Some("READY FOR OWNER TEST")),
        ("packet:duplicate", 203, 203, Some("READY FOR OWNER TEST")),
        ("", 203, 203, Some("READY FOR OWNER TEST")),
        (
            "packet:wrong-result",
            203,
            203,
            Some("READY FOR OWNER TEST "),
        ),
        ("packet:missing-result", 203, 203, None),
    ] {
        assert!(store
            .ready_for_owner_test(
                ProofInput {
                    objective_id: "REMH-OWNER-PACKET",
                    expected_generation: 2,
                    kind: ProofKind::OwnerTestPacket,
                    occurred_at,
                    observed_at,
                    reference,
                    digest: None,
                    result,
                },
                observed_at,
                "authority:owner-packet",
            )
            .is_err());
        assert_eq!(
            store.objective("REMH-OWNER-PACKET").unwrap().unwrap(),
            before_objective
        );
        assert_eq!(store.proofs("REMH-OWNER-PACKET").unwrap(), before_proofs);
        let blockers: i64 = rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM blocker_routes WHERE objective_id='REMH-OWNER-PACKET'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(blockers, before_blockers);
    }
}

#[test]
fn stale_generation_authority_cannot_enter_awaiting_owner_or_mutate_ledgers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-STALE-AUTHORITY", "kenny", 100, 1_000))
        .unwrap();
    admit_owner_decision(
        &mut store,
        "REMH-STALE-AUTHORITY",
        1,
        "authority:stale-generation",
        150,
    );
    store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-STALE-AUTHORITY",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 180,
            observed_at: 180,
            reference: "proof:advance-generation",
            digest: None,
            result: Some("PASS"),
        })
        .unwrap();
    let before = store.objective("REMH-STALE-AUTHORITY").unwrap().unwrap();
    let proof_count = store.proofs("REMH-STALE-AUTHORITY").unwrap().len();

    assert!(store
        .ready_for_owner_test(
            ProofInput {
                objective_id: "REMH-STALE-AUTHORITY",
                expected_generation: 2,
                kind: ProofKind::OwnerTestPacket,
                occurred_at: 200,
                observed_at: 200,
                reference: "packet:stale-authority",
                digest: None,
                result: Some("READY FOR OWNER TEST"),
            },
            200,
            "authority:stale-generation",
        )
        .is_err());
    assert_eq!(
        store.objective("REMH-STALE-AUTHORITY").unwrap().unwrap(),
        before
    );
    assert_eq!(
        store.proofs("REMH-STALE-AUTHORITY").unwrap().len(),
        proof_count
    );
    assert_eq!(
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM blocker_routes WHERE objective_id='REMH-STALE-AUTHORITY'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn concurrent_current_generation_evaluations_create_at_most_one_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-7", "kenny-live-key", 100, 200))
        .unwrap();
    drop(store);

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut store = Store::open(path).unwrap();
            barrier.wait();
            store.evaluate("REMH-7", 1, 300, "kenny-live-key").unwrap()
        }));
    }
    barrier.wait();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Evaluation::Dispatch(_)))
            .count(),
        1
    );
    let reopened = Store::open(path).unwrap();
    assert_eq!(reopened.dispatches("REMH-7").unwrap().len(), 1);
}

#[test]
fn dispatch_failure_preserves_objective_records_exception_and_blocks_retry_storm() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-8", "kenny-live-key", 100, 200))
        .unwrap();
    let dispatch_id = reserve(&mut store, "REMH-8", 1, 300);
    store
        .record_dispatch_failure(dispatch_id, "provider_unavailable", 300, 900)
        .unwrap();

    let objective = store.objective("REMH-8").unwrap().unwrap();
    assert_eq!(objective.state, ObjectiveState::Active);
    assert_eq!(store.exceptions("REMH-8").unwrap().len(), 1);
    assert_eq!(
        store.provider_circuit("REMH").unwrap().unwrap().open_until,
        900
    );
    assert_eq!(
        store.evaluate("REMH-8", 1, 301, "kenny-live-key").unwrap(),
        Evaluation::Silent(SilentReason::ProviderCircuitOpen)
    );
}

#[test]
fn durable_outbox_advances_pending_claimed_wake_intent_sent_acked() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-OUTBOX", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-OUTBOX", 1, 300);
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Pending
    );
    store
        .claim_dispatch(id, "native-evaluator", 301, 361)
        .unwrap();
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Claimed
    );
    store
        .record_wake_intent(
            id,
            "native-evaluator",
            WakeIntentInput {
                target_agent_key: "fgd-smoke-agent",
                target_pubkey: TARGET_PUBKEY,
                channel_id: CHANNEL,
                thread_root_id: THREAD_ROOT,
                idempotency_key: "dispatch-1",
                instruction: "Perform the isolated native wake smoke.",
            },
            302,
        )
        .unwrap();
    let intent = store.dispatch(id).unwrap().unwrap();
    assert_eq!(intent.state, DispatchState::WakeIntent);
    assert_eq!(
        intent.ack_marker.as_deref(),
        Some("FGD_ACK:REMH-OUTBOX:1:dispatch-1")
    );
    assert!(intent
        .wake_payload
        .as_deref()
        .unwrap()
        .ends_with("FGD_ACK:REMH-OUTBOX:1:dispatch-1"));
    store.record_sent(id, 303).unwrap();
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Sent
    );
    assert_eq!(
        store
            .acknowledge_signed_event(ack("event-1", "FGD_ACK:REMH-OUTBOX:1:dispatch-1", 304,))
            .unwrap(),
        AckOutcome::Acknowledged(id)
    );
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Acked
    );
}

#[test]
fn only_the_current_unexpired_claim_holder_can_freeze_wake_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-CLAIM", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-CLAIM", 1, 300);
    store
        .claim_dispatch(id, "native-evaluator", 301, 361)
        .unwrap();
    let intent = || WakeIntentInput {
        target_agent_key: "fgd-smoke-agent",
        target_pubkey: TARGET_PUBKEY,
        channel_id: CHANNEL,
        thread_root_id: THREAD_ROOT,
        idempotency_key: "claim-1",
        instruction: "Perform the isolated native wake smoke.",
    };
    assert!(store
        .record_wake_intent(id, "foreign-evaluator", intent(), 302)
        .is_err());
    assert!(store
        .record_wake_intent(id, "native-evaluator", intent(), 400)
        .is_err());
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Claimed
    );
}

#[test]
fn terminal_ack_cannot_be_overwritten_by_dispatch_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-ACKED", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-ACKED", 1, 300);
    freeze(&mut store, id, 301, "acked-1");
    store.record_sent(id, 303).unwrap();
    store
        .acknowledge_signed_event(ack("event-acked", "FGD_ACK:REMH-ACKED:1:acked-1", 304))
        .unwrap();
    assert!(store
        .record_dispatch_failure(id, "late_failure", 305, 900)
        .is_err());
    let dispatch = store.dispatch(id).unwrap().unwrap();
    assert_eq!(dispatch.state, DispatchState::Acked);
    assert_eq!(dispatch.ack_event_id.as_deref(), Some("event-acked"));
    assert!(store.exceptions("REMH-ACKED").unwrap().is_empty());
}

#[test]
fn objective_state_and_review_deadline_are_schema_coherent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-COHERENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    drop(store);
    let db = rusqlite::Connection::open(path).unwrap();
    assert!(db
        .execute(
            "UPDATE objectives SET next_review_at=NULL WHERE objective_id='REMH-COHERENT'",
            [],
        )
        .is_err());
    assert!(db
        .execute(
            "UPDATE objectives SET state='AWAITING_OWNER' WHERE objective_id='REMH-COHERENT'",
            [],
        )
        .is_err());
    assert_objective_constraint_parity(&db);
    assert_v4_migration_parity(&db);
}

#[test]
fn sent_and_uncertain_wake_intent_are_never_replayed_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-SENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    store
        .create_active(active("REMH-INTENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let sent = reserve(&mut store, "REMH-SENT", 1, 300);
    freeze(&mut store, sent, 301, "sent-1");
    store.record_sent(sent, 303).unwrap();
    let uncertain = reserve(&mut store, "REMH-INTENT", 1, 300);
    freeze(&mut store, uncertain, 301, "intent-1");
    drop(store);

    let mut reopened = Store::open(path).unwrap();
    let reconciled = reopened.reconcile_startup(&[], 1_000, 900).unwrap();
    assert!(reconciled.wake_dispatches.is_empty());
    assert_eq!(
        reopened.dispatch(sent).unwrap().unwrap().state,
        DispatchState::Sent
    );
    assert_eq!(
        reopened.dispatch(uncertain).unwrap().unwrap().state,
        DispatchState::VerifyBeforeRetry
    );
}

#[test]
fn signed_ack_requires_exact_pubkey_channel_root_marker_and_time() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-MATCH", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-MATCH", 1, 300);
    freeze(&mut store, id, 301, "match-1");
    store.record_sent(id, 303).unwrap();
    let marker = "FGD_ACK:REMH-MATCH:1:match-1";
    for event in [
        SignedAckEvent {
            author_pubkey: OTHER_PUBKEY,
            ..ack("wrong-pubkey", marker, 304)
        },
        SignedAckEvent {
            channel_id: "wrong-channel",
            ..ack("wrong-channel", marker, 304)
        },
        SignedAckEvent {
            thread_root_id: OTHER_ROOT,
            ..ack("wrong-root", marker, 304)
        },
        SignedAckEvent {
            content: "FGD_ACK:wrong",
            ..ack("wrong-marker", marker, 304)
        },
        ack("too-old", marker, 301),
    ] {
        assert_eq!(
            store.acknowledge_signed_event(event).unwrap(),
            AckOutcome::NoMatch
        );
    }
    assert_eq!(
        store.dispatch(id).unwrap().unwrap().state,
        DispatchState::Sent
    );
}

#[test]
fn old_dispatch_marker_cannot_ack_a_newer_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-ATTEMPTS", "fgd-smoke-agent", 100, 200))
        .unwrap();

    let old = reserve(&mut store, "REMH-ATTEMPTS", 1, 300);
    freeze(&mut store, old, 301, "old-dispatch");
    store.record_sent(old, 303).unwrap();

    store
        .record_verified_progress(proof("REMH-ATTEMPTS", 400, 500))
        .unwrap();
    let current = reserve(&mut store, "REMH-ATTEMPTS", 2, 600);
    freeze(&mut store, current, 601, "current-dispatch");
    store.record_sent(current, 603).unwrap();

    assert_eq!(
        store
            .acknowledge_signed_event(ack(
                "event-old-dispatch",
                "FGD_ACK:REMH-ATTEMPTS:1:old-dispatch",
                604,
            ))
            .unwrap(),
        AckOutcome::Acknowledged(old)
    );
    assert_eq!(
        store.dispatch(current).unwrap().unwrap().state,
        DispatchState::Sent
    );
    assert_eq!(
        store
            .dispatch(current)
            .unwrap()
            .unwrap()
            .ack_event_id
            .as_deref(),
        None
    );
}

#[test]
fn matching_ack_is_exactly_once_and_event_cannot_be_consumed_twice() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-ONCE", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-ONCE", 1, 300);
    freeze(&mut store, id, 301, "once-1");
    store.record_sent(id, 303).unwrap();
    let event = ack("event-once", "FGD_ACK:REMH-ONCE:1:once-1", 304);
    assert_eq!(
        store.acknowledge_signed_event(event.clone()).unwrap(),
        AckOutcome::Acknowledged(id)
    );
    assert_eq!(
        store.acknowledge_signed_event(event).unwrap(),
        AckOutcome::AlreadyAcknowledged(id)
    );
}

#[test]
fn startup_reconciles_existing_signed_ack_without_replaying_wake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-RECONCILE", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-RECONCILE", 1, 300);
    freeze(&mut store, id, 301, "reconcile-1");
    store.record_sent(id, 303).unwrap();
    drop(store);

    let mut reopened = Store::open(path).unwrap();
    let marker = "FGD_ACK:REMH-RECONCILE:1:reconcile-1";
    let result = reopened
        .reconcile_startup(&[ack("event-reconcile", marker, 304)], 1_000, 900)
        .unwrap();
    assert_eq!(result.acknowledged, vec![id]);
    assert!(result.wake_dispatches.is_empty());
    assert_eq!(
        reopened.dispatch(id).unwrap().unwrap().state,
        DispatchState::Acked
    );
}

#[test]
fn dispatch_ack_does_not_advance_objective_and_old_generation_cannot_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-INDEPENDENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-INDEPENDENT", 1, 300);
    freeze(&mut store, id, 301, "independent-1");
    store.record_sent(id, 303).unwrap();
    store
        .acknowledge_signed_event(ack(
            "event-independent",
            "FGD_ACK:REMH-INDEPENDENT:1:independent-1",
            304,
        ))
        .unwrap();
    assert_eq!(
        store
            .objective("REMH-INDEPENDENT")
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    store
        .record_verified_progress(proof("REMH-INDEPENDENT", 400, 500))
        .unwrap();
    assert_eq!(
        store
            .evaluate("REMH-INDEPENDENT", 1, 1_000, "fgd-smoke-agent")
            .unwrap(),
        Evaluation::Silent(SilentReason::StaleGeneration)
    );
    drop(store);
    let mut reopened = Store::open(path).unwrap();
    assert!(reopened
        .reconcile_startup(&[], 1_000, 900)
        .unwrap()
        .wake_dispatches
        .is_empty());
    assert_eq!(reopened.dispatches("REMH-INDEPENDENT").unwrap().len(), 1);
}

fn smoke_prepare<'a>(now: i64) -> SmokePrepareInput<'a> {
    SmokePrepareInput {
        objective_id: SMOKE_OBJECTIVE,
        target_agent_key: "pip-agent-key",
        target_pubkey: SMOKE_PUBKEY,
        channel_id: SMOKE_CHANNEL,
        thread_root_id: SMOKE_ROOT,
        now,
    }
}

#[test]
fn smoke_prepare_is_fgd_test_only_observable_and_does_not_touch_remh() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();

    let prepared = store.prepare_smoke_dispatch(smoke_prepare(100)).unwrap();

    assert_eq!(prepared.state, DispatchState::Claimed);
    assert_eq!(prepared.client_scope, "FGD_TEST");
    assert_eq!(prepared.lane, "CONTROL_PLANE_SMOKE");
    assert_eq!(prepared.objective_id, SMOKE_OBJECTIVE);
    assert_eq!(prepared.target_pubkey, SMOKE_PUBKEY);
    assert_eq!(prepared.channel_id, SMOKE_CHANNEL);
    assert_eq!(prepared.thread_root_id, SMOKE_ROOT);
    assert!(!prepared.idempotency_key.is_empty());
    assert!(prepared.ack_marker.ends_with(&prepared.idempotency_key));
    assert!(prepared.wake_payload.ends_with(&prepared.ack_marker));
    assert_eq!(prepared.wake_intent_at, None);
    assert_eq!(store.proofs(SMOKE_OBJECTIVE).unwrap().len(), 0);
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    let remh_objectives: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM objectives WHERE client_scope='REMH' OR lane='REMH'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let remh_proofs: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM proof_events p JOIN objectives o USING(objective_id) WHERE o.client_scope='REMH' OR o.lane='REMH'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remh_objectives, 0);
    assert_eq!(remh_proofs, 0);
}

#[test]
fn smoke_prepare_rejects_any_target_outside_the_approved_contract() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    let mut wrong = smoke_prepare(100);
    wrong.target_pubkey = TARGET_PUBKEY;

    assert!(store.prepare_smoke_dispatch(wrong).is_err());
    assert!(store.prepare_smoke_dispatch(smoke_prepare(100)).is_ok());
}

#[test]
fn smoke_prepare_fetches_the_same_frozen_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();

    let first = store.prepare_smoke_dispatch(smoke_prepare(100)).unwrap();
    let second = store.prepare_smoke_dispatch(smoke_prepare(200)).unwrap();

    assert_eq!(second, first);
}

#[test]
fn smoke_release_is_one_shot_and_sent_is_recorded_only_after_admission() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    let prepared = store.prepare_smoke_dispatch(smoke_prepare(100)).unwrap();

    let intent = store.begin_smoke_release(prepared.id, 101).unwrap();
    assert_eq!(intent.state, DispatchState::WakeIntent);
    assert_eq!(intent.sent_at, None);
    assert!(store.begin_smoke_release(prepared.id, 102).is_err());

    let sent = store.record_smoke_sent(prepared.id, 102).unwrap();
    assert_eq!(sent.state, DispatchState::Sent);
    assert_eq!(sent.sent_at, Some(102));
    assert!(store.record_smoke_sent(prepared.id, 103).is_err());
}

#[test]
fn smoke_wake_failure_and_restart_are_non_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    let prepared = store.prepare_smoke_dispatch(smoke_prepare(100)).unwrap();
    store.begin_smoke_release(prepared.id, 101).unwrap();
    drop(store);

    let mut reopened = Store::open(path).unwrap();
    assert!(reopened.begin_smoke_release(prepared.id, 102).is_err());
    let result = reopened.reconcile_startup(&[], 200, 200).unwrap();
    assert!(result.wake_dispatches.is_empty());
    assert_eq!(
        reopened.smoke_dispatch(prepared.id).unwrap().unwrap().state,
        DispatchState::VerifyBeforeRetry
    );
}

#[test]
fn smoke_reconciliation_is_id_scoped_and_does_not_mutate_remh_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active(
            "REMH-RECONCILE-ISOLATION",
            "kenny-live-key",
            100,
            200,
        ))
        .unwrap();
    let remh_id = reserve(&mut store, "REMH-RECONCILE-ISOLATION", 1, 300);
    freeze(&mut store, remh_id, 301, "remh-isolation");
    let smoke = store.prepare_smoke_dispatch(smoke_prepare(400)).unwrap();
    store.begin_smoke_release(smoke.id, 401).unwrap();

    let reconciled = store.reconcile_smoke_dispatch(smoke.id, 500).unwrap();

    assert_eq!(reconciled.state, DispatchState::VerifyBeforeRetry);
    assert_eq!(
        store.dispatch(remh_id).unwrap().unwrap().state,
        DispatchState::WakeIntent
    );
}

#[test]
fn smoke_ack_is_separate_from_remh_proof_and_generation_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    let prepared = store.prepare_smoke_dispatch(smoke_prepare(100)).unwrap();
    store.begin_smoke_release(prepared.id, 101).unwrap();
    store.record_smoke_sent(prepared.id, 102).unwrap();

    let outcome = store
        .acknowledge_signed_event(SignedAckEvent {
            event_id: "smoke-event-1",
            author_pubkey: SMOKE_PUBKEY,
            channel_id: SMOKE_CHANNEL,
            thread_root_id: SMOKE_ROOT,
            content: &prepared.ack_marker,
            created_at: 103,
        })
        .unwrap();
    assert_eq!(outcome, AckOutcome::Acknowledged(prepared.id));
    assert_eq!(
        store.smoke_dispatch(prepared.id).unwrap().unwrap().state,
        DispatchState::Acked
    );
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    let objective_count: i64 = db
        .query_row("SELECT COUNT(*) FROM objectives", [], |row| row.get(0))
        .unwrap();
    let proof_count: i64 = db
        .query_row("SELECT COUNT(*) FROM proof_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(objective_count, 0);
    assert_eq!(proof_count, 0);
}

#[test]
fn schema_v3_fixture_migrates_to_v4_without_losing_dispatch_or_smoke_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE objectives(objective_id TEXT PRIMARY KEY, client_scope TEXT NOT NULL, lane TEXT NOT NULL, owner_agent TEXT NOT NULL, state TEXT NOT NULL, generation INTEGER NOT NULL, last_verified_progress_at INTEGER NOT NULL, next_review_at INTEGER, last_intervention_at INTEGER, recovery_policy TEXT NOT NULL, next_action TEXT NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE proof_events(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), generation INTEGER NOT NULL, proof_type TEXT NOT NULL, occurred_at INTEGER NOT NULL, reference TEXT NOT NULL, digest TEXT, result TEXT, created_at INTEGER NOT NULL, UNIQUE(objective_id,generation)) STRICT;
         CREATE TABLE dispatch_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), objective_generation INTEGER NOT NULL, attempt INTEGER NOT NULL, state TEXT NOT NULL, target_agent_key TEXT NOT NULL, target_pubkey TEXT, channel_id TEXT, thread_root_id TEXT, idempotency_key TEXT, ack_marker TEXT, wake_payload TEXT, lease_holder TEXT, claimed_at INTEGER, claim_expires_at INTEGER, wake_intent_at INTEGER, sent_at INTEGER, ack_event_id TEXT, acknowledged_at INTEGER, recovery_state TEXT, error_code TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE dispatch_exceptions(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT NOT NULL REFERENCES objectives(objective_id), dispatch_id INTEGER NOT NULL REFERENCES dispatch_outbox(id), code TEXT NOT NULL, occurred_at INTEGER NOT NULL) STRICT;
         CREATE TABLE provider_circuits(client_scope TEXT PRIMARY KEY, failure_count INTEGER NOT NULL, last_failure_at INTEGER NOT NULL, open_until INTEGER NOT NULL, last_error_code TEXT NOT NULL) STRICT;
         CREATE TABLE leases(objective_id TEXT NOT NULL REFERENCES objectives(objective_id), generation INTEGER NOT NULL, holder TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY(objective_id,generation)) STRICT;
         CREATE TABLE event_cursors(source TEXT PRIMARY KEY, cursor TEXT NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         CREATE TABLE smoke_dispatch_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, objective_id TEXT UNIQUE NOT NULL, objective_generation INTEGER NOT NULL, client_scope TEXT NOT NULL, lane TEXT NOT NULL, state TEXT NOT NULL, target_agent_key TEXT NOT NULL, target_pubkey TEXT NOT NULL, channel_id TEXT NOT NULL, thread_root_id TEXT NOT NULL, idempotency_key TEXT UNIQUE NOT NULL, ack_marker TEXT UNIQUE NOT NULL, wake_payload TEXT NOT NULL, wake_intent_at INTEGER, sent_at INTEGER, ack_event_id TEXT UNIQUE, acknowledged_at INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL) STRICT;
         INSERT INTO objectives VALUES ('REMH-V3','REMH','OPERATIONS_CONTROL','kenny','ACTIVE',2,200,1100,150,'wake-owner-once-per-generation','continue',200);
         INSERT INTO objectives VALUES ('REMH-V3-WAITING','REMH','OPERATIONS_CONTROL','kenny','AWAITING_OWNER',3,210,NULL,160,'wake-owner-once-per-generation','await owner instruction',210);
         INSERT INTO proof_events(objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at) VALUES ('REMH-V3',2,'TEST_RESULT',200,'fixture:test','sha256:test','PASS',200);
         INSERT INTO dispatch_outbox(objective_id,objective_generation,attempt,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,lease_holder,claimed_at,claim_expires_at,wake_intent_at,sent_at,ack_event_id,acknowledged_at,recovery_state,error_code,created_at,updated_at) VALUES ('REMH-V3',2,1,'ACKED','kenny','1111111111111111111111111111111111111111111111111111111111111111','fixture-channel','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','fixture-dispatch-key','fixture-dispatch-marker','fixture-dispatch-payload','fixture-holder',201,261,202,203,'fixture-dispatch-event',204,'fixture-recovery','fixture-error',201,204);
         INSERT INTO smoke_dispatch_outbox(objective_id,objective_generation,client_scope,lane,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,created_at,updated_at) VALUES ('FGD-SMOKE-PIP-NATIVE-1',1,'FGD_TEST','CONTROL_PLANE_SMOKE','CLAIMED','pip','ec40342bf26a9c31fe647b0122de9defe94656cae804383e49b191541b77ce6b','51d885db-c687-44a6-8f2c-01bd679e7bb0','a5b8732af2784b1525f39f4fc5d1472835ced60cc8aa1ea43968bd6e92b32e3b','fixture-key','fixture-marker','fixture-payload',201,201);
         PRAGMA user_version=3;",
    )
    .unwrap();
    let objectives_before = query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12);
    let proof_before = query_values(
        &db,
        "SELECT id,objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
        9,
    );
    let dispatch_before = query_values(&db, "SELECT * FROM dispatch_outbox ORDER BY id", 23);
    let smoke_before = query_values(&db, "SELECT * FROM smoke_dispatch_outbox ORDER BY id", 19);
    drop(db);

    let store = Store::open(path.clone()).unwrap();
    assert_eq!(store.objective("REMH-V3").unwrap().unwrap().generation, 2);
    assert_eq!(store.dispatches("REMH-V3").unwrap().len(), 1);
    assert!(store.smoke_dispatch(1).unwrap().is_some());
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        db.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        4
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM objective_resources", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM human_authority_admissions",
            [],
            |row| { row.get::<_, i64>(0) }
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT advances_generation FROM proof_events WHERE objective_id='REMH-V3'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12),
        objectives_before
    );
    assert_eq!(
        query_values(
            &db,
            "SELECT id,objective_id,resulting_generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
            9,
        ),
        proof_before
    );
    assert_eq!(
        query_values(&db, "SELECT * FROM dispatch_outbox ORDER BY id", 23),
        dispatch_before
    );
    assert_eq!(
        query_values(&db, "SELECT * FROM smoke_dispatch_outbox ORDER BY id", 19,),
        smoke_before
    );
    assert!(db
        .execute(
            "UPDATE objectives SET next_review_at=NULL WHERE objective_id='REMH-V3'",
            [],
        )
        .is_err());
    assert!(db
        .execute(
            "UPDATE objectives SET next_review_at=1200 WHERE objective_id='REMH-V3-WAITING'",
            [],
        )
        .is_err());
    assert_v4_migration_parity(&db);
}

#[test]
fn duplicate_stale_old_generation_and_future_skew_proof_cannot_advance() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-QUALIFY", "kenny", 100, 1_000))
        .unwrap();

    let accepted = store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-QUALIFY",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 200,
            observed_at: 205,
            reference: "test:qualified:1",
            digest: Some("sha256:qualified"),
            result: Some("PASS"),
        })
        .unwrap();
    assert_eq!(accepted.generation, 2);

    for rejected in [
        ProofInput {
            objective_id: "REMH-QUALIFY",
            expected_generation: 2,
            kind: ProofKind::TestResult,
            occurred_at: 210,
            observed_at: 211,
            reference: "test:qualified:1",
            digest: Some("sha256:qualified"),
            result: Some("PASS"),
        },
        ProofInput {
            objective_id: "REMH-QUALIFY",
            expected_generation: 2,
            kind: ProofKind::Commit,
            occurred_at: 199,
            observed_at: 220,
            reference: "commit:stale",
            digest: Some("sha256:stale"),
            result: None,
        },
        ProofInput {
            objective_id: "REMH-QUALIFY",
            expected_generation: 1,
            kind: ProofKind::Commit,
            occurred_at: 220,
            observed_at: 220,
            reference: "commit:old-generation",
            digest: Some("sha256:old"),
            result: None,
        },
        ProofInput {
            objective_id: "REMH-QUALIFY",
            expected_generation: 2,
            kind: ProofKind::Commit,
            occurred_at: 10_000,
            observed_at: 220,
            reference: "commit:future",
            digest: Some("sha256:future"),
            result: None,
        },
    ] {
        assert!(store.record_qualified_proof(rejected).is_err());
    }
    assert_eq!(
        store.objective("REMH-QUALIFY").unwrap().unwrap().generation,
        2
    );
    assert_eq!(store.proofs("REMH-QUALIFY").unwrap().len(), 1);
}

#[test]
fn caller_supplied_future_timestamps_cannot_advance_or_postpone_review() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-TRUSTED-TIME", "kenny", 100, 1_000))
        .unwrap();
    let before = store.objective("REMH-TRUSTED-TIME").unwrap().unwrap();
    let future = unix_now() + 3_600;

    assert!(store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-TRUSTED-TIME",
            expected_generation: 1,
            kind: ProofKind::Commit,
            occurred_at: future,
            observed_at: future,
            reference: "commit:future-caller-clock",
            digest: Some("sha256:future-caller-clock"),
            result: None,
        })
        .is_err());

    assert_eq!(
        store.objective("REMH-TRUSTED-TIME").unwrap().unwrap(),
        before
    );
    assert!(store.proofs("REMH-TRUSTED-TIME").unwrap().is_empty());
}

#[test]
fn canonical_digest_identity_rejects_replay_under_a_different_reference() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-CANONICAL-PROOF", "kenny", 100, 1_000))
        .unwrap();
    let first = store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-CANONICAL-PROOF",
            expected_generation: 1,
            kind: ProofKind::Commit,
            occurred_at: 200,
            observed_at: 201,
            reference: "commit:first-reference",
            digest: Some("sha256:immutable-evidence"),
            result: None,
        })
        .unwrap();

    assert!(store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-CANONICAL-PROOF",
            expected_generation: 2,
            kind: ProofKind::Commit,
            occurred_at: 300,
            observed_at: 301,
            reference: "commit:different-reference",
            digest: Some("sha256:immutable-evidence"),
            result: None,
        })
        .is_err());

    assert_eq!(
        store.objective("REMH-CANONICAL-PROOF").unwrap().unwrap(),
        first
    );
    assert_eq!(store.proofs("REMH-CANONICAL-PROOF").unwrap().len(), 1);
}

#[test]
fn empty_or_blank_evidence_digests_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-DIGEST-VALIDATION", "kenny", 100, 1_000))
        .unwrap();

    for (reference, digest) in [("commit:empty", ""), ("commit:blank", "   ")] {
        assert!(store
            .record_qualified_proof(ProofInput {
                objective_id: "REMH-DIGEST-VALIDATION",
                expected_generation: 1,
                kind: ProofKind::Commit,
                occurred_at: 200,
                observed_at: 201,
                reference,
                digest: Some(digest),
                result: None,
            })
            .is_err());
    }

    let objective = store.objective("REMH-DIGEST-VALIDATION").unwrap().unwrap();
    assert_eq!(objective.generation, 1);
    assert_eq!(objective.last_verified_progress_at, 100);
    assert_eq!(objective.next_review_at, Some(1_000));
    assert!(store.proofs("REMH-DIGEST-VALIDATION").unwrap().is_empty());
}

#[test]
fn test_started_and_chatter_are_non_advancing_and_cannot_postpone_review() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    store
        .create_active(active("REMH-NONADVANCING", "kenny", 100, 1_000))
        .unwrap();
    let before = store.objective("REMH-NONADVANCING").unwrap().unwrap();

    let after_test_started = store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-NONADVANCING",
            expected_generation: 1,
            kind: ProofKind::TestStarted,
            occurred_at: 150,
            observed_at: 151,
            reference: "test:started",
            digest: None,
            result: None,
        })
        .unwrap();
    let after_diagnosis = store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-NONADVANCING",
            expected_generation: 1,
            kind: ProofKind::BlockerDiagnosis,
            occurred_at: 160,
            observed_at: 161,
            reference: "blocker:diagnosed",
            digest: None,
            result: None,
        })
        .unwrap();
    let after_status = store
        .record_status("REMH-NONADVANCING", "still working", 900)
        .unwrap();

    assert_eq!(after_test_started.generation, before.generation);
    assert_eq!(
        after_test_started.last_verified_progress_at,
        before.last_verified_progress_at
    );
    assert_eq!(after_test_started.next_review_at, before.next_review_at);
    assert_eq!(after_diagnosis.generation, before.generation);
    assert_eq!(after_diagnosis.next_review_at, before.next_review_at);
    assert_eq!(after_status.next_review_at, before.next_review_at);
    assert_eq!(store.proofs("REMH-NONADVANCING").unwrap().len(), 2);
}

#[test]
fn next_review_is_derived_from_persisted_policy_and_proof_kind() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    let mut objective = active("REMH-DEADLINE", "kenny", 100, 99_999);
    objective.recovery_policy = "wake-owner-once-per-generation";
    store.create_active(objective).unwrap();
    assert_eq!(
        store
            .objective("REMH-DEADLINE")
            .unwrap()
            .unwrap()
            .next_review_at,
        Some(1_000)
    );

    let objective = store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-DEADLINE",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 200,
            observed_at: 201,
            reference: "test:deadline",
            digest: Some("sha256:deadline"),
            result: Some("PASS"),
        })
        .unwrap();
    assert_eq!(objective.next_review_at, Some(1_100));
}

#[test]
fn blocker_routes_are_explicit_and_only_admitted_human_authority_can_await_owner() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    for route in [
        BlockerRoute::Specialist,
        BlockerRoute::Supervisor,
        BlockerRoute::Scrum,
        BlockerRoute::DependencyRetry,
    ] {
        let id = format!("REMH-BLOCK-{route:?}");
        store
            .create_active(active(&id, "kenny", 100, 1_000))
            .unwrap();
        let routed = store
            .route_blocker(BlockerInput {
                objective_id: &id,
                expected_generation: 1,
                route,
                diagnosed_at: 200,
                reference: "blocker:bounded",
                authority_admission_reference: None,
            })
            .unwrap();
        assert_ne!(routed.state, ObjectiveState::AwaitingOwner);
        assert!(routed.next_action.starts_with("route:"));
    }

    store
        .create_active(active("REMH-HUMAN-GATE", "kenny", 100, 1_000))
        .unwrap();
    assert!(store
        .route_blocker(BlockerInput {
            objective_id: "REMH-HUMAN-GATE",
            expected_generation: 1,
            route: BlockerRoute::HumanOwner,
            diagnosed_at: 200,
            reference: "gate:missing-authority",
            authority_admission_reference: None,
        })
        .is_err());
    admit_owner_decision(
        &mut store,
        "REMH-HUMAN-GATE",
        1,
        "authority:human-gate",
        190,
    );
    let waiting = store
        .route_blocker(BlockerInput {
            objective_id: "REMH-HUMAN-GATE",
            expected_generation: 1,
            route: BlockerRoute::HumanOwner,
            diagnosed_at: 200,
            reference: "gate:approved",
            authority_admission_reference: Some("authority:human-gate"),
        })
        .unwrap();
    assert_eq!(waiting.state, ObjectiveState::AwaitingOwner);
}

#[test]
fn completion_requires_generation_bound_resources_and_final_proof_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-COMPLETE", "kenny", 100, 1_000))
        .unwrap();
    store
        .register_required_resource("REMH-COMPLETE", 1, "source", 110)
        .unwrap();
    store
        .register_required_resource("REMH-COMPLETE", 1, "tests", 111)
        .unwrap();

    let final_proof = || ProofInput {
        objective_id: "REMH-COMPLETE",
        expected_generation: 1,
        kind: ProofKind::ReviewResult,
        occurred_at: 200,
        observed_at: 201,
        reference: "review:final",
        digest: Some("sha256:final"),
        result: Some("FINAL_PASS"),
    };
    assert!(store.complete_objective(final_proof()).is_err());
    assert_eq!(
        store.objective("REMH-COMPLETE").unwrap().unwrap().state,
        ObjectiveState::Active
    );
    assert!(store.proofs("REMH-COMPLETE").unwrap().is_empty());

    store
        .reconcile_resource("REMH-COMPLETE", 1, "source", "source:sha256", 180)
        .unwrap();
    assert!(store.complete_objective(final_proof()).is_err());
    assert!(store.proofs("REMH-COMPLETE").unwrap().is_empty());
    store
        .reconcile_resource("REMH-COMPLETE", 1, "tests", "cargo:test:green", 190)
        .unwrap();

    let complete = store.complete_objective(final_proof()).unwrap();
    assert_eq!(complete.state, ObjectiveState::ObjectiveComplete);
    assert_eq!(complete.generation, 2);
    assert_eq!(complete.next_review_at, None);
    assert_eq!(store.proofs("REMH-COMPLETE").unwrap().len(), 1);
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(db.query_row("SELECT COUNT(*) FROM objective_resources WHERE objective_id='REMH-COMPLETE' AND generation=1 AND reconciled_at IS NOT NULL", [], |row| row.get::<_, i64>(0)).unwrap(), 2);
}

#[test]
fn v2_duplicate_legacy_proof_references_migrate_without_loss_and_remain_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    create_legacy_duplicate_proof_fixture(&path, 2);

    let (objectives_before, proofs_before) = {
        let db = rusqlite::Connection::open(&path).unwrap();
        (
            query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12),
            query_values(
                &db,
                "SELECT id,objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
                9,
            ),
        )
    };
    let mut store = Store::open(path.clone()).unwrap();
    assert_eq!(store.proofs("REMH-LEGACY-DUP").unwrap().len(), 2);
    assert!(store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-LEGACY-DUP",
            expected_generation: 3,
            kind: ProofKind::TestStarted,
            occurred_at: 301,
            observed_at: 301,
            reference: "legacy:repeated",
            digest: None,
            result: None,
        })
        .is_err());
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12),
        objectives_before
    );
    assert_eq!(
        query_values(
            &db,
            "SELECT id,objective_id,resulting_generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
            9,
        ),
        proofs_before
    );
    assert_v4_migration_parity(&db);
}

#[test]
fn v3_duplicate_legacy_proof_references_migrate_without_loss_and_remain_reserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    create_legacy_duplicate_proof_fixture(&path, 3);

    let (objectives_before, proofs_before) = {
        let db = rusqlite::Connection::open(&path).unwrap();
        (
            query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12),
            query_values(
                &db,
                "SELECT id,objective_id,generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
                9,
            ),
        )
    };
    let store = Store::open(path.clone()).unwrap();
    assert_eq!(store.proofs("REMH-LEGACY-DUP").unwrap().len(), 2);
    drop(store);

    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        query_values(&db, "SELECT * FROM objectives ORDER BY objective_id", 12),
        objectives_before
    );
    assert_eq!(
        query_values(
            &db,
            "SELECT id,objective_id,resulting_generation,proof_type,occurred_at,reference,digest,result,created_at FROM proof_events ORDER BY id",
            9,
        ),
        proofs_before
    );
    assert_v4_migration_parity(&db);
}

#[test]
fn timestamp_overflow_is_rejected_without_panicking_in_all_proof_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    for id in [
        "REMH-OVERFLOW-PROOF",
        "REMH-OVERFLOW-OWNER",
        "REMH-OVERFLOW-COMPLETE",
    ] {
        store
            .create_active(active(id, "kenny", 100, 1_000))
            .unwrap();
    }

    assert!(store
        .record_qualified_proof(ProofInput {
            objective_id: "REMH-OVERFLOW-PROOF",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: i64::MAX,
            observed_at: i64::MAX,
            reference: "overflow:proof",
            digest: None,
            result: Some("PASS"),
        })
        .is_err());

    admit_owner_decision(
        &mut store,
        "REMH-OVERFLOW-OWNER",
        1,
        "authority:overflow-owner",
        150,
    );
    assert!(store
        .ready_for_owner_test(
            ProofInput {
                objective_id: "REMH-OVERFLOW-OWNER",
                expected_generation: 1,
                kind: ProofKind::OwnerTestPacket,
                occurred_at: i64::MAX,
                observed_at: i64::MAX,
                reference: "overflow:owner",
                digest: None,
                result: Some("READY FOR OWNER TEST"),
            },
            i64::MAX,
            "authority:overflow-owner",
        )
        .is_err());

    store
        .register_required_resource("REMH-OVERFLOW-COMPLETE", 1, "tests", 150)
        .unwrap();
    store
        .reconcile_resource("REMH-OVERFLOW-COMPLETE", 1, "tests", "tests:green", 160)
        .unwrap();
    assert!(store
        .complete_objective(ProofInput {
            objective_id: "REMH-OVERFLOW-COMPLETE",
            expected_generation: 1,
            kind: ProofKind::ReviewResult,
            occurred_at: i64::MAX,
            observed_at: i64::MAX,
            reference: "overflow:complete",
            digest: Some("sha256:complete"),
            result: Some("FINAL_PASS"),
        })
        .is_err());
}

#[test]
fn blocker_deadline_overflow_is_rejected_for_every_timed_route() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("control-plane.sqlite3")).unwrap();
    for route in [
        BlockerRoute::Specialist,
        BlockerRoute::Supervisor,
        BlockerRoute::Scrum,
        BlockerRoute::DependencyRetry,
    ] {
        let id = format!("REMH-OVERFLOW-{route:?}");
        store
            .create_active(active(&id, "kenny", 100, 1_000))
            .unwrap();
        assert!(store
            .route_blocker(BlockerInput {
                objective_id: &id,
                expected_generation: 1,
                route,
                diagnosed_at: i64::MAX,
                reference: "overflow:blocker",
                authority_admission_reference: None,
            })
            .is_err());
        assert_eq!(
            store.objective(&id).unwrap().unwrap().next_review_at,
            Some(1_000)
        );
    }
}

#[test]
fn resource_registration_waits_for_transition_and_rejects_the_stale_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-RESOURCE-RACE", "kenny", 100, 1_000))
        .unwrap();
    drop(store);

    let mut contender = Store::open(path.clone()).unwrap();
    let transition = rusqlite::Connection::open(&path).unwrap();
    transition.busy_timeout(Duration::from_secs(2)).unwrap();
    transition.execute_batch("BEGIN IMMEDIATE").unwrap();
    transition
        .execute(
            "UPDATE objectives SET generation=2,updated_at=200 WHERE objective_id='REMH-RESOURCE-RACE' AND generation=1",
            [],
        )
        .unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result =
            contender.register_required_resource("REMH-RESOURCE-RACE", 1, "stale-resource", 201);
        done_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
    transition.execute_batch("COMMIT").unwrap();

    assert!(done_rx
        .recv_timeout(Duration::from_secs(3))
        .unwrap()
        .is_err());
    handle.join().unwrap();
    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM objective_resources", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        0
    );
}

#[derive(Clone, Copy)]
enum FakeAdmission {
    Admitted,
    Uncertain,
    PanicBeforeCall,
    PanicAfterCall,
}

#[derive(Default)]
struct FakeWakeState {
    preflight_count: usize,
    admission_count: usize,
    persisted_intents: Vec<bool>,
    idempotency_keys: Vec<String>,
}

struct FakeWakeAdapter {
    path: std::path::PathBuf,
    state: Arc<Mutex<FakeWakeState>>,
    failure: Option<DispatchFailure>,
    admissions: VecDeque<FakeAdmission>,
    open_circuit_after_intent: Option<i64>,
}

impl FakeWakeAdapter {
    fn new(path: std::path::PathBuf, state: Arc<Mutex<FakeWakeState>>) -> Self {
        Self {
            path,
            state,
            failure: None,
            admissions: VecDeque::from([FakeAdmission::Admitted]),
            open_circuit_after_intent: None,
        }
    }
}

impl WakeAdapter for FakeWakeAdapter {
    fn preflight(
        &mut self,
        _dispatch: &super::store::Dispatch,
        _target: &WakeTarget<'_>,
        _idempotency_key: &str,
    ) -> std::result::Result<(), DispatchFailure> {
        let mut state = self.state.lock().unwrap();
        state.preflight_count += 1;
        state.idempotency_keys.push(_idempotency_key.to_string());
        drop(state);
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        Ok(())
    }

    fn before_admission_guard(&mut self, _dispatch: &super::store::Dispatch) {
        if let Some(dispatch_id) = self.open_circuit_after_intent.take() {
            Store::open(self.path.clone())
                .unwrap()
                .record_dispatch_failure(dispatch_id, "sibling_circuit", 302, 900)
                .unwrap();
        }
    }

    fn admit(&mut self, dispatch: &super::store::Dispatch) -> WakeAdmission {
        match self
            .admissions
            .pop_front()
            .unwrap_or(FakeAdmission::Admitted)
        {
            FakeAdmission::PanicBeforeCall => panic!("simulated crash before fake admission"),
            behavior => {
                let persisted = rusqlite::Connection::open(&self.path)
                    .unwrap()
                    .query_row(
                        "SELECT state='WAKE_INTENT' AND idempotency_key=?2 FROM dispatch_outbox WHERE id=?1",
                        rusqlite::params![dispatch.id, dispatch.idempotency_key],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap();
                let mut state = self.state.lock().unwrap();
                state.admission_count += 1;
                state.persisted_intents.push(persisted);
                drop(state);
                match behavior {
                    FakeAdmission::Admitted => WakeAdmission::Admitted,
                    FakeAdmission::Uncertain => WakeAdmission::Uncertain,
                    FakeAdmission::PanicAfterCall => panic!("simulated crash after fake admission"),
                    FakeAdmission::PanicBeforeCall => unreachable!(),
                }
            }
        }
    }
}

fn coordinator_target<'a>() -> WakeTarget<'a> {
    WakeTarget {
        agent_key: "fgd-smoke-agent",
        pubkey: TARGET_PUBKEY,
        channel_id: CHANNEL,
        thread_root_id: THREAD_ROOT,
        instruction: "Perform the isolated coordinator wake.",
    }
}

fn coordinator_for(
    path: &std::path::Path,
    objective_id: &str,
    adapter: FakeWakeAdapter,
) -> Coordinator<FakeWakeAdapter> {
    let mut store = Store::open(path.to_path_buf()).unwrap();
    if store.objective(objective_id).unwrap().is_none() {
        store
            .create_active(active(objective_id, "fgd-smoke-agent", 100, 200))
            .unwrap();
    }
    Coordinator::new(store, adapter, "test-coordinator")
}

fn ledger_counts(path: &std::path::Path) -> (i64, i64) {
    let db = rusqlite::Connection::open(path).unwrap();
    let circuits = db
        .query_row("SELECT COUNT(*) FROM provider_circuits", [], |row| {
            row.get(0)
        })
        .unwrap();
    let exceptions = db
        .query_row("SELECT COUNT(*) FROM dispatch_exceptions", [], |row| {
            row.get(0)
        })
        .unwrap();
    (circuits, exceptions)
}

#[test]
fn coordinator_runs_due_wake_exact_ack_and_proof_without_duplicate_intervention() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let adapter = FakeWakeAdapter::new(path.clone(), state.clone());
    let mut coordinator = coordinator_for(&path, "REMH-COORDINATOR", adapter);

    let sent = match coordinator
        .coordinate_due("REMH-COORDINATOR", 300, coordinator_target())
        .unwrap()
    {
        Coordination::Sent(dispatch) => dispatch,
        other => panic!("expected sent dispatch, got {other:?}"),
    };
    assert_eq!(sent.state, DispatchState::Sent);
    assert_eq!(
        coordinator
            .store()
            .dispatches("REMH-COORDINATOR")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(state.lock().unwrap().admission_count, 1);
    assert_eq!(state.lock().unwrap().persisted_intents, vec![true]);
    assert_eq!(
        coordinator
            .coordinate_due("REMH-COORDINATOR", 301, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::AlreadyIntervened)
    );
    assert_eq!(state.lock().unwrap().admission_count, 1);

    assert_eq!(
        coordinator
            .acknowledge(SignedAckEvent {
                event_id: "wrong-coordinator-ack",
                author_pubkey: OTHER_PUBKEY,
                channel_id: CHANNEL,
                thread_root_id: THREAD_ROOT,
                content: sent.ack_marker.as_deref().unwrap(),
                created_at: 304,
            })
            .unwrap(),
        AckOutcome::NoMatch
    );
    assert_eq!(
        coordinator
            .acknowledge(ack(
                "exact-coordinator-ack",
                sent.ack_marker.as_deref().unwrap(),
                305,
            ))
            .unwrap(),
        AckOutcome::Acknowledged(sent.id)
    );
    assert_eq!(
        coordinator
            .store()
            .objective("REMH-COORDINATOR")
            .unwrap()
            .unwrap()
            .generation,
        1
    );

    let advanced = coordinator
        .record_proof(ProofInput {
            objective_id: "REMH-COORDINATOR",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 400,
            observed_at: 401,
            reference: "coordinator:test-result",
            digest: Some("sha256:coordinator-green"),
            result: Some("PASS"),
        })
        .unwrap();
    assert_eq!(advanced.generation, 2);
    assert_eq!(
        coordinator
            .store_mut()
            .evaluate("REMH-COORDINATOR", 1, 2_000, "fgd-smoke-agent")
            .unwrap(),
        Evaluation::Silent(SilentReason::StaleGeneration)
    );
    assert_eq!(state.lock().unwrap().admission_count, 1);
}

#[test]
fn stale_generation_between_evaluate_claim_and_intent_is_rejected_without_mutation_or_failure_ledgers(
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-STALE-CLAIM", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let claim_id = reserve(&mut store, "REMH-STALE-CLAIM", 1, 300);
    store
        .record_verified_progress(proof("REMH-STALE-CLAIM", 400, 500))
        .unwrap();
    assert!(store.claim_dispatch(claim_id, "test", 401, 461).is_err());
    assert_eq!(
        store.dispatch(claim_id).unwrap().unwrap().state,
        DispatchState::Pending
    );
    assert_eq!(ledger_counts(&path), (0, 0));

    store
        .create_active(active("REMH-STALE-INTENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let intent_id = reserve(&mut store, "REMH-STALE-INTENT", 1, 300);
    store.claim_dispatch(intent_id, "test", 301, 500).unwrap();
    store
        .record_verified_progress(proof("REMH-STALE-INTENT", 400, 500))
        .unwrap();
    assert!(store
        .record_wake_intent(
            intent_id,
            "test",
            WakeIntentInput {
                target_agent_key: "fgd-smoke-agent",
                target_pubkey: TARGET_PUBKEY,
                channel_id: CHANNEL,
                thread_root_id: THREAD_ROOT,
                idempotency_key: "stale-intent",
                instruction: "must not wake",
            },
            401,
        )
        .is_err());
    let intent = store.dispatch(intent_id).unwrap().unwrap();
    assert_eq!(intent.state, DispatchState::Claimed);
    assert!(intent.wake_intent_at.is_none());
    assert_eq!(ledger_counts(&path), (0, 0));
}

#[test]
fn provider_circuit_blocks_claim_and_failed_generation_never_retries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let mut adapter = FakeWakeAdapter::new(path.clone(), state.clone());
    adapter.failure = Some(DispatchFailure {
        code: "fake_provider_unavailable".into(),
        open_until: 900,
    });
    let mut coordinator = coordinator_for(&path, "REMH-COORDINATOR-FAIL", adapter);

    let failed = match coordinator
        .coordinate_due("REMH-COORDINATOR-FAIL", 300, coordinator_target())
        .unwrap()
    {
        Coordination::Failed(dispatch) => dispatch,
        other => panic!("expected failure, got {other:?}"),
    };
    assert_eq!(failed.state, DispatchState::Failed);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    assert_eq!(
        coordinator
            .coordinate_due("REMH-COORDINATOR-FAIL", 501, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::ProviderCircuitOpen)
    );

    let advanced = coordinator
        .record_proof(ProofInput {
            objective_id: "REMH-COORDINATOR-FAIL",
            expected_generation: 1,
            kind: ProofKind::TestResult,
            occurred_at: 600,
            observed_at: 601,
            reference: "coordinator:recovered",
            digest: Some("sha256:coordinator-recovered"),
            result: Some("PASS"),
        })
        .unwrap();
    assert_eq!(advanced.generation, 2);
    assert_eq!(
        coordinator
            .coordinate_due("REMH-COORDINATOR-FAIL", 800, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::ProviderCircuitOpen)
    );
    assert!(matches!(
        coordinator
            .coordinate_due("REMH-COORDINATOR-FAIL", 1_500, coordinator_target())
            .unwrap(),
        Coordination::Sent(_)
    ));
    let dispatches = coordinator
        .store()
        .dispatches("REMH-COORDINATOR-FAIL")
        .unwrap();
    assert_eq!(dispatches.len(), 2);
    assert_eq!(dispatches[0].state, DispatchState::Failed);
    assert_eq!(dispatches[1].objective_generation, 2);
    assert_eq!(dispatches[1].attempt, 1);
}

#[test]
fn open_provider_circuit_refuses_an_existing_pending_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path).unwrap();
    store
        .create_active(active("REMH-CIRCUIT-A", "fgd-smoke-agent", 100, 200))
        .unwrap();
    store
        .create_active(active("REMH-CIRCUIT-B", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let blocked = reserve(&mut store, "REMH-CIRCUIT-A", 1, 300);
    let failure = reserve(&mut store, "REMH-CIRCUIT-B", 1, 300);
    store
        .record_dispatch_failure(failure, "provider_down", 301, 900)
        .unwrap();
    assert!(store.claim_dispatch(blocked, "test", 302, 362).is_err());
    assert_eq!(
        store.dispatch(blocked).unwrap().unwrap().state,
        DispatchState::Pending
    );
}

#[test]
fn dormant_objective_is_silent_and_coordinator_has_no_human_transition_surface() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active(
            "REMH-DORMANT-COORDINATOR",
            "fgd-smoke-agent",
            100,
            200,
        ))
        .unwrap();
    admit_owner_decision(
        &mut store,
        "REMH-DORMANT-COORDINATOR",
        1,
        "authority:dormant",
        150,
    );
    store
        .route_blocker(BlockerInput {
            objective_id: "REMH-DORMANT-COORDINATOR",
            expected_generation: 1,
            route: BlockerRoute::HumanOwner,
            diagnosed_at: 160,
            reference: "owner:pause",
            authority_admission_reference: Some("authority:dormant"),
        })
        .unwrap();
    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let adapter = FakeWakeAdapter::new(path, state.clone());
    let mut coordinator = Coordinator::new(store, adapter, "test-coordinator");
    assert_eq!(
        coordinator
            .coordinate_due("REMH-DORMANT-COORDINATOR", 1_000, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::Dormant)
    );
    assert_eq!(state.lock().unwrap().preflight_count, 0);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    let source = include_str!("coordinator.rs");
    assert!(!source.contains("admit_human_authority"));
    assert!(!source.contains("route_blocker"));
    assert!(!source.contains("ready_for_owner_test"));
}

#[test]
fn coordinator_crash_boundaries_never_blindly_replay_fake_admission() {
    for (name, behavior, expected_calls) in [
        ("before-call", FakeAdmission::PanicBeforeCall, 0),
        ("after-call", FakeAdmission::PanicAfterCall, 1),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-plane.sqlite3");
        let objective_id = format!("REMH-CRASH-{name}");
        let state = Arc::new(Mutex::new(FakeWakeState::default()));
        let mut adapter = FakeWakeAdapter::new(path.clone(), state.clone());
        adapter.admissions = VecDeque::from([behavior]);
        let mut coordinator = coordinator_for(&path, &objective_id, adapter);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            coordinator.coordinate_due(&objective_id, 300, coordinator_target())
        }));
        assert!(result.is_err());
        assert_eq!(state.lock().unwrap().admission_count, expected_calls);
        let dispatch = coordinator
            .store()
            .dispatches(&objective_id)
            .unwrap()
            .remove(0);
        assert_eq!(dispatch.state, DispatchState::WakeIntent);
        drop(coordinator);

        let restarted_state = Arc::new(Mutex::new(FakeWakeState::default()));
        let restarted_adapter = FakeWakeAdapter::new(path.clone(), restarted_state.clone());
        let reopened = Store::open(path).unwrap();
        let mut restarted = Coordinator::new(reopened, restarted_adapter, "test-coordinator");
        restarted.reconcile_startup(&[], 1_000, 900).unwrap();
        assert_eq!(
            restarted
                .store()
                .dispatch(dispatch.id)
                .unwrap()
                .unwrap()
                .state,
            DispatchState::VerifyBeforeRetry
        );
        assert_eq!(
            restarted
                .coordinate_due(&objective_id, 1_001, coordinator_target())
                .unwrap(),
            Coordination::Silent(SilentReason::AlreadyIntervened)
        );
        assert_eq!(restarted_state.lock().unwrap().admission_count, 0);
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let adapter = FakeWakeAdapter::new(path.clone(), state.clone());
    let mut coordinator = coordinator_for(&path, "REMH-CRASH-AFTER-SENT", adapter);
    let sent = match coordinator
        .coordinate_due("REMH-CRASH-AFTER-SENT", 300, coordinator_target())
        .unwrap()
    {
        Coordination::Sent(dispatch) => dispatch,
        other => panic!("expected sent dispatch, got {other:?}"),
    };
    assert_eq!(state.lock().unwrap().admission_count, 1);
    drop(coordinator);
    let restarted_state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut restarted = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, restarted_state.clone()),
        "test-coordinator",
    );
    restarted.reconcile_startup(&[], 1_000, 900).unwrap();
    assert_eq!(
        restarted.store().dispatch(sent.id).unwrap().unwrap().state,
        DispatchState::Sent
    );
    assert_eq!(restarted_state.lock().unwrap().admission_count, 0);
}

#[test]
fn circuit_opened_after_claim_blocks_intent_until_equality_closes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path).unwrap();
    store
        .create_active(active("REMH-HELD-CLAIM", "fgd-smoke-agent", 100, 200))
        .unwrap();
    store
        .create_active(active("REMH-SIBLING-FAIL", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let held = reserve(&mut store, "REMH-HELD-CLAIM", 1, 300);
    let sibling = reserve(&mut store, "REMH-SIBLING-FAIL", 1, 300);
    store.claim_dispatch(held, "test", 301, 1_000).unwrap();
    store
        .record_dispatch_failure(sibling, "provider_down", 302, 900)
        .unwrap();
    let intent = |key| WakeIntentInput {
        target_agent_key: "fgd-smoke-agent",
        target_pubkey: TARGET_PUBKEY,
        channel_id: CHANNEL,
        thread_root_id: THREAD_ROOT,
        idempotency_key: key,
        instruction: "guard the wake boundary",
    };
    assert!(store
        .record_wake_intent(held, "test", intent("blocked"), 303)
        .is_err());
    assert_eq!(
        store.dispatch(held).unwrap().unwrap().state,
        DispatchState::Claimed
    );
    let frozen = store
        .record_wake_intent(held, "test", intent("equality-closed"), 900)
        .unwrap();
    assert_eq!(frozen.state, DispatchState::WakeIntent);
}

#[test]
fn circuit_opened_after_frozen_intent_prevents_admission_and_restart_quarantines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-POST-INTENT", "fgd-smoke-agent", 100, 200))
        .unwrap();
    store
        .create_active(active(
            "REMH-POST-INTENT-SIBLING",
            "fgd-smoke-agent",
            100,
            200,
        ))
        .unwrap();
    let sibling = reserve(&mut store, "REMH-POST-INTENT-SIBLING", 1, 300);
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let mut adapter = FakeWakeAdapter::new(path.clone(), state.clone());
    adapter.open_circuit_after_intent = Some(sibling);
    let mut coordinator = coordinator_for(&path, "REMH-POST-INTENT", adapter);
    let uncertain = match coordinator
        .coordinate_due("REMH-POST-INTENT", 300, coordinator_target())
        .unwrap()
    {
        Coordination::Uncertain(dispatch) => dispatch,
        other => panic!("expected guarded uncertain intent, got {other:?}"),
    };
    assert_eq!(uncertain.state, DispatchState::WakeIntent);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    drop(coordinator);

    let reopened = Store::open(path).unwrap();
    let restart_state = Arc::new(Mutex::new(FakeWakeState::default()));
    let mut restarted = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(
            dir.path().join("control-plane.sqlite3"),
            restart_state.clone(),
        ),
        "test-coordinator",
    );
    restarted.reconcile_startup(&[], 1_000, 900).unwrap();
    assert_eq!(
        restarted
            .store()
            .dispatch(uncertain.id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::VerifyBeforeRetry
    );
    assert_eq!(restart_state.lock().unwrap().admission_count, 0);
}

#[test]
fn expired_pre_boundary_claim_recovers_pending_at_same_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path).unwrap();
    store
        .create_active(active("REMH-EXPIRED-CLAIM", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let id = reserve(&mut store, "REMH-EXPIRED-CLAIM", 1, 300);
    store.claim_dispatch(id, "first-holder", 301, 361).unwrap();
    store.reconcile_startup(&[], 1_000, 900).unwrap();
    let recovered = store.dispatch(id).unwrap().unwrap();
    assert_eq!(recovered.state, DispatchState::Pending);
    assert_eq!(recovered.attempt, 1);
    let reclaimed = store
        .claim_dispatch(id, "second-holder", 1_001, 1_061)
        .unwrap();
    assert_eq!(reclaimed.state, DispatchState::Claimed);
    assert_eq!(reclaimed.attempt, 1);
}

#[test]
fn coordinator_reopens_and_resumes_the_same_pending_dispatch_before_claim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-RESUME-PENDING", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let dispatch_id = reserve(&mut store, "REMH-RESUME-PENDING", 1, 300);
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut coordinator = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, state.clone()),
        "restart-coordinator",
    );
    let sent = match coordinator
        .coordinate_due("REMH-RESUME-PENDING", 301, coordinator_target())
        .unwrap()
    {
        Coordination::Sent(dispatch) => dispatch,
        other => panic!("expected resumed pending dispatch, got {other:?}"),
    };

    assert_eq!(sent.id, dispatch_id);
    assert_eq!(sent.state, DispatchState::Sent);
    assert_eq!(sent.attempt, 1);
    let dispatches = coordinator
        .store()
        .dispatches("REMH-RESUME-PENDING")
        .unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].id, dispatch_id);
    assert_eq!(state.lock().unwrap().preflight_count, 1);
    assert_eq!(state.lock().unwrap().admission_count, 1);
}

#[test]
fn coordinator_waits_for_claim_expiry_then_resumes_the_same_reconciled_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-RESUME-CLAIMED", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let dispatch_id = reserve(&mut store, "REMH-RESUME-CLAIMED", 1, 300);
    store
        .claim_dispatch(dispatch_id, "crashed-holder", 301, 1_000)
        .unwrap();
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut coordinator = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, state.clone()),
        "restart-coordinator",
    );
    coordinator.reconcile_startup(&[], 500, 500).unwrap();
    assert_eq!(
        coordinator
            .coordinate_due("REMH-RESUME-CLAIMED", 501, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::AlreadyIntervened)
    );
    assert_eq!(state.lock().unwrap().preflight_count, 0);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    assert_eq!(
        coordinator
            .store()
            .dispatch(dispatch_id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::Claimed
    );

    coordinator.reconcile_startup(&[], 1_000, 1_000).unwrap();
    let sent = match coordinator
        .coordinate_due("REMH-RESUME-CLAIMED", 1_001, coordinator_target())
        .unwrap()
    {
        Coordination::Sent(dispatch) => dispatch,
        other => panic!("expected resumed reconciled dispatch, got {other:?}"),
    };
    assert_eq!(sent.id, dispatch_id);
    assert_eq!(sent.state, DispatchState::Sent);
    assert_eq!(sent.attempt, 1);
    assert_eq!(
        coordinator
            .store()
            .dispatches("REMH-RESUME-CLAIMED")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(state.lock().unwrap().preflight_count, 1);
    assert_eq!(state.lock().unwrap().admission_count, 1);
}

#[test]
fn coordinator_does_not_resume_failed_or_verify_before_retry_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-NO-RESUME-FAILED", "fgd-smoke-agent", 100, 200))
        .unwrap();
    store
        .create_active(active("REMH-NO-RESUME-VERIFY", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let failed_id = reserve(&mut store, "REMH-NO-RESUME-FAILED", 1, 300);
    let verify_id = reserve(&mut store, "REMH-NO-RESUME-VERIFY", 1, 300);
    freeze(&mut store, verify_id, 301, "no-resume-verify");
    store
        .record_dispatch_failure(failed_id, "provider_down", 301, 900)
        .unwrap();
    store.reconcile_startup(&[], 1_000, 900).unwrap();
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut coordinator = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, state.clone()),
        "restart-coordinator",
    );
    for objective_id in ["REMH-NO-RESUME-FAILED", "REMH-NO-RESUME-VERIFY"] {
        assert_eq!(
            coordinator
                .coordinate_due(objective_id, 1_001, coordinator_target())
                .unwrap(),
            Coordination::Silent(SilentReason::AlreadyIntervened)
        );
    }
    assert_eq!(state.lock().unwrap().preflight_count, 0);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    assert_eq!(
        coordinator
            .store()
            .dispatch(failed_id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::Failed
    );
    assert_eq!(
        coordinator
            .store()
            .dispatch(verify_id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::VerifyBeforeRetry
    );
}

#[test]
fn coordinator_never_selects_an_older_generation_pending_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active(
            "REMH-CURRENT-GENERATION-ONLY",
            "fgd-smoke-agent",
            100,
            200,
        ))
        .unwrap();
    let older_id = reserve(&mut store, "REMH-CURRENT-GENERATION-ONLY", 1, 300);
    store
        .record_verified_progress(proof("REMH-CURRENT-GENERATION-ONLY", 400, 500))
        .unwrap();
    let current_id = reserve(&mut store, "REMH-CURRENT-GENERATION-ONLY", 2, 600);
    freeze(&mut store, current_id, 601, "current-generation-only");
    store.reconcile_startup(&[], 1_000, 900).unwrap();
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut coordinator = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, state.clone()),
        "restart-coordinator",
    );
    assert_eq!(
        coordinator
            .coordinate_due("REMH-CURRENT-GENERATION-ONLY", 1_001, coordinator_target())
            .unwrap(),
        Coordination::Silent(SilentReason::AlreadyIntervened)
    );
    assert_eq!(state.lock().unwrap().preflight_count, 0);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    assert_eq!(
        coordinator
            .store()
            .dispatch(older_id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::Pending
    );
    assert_eq!(
        coordinator
            .store()
            .dispatch(current_id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::VerifyBeforeRetry
    );
}

#[test]
fn resumed_pending_dispatch_preserves_target_and_rejects_a_mismatched_target_before_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    store
        .create_active(active("REMH-RESUME-TARGET", "fgd-smoke-agent", 100, 200))
        .unwrap();
    let dispatch_id = reserve(&mut store, "REMH-RESUME-TARGET", 1, 300);
    drop(store);

    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let reopened = Store::open(path.clone()).unwrap();
    let mut coordinator = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, state.clone()),
        "restart-coordinator",
    );
    let result = coordinator.coordinate_due(
        "REMH-RESUME-TARGET",
        301,
        WakeTarget {
            agent_key: "different-agent",
            ..coordinator_target()
        },
    );
    assert!(result.is_err());
    assert_eq!(state.lock().unwrap().preflight_count, 1);
    assert_eq!(state.lock().unwrap().admission_count, 0);
    let dispatch = coordinator.store().dispatch(dispatch_id).unwrap().unwrap();
    assert_eq!(dispatch.id, dispatch_id);
    assert_eq!(dispatch.attempt, 1);
    assert_eq!(dispatch.target_agent_key, "fgd-smoke-agent");
    assert_eq!(dispatch.state, DispatchState::Claimed);
}

#[test]
fn adapter_uncertainty_remains_frozen_and_non_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let state = Arc::new(Mutex::new(FakeWakeState::default()));
    let mut adapter = FakeWakeAdapter::new(path.clone(), state.clone());
    adapter.admissions = VecDeque::from([FakeAdmission::Uncertain]);
    let mut coordinator = coordinator_for(&path, "REMH-ADAPTER-UNCERTAIN", adapter);
    let dispatch = match coordinator
        .coordinate_due("REMH-ADAPTER-UNCERTAIN", 300, coordinator_target())
        .unwrap()
    {
        Coordination::Uncertain(dispatch) => dispatch,
        other => panic!("expected uncertain admission, got {other:?}"),
    };
    assert_eq!(dispatch.state, DispatchState::WakeIntent);
    assert_eq!(state.lock().unwrap().admission_count, 1);
    drop(coordinator);

    let reopened = Store::open(path.clone()).unwrap();
    let restart_state = Arc::new(Mutex::new(FakeWakeState::default()));
    let mut restarted = Coordinator::new(
        reopened,
        FakeWakeAdapter::new(path, restart_state.clone()),
        "test-coordinator",
    );
    restarted.reconcile_startup(&[], 1_000, 900).unwrap();
    assert_eq!(
        restarted
            .store()
            .dispatch(dispatch.id)
            .unwrap()
            .unwrap()
            .state,
        DispatchState::VerifyBeforeRetry
    );
    assert_eq!(restart_state.lock().unwrap().admission_count, 0);
}

#[test]
fn coordinator_idempotency_key_is_deterministic_from_stable_dispatch_fields() {
    let mut keys = Vec::new();
    for _ in 0..2 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-plane.sqlite3");
        let state = Arc::new(Mutex::new(FakeWakeState::default()));
        let adapter = FakeWakeAdapter::new(path.clone(), state.clone());
        let mut coordinator = coordinator_for(&path, "REMH-DETERMINISTIC-KEY", adapter);
        assert!(matches!(
            coordinator
                .coordinate_due("REMH-DETERMINISTIC-KEY", 300, coordinator_target())
                .unwrap(),
            Coordination::Sent(_)
        ));
        keys.push(state.lock().unwrap().idempotency_keys[0].clone());
    }
    assert_eq!(keys[0], keys[1]);
    assert!(keys[0].starts_with("remh-oi:"));
    assert_eq!(keys[0].len(), 72);
}
