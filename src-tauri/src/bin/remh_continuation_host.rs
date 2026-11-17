#[path = "remh-continuation-host/accepted.rs"]
mod accepted;

use accepted::{DispatchPlan, PlannerState, Stage};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const REVIEW_SECONDS: i64 = 60;
const APPROVED_PROJECT: &str = "REM108_APPROVED_PROJECT";
const CONDITION_REPORT_DEPENDENCY_EVENT: &str =
    "504370148108d2758ce40c47c5a1f58ea97e16831ad4aa5b151dc6076f1fff30";
const OPERATIONAL_FINDINGS_OBJECTIVE_EVENT: &str =
    "3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2";
const CONDITION_REPORT_IDEMPOTENCY_KEY: &str =
    "remh-oi-condition-report-owner-test-ready→operational-findings-v1";
const CONDITION_REPORT_TERMINAL_AND_START_EVENT: &str =
    "ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339";
const KENNY_PUBKEY: &str = "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9";

type Result<T> = std::result::Result<T, String>;

#[derive(Deserialize)]
struct Request {
    action: String,
    now: Option<i64>,
    route_identity: Option<String>,
    objective_id: Option<String>,
    generation: Option<i64>,
    marker: Option<String>,
    event_id: Option<String>,
    caller_pubkey: Option<String>,
    project_id: Option<String>,
    manifest_json: Option<String>,
    approved_checksum: Option<String>,
    evidence_source: Option<String>,
    transport_failure: Option<bool>,
    dependency_event_id: Option<String>,
    objective_event_id: Option<String>,
    idempotency_key: Option<String>,
    upstream_state: Option<String>,
    downstream_state: Option<String>,
    downstream_agent_key: Option<String>,
    terminal_event_id: Option<String>,
    predecessor_event_id: Option<String>,
    start_receipt_event_id: Option<String>,
    implementer_pubkey: Option<String>,
    activation_generation: Option<i64>,
}

#[derive(Clone)]
struct StateRow {
    generation: i64,
    stage: Stage,
    label: String,
    next_review_at: Option<i64>,
    blocker_opened_generation: Option<i64>,
    blocker_resolved: bool,
    source_event_id: String,
    occurred_at: i64,
    public_result_count: i64,
    last_public_result: Option<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let state_path = parse_state_path()?;
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|_| "Continuation request could not be read".to_string())?;
    let request: Request =
        serde_json::from_str(&input).map_err(|_| "Continuation request is invalid".to_string())?;
    let response = handle(&state_path, request)?;
    println!(
        "{}",
        serde_json::to_string(&response)
            .map_err(|_| "Continuation response could not be encoded".to_string())?
    );
    Ok(())
}

fn parse_state_path() -> Result<PathBuf> {
    let mut args = std::env::args().skip(1);
    match (args.next().as_deref(), args.next(), args.next()) {
        (Some("--state"), Some(path), None) => Ok(PathBuf::from(path)),
        (None, None, None) => {
            let home = dirs::home_dir().ok_or("Home directory unavailable")?;
            Ok(home
                .join("Library/Application Support/xyz.block.buzz.desktop")
                .join("remh-continuation-pilot.sqlite3"))
        }
        _ => Err("Usage: remh-continuation-host [--state PATH]".into()),
    }
}

fn open(path: &Path) -> Result<Connection> {
    let parent = path
        .parent()
        .ok_or("Continuation state location unavailable")?;
    std::fs::create_dir_all(parent)
        .map_err(|_| "Continuation state directory unavailable".to_string())?;
    let connection =
        Connection::open(path).map_err(|error| format!("Continuation storage failed: {error}"))?;
    connection
        .execute_batch(
            "PRAGMA foreign_keys=ON;
             PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS remh_pilot_state(
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               generation INTEGER NOT NULL,
               stage TEXT NOT NULL,
               label TEXT NOT NULL,
               next_review_at INTEGER,
               blocker_opened_generation INTEGER,
               blocker_resolved INTEGER NOT NULL DEFAULT 0,
               source_event_id TEXT NOT NULL,
               occurred_at INTEGER NOT NULL,
               public_result_count INTEGER NOT NULL DEFAULT 0,
               last_public_result TEXT
             ) STRICT;
             CREATE TABLE IF NOT EXISTS remh_pilot_dispatches(
               route_identity TEXT PRIMARY KEY NOT NULL,
               objective_id TEXT NOT NULL,
               generation INTEGER NOT NULL,
               stage TEXT NOT NULL,
               target_role TEXT NOT NULL,
               target_agent_key TEXT NOT NULL,
               target_pubkey TEXT NOT NULL,
               channel_id TEXT NOT NULL,
               thread_root_id TEXT NOT NULL,
               state TEXT NOT NULL CHECK(state IN ('PENDING','WAKE_INTENT','SENT','ACKED')),
               ack_marker TEXT UNIQUE NOT NULL,
               wake_payload TEXT NOT NULL,
               wake_intent_at INTEGER,
               sent_at INTEGER,
               ack_event_id TEXT UNIQUE,
               acknowledged_at INTEGER,
               created_at INTEGER NOT NULL,
               updated_at INTEGER NOT NULL
             ) STRICT;
             CREATE TABLE IF NOT EXISTS remh_pilot_counters(
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               deterministic_evaluations INTEGER NOT NULL DEFAULT 0,
               no_change_evaluations INTEGER NOT NULL DEFAULT 0,
               no_change_invocations INTEGER NOT NULL DEFAULT 0,
               routed_invocations INTEGER NOT NULL DEFAULT 0,
               duplicate_invocations INTEGER NOT NULL DEFAULT 0,
               reviewer_invocations INTEGER NOT NULL DEFAULT 0,
               owner_invocations INTEGER NOT NULL DEFAULT 0
             ) STRICT;
             INSERT OR IGNORE INTO remh_pilot_counters(singleton) VALUES(1);
             CREATE TABLE IF NOT EXISTS remh_condition_report_reconciliation(
               idempotency_key TEXT PRIMARY KEY NOT NULL,
               dependency_event_id TEXT UNIQUE NOT NULL,
               objective_event_id TEXT NOT NULL,
               upstream_state TEXT NOT NULL,
               terminal_event_id TEXT UNIQUE NOT NULL,
               dependency_state TEXT NOT NULL CHECK(dependency_state='SATISFIED'),
               downstream_state TEXT NOT NULL,
               downstream_agent_key TEXT NOT NULL,
               implementer_pubkey TEXT NOT NULL,
               predecessor_event_id TEXT NOT NULL,
               start_receipt_event_id TEXT UNIQUE NOT NULL,
               activation_generation INTEGER NOT NULL CHECK(activation_generation > 0),
               activation_state TEXT NOT NULL CHECK(activation_state='ACCOUNTED'),
               reconciled_at INTEGER NOT NULL
             ) STRICT;",
        )
        .map_err(|error| format!("Continuation schema failed: {error}"))?;
    Ok(connection)
}

fn handle(path: &Path, request: Request) -> Result<Value> {
    match request.action.as_str() {
        "condition_report_gate" => condition_report_gate(&request),
        _ => {
            let connection = open(path)?;
            match request.action.as_str() {
                "arm" => arm(path, &connection, required_now(&request)?),
                "pump" => pump(path, &connection, required_now(&request)?),
                "progress" => transition(
                    path,
                    &connection,
                    required_now(&request)?,
                    Stage::Implementing,
                    "material-progress",
                    None,
                    false,
                    "PROGRESSED",
                ),
                "infrastructure_blocked" => transition(
                    path,
                    &connection,
                    required_now(&request)?,
                    Stage::InfraBlocked,
                    "infrastructure-blocked",
                    None,
                    false,
                    "PENDING",
                ),
                "response_blocked" => response_blocked(path, &connection, &request),
                "infrastructure_resolved" => {
                    let current = state(&connection)?.ok_or("Pilot is not armed")?;
                    transition(
                        path,
                        &connection,
                        required_now(&request)?,
                        Stage::Implementing,
                        "infrastructure-resolved",
                        current.blocker_opened_generation,
                        true,
                        "PENDING",
                    )
                }
                "review_ready" => transition(
                    path,
                    &connection,
                    required_now(&request)?,
                    Stage::ReviewReady,
                    "review-ready",
                    None,
                    false,
                    "PENDING",
                ),
                "owner_gate" => transition(
                    path,
                    &connection,
                    required_now(&request)?,
                    Stage::AwaitingOwner,
                    "owner-gate",
                    None,
                    false,
                    "PENDING",
                ),
                "owner_action" => transition(
                    path,
                    &connection,
                    required_now(&request)?,
                    Stage::Implementing,
                    "owner-action-received",
                    None,
                    false,
                    "PENDING",
                ),
                "complete" => complete(&connection, required_now(&request)?),
                "claim" => claim(&connection, &request),
                "sent" => sent(&connection, &request),
                "ack" => ack(&connection, &request),
                "response_fallback" => response_fallback(&connection, &request),
                "reconcile_condition_report_dependency" => {
                    reconcile_condition_report_dependency(&connection, &request)
                }
                "condition_report_dependency_status" => {
                    condition_report_dependency_status(&connection)
                }
                "counters" => counters(&connection),
                "status" => status(&connection),
                _ => Err("Unsupported continuation action".into()),
            }
        }
    }
}

fn required_now(request: &Request) -> Result<i64> {
    request
        .now
        .filter(|value| *value >= 0)
        .ok_or("A non-negative trusted time is required".into())
}

fn arm(path: &Path, connection: &Connection, now: i64) -> Result<Value> {
    if state(connection)?.is_some() {
        return status(connection);
    }
    increment(connection, "deterministic_evaluations")?;
    let planner = PlannerState {
        generation: 1,
        stage: Stage::Active,
        label: "objective-enrolled".to_string(),
        next_review_at: Some(now + REVIEW_SECONDS),
        blocker_opened_generation: None,
        blocker_resolved: false,
        source_event_id: "pilot-arm-1".to_string(),
        occurred_at: now,
    };
    let plan = accepted::evaluate(path.to_path_buf(), &planner, now)?
        .ok_or("Initial implementer route was not planned")?;
    persist_state(connection, &planner, 0, None)?;
    persist_plan(connection, &plan, now)
}

fn pump(path: &Path, connection: &Connection, now: i64) -> Result<Value> {
    let Some(current) = state(connection)? else {
        return Ok(json!({"status":"UNARMED","dispatch":null}));
    };
    increment(connection, "deterministic_evaluations")?;
    if current.stage == Stage::Complete {
        return Ok(base_response("COMPLETE", None, &current));
    }
    if let Some(dispatch) = pending_dispatch(connection)? {
        if dispatch["state"] != "PENDING" {
            increment(connection, "no_change_evaluations")?;
        }
        return Ok(json!({
            "status": if dispatch["state"] == "PENDING" { "PENDING" } else { "DUPLICATE" },
            "dispatch": if dispatch["state"] == "PENDING" { dispatch } else { Value::Null },
            "next_review_at": current.next_review_at
        }));
    }
    let planner = planner(&current);
    match accepted::evaluate(path.to_path_buf(), &planner, now)? {
        Some(plan) => persist_plan(connection, &plan, now),
        None => {
            increment(connection, "no_change_evaluations")?;
            Ok(base_response("NO_CHANGE", None, &current))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn transition(
    path: &Path,
    connection: &Connection,
    now: i64,
    stage: Stage,
    label: &str,
    blocker_opened_generation: Option<i64>,
    blocker_resolved: bool,
    silent_status: &str,
) -> Result<Value> {
    let current = state(connection)?.ok_or("Pilot is not armed")?;
    if current.stage == Stage::Complete {
        return Err("Completed pilot is terminal".into());
    }
    increment(connection, "deterministic_evaluations")?;
    let generation = current.generation + 1;
    let opened = if stage == Stage::InfraBlocked {
        Some(generation)
    } else {
        blocker_opened_generation
    };
    let planner = PlannerState {
        generation,
        stage,
        label: label.to_string(),
        next_review_at: (stage != Stage::Complete).then_some(now + REVIEW_SECONDS),
        blocker_opened_generation: opened,
        blocker_resolved,
        source_event_id: format!("pilot-{generation}-{label}"),
        occurred_at: now,
    };
    let plan = accepted::evaluate(path.to_path_buf(), &planner, now)?;
    persist_state(
        connection,
        &planner,
        current.public_result_count,
        current.last_public_result.as_deref(),
    )?;
    match plan {
        Some(plan) => persist_plan(connection, &plan, now),
        None => Ok(base_response(
            silent_status,
            None,
            &state(connection)?.unwrap(),
        )),
    }
}

fn complete(connection: &Connection, now: i64) -> Result<Value> {
    let current = state(connection)?.ok_or("Pilot is not armed")?;
    increment(connection, "deterministic_evaluations")?;
    connection
        .execute(
            "UPDATE remh_pilot_state SET generation=generation+1,stage='OBJECTIVE_COMPLETE',label='complete',next_review_at=NULL,source_event_id=?1,occurred_at=?2 WHERE singleton=1",
            params![format!("pilot-{}-complete", current.generation + 1), now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    Ok(base_response(
        "COMPLETE",
        None,
        &state(connection)?.unwrap(),
    ))
}

fn response_blocked(path: &Path, connection: &Connection, request: &Request) -> Result<Value> {
    let event_id = request
        .event_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("A valid BLOCKED event id is required")?;
    let mut current = state(connection)?.ok_or("Pilot is not armed")?;
    if current.stage == Stage::Active {
        transition(
            path,
            connection,
            required_now(request)?,
            Stage::Implementing,
            "agent-started-before-blocker",
            None,
            false,
            "PROGRESSED",
        )?;
        current = state(connection)?.ok_or("Pilot state disappeared")?;
    }
    let result = transition(
        path,
        connection,
        required_now(request)?,
        Stage::InfraBlocked,
        event_id,
        None,
        false,
        "PENDING",
    )?;
    let changed = connection
        .execute(
            "UPDATE remh_pilot_state SET public_result_count=public_result_count+1,last_public_result='BLOCKER_ROUTED' WHERE singleton=1 AND public_result_count=0",
            [],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    if changed != 1 || current.public_result_count != 0 {
        return Err("A deliberate public result already exists".into());
    }
    let mut output = result;
    output["public_result"] = json!("BLOCKER_ROUTED");
    output["public_result_count"] = json!(1);
    output["generic_failure_allowed"] = json!(false);
    Ok(output)
}

fn response_fallback(connection: &Connection, request: &Request) -> Result<Value> {
    let current = state(connection)?.ok_or("Pilot is not armed")?;
    let transport_failure = request.transport_failure.unwrap_or(false);
    if current.public_result_count > 0 || !transport_failure {
        return Ok(json!({
            "status":"FALLBACK_SUPPRESSED",
            "dispatch":null,
            "public_result_count":current.public_result_count,
            "current_stage":current.stage.as_str(),
            "generic_failure_allowed":false
        }));
    }
    connection
        .execute(
            "UPDATE remh_pilot_state SET public_result_count=1,last_public_result='GENERIC_RUNTIME_FAILURE' WHERE singleton=1 AND public_result_count=0",
            [],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    Ok(json!({
        "status":"GENERIC_RUNTIME_FAILURE",
        "dispatch":null,
        "public_result":"⚠️ I couldn't finish that: Internal error",
        "public_result_count":1,
        "current_stage":current.stage.as_str(),
        "generic_failure_allowed":true
    }))
}

fn claim(connection: &Connection, request: &Request) -> Result<Value> {
    let route = required_route(request)?;
    let now = required_now(request)?;
    let changed = connection
        .execute(
            "UPDATE remh_pilot_dispatches SET state='WAKE_INTENT',wake_intent_at=?2,updated_at=?2 WHERE route_identity=?1 AND state='PENDING'",
            params![route, now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    if changed != 1 {
        return Err("Dispatch is not pending".into());
    }
    dispatch_response(connection, "WAKE_INTENT", route)
}

fn sent(connection: &Connection, request: &Request) -> Result<Value> {
    let route = required_route(request)?;
    let now = required_now(request)?;
    let role: String = connection
        .query_row(
            "SELECT target_role FROM remh_pilot_dispatches WHERE route_identity=?1 AND state='WAKE_INTENT'",
            [route],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))?
        .ok_or("Dispatch has no wake intent")?;
    connection
        .execute(
            "UPDATE remh_pilot_dispatches SET state='SENT',sent_at=?2,updated_at=?2 WHERE route_identity=?1 AND state='WAKE_INTENT'",
            params![route, now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    increment(connection, "routed_invocations")?;
    if role == "TECHNICAL_REVIEWER" {
        increment(connection, "reviewer_invocations")?;
    }
    if role == "OWNER" {
        increment(connection, "owner_invocations")?;
    }
    dispatch_response(connection, "SENT", route)
}

fn ack(connection: &Connection, request: &Request) -> Result<Value> {
    let marker = request
        .marker
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("ACK marker is required")?;
    let event_id = request
        .event_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("ACK event id is required")?;
    let now = required_now(request)?;
    let supplied_route = required_route(request)?;
    let supplied_objective = request
        .objective_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("ACK objective id is required")?;
    let supplied_generation = request
        .generation
        .filter(|value| *value > 0)
        .ok_or("ACK generation is required")?;
    let caller_pubkey = request
        .caller_pubkey
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or("ACK caller identity is required")?;
    let dispatch: Option<(String, String, i64, String, String, Option<String>)> = connection
        .query_row(
            "SELECT route_identity,objective_id,generation,target_pubkey,state,ack_event_id FROM remh_pilot_dispatches WHERE ack_marker=?1",
            [marker],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    let Some((route, objective, generation, target_pubkey, state, prior_event_id)) = dispatch
    else {
        return Ok(json!({"status":"ACK_REJECTED","dispatch":null}));
    };
    let expected_event_id = ack_event_id(&route);
    if supplied_route != route
        || supplied_objective != objective
        || supplied_generation != generation
        || caller_pubkey != target_pubkey
        || event_id != expected_event_id
    {
        return Ok(json!({"status":"ACK_REJECTED","dispatch":null}));
    }
    if state == "ACKED" && prior_event_id.as_deref() == Some(event_id) {
        return dispatch_response(connection, "ALREADY_ACKED", &route);
    }
    if state != "SENT" {
        return Ok(json!({"status":"ACK_REJECTED","dispatch":null}));
    }
    let changed = connection
        .execute(
            "UPDATE remh_pilot_dispatches SET state='ACKED',ack_event_id=?2,acknowledged_at=?3,updated_at=?3 WHERE route_identity=?1 AND state='SENT'",
            params![route, event_id, now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    if changed != 1 {
        return Err("ACK persistence lost its exact dispatch claim".into());
    }
    dispatch_response(connection, "ACKED", &route)
}

fn reconcile_condition_report_dependency(
    connection: &Connection,
    request: &Request,
) -> Result<Value> {
    let now = required_now(request)?;
    let dependency_event_id = required_exact(
        request.dependency_event_id.as_deref(),
        CONDITION_REPORT_DEPENDENCY_EVENT,
        "Exact condition-report dependency event is required",
    )?;
    let objective_event_id = required_exact(
        request.objective_event_id.as_deref(),
        OPERATIONAL_FINDINGS_OBJECTIVE_EVENT,
        "Exact operational-findings objective event is required",
    )?;
    let idempotency_key = required_exact(
        request.idempotency_key.as_deref(),
        CONDITION_REPORT_IDEMPOTENCY_KEY,
        "Exact condition-report idempotency key is required",
    )?;
    let upstream_state = required_exact(
        request.upstream_state.as_deref(),
        "OWNER_TEST_READY",
        "Condition report is not terminal OWNER_TEST_READY",
    )?;
    let downstream_state = required_exact(
        request.downstream_state.as_deref(),
        "IMPLEMENTING",
        "Operational findings is not already IMPLEMENTING",
    )?;
    let downstream_agent_key = required_exact(
        request.downstream_agent_key.as_deref(),
        "kenny-remh",
        "Operational findings is not active with Kenny",
    )?;
    let terminal_event_id = required_exact(
        request.terminal_event_id.as_deref(),
        CONDITION_REPORT_TERMINAL_AND_START_EVENT,
        "Exact Condition Report terminal event is required",
    )?;
    let predecessor_event_id = required_exact(
        request.predecessor_event_id.as_deref(),
        CONDITION_REPORT_TERMINAL_AND_START_EVENT,
        "Exact admitted IMPLEMENTING predecessor event is required",
    )?;
    let start_receipt_event_id = required_exact(
        request.start_receipt_event_id.as_deref(),
        CONDITION_REPORT_TERMINAL_AND_START_EVENT,
        "Exact Kenny start receipt event is required",
    )?;
    let implementer_pubkey = required_exact(
        request.implementer_pubkey.as_deref(),
        KENNY_PUBKEY,
        "Exact Kenny implementer identity is required",
    )?;
    let activation_generation = request
        .activation_generation
        .filter(|generation| *generation > 0)
        .ok_or("A positive activation generation is required")?;

    let changed = connection
        .execute(
            "INSERT OR IGNORE INTO remh_condition_report_reconciliation(idempotency_key,dependency_event_id,objective_event_id,upstream_state,terminal_event_id,dependency_state,downstream_state,downstream_agent_key,implementer_pubkey,predecessor_event_id,start_receipt_event_id,activation_generation,activation_state,reconciled_at) VALUES(?1,?2,?3,?4,?5,'SATISFIED',?6,?7,?8,?9,?10,?11,'ACCOUNTED',?12)",
            params![idempotency_key, dependency_event_id, objective_event_id, upstream_state, terminal_event_id, downstream_state, downstream_agent_key, implementer_pubkey, predecessor_event_id, start_receipt_event_id, activation_generation, now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    let persisted = condition_report_reconciliation_row(connection)?
        .ok_or("Condition-report reconciliation was not persisted")?;
    if persisted["dependency"]["source_event_id"] != dependency_event_id
        || persisted["dependency"]["objective_event_id"] != objective_event_id
        || persisted["dependency"]["idempotency_key"] != idempotency_key
        || persisted["dependency"]["terminal_event_id"] != terminal_event_id
        || persisted["activation"]["target_agent_key"] != downstream_agent_key
        || persisted["activation"]["implementer_pubkey"] != implementer_pubkey
        || persisted["activation"]["predecessor_event_id"] != predecessor_event_id
        || persisted["activation"]["start_receipt_event_id"] != start_receipt_event_id
        || persisted["activation"]["generation"] != activation_generation
    {
        return Err("Condition-report reconciliation conflicts with durable state".into());
    }
    Ok(reconciliation_response(
        if changed == 1 {
            "RECONCILED"
        } else {
            "ALREADY_RECONCILED"
        },
        persisted,
    ))
}

fn condition_report_dependency_status(connection: &Connection) -> Result<Value> {
    match condition_report_reconciliation_row(connection)? {
        Some(persisted) => Ok(reconciliation_response("RECONCILED", persisted)),
        None => Ok(json!({
            "status":"UNRECONCILED",
            "dispatch":null,
            "wake_count":0,
            "duplicate_wake_count":0
        })),
    }
}

fn condition_report_reconciliation_row(connection: &Connection) -> Result<Option<Value>> {
    connection
        .query_row(
            "SELECT idempotency_key,dependency_event_id,objective_event_id,upstream_state,terminal_event_id,dependency_state,downstream_state,downstream_agent_key,implementer_pubkey,predecessor_event_id,start_receipt_event_id,activation_generation,activation_state,reconciled_at FROM remh_condition_report_reconciliation WHERE idempotency_key=?1",
            [CONDITION_REPORT_IDEMPOTENCY_KEY],
            |row| {
                Ok(json!({
                    "dependency":{
                        "idempotency_key":row.get::<_,String>(0)?,
                        "source_event_id":row.get::<_,String>(1)?,
                        "objective_event_id":row.get::<_,String>(2)?,
                        "upstream_state":row.get::<_,String>(3)?,
                        "terminal_event_id":row.get::<_,String>(4)?,
                        "state":row.get::<_,String>(5)?
                    },
                    "activation":{
                        "downstream_state":row.get::<_,String>(6)?,
                        "target_agent_key":row.get::<_,String>(7)?,
                        "implementer_pubkey":row.get::<_,String>(8)?,
                        "predecessor_event_id":row.get::<_,String>(9)?,
                        "start_receipt_event_id":row.get::<_,String>(10)?,
                        "generation":row.get::<_,i64>(11)?,
                        "state":row.get::<_,String>(12)?,
                        "predecessor_state":"IMPLEMENTING",
                        "reconciled_at":row.get::<_,i64>(13)?
                    }
                }))
            },
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))
}

fn reconciliation_response(status: &str, persisted: Value) -> Value {
    json!({
        "status":status,
        "dispatch":null,
        "wake_count":0,
        "duplicate_wake_count":0,
        "dependency":persisted["dependency"],
        "activation":persisted["activation"],
        "boundaries":{
            "attention":"OFF",
            "model_polling":false,
            "tcc_enrolled":false,
            "live_business_enrollment":false
        }
    })
}

fn required_exact<'a>(value: Option<&'a str>, expected: &str, error: &str) -> Result<&'a str> {
    value.filter(|value| *value == expected).ok_or(error.into())
}

fn required_route(request: &Request) -> Result<&str> {
    request
        .route_identity
        .as_deref()
        .filter(|value| value.starts_with("remh-route:"))
        .ok_or("Exact route identity is required".into())
}

fn persist_plan(connection: &Connection, plan: &DispatchPlan, now: i64) -> Result<Value> {
    let digest = format!("{:x}", Sha256::digest(plan.route_identity.as_bytes()));
    let marker = format!("REMH_ACK_{digest}");
    let event_id = ack_event_id(&plan.route_identity);
    let payload = if plan.target_role == "OWNER" {
        plan.owner_action_message
            .clone()
            .ok_or("OWNER route is missing its exact action packet")?
    } else {
        format!(
            "Deterministic REMH continuation pilot data. Perform no REMH business work, file changes, external calls or messages. Call the remh_continuation_ack tool exactly once with marker `{marker}`, event_id `{event_id}`, objective_id `{}`, generation {}, and route_identity `{}`.",
            plan.objective_id, plan.generation, plan.route_identity
        )
    };
    let changed = connection
        .execute(
            "INSERT OR IGNORE INTO remh_pilot_dispatches(route_identity,objective_id,generation,stage,target_role,target_agent_key,target_pubkey,channel_id,thread_root_id,state,ack_marker,wake_payload,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'PENDING',?10,?11,?12,?12)",
            params![plan.route_identity,plan.objective_id,plan.generation,plan.state,plan.target_role,plan.target_agent_key,plan.target_pubkey,plan.channel_id,plan.thread_root_id,marker,payload,now],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    if changed == 0 {
        let current = state(connection)?.ok_or("Pilot state disappeared")?;
        return Ok(base_response("DUPLICATE", None, &current));
    }
    dispatch_response(connection, "PENDING", &plan.route_identity)
}

fn ack_event_id(route: &str) -> String {
    format!("pilot-ack-{:x}", Sha256::digest(route.as_bytes()))
}

fn dispatch_response(connection: &Connection, status: &str, route: &str) -> Result<Value> {
    let dispatch = dispatch_by_route(connection, route)?.ok_or("Dispatch disappeared")?;
    let current = state(connection)?.ok_or("Pilot state disappeared")?;
    Ok(json!({
        "status":status,
        "dispatch":dispatch,
        "next_review_at":current.next_review_at,
        "public_result_count":current.public_result_count,
        "current_stage":current.stage.as_str()
    }))
}

fn dispatch_by_route(connection: &Connection, route: &str) -> Result<Option<Value>> {
    connection
        .query_row(
            "SELECT route_identity,objective_id,generation,stage,target_role,target_agent_key,target_pubkey,channel_id,thread_root_id,state,ack_marker,wake_payload,wake_intent_at,sent_at,ack_event_id,acknowledged_at FROM remh_pilot_dispatches WHERE route_identity=?1",
            [route],
            |row| {
                let route_identity = row.get::<_, String>(0)?;
                Ok(json!({
                    "route_identity":route_identity,
                    "objective_id":row.get::<_,String>(1)?,
                    "generation":row.get::<_,i64>(2)?,
                    "stage":row.get::<_,String>(3)?,
                    "target_role":row.get::<_,String>(4)?,
                    "target_agent_key":row.get::<_,String>(5)?,
                    "target_pubkey":row.get::<_,String>(6)?,
                    "channel_id":row.get::<_,String>(7)?,
                    "thread_root_id":row.get::<_,String>(8)?,
                    "state":row.get::<_,String>(9)?,
                    "ack_marker":row.get::<_,String>(10)?,
                    "wake_payload":row.get::<_,String>(11)?,
                    "wake_intent_at":row.get::<_,Option<i64>>(12)?,
                    "sent_at":row.get::<_,Option<i64>>(13)?,
                    "ack_event_id":row.get::<_,Option<String>>(14)?,
                    "acknowledged_at":row.get::<_,Option<i64>>(15)?,
                    "expected_ack_event_id":ack_event_id(&route_identity)
                }))
            },
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))
}

fn pending_dispatch(connection: &Connection) -> Result<Option<Value>> {
    connection
        .query_row(
            "SELECT route_identity FROM remh_pilot_dispatches WHERE state IN ('PENDING','WAKE_INTENT','SENT') ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))?
        .map(|route| dispatch_by_route(connection, &route))
        .transpose()
        .map(Option::flatten)
}

fn persist_state(
    connection: &Connection,
    planner: &PlannerState,
    public_result_count: i64,
    last_public_result: Option<&str>,
) -> Result<()> {
    connection
        .execute(
            "INSERT INTO remh_pilot_state(singleton,generation,stage,label,next_review_at,blocker_opened_generation,blocker_resolved,source_event_id,occurred_at,public_result_count,last_public_result) VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation,stage=excluded.stage,label=excluded.label,next_review_at=excluded.next_review_at,blocker_opened_generation=excluded.blocker_opened_generation,blocker_resolved=excluded.blocker_resolved,source_event_id=excluded.source_event_id,occurred_at=excluded.occurred_at,public_result_count=excluded.public_result_count,last_public_result=excluded.last_public_result",
            params![planner.generation,planner.stage.as_str(),planner.label,planner.next_review_at,planner.blocker_opened_generation,planner.blocker_resolved as i64,planner.source_event_id,planner.occurred_at,public_result_count,last_public_result],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    Ok(())
}

fn state(connection: &Connection) -> Result<Option<StateRow>> {
    connection
        .query_row(
            "SELECT generation,stage,label,next_review_at,blocker_opened_generation,blocker_resolved,source_event_id,occurred_at,public_result_count,last_public_result FROM remh_pilot_state WHERE singleton=1",
            [],
            |row| {
                let stage: String = row.get(1)?;
                Ok(StateRow {
                    generation: row.get(0)?,
                    stage: parse_stage(&stage),
                    label: row.get(2)?,
                    next_review_at: row.get(3)?,
                    blocker_opened_generation: row.get(4)?,
                    blocker_resolved: row.get::<_, i64>(5)? != 0,
                    source_event_id: row.get(6)?,
                    occurred_at: row.get(7)?,
                    public_result_count: row.get(8)?,
                    last_public_result: row.get(9)?,
                })
            },
        )
        .optional()
        .map_err(|error| format!("Continuation storage failed: {error}"))
}

fn parse_stage(value: &str) -> Stage {
    match value {
        "ACTIVE" => Stage::Active,
        "IMPLEMENTING" => Stage::Implementing,
        "INFRA_BLOCKED" => Stage::InfraBlocked,
        "REVIEW_READY" => Stage::ReviewReady,
        "AWAITING_OWNER" => Stage::AwaitingOwner,
        "OBJECTIVE_COMPLETE" => Stage::Complete,
        _ => Stage::Active,
    }
}

fn planner(state: &StateRow) -> PlannerState {
    PlannerState {
        generation: state.generation,
        stage: state.stage,
        label: state.label.clone(),
        next_review_at: state.next_review_at,
        blocker_opened_generation: state.blocker_opened_generation,
        blocker_resolved: state.blocker_resolved,
        source_event_id: state.source_event_id.clone(),
        occurred_at: state.occurred_at,
    }
}

fn increment(connection: &Connection, column: &str) -> Result<()> {
    const ALLOWED: &[&str] = &[
        "deterministic_evaluations",
        "no_change_evaluations",
        "no_change_invocations",
        "routed_invocations",
        "duplicate_invocations",
        "reviewer_invocations",
        "owner_invocations",
    ];
    if !ALLOWED.contains(&column) {
        return Err("Invalid counter".into());
    }
    connection
        .execute(
            &format!("UPDATE remh_pilot_counters SET {column}={column}+1 WHERE singleton=1"),
            [],
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))?;
    Ok(())
}

fn counters(connection: &Connection) -> Result<Value> {
    connection
        .query_row(
            "SELECT deterministic_evaluations,no_change_evaluations,no_change_invocations,routed_invocations,duplicate_invocations,reviewer_invocations,owner_invocations FROM remh_pilot_counters WHERE singleton=1",
            [],
            |row| {
                Ok(json!({
                    "status":"COUNTERS",
                    "dispatch":null,
                    "counters":{
                        "deterministic_evaluations":row.get::<_,i64>(0)?,
                        "no_change_evaluations":row.get::<_,i64>(1)?,
                        "no_change_invocations":row.get::<_,i64>(2)?,
                        "routed_invocations":row.get::<_,i64>(3)?,
                        "duplicate_invocations":row.get::<_,i64>(4)?,
                        "reviewer_invocations":row.get::<_,i64>(5)?,
                        "owner_invocations":row.get::<_,i64>(6)?
                    }
                }))
            },
        )
        .map_err(|error| format!("Continuation storage failed: {error}"))
}

fn status(connection: &Connection) -> Result<Value> {
    let Some(current) = state(connection)? else {
        return Ok(json!({"status":"UNARMED","dispatch":null}));
    };
    Ok(base_response(
        if current.stage == Stage::Complete {
            "COMPLETE"
        } else {
            "ARMED"
        },
        pending_dispatch(connection)?,
        &current,
    ))
}

fn base_response(status: &str, dispatch: Option<Value>, current: &StateRow) -> Value {
    json!({
        "status":status,
        "dispatch":dispatch,
        "next_review_at":current.next_review_at,
        "current_stage":current.stage.as_str(),
        "public_result_count":current.public_result_count,
        "last_public_result":current.last_public_result
    })
}

fn condition_report_gate(request: &Request) -> Result<Value> {
    let manifest = request.manifest_json.as_deref().unwrap_or("");
    let parsed: Option<Value> = serde_json::from_str(manifest).ok();
    let uploads = parsed
        .as_ref()
        .and_then(|value| value.get("approved_uploads"))
        .and_then(Value::as_array);
    let pdf_count = uploads
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| name.ends_with(".pdf"))
                && entry.get("mime_type").and_then(Value::as_str) == Some("application/pdf")
                && entry
                    .get("checksum")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
        })
        .count();
    let all_pdf = uploads.is_some_and(|entries| entries.len() == pdf_count);
    let actual_checksum = format!("{:x}", Sha256::digest(manifest.as_bytes()));
    let ready = request.project_id.as_deref() == Some(APPROVED_PROJECT)
        && request.evidence_source.as_deref() == Some("RUNTIME_READBACK")
        && manifest.len() == 272
        && pdf_count == 1
        && all_pdf
        && request.approved_checksum.as_deref() == Some(actual_checksum.as_str());
    Ok(json!({
        "status":if ready {"READY"} else {"HOLD"},
        "dispatch":null,
        "content_length":manifest.len(),
        "approved_pdf_count":pdf_count,
        "actual_checksum":actual_checksum,
        "condition_report_enrolled":false
    }))
}
