use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn invoke(state: &Path, request: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_remh-continuation-host"))
        .args(["--state", state.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(request.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "host failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn invoke_error(state: &Path, request: Value) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_remh-continuation-host"))
        .args(["--state", state.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(request.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

fn ack_request(dispatch: &Value, now: i64) -> Value {
    json!({
        "action":"ack",
        "now":now,
        "marker":dispatch["ack_marker"],
        "event_id":dispatch["expected_ack_event_id"],
        "objective_id":dispatch["objective_id"],
        "generation":dispatch["generation"],
        "route_identity":dispatch["route_identity"],
        "caller_pubkey":dispatch["target_pubkey"]
    })
}

#[test]
fn unarmed_silence_initial_wake_no_change_and_stall_wake_are_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");

    let unarmed = invoke(&state, json!({"action":"pump","now":100}));
    assert_eq!(unarmed["status"], "UNARMED");
    assert!(unarmed["dispatch"].is_null());

    let armed = invoke(&state, json!({"action":"arm","now":100}));
    assert_eq!(armed["status"], "PENDING");
    assert_eq!(armed["dispatch"]["target_role"], "IMPLEMENTER");
    let initial_route = armed["dispatch"]["route_identity"].as_str().unwrap();
    let claimed = invoke(
        &state,
        json!({"action":"claim","now":101,"route_identity":initial_route}),
    );
    assert_eq!(claimed["dispatch"]["state"], "WAKE_INTENT");
    let sent = invoke(
        &state,
        json!({"action":"sent","now":102,"route_identity":initial_route}),
    );
    assert_eq!(sent["dispatch"]["state"], "SENT");
    let acked = invoke(&state, ack_request(&sent["dispatch"], 103));
    assert_eq!(acked["dispatch"]["state"], "ACKED");
    let replayed = invoke(&state, ack_request(&sent["dispatch"], 104));
    assert_eq!(replayed["status"], "ALREADY_ACKED");
    assert_eq!(replayed["dispatch"]["acknowledged_at"], 103);

    let early = invoke(&state, json!({"action":"pump","now":159}));
    assert_eq!(early["status"], "NO_CHANGE");
    assert!(early["dispatch"].is_null());

    let due = invoke(&state, json!({"action":"pump","now":160}));
    assert_eq!(due["status"], "PENDING");
    assert_eq!(due["dispatch"]["target_role"], "IMPLEMENTER");
    assert_eq!(due["dispatch"]["state"], "PENDING");
    let route_identity = due["dispatch"]["route_identity"].as_str().unwrap();

    let claimed = invoke(
        &state,
        json!({"action":"claim","now":161,"route_identity":route_identity}),
    );
    assert_eq!(claimed["dispatch"]["state"], "WAKE_INTENT");

    let duplicate = invoke(&state, json!({"action":"pump","now":162}));
    assert_eq!(duplicate["status"], "DUPLICATE");
    assert!(duplicate["dispatch"].is_null());

    let sent = invoke(
        &state,
        json!({"action":"sent","now":163,"route_identity":route_identity}),
    );
    assert_eq!(sent["dispatch"]["state"], "SENT");
    assert_ne!(
        sent["dispatch"]["expected_ack_event_id"],
        acked["dispatch"]["expected_ack_event_id"]
    );
    let mut wrong_request = ack_request(&sent["dispatch"], 164);
    wrong_request["marker"] = json!("wrong");
    let wrong_ack = invoke(&state, wrong_request);
    assert_eq!(wrong_ack["status"], "ACK_REJECTED");

    let mut wrong_caller = ack_request(&sent["dispatch"], 165);
    wrong_caller["caller_pubkey"] = json!("wrong-agent");
    assert_eq!(invoke(&state, wrong_caller)["status"], "ACK_REJECTED");

    let mut wrong_route = ack_request(&sent["dispatch"], 166);
    wrong_route["route_identity"] = json!(initial_route);
    assert_eq!(invoke(&state, wrong_route)["status"], "ACK_REJECTED");

    let acked = invoke(&state, ack_request(&sent["dispatch"], 167));
    assert_eq!(acked["dispatch"]["state"], "ACKED");
}

#[test]
fn material_progress_blocker_recovery_review_and_completion_use_fixed_non_owner_routes() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");

    let initial = invoke(&state, json!({"action":"arm","now":100}));
    let initial_route = initial["dispatch"]["route_identity"].as_str().unwrap();
    invoke(
        &state,
        json!({"action":"claim","now":101,"route_identity":initial_route}),
    );
    let initial_sent = invoke(
        &state,
        json!({"action":"sent","now":102,"route_identity":initial_route}),
    );
    invoke(&state, ack_request(&initial_sent["dispatch"], 103));
    let progress = invoke(&state, json!({"action":"progress","now":150}));
    assert_eq!(progress["status"], "PROGRESSED");
    assert!(progress["dispatch"].is_null());

    let early = invoke(&state, json!({"action":"pump","now":209}));
    assert_eq!(early["status"], "NO_CHANGE");
    let due = invoke(&state, json!({"action":"pump","now":210}));
    assert_eq!(due["dispatch"]["target_role"], "IMPLEMENTER");

    let infrastructure = invoke(&state, json!({"action":"infrastructure_blocked","now":220}));
    assert_eq!(infrastructure["dispatch"]["target_role"], "INFRASTRUCTURE");
    assert_eq!(
        infrastructure["dispatch"]["target_agent_key"],
        "jimmy-wiring"
    );

    let recovered = invoke(
        &state,
        json!({"action":"infrastructure_resolved","now":230}),
    );
    assert_eq!(recovered["dispatch"]["target_role"], "IMPLEMENTER");
    assert_eq!(recovered["dispatch"]["target_agent_key"], "kenny-remh");

    let reviewer = invoke(&state, json!({"action":"review_ready","now":240}));
    assert_eq!(reviewer["dispatch"]["target_role"], "TECHNICAL_REVIEWER");
    let review_route = reviewer["dispatch"]["route_identity"].as_str().unwrap();
    invoke(
        &state,
        json!({"action":"claim","now":241,"route_identity":review_route}),
    );
    invoke(
        &state,
        json!({"action":"sent","now":242,"route_identity":review_route}),
    );

    let duplicate_review = invoke(&state, json!({"action":"pump","now":243}));
    assert!(duplicate_review["dispatch"].is_null());

    let complete = invoke(&state, json!({"action":"complete","now":250}));
    assert_eq!(complete["status"], "COMPLETE");
    assert!(complete["dispatch"].is_null());
    let terminal = invoke(&state, json!({"action":"pump","now":10_000}));
    assert_eq!(terminal["status"], "COMPLETE");
    assert!(terminal["dispatch"].is_null());

    let counters = invoke(&state, json!({"action":"counters"}));
    assert!(
        counters["counters"]["deterministic_evaluations"]
            .as_i64()
            .unwrap()
            >= 9
    );
    assert!(
        counters["counters"]["no_change_evaluations"]
            .as_i64()
            .unwrap()
            >= 2
    );
    assert_eq!(counters["counters"]["no_change_invocations"], 0);
    assert_eq!(counters["counters"]["duplicate_invocations"], 0);
    assert_eq!(counters["counters"]["reviewer_invocations"], 1);
    assert_eq!(counters["counters"]["owner_invocations"], 0);
}

#[test]
fn condition_report_gate_requires_runtime_readback_and_exact_manifest_contract() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");
    let held = invoke(
        &state,
        json!({"action":"condition_report_gate","project_id":"wrong","manifest_json":"{}","approved_checksum":"owner-says-ok","evidence_source":"OWNER_STATEMENT"}),
    );
    assert_eq!(held["status"], "HOLD");

    let mut manifest = r#"{"approved_uploads":[{"name":"condition-report-approved.pdf","mime_type":"application/pdf","checksum":"approved-pdf-checksum"}]}"#.to_string();
    manifest.push_str(&" ".repeat(272 - manifest.len()));
    assert_eq!(manifest.len(), 272);
    let checksum = format!("{:x}", Sha256::digest(manifest.as_bytes()));
    let ready = invoke(
        &state,
        json!({
            "action":"condition_report_gate",
            "project_id":"REM108_APPROVED_PROJECT",
            "manifest_json":manifest,
            "approved_checksum":checksum,
            "evidence_source":"RUNTIME_READBACK"
        }),
    );
    assert_eq!(ready["status"], "READY");
    assert_eq!(ready["approved_pdf_count"], 1);
    assert_eq!(ready["content_length"], 272);
}

#[test]
fn valid_blocked_result_stays_authoritative_and_suppresses_contradictory_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");
    invoke(&state, json!({"action":"arm","now":100}));
    let blocked = invoke(
        &state,
        json!({"action":"response_blocked","now":120,"event_id":"blocked-event-1"}),
    );
    assert_eq!(blocked["dispatch"]["target_role"], "INFRASTRUCTURE");
    assert_eq!(blocked["public_result"], "BLOCKER_ROUTED");
    assert_eq!(blocked["public_result_count"], 1);
    assert_eq!(blocked["generic_failure_allowed"], false);

    let fallback = invoke(
        &state,
        json!({"action":"response_fallback","now":121,"transport_failure":false}),
    );
    assert_eq!(fallback["status"], "FALLBACK_SUPPRESSED");
    assert_eq!(fallback["public_result_count"], 1);
    assert_eq!(fallback["current_stage"], "INFRA_BLOCKED");
    assert_eq!(fallback["generic_failure_allowed"], false);
}

#[test]
fn terminal_condition_report_reconciles_existing_kenny_activation_without_duplicate_wake() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");
    let request = json!({
        "action":"reconcile_condition_report_dependency",
        "now":700,
        "dependency_event_id":"504370148108d2758ce40c47c5a1f58ea97e16831ad4aa5b151dc6076f1fff30",
        "objective_event_id":"3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2",
        "idempotency_key":"remh-oi-condition-report-owner-test-ready→operational-findings-v1",
        "upstream_state":"OWNER_TEST_READY",
        "downstream_state":"IMPLEMENTING",
        "downstream_agent_key":"kenny-remh",
        "terminal_event_id":"ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
        "predecessor_event_id":"ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
        "start_receipt_event_id":"ead170f787ca0929a04f9686f46c55cc2499a6e45f404626b6e3d2561aac6339",
        "implementer_pubkey":"801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
        "activation_generation":1
    });

    let reconciled = invoke(&state, request.clone());
    assert_eq!(reconciled["status"], "RECONCILED");
    assert!(reconciled["dispatch"].is_null());
    assert_eq!(reconciled["wake_count"], 0);
    assert_eq!(reconciled["duplicate_wake_count"], 0);
    assert_eq!(reconciled["dependency"]["state"], "SATISFIED");
    assert_eq!(
        reconciled["dependency"]["source_event_id"],
        "504370148108d2758ce40c47c5a1f58ea97e16831ad4aa5b151dc6076f1fff30"
    );
    assert_eq!(
        reconciled["dependency"]["objective_event_id"],
        "3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2"
    );
    assert_eq!(reconciled["activation"]["state"], "ACCOUNTED");
    assert_eq!(reconciled["activation"]["target_agent_key"], "kenny-remh");
    assert_eq!(
        reconciled["activation"]["implementer_pubkey"],
        "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9"
    );
    assert_eq!(reconciled["activation"]["generation"], 1);
    assert_eq!(
        reconciled["activation"]["predecessor_state"],
        "IMPLEMENTING"
    );
    assert_eq!(reconciled["boundaries"]["attention"], "OFF");
    assert_eq!(reconciled["boundaries"]["model_polling"], false);
    assert_eq!(reconciled["boundaries"]["tcc_enrolled"], false);
    assert_eq!(reconciled["boundaries"]["live_business_enrollment"], false);

    let repeated = invoke(&state, request);
    assert_eq!(repeated["status"], "ALREADY_RECONCILED");
    assert!(repeated["dispatch"].is_null());
    assert_eq!(repeated["wake_count"], 0);
    assert_eq!(repeated["duplicate_wake_count"], 0);
    assert_eq!(
        repeated["dependency"]["objective_event_id"],
        "3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2"
    );

    let restarted = invoke(
        &state,
        json!({"action":"condition_report_dependency_status"}),
    );
    assert_eq!(restarted["status"], "RECONCILED");
    assert!(restarted["dispatch"].is_null());
    assert_eq!(restarted["wake_count"], 0);
    assert_eq!(restarted["duplicate_wake_count"], 0);
    assert_eq!(restarted["activation"]["state"], "ACCOUNTED");
    assert_eq!(
        restarted["dependency"]["objective_event_id"],
        "3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2"
    );

    let counters = invoke(&state, json!({"action":"counters"}));
    assert_eq!(counters["counters"]["routed_invocations"], 0);
    assert_eq!(counters["counters"]["duplicate_invocations"], 0);
}

#[test]
fn condition_report_reconciliation_rejects_unverified_receipt_assertions() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");
    let rejected = invoke_error(
        &state,
        json!({
            "action":"reconcile_condition_report_dependency",
            "now":700,
            "dependency_event_id":"504370148108d2758ce40c47c5a1f58ea97e16831ad4aa5b151dc6076f1fff30",
            "objective_event_id":"3655046804e51d1d7a8b5823bc32be8e12b91c5345b46c7b1f2fb91aba4067f2",
            "idempotency_key":"remh-oi-condition-report-owner-test-ready→operational-findings-v1",
            "upstream_state":"OWNER_TEST_READY",
            "downstream_state":"IMPLEMENTING",
            "downstream_agent_key":"kenny-remh",
            "terminal_event_id":"x",
            "predecessor_event_id":"x",
            "start_receipt_event_id":"y",
            "implementer_pubkey":"801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
            "activation_generation":1
        }),
    );
    assert!(rejected.contains("Exact Condition Report terminal event is required"));
    assert_eq!(
        invoke(
            &state,
            json!({"action":"condition_report_dependency_status"})
        )["status"],
        "UNRECONCILED"
    );
}

#[test]
fn owner_gate_emits_exact_packet_and_owner_action_resumes_implementer() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("pilot.sqlite3");
    invoke(&state, json!({"action":"arm","now":100}));
    let progressed = invoke(&state, json!({"action":"progress","now":110}));
    assert_eq!(progressed["status"], "PROGRESSED");
    assert_eq!(progressed["current_stage"], "IMPLEMENTING");

    let owner = invoke(&state, json!({"action":"owner_gate","now":120}));
    assert_eq!(owner["dispatch"]["target_role"], "OWNER");
    assert_eq!(
        owner["dispatch"]["target_pubkey"],
        "f22e6b13c6e235caa2162ebc40ead515ba096ab1eeccc2aea87a55edefad4541"
    );
    let message = owner["dispatch"]["wake_payload"].as_str().unwrap();
    for label in [
        "OBJECTIVE:",
        "CURRENT STAGE:",
        "BLOCKER:",
        "WHY AGENTS CANNOT RESOLVE IT:",
        "EXACT HUMAN ACTION REQUIRED:",
        "WHAT WILL RESUME AFTER ACTION:",
        "RISK OF NO ACTION:",
    ] {
        assert_eq!(message.matches(label).count(), 1);
    }

    let route = owner["dispatch"]["route_identity"].as_str().unwrap();
    invoke(
        &state,
        json!({"action":"claim","now":121,"route_identity":route}),
    );
    invoke(
        &state,
        json!({"action":"sent","now":122,"route_identity":route}),
    );
    let resumed = invoke(&state, json!({"action":"owner_action","now":130}));
    assert_eq!(resumed["dispatch"]["target_role"], "IMPLEMENTER");
}
