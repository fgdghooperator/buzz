use super::continuation_contract::{
    DisabledContinuationAdapter, GateEvidence, Lifecycle, MaterialProof, ObjectiveSnapshot,
    ObserveOutcome, RouteReason, RouteTarget, SandboxEffectSink, Stage,
};
use super::store::{
    ActivationReceipt, ActivationResolutionInput, ActivationResolutionOutcome, DispatchState,
};
use super::tests::{active, boundary_agent_records, boundary_routes, ready_activation};
use super::{activation_boundary::fence_binding, store::Store};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct SinkCounts {
    preflight: usize,
    admit: usize,
    status: usize,
}

#[derive(Clone, Default)]
struct CountingSink {
    counts: Arc<Mutex<SinkCounts>>,
}

impl SandboxEffectSink for CountingSink {
    fn preflight(
        &mut self,
        _dispatch: &super::continuation_contract::PlannedDispatch,
    ) -> super::store::Result<()> {
        self.counts.lock().unwrap().preflight += 1;
        Ok(())
    }

    fn admit(
        &mut self,
        _dispatch: &super::continuation_contract::PlannedDispatch,
    ) -> super::store::Result<()> {
        self.counts.lock().unwrap().admit += 1;
        Ok(())
    }

    fn status(
        &mut self,
        _dispatch: &super::continuation_contract::PlannedDispatch,
    ) -> super::store::Result<()> {
        self.counts.lock().unwrap().status += 1;
        Ok(())
    }
}

fn snapshot<'a>(
    objective_id: &'a str,
    generation: i64,
    stage: Stage,
    updated_at: i64,
) -> ObjectiveSnapshot<'a> {
    ObjectiveSnapshot {
        contract_version: 1,
        objective_id,
        generation,
        lifecycle: Lifecycle::Active,
        stage,
        stage_entered_at: 100,
        material_proof: MaterialProof {
            kind: "COMMIT",
            reference: "commit:abc",
            digest: Some("sha256:abc"),
            occurred_at: 100,
        },
        last_verified_material_progress_at: 100,
        next_review_at: 200,
        next_action: "continue bounded implementation",
        gate_evidence: vec![
            GateEvidence {
                kind: "REVIEW",
                reference: "review:2",
                digest: Some("sha256:2"),
            },
            GateEvidence {
                kind: "BLOCKER",
                reference: "blocker:1",
                digest: None,
            },
        ],
        updated_at,
    }
}

#[test]
fn material_fingerprint_is_canonical_versioned_and_excludes_audit_only_fields() {
    let first = snapshot("REM-165", 7, Stage::Implementing, 1_000);
    let mut equivalent = snapshot("REM-165", 7, Stage::Implementing, 9_000);
    equivalent.gate_evidence.reverse();

    assert_eq!(
        first.material_fingerprint().unwrap(),
        equivalent.material_fingerprint().unwrap()
    );
    assert_ne!(
        first.material_fingerprint().unwrap(),
        snapshot("REM-165", 8, Stage::Implementing, 1_000)
            .material_fingerprint()
            .unwrap()
    );
}

#[test]
fn routing_matrix_selects_one_exact_target_and_completion_is_silent() {
    let cases = [
        (
            Stage::Implementing,
            RouteTarget::Kenny,
            RouteReason::ContinueImplementation,
        ),
        (
            Stage::SelfTest,
            RouteTarget::Kenny,
            RouteReason::ContinueSelfTest,
        ),
        (
            Stage::ReviewReadyGrok,
            RouteTarget::Grok,
            RouteReason::ReviewReady,
        ),
        (
            Stage::CorrectionRequired,
            RouteTarget::Kenny,
            RouteReason::CorrectionRequired,
        ),
        (
            Stage::FinalAuditReadyClaude,
            RouteTarget::Claude,
            RouteReason::FinalAuditReady,
        ),
        (
            Stage::OwnerTestReady,
            RouteTarget::Matt,
            RouteReason::OwnerTestReady,
        ),
        (
            Stage::InfraBlockedJimmy,
            RouteTarget::Jimmy,
            RouteReason::InfrastructureBlocked,
        ),
        (
            Stage::AwaitingOwner,
            RouteTarget::Matt,
            RouteReason::AwaitingOwner,
        ),
    ];
    for (stage, target, reason) in cases {
        let route = stage.route().expect("nonterminal stage must route");
        assert_eq!(route.target, target);
        assert_eq!(route.reason, reason);
    }
    assert!(Stage::ObjectiveComplete.route().is_none());
}

#[test]
fn unchanged_snapshot_is_silent_before_any_sink_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let mut adapter = DisabledContinuationAdapter::open(path, sink).unwrap();
    let observed = snapshot("REM-165", 1, Stage::Implementing, 100);

    assert!(matches!(
        adapter.observe(&observed, 150).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    assert!(matches!(
        adapter
            .observe(&snapshot("REM-165", 1, Stage::Implementing, 999), 151)
            .unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!(counts.preflight, 1);
    assert_eq!(counts.admit, 1);
    assert_eq!(counts.status, 0);
}

#[test]
fn completion_and_paused_lifecycle_never_call_the_sink() {
    let dir = tempfile::tempdir().unwrap();
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let mut adapter =
        DisabledContinuationAdapter::open(dir.path().join("continuation.sqlite3"), sink).unwrap();
    let mut complete = snapshot("REM-165-COMPLETE", 1, Stage::ObjectiveComplete, 100);
    complete.lifecycle = Lifecycle::ObjectiveComplete;
    let mut paused = snapshot("REM-165-PAUSED", 1, Stage::Implementing, 100);
    paused.lifecycle = Lifecycle::OwnerPaused;

    assert!(matches!(
        adapter.observe(&complete, 500).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    assert!(matches!(
        adapter.observe(&paused, 500).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit, counts.status), (0, 0, 0));
}

#[test]
fn stall_recovery_is_one_shot_across_restart_and_new_progress_advances_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let first = snapshot("REM-165-STALL", 1, Stage::SelfTest, 100);
    let mut adapter = DisabledContinuationAdapter::open(path.clone(), sink).unwrap();
    assert!(matches!(
        adapter.observe(&first, 150).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    assert!(matches!(
        adapter.observe(&first, 199).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let recovery = match adapter.observe(&first, 200).unwrap() {
        ObserveOutcome::Planned(dispatch) => dispatch,
        other => panic!("expected one recovery dispatch, got {other:?}"),
    };
    assert!(recovery.recovery_identity.is_some());
    drop(adapter);

    let mut restarted = DisabledContinuationAdapter::open(
        path,
        CountingSink {
            counts: counts.clone(),
        },
    )
    .unwrap();
    assert!(matches!(
        restarted.observe(&first, 500).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let second = snapshot("REM-165-STALL", 2, Stage::CorrectionRequired, 600);
    assert!(matches!(
        restarted.observe(&second, 601).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!(counts.preflight, 3);
    assert_eq!(counts.admit, 3);
}

fn exact_receipt(dispatch: &super::store::Dispatch, admitted_at: i64) -> ActivationReceipt {
    ActivationReceipt::from_dispatch(dispatch, admitted_at).unwrap()
}

#[test]
fn activation_sent_is_receipt_bound_and_future_receipts_remain_quarantined() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path).unwrap();
    let idempotency_key = format!("remh-oi:{}:receipt-bound", "a".repeat(64));
    let spent = ready_activation(
        &mut store,
        "REMH-RECEIPT-BOUND",
        "holder",
        &idempotency_key,
        301,
        500,
        302,
    );
    let binding = fence_binding(&spent).unwrap();
    store
        .consume_activation_fence(&binding, 303)
        .unwrap()
        .unwrap();
    let spent = store.dispatch(spent.id).unwrap().unwrap();

    assert!(store.record_activation_sent(spent.id, None, 304).is_err());
    let mut mismatch = exact_receipt(&spent, 303);
    mismatch.attempt += 1;
    assert!(store
        .record_activation_sent(spent.id, Some(&mismatch), 304)
        .is_err());
    let future = exact_receipt(&spent, 305);
    assert!(store
        .record_activation_sent(spent.id, Some(&future), 304)
        .is_err());
    assert_eq!(
        store.dispatch(spent.id).unwrap().unwrap().state,
        DispatchState::WakeIntent
    );

    let exact = exact_receipt(&spent, 304);
    assert_eq!(
        store
            .record_activation_sent(spent.id, Some(&exact), 304)
            .unwrap()
            .state,
        DispatchState::Sent
    );
}

#[test]
fn authorised_abandonment_is_auditable_non_replayable_and_does_not_advance_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path.clone()).unwrap();
    let idempotency_key = format!("remh-oi:{}:abandon-key", "b".repeat(64));
    let frozen = ready_activation(
        &mut store,
        "REMH-ABANDON",
        "holder",
        &idempotency_key,
        301,
        500,
        302,
    );
    let binding = fence_binding(&frozen).unwrap();
    store
        .consume_activation_fence(&binding, 303)
        .unwrap()
        .unwrap();
    store
        .reconcile_activation_startup("relay", "1", &[], 600, 500)
        .unwrap();
    let quarantined = store.dispatch(frozen.id).unwrap().unwrap();

    let resolved = store
        .resolve_uncertain_activation(
            ActivationResolutionInput {
                dispatch_id: quarantined.id,
                actor: "matt",
                authority_role: "OWNER",
                admission_reference: "owner:rem-165:abandon-1",
                decided_at: 601,
                reason: "sandbox evidence cannot establish whether admission occurred",
                outcome: ActivationResolutionOutcome::AbandonedNoReplay,
                receipt: None,
            },
            601,
        )
        .unwrap();
    assert_eq!(resolved.state, DispatchState::VerifyBeforeRetry);
    assert!(resolved
        .recovery_state
        .as_deref()
        .is_some_and(|value| value.starts_with("ABANDONED_NO_REPLAY:")));
    assert_eq!(
        store.objective("REMH-ABANDON").unwrap().unwrap().generation,
        1
    );
    assert_eq!(
        store
            .activation_resolution_audits(quarantined.id)
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .resolve_uncertain_activation(
            ActivationResolutionInput {
                dispatch_id: quarantined.id,
                actor: "matt",
                authority_role: "OWNER",
                admission_reference: "owner:rem-165:abandon-replay",
                decided_at: 602,
                reason: "replay must fail",
                outcome: ActivationResolutionOutcome::AbandonedNoReplay,
                receipt: None,
            },
            602
        )
        .is_err());

    store
        .record_verified_progress(super::tests::proof("REMH-ABANDON", 700, 800))
        .unwrap();
    assert_eq!(
        store.objective("REMH-ABANDON").unwrap().unwrap().generation,
        2
    );
    assert!(!store
        .has_unresolved_prior_activation("REMH-ABANDON", 2)
        .unwrap());
}

#[test]
fn confirmed_sent_resolution_still_requires_the_exact_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control-plane.sqlite3");
    let mut store = Store::open(path).unwrap();
    let idempotency_key = format!("remh-oi:{}:confirmed-key", "c".repeat(64));
    let frozen = ready_activation(
        &mut store,
        "REMH-CONFIRMED",
        "holder",
        &idempotency_key,
        301,
        500,
        302,
    );
    let binding = fence_binding(&frozen).unwrap();
    store
        .consume_activation_fence(&binding, 303)
        .unwrap()
        .unwrap();
    store
        .reconcile_activation_startup("relay", "1", &[], 600, 500)
        .unwrap();
    let quarantined = store.dispatch(frozen.id).unwrap().unwrap();
    let receipt = exact_receipt(&quarantined, 303);

    let resolved = store
        .resolve_uncertain_activation(
            ActivationResolutionInput {
                dispatch_id: quarantined.id,
                actor: "supervisor",
                authority_role: "SUPERVISOR",
                admission_reference: "supervisor:rem-165:confirmed-1",
                decided_at: 601,
                reason: "sandbox receipt confirms exact admission",
                outcome: ActivationResolutionOutcome::ConfirmedSent,
                receipt: Some(&receipt),
            },
            601,
        )
        .unwrap();
    assert_eq!(resolved.state, DispatchState::Sent);
    assert_eq!(resolved.sent_at, Some(303));
    assert_eq!(
        store
            .activation_resolution_audits(quarantined.id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn disabled_contract_has_no_production_registration_or_real_effect_surface() {
    let module = include_str!("continuation_contract.rs");
    let native_lib = include_str!("../lib.rs");
    let build = include_str!("../../build.rs");
    for forbidden in ["tauri::command", "ctx.agents.wake", "tool_call", "provider"] {
        assert!(
            !module.contains(forbidden),
            "found forbidden production surface: {forbidden}"
        );
    }
    assert!(!native_lib.contains("continuation_contract"));
    assert!(!build.contains("continuation_contract"));

    let _ = active("REMH-DISABLED", "kenny", 100, 200);
    let _ = boundary_agent_records();
    let _ = boundary_routes();
}
