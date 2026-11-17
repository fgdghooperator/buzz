use super::continuation_contract::{
    AgentCreationEvidence, CapabilityRole, DisabledContinuationAdapter, GateEvidence,
    MaterialProof, ObjectiveSnapshot, ObjectiveState, ObserveOutcome, PlannedDispatch,
    RemediationRequest, RoleBinding, RoleRegistry, SandboxEffectSink,
};
use super::store::{
    ActivationReceipt, ActivationResolutionAuthorityInput, ActivationResolutionInput,
    ActivationResolutionOutcome, DispatchState,
};
use super::tests::{active, boundary_agent_records, boundary_routes, ready_activation};
use super::{activation_boundary::fence_binding, store::Store};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

const CHANNEL: &str = "remh-contract-test";
const CONTROL: &str = "remh-control-test";
const THREAD: &str = "contract-thread";

fn role_registry() -> RoleRegistry {
    let roles = [
        CapabilityRole::Implementer,
        CapabilityRole::Infrastructure,
        CapabilityRole::TechnicalReviewer,
        CapabilityRole::IndependentAuditor,
        CapabilityRole::Supervisor,
        CapabilityRole::Owner,
    ];
    RoleRegistry::new(
        1,
        roles
            .into_iter()
            .enumerate()
            .map(|(index, role)| RoleBinding {
                role,
                agent_key: format!("role-{index}"),
                pubkey: format!("{index:064x}"),
                display_name: format!("Role {index}"),
                agent_revision: 1,
                registry_revision: 1,
                allowed_channel_ids: vec![CHANNEL.to_string(), CONTROL.to_string()],
                enabled: true,
            })
            .collect(),
    )
    .unwrap()
}

fn snapshot(objective_id: &str, generation: i64, state: ObjectiveState) -> ObjectiveSnapshot {
    ObjectiveSnapshot {
        contract_version: 1,
        objective_id: objective_id.to_string(),
        objective_title: "contract test".to_string(),
        objective_channel_id: CHANNEL.to_string(),
        control_channel_id: CONTROL.to_string(),
        thread_root: THREAD.to_string(),
        state,
        generation,
        implementing_role: CapabilityRole::Implementer,
        current_route_role: None,
        prior_implementation_state: None,
        blocker: None,
        material_proof: MaterialProof {
            kind: "COMMIT".to_string(),
            reference: format!("commit:{generation}"),
            digest: Some(format!("sha256:{generation}")),
            occurred_at: generation * 100,
        },
        gate_evidence: vec![
            GateEvidence {
                kind: "REVIEW".to_string(),
                reference: "review:2".to_string(),
                digest: Some("sha256:2".to_string()),
            },
            GateEvidence {
                kind: "CHECK".to_string(),
                reference: "check:1".to_string(),
                digest: None,
            },
        ],
        last_verified_material_progress_at: generation * 100,
        next_review_at: Some(generation * 100 + 100),
        stale_generation: 0,
        route_registry_revision: 1,
        source_event_id: format!("source:{objective_id}:{generation}"),
        next_action: "continue bounded work".to_string(),
        owner_action_packet: None,
        created_at: 100,
        updated_at: generation * 100,
    }
}

#[derive(Default)]
struct SinkCounts {
    preflight: usize,
    admit: usize,
}

#[derive(Clone, Default)]
struct CountingSink {
    counts: Arc<Mutex<SinkCounts>>,
    fail_preflight: bool,
}

impl SandboxEffectSink for CountingSink {
    fn preflight(&mut self, _dispatch: &PlannedDispatch) -> super::store::Result<()> {
        self.counts.lock().unwrap().preflight += 1;
        if self.fail_preflight {
            return Err("synthetic uncertain effect".into());
        }
        Ok(())
    }

    fn admit(&mut self, _dispatch: &PlannedDispatch) -> super::store::Result<()> {
        self.counts.lock().unwrap().admit += 1;
        Ok(())
    }
}

#[test]
fn material_fingerprint_is_canonical_and_excludes_audit_deadline_fields() {
    let first = snapshot("REM-165-FP", 1, ObjectiveState::Implementing);
    let mut equivalent = first.clone();
    equivalent.gate_evidence.reverse();
    equivalent.updated_at = 9_999;
    equivalent.next_review_at = Some(50_000);

    assert_eq!(
        first.material_fingerprint().unwrap(),
        equivalent.material_fingerprint().unwrap()
    );
    let mut changed = first.clone();
    changed.next_action = "different semantic action".to_string();
    assert_ne!(
        first.material_fingerprint().unwrap(),
        changed.material_fingerprint().unwrap()
    );
}

#[test]
fn stale_generation_routes_once_across_restart_and_mutable_deadline_cannot_mint_another() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let accepted = snapshot("REM-165-STALL", 1, ObjectiveState::Implementing);
    let mut adapter =
        DisabledContinuationAdapter::open(path.clone(), role_registry(), sink).unwrap();

    assert!(matches!(
        adapter.observe(&accepted, 150).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    assert!(matches!(
        adapter.observe(&accepted, 199).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    assert!(matches!(
        adapter.observe(&accepted, 200).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    drop(adapter);

    let mut mutable = accepted.clone();
    mutable.next_review_at = Some(10_000);
    mutable.updated_at = 500;
    let mut restarted = DisabledContinuationAdapter::open(
        path,
        role_registry(),
        CountingSink {
            counts: counts.clone(),
            fail_preflight: false,
        },
    )
    .unwrap();
    assert!(matches!(
        restarted.observe(&mutable, 10_000).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit), (2, 2));
}

#[test]
fn future_evidence_is_rejected_before_any_effect() {
    let dir = tempfile::tempdir().unwrap();
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let mut future = snapshot("REM-165-FUTURE", 1, ObjectiveState::Active);
    future.material_proof.occurred_at = 500;
    let mut adapter = DisabledContinuationAdapter::open(
        dir.path().join("continuation.sqlite3"),
        role_registry(),
        sink,
    )
    .unwrap();
    assert!(adapter.observe(&future, 499).is_err());
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit), (0, 0));
}

#[test]
fn persisted_route_claim_quarantines_uncertain_effect_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let counts = Arc::new(Mutex::new(SinkCounts::default()));
    let objective = snapshot("REM-165-UNCERTAIN", 1, ObjectiveState::Active);
    let mut adapter = DisabledContinuationAdapter::open(
        path.clone(),
        role_registry(),
        CountingSink {
            counts: counts.clone(),
            fail_preflight: true,
        },
    )
    .unwrap();
    assert!(adapter.observe(&objective, 150).is_err());
    drop(adapter);

    let mut restarted = DisabledContinuationAdapter::open(
        path,
        role_registry(),
        CountingSink {
            counts: counts.clone(),
            fail_preflight: false,
        },
    )
    .unwrap();
    assert!(matches!(
        restarted.observe(&objective, 151).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit), (1, 0));
}

#[test]
fn duplicate_source_event_is_suppressed_after_later_generations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let mut first = snapshot("REM-165-DUPLICATE", 1, ObjectiveState::Active);
    first.source_event_id = "durable-source-event".to_string();
    let mut adapter = DisabledContinuationAdapter::open(path, role_registry(), sink).unwrap();
    assert!(matches!(
        adapter.observe(&first, 150).unwrap(),
        ObserveOutcome::Planned(_)
    ));
    let second = snapshot("REM-165-DUPLICATE", 2, ObjectiveState::Implementing);
    assert!(matches!(
        adapter.observe(&second, 250).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let mut replay = snapshot("REM-165-DUPLICATE", 3, ObjectiveState::ReviewReady);
    replay.source_event_id = "durable-source-event".to_string();
    assert!(matches!(
        adapter.observe(&replay, 350).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit), (1, 1));
}

#[test]
fn registry_revision_cannot_be_reused_for_different_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let objective = snapshot("REM-165-REGISTRY", 1, ObjectiveState::Active);
    let mut adapter =
        DisabledContinuationAdapter::open(path.clone(), role_registry(), CountingSink::default())
            .unwrap();
    adapter.observe(&objective, 150).unwrap();
    drop(adapter);

    let mut changed = role_registry();
    changed.replace_binding(RoleBinding {
        role: CapabilityRole::Implementer,
        agent_key: "changed-without-revision".to_string(),
        pubkey: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
        display_name: "Changed without revision".to_string(),
        agent_revision: 2,
        registry_revision: 1,
        allowed_channel_ids: vec![CHANNEL.to_string(), CONTROL.to_string()],
        enabled: true,
    });
    let mut restarted =
        DisabledContinuationAdapter::open(path, changed, CountingSink::default()).unwrap();
    assert!(restarted.observe(&objective, 151).is_err());
}

#[test]
fn completion_requires_evidence_and_terminal_state_cannot_transition() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("continuation.sqlite3");
    let mut adapter =
        DisabledContinuationAdapter::open(path, role_registry(), CountingSink::default()).unwrap();
    let implementing = snapshot("REM-165-TRANSITIONS", 1, ObjectiveState::Implementing);
    adapter.observe(&implementing, 150).unwrap();
    let mut incomplete = snapshot("REM-165-TRANSITIONS", 2, ObjectiveState::ObjectiveComplete);
    incomplete.next_review_at = None;
    assert!(adapter.observe(&incomplete, 250).is_err());

    incomplete.gate_evidence.push(GateEvidence {
        kind: "COMPLETION_EVIDENCE".to_string(),
        reference: "sandbox:complete".to_string(),
        digest: Some("sha256:complete".to_string()),
    });
    assert!(matches!(
        adapter.observe(&incomplete, 250).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let resumed = snapshot("REM-165-TRANSITIONS", 3, ObjectiveState::Implementing);
    assert!(adapter.observe(&resumed, 350).is_err());
}

#[test]
fn owner_pause_and_completion_are_dormant_and_call_no_sink() {
    let dir = tempfile::tempdir().unwrap();
    let sink = CountingSink::default();
    let counts = sink.counts.clone();
    let mut adapter = DisabledContinuationAdapter::open(
        dir.path().join("continuation.sqlite3"),
        role_registry(),
        sink,
    )
    .unwrap();
    let mut paused = snapshot("REM-165-PAUSED", 1, ObjectiveState::OwnerPaused);
    paused.next_review_at = None;
    let mut complete = snapshot("REM-165-COMPLETE", 1, ObjectiveState::ObjectiveComplete);
    complete.next_review_at = None;
    complete.gate_evidence.push(GateEvidence {
        kind: "COMPLETION_EVIDENCE".to_string(),
        reference: "sandbox:complete".to_string(),
        digest: Some("sha256:complete".to_string()),
    });
    assert!(matches!(
        adapter.observe(&paused, 150).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    assert!(matches!(
        adapter.observe(&complete, 150).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    let counts = counts.lock().unwrap();
    assert_eq!((counts.preflight, counts.admit), (0, 0));
}

#[test]
fn autonomous_remediation_and_agent_creation_policies_fail_closed() {
    let admissible = RemediationRequest {
        non_destructive: true,
        reversible: true,
        inside_objective: true,
        credentials_available: true,
        creates_external_commitment: false,
        weakens_controls: false,
        production_approval_required: false,
        bounded_evidence: true,
    };
    assert!(admissible.autonomously_admissible());
    let mut blocked = admissible.clone();
    blocked.production_approval_required = true;
    assert!(!blocked.autonomously_admissible());

    let creation = AgentCreationEvidence {
        existing_identity_keys: HashSet::new(),
        proposed_identity_key: "new-supervisor".to_string(),
        approved_template: Some("approved-supervisor-template".to_string()),
        bot_participant_only: true,
        durable_channel_owner: false,
    };
    assert!(creation.validate().is_ok());
    let mut duplicate = creation.clone();
    duplicate
        .existing_identity_keys
        .insert("new-supervisor".to_string());
    assert!(duplicate.validate().is_err());
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

    assert!(store
        .resolve_uncertain_activation(
            ActivationResolutionInput {
                dispatch_id: quarantined.id,
                actor: "matt",
                authority_role: "OWNER",
                admission_reference: "owner:rem-165:abandon-1",
                decided_at: 601,
                reason: "a role string alone must not authorize permanent abandonment",
                outcome: ActivationResolutionOutcome::AbandonedNoReplay,
                receipt: None,
            },
            601,
        )
        .is_err());
    store
        .admit_activation_resolution_authority(ActivationResolutionAuthorityInput {
            dispatch_id: quarantined.id,
            actor: "matt",
            authority_role: "OWNER",
            admission_reference: "owner:rem-165:wrong-outcome",
            outcome: ActivationResolutionOutcome::ConfirmedSent,
            admitted_at: 601,
        })
        .unwrap();
    assert!(store
        .resolve_uncertain_activation(
            ActivationResolutionInput {
                dispatch_id: quarantined.id,
                actor: "matt",
                authority_role: "OWNER",
                admission_reference: "owner:rem-165:wrong-outcome",
                decided_at: 601,
                reason: "authority for another outcome must not authorize abandonment",
                outcome: ActivationResolutionOutcome::AbandonedNoReplay,
                receipt: None,
            },
            601,
        )
        .is_err());
    store
        .admit_activation_resolution_authority(ActivationResolutionAuthorityInput {
            dispatch_id: quarantined.id,
            actor: "matt",
            authority_role: "OWNER",
            admission_reference: "owner:rem-165:abandon-1",
            outcome: ActivationResolutionOutcome::AbandonedNoReplay,
            admitted_at: 601,
        })
        .unwrap();

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
    store
        .admit_activation_resolution_authority(ActivationResolutionAuthorityInput {
            dispatch_id: quarantined.id,
            actor: "supervisor",
            authority_role: "SUPERVISOR",
            admission_reference: "supervisor:rem-165:confirmed-1",
            outcome: ActivationResolutionOutcome::ConfirmedSent,
            admitted_at: 601,
        })
        .unwrap();

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

    let _ = active("REMH-DISABLED", "implementer", 100, 200);
    let _ = boundary_agent_records();
    let _ = boundary_routes();
}
