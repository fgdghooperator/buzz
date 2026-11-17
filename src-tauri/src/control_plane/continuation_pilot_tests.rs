use super::continuation_contract::{
    Blocker, BlockerClass, BlockerClassifierInput, BlockerStatus, CapabilityRole, ControlEventType,
    DisabledContinuationAdapter, GateEvidence, MaterialProof, ObjectiveSnapshot, ObjectiveState,
    ObserveOutcome, OwnerActionPacket, PlannedDispatch, RoleBinding, RoleRegistry,
    SandboxEffectSink,
};
use super::store::Result;
use std::sync::{Arc, Mutex};

const PILOT_CHANNEL_ID: &str = "remh-trey-pilot-sandbox";
const CONTROL_CHANNEL_ID: &str = "2dcedaf8-3cb6-4ab3-ad6e-389248616972";
const THREAD_ROOT: &str = "513d637a6ed0e4782b091ecd47cd6476db42df379f6e81f82bcadbd650b0f5c1";
const PILOT_OBJECTIVE: &str = "REM-165-TREY-PILOT-001";

fn binding(role: CapabilityRole, agent_key: &str, display_name: &str, pubkey: &str) -> RoleBinding {
    RoleBinding {
        role,
        agent_key: agent_key.to_string(),
        pubkey: pubkey.to_string(),
        display_name: display_name.to_string(),
        agent_revision: 1,
        registry_revision: 7,
        allowed_channel_ids: vec![PILOT_CHANNEL_ID.to_string(), CONTROL_CHANNEL_ID.to_string()],
        enabled: true,
    }
}

fn registry() -> RoleRegistry {
    RoleRegistry::new(
        7,
        vec![
            binding(
                CapabilityRole::Implementer,
                "remh-implementer",
                "Kenny | REMH - RE Management Holdings",
                "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
            ),
            binding(
                CapabilityRole::Infrastructure,
                "fgd-infrastructure",
                "Jimmy | Wiring",
                "03f0cc2c1b1c292fec9d1813954b2b4986ff5d0f7a6d1fa2cd4e3f3188a05ab6",
            ),
            binding(
                CapabilityRole::TechnicalReviewer,
                "remh-technical-reviewer",
                "REMH Grok Build Specialist",
                "149a9f86ef67dad94202432aa889fd8075f82d6f2282083d1455e659c9f29fd5",
            ),
            binding(
                CapabilityRole::IndependentAuditor,
                "remh-independent-auditor",
                "Claude | REMH Audit",
                "116cfc840c6ee3a95cdb4bc91d525ec45dc6a37bc2987b0f6c3e18d04d0bce86",
            ),
            RoleBinding {
                role: CapabilityRole::Supervisor,
                agent_key: "trey-supervisor-unresolved".to_string(),
                pubkey: String::new(),
                display_name: "Trey".to_string(),
                agent_revision: 0,
                registry_revision: 7,
                allowed_channel_ids: vec![
                    PILOT_CHANNEL_ID.to_string(),
                    CONTROL_CHANNEL_ID.to_string(),
                ],
                enabled: false,
            },
            binding(
                CapabilityRole::Owner,
                "fgd-owner",
                "Matt",
                "f22e6b13c6e235caa2162ebc40ead515ba096ab1eeccc2aea87a55edefad4541",
            ),
        ],
    )
    .unwrap()
}

fn proof(generation: i64, label: &str) -> MaterialProof {
    MaterialProof {
        kind: "SANDBOX_CHECKPOINT".to_string(),
        reference: format!("sandbox:{label}"),
        digest: Some(format!("sha256:{label}")),
        occurred_at: generation * 100,
    }
}

fn snapshot(generation: i64, state: ObjectiveState, label: &str) -> ObjectiveSnapshot {
    ObjectiveSnapshot {
        contract_version: 1,
        objective_id: PILOT_OBJECTIVE.to_string(),
        objective_title: "deterministic continuation routing acceptance".to_string(),
        objective_channel_id: PILOT_CHANNEL_ID.to_string(),
        control_channel_id: CONTROL_CHANNEL_ID.to_string(),
        thread_root: THREAD_ROOT.to_string(),
        state,
        generation,
        implementing_role: CapabilityRole::Implementer,
        current_route_role: None,
        prior_implementation_state: None,
        blocker: None,
        material_proof: proof(generation, label),
        gate_evidence: vec![GateEvidence {
            kind: "PILOT_ISOLATION".to_string(),
            reference: "fake-effect-path".to_string(),
            digest: Some("sha256:no-live-send".to_string()),
        }],
        last_verified_material_progress_at: generation * 100,
        next_review_at: Some(generation * 100 + 50),
        stale_generation: 0,
        route_registry_revision: 7,
        source_event_id: format!("pilot-source-{generation}-{label}"),
        next_action: "sandbox effect only; no Buzz send".to_string(),
        owner_action_packet: None,
        created_at: 100,
        updated_at: generation * 100,
    }
}

fn blocker(generation: i64, class: BlockerClass, source_code: &str, summary: &str) -> Blocker {
    Blocker {
        blocker_id: format!("blocker-{generation}-{source_code}"),
        class,
        source_code: source_code.to_string(),
        summary: summary.to_string(),
        exact_required_action: "apply the bounded correction".to_string(),
        evidence_digest: format!("sha256:blocker-{generation}"),
        opened_generation: generation,
        status: BlockerStatus::Open,
        resolved_proof: None,
    }
}

#[derive(Default)]
struct FakeEffectState {
    preflight_count: usize,
    model_calls: usize,
    dispatches: Vec<PlannedDispatch>,
}

#[derive(Clone, Default)]
struct FakeEffectSink {
    state: Arc<Mutex<FakeEffectState>>,
}

impl SandboxEffectSink for FakeEffectSink {
    fn preflight(&mut self, dispatch: &PlannedDispatch) -> Result<()> {
        assert_eq!(dispatch.objective_id, PILOT_OBJECTIVE);
        assert_eq!(dispatch.channel_id, PILOT_CHANNEL_ID);
        assert_eq!(dispatch.thread_root, THREAD_ROOT);
        self.state.lock().unwrap().preflight_count += 1;
        Ok(())
    }

    fn admit(&mut self, dispatch: &PlannedDispatch) -> Result<()> {
        self.state.lock().unwrap().dispatches.push(dispatch.clone());
        Ok(())
    }
}

fn reopen(
    path: &std::path::Path,
    sink: &FakeEffectSink,
) -> DisabledContinuationAdapter<FakeEffectSink> {
    DisabledContinuationAdapter::open(path.to_path_buf(), registry(), sink.clone()).unwrap()
}

#[test]
fn controlled_pilot_proves_all_twelve_required_transitions_without_live_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trey-pilot.sqlite3");
    let sink = FakeEffectSink::default();

    // 1-2: objective creation and one implementer route.
    let created = snapshot(1, ObjectiveState::Active, "objective-created");
    let mut adapter = reopen(&path, &sink);
    let first = adapter.observe(&created, 101).unwrap();
    assert!(matches!(first, ObserveOutcome::Planned(_)));
    assert_eq!(
        adapter
            .control_event_count(ControlEventType::ObjectiveCreated)
            .unwrap(),
        1
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&created, 102).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 3: identical state causes no sink or model call.
    let before = sink.state.lock().unwrap().preflight_count;
    let mut audit_refresh = created.clone();
    audit_refresh.updated_at = 999;
    assert!(matches!(
        adapter.observe(&audit_refresh, 103).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    assert_eq!(sink.state.lock().unwrap().preflight_count, before);
    assert_eq!(sink.state.lock().unwrap().model_calls, 0);

    // 4: real material progress before deadline is accepted without supervisor interference.
    let progress = snapshot(2, ObjectiveState::Implementing, "material-progress");
    assert!(matches!(
        adapter.observe(&progress, 201).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    assert_eq!(sink.state.lock().unwrap().preflight_count, before);
    drop(adapter);

    // 5: engineering blocker routes to IMPLEMENTER in CORRECTION_REQUIRED.
    let mut engineering = snapshot(3, ObjectiveState::CorrectionRequired, "engineering-blocker");
    engineering.blocker = Some(blocker(
        3,
        BlockerClass::Engineering,
        "ENGINEERING",
        "deterministic implementation defect",
    ));
    let mut adapter = reopen(&path, &sink);
    let routed = adapter.observe(&engineering, 301).unwrap();
    let engineering_dispatch = routed.planned().unwrap();
    assert_eq!(
        engineering_dispatch.target_role,
        CapabilityRole::Implementer
    );
    assert_eq!(
        engineering_dispatch.state,
        ObjectiveState::CorrectionRequired
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&engineering, 302).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 6: infrastructure alias routes exactly once to INFRASTRUCTURE.
    let mut infrastructure = snapshot(4, ObjectiveState::InfraBlocked, "infrastructure-blocker");
    infrastructure.blocker = Some(blocker(
        4,
        BlockerClass::Infrastructure,
        "MCP",
        "test MCP route unavailable",
    ));
    let infrastructure_dispatch = adapter
        .observe(&infrastructure, 401)
        .unwrap()
        .planned()
        .unwrap();
    assert_eq!(
        infrastructure_dispatch.target_role,
        CapabilityRole::Infrastructure
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&infrastructure, 402).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 7: recovery proof restores IMPLEMENTING and routes once to IMPLEMENTER.
    let mut recovered = snapshot(5, ObjectiveState::Implementing, "infrastructure-recovered");
    let mut resolved = infrastructure.blocker.clone().unwrap();
    resolved.status = BlockerStatus::Resolved;
    resolved.resolved_proof = Some("sha256:mcp-recovered".to_string());
    recovered.blocker = Some(resolved);
    recovered.prior_implementation_state = Some(ObjectiveState::Implementing);
    let recovered_dispatch = adapter.observe(&recovered, 501).unwrap().planned().unwrap();
    assert_eq!(recovered_dispatch.target_role, CapabilityRole::Implementer);
    assert_eq!(
        adapter
            .control_event_count(ControlEventType::BlockerResolved)
            .unwrap(),
        1
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&recovered, 502).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 8: review gate routes one reviewer once.
    let review = snapshot(6, ObjectiveState::ReviewReady, "review-ready");
    let review_dispatch = adapter.observe(&review, 601).unwrap().planned().unwrap();
    assert_eq!(
        review_dispatch.target_role,
        CapabilityRole::TechnicalReviewer
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&review, 602).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 9: owner gate produces the exact seven-field packet, but the sink remains fake.
    let mut owner_gate = snapshot(7, ObjectiveState::AwaitingOwner, "owner-gate");
    owner_gate.blocker = Some(blocker(
        7,
        BlockerClass::Credentials,
        "MFA",
        "interactive MFA is required",
    ));
    owner_gate.owner_action_packet = Some(OwnerActionPacket {
        objective: "REM-165-TREY-PILOT-001 — deterministic continuation routing acceptance"
            .to_string(),
        current_stage: "AWAITING_OWNER".to_string(),
        blocker: "CREDENTIALS — interactive MFA is required".to_string(),
        why_agents_cannot_resolve_it: "Agents cannot enter the owner's MFA code.".to_string(),
        exact_human_action_required: "Complete the interactive MFA prompt once.".to_string(),
        what_will_resume_after_action: "IMPLEMENTING under IMPLEMENTER.".to_string(),
        risk_of_no_action: "The isolated pilot remains paused; no production state changes."
            .to_string(),
    });
    let owner_dispatch = adapter
        .observe(&owner_gate, 701)
        .unwrap()
        .planned()
        .unwrap();
    assert_eq!(owner_dispatch.target_role, CapabilityRole::Owner);
    let rendered = owner_dispatch.owner_action_message.unwrap();
    assert_eq!(rendered.lines().count(), 8);
    for label in [
        "OBJECTIVE:",
        "CURRENT STAGE:",
        "BLOCKER:",
        "WHY AGENTS CANNOT RESOLVE IT:",
        "EXACT HUMAN ACTION REQUIRED:",
        "WHAT WILL RESUME AFTER ACTION:",
        "RISK OF NO ACTION:",
    ] {
        assert_eq!(
            rendered.matches(label).count(),
            1,
            "missing/duplicate {label}"
        );
    }
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&owner_gate, 702).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 10: authorised owner evidence resumes implementation once.
    let mut resumed = snapshot(8, ObjectiveState::Implementing, "owner-action-received");
    resumed.gate_evidence.push(GateEvidence {
        kind: "OWNER_ACTION_RECEIVED".to_string(),
        reference: "owner-action:mfa-complete".to_string(),
        digest: Some("sha256:owner-action".to_string()),
    });
    let resume_dispatch = adapter.observe(&resumed, 801).unwrap().planned().unwrap();
    assert_eq!(resume_dispatch.target_role, CapabilityRole::Implementer);
    assert_eq!(
        adapter
            .control_event_count(ControlEventType::OwnerActionReceived)
            .unwrap(),
        1
    );
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    assert!(matches!(
        adapter.observe(&resumed, 802).unwrap(),
        ObserveOutcome::Silent(_)
    ));

    // 11: completion and every later evaluation are silent.
    let mut complete = snapshot(9, ObjectiveState::ObjectiveComplete, "complete");
    complete.next_review_at = None;
    complete.gate_evidence.push(GateEvidence {
        kind: "COMPLETION_EVIDENCE".to_string(),
        reference: "sandbox:complete".to_string(),
        digest: Some("sha256:complete".to_string()),
    });
    assert!(matches!(
        adapter.observe(&complete, 901).unwrap(),
        ObserveOutcome::Silent(_)
    ));
    drop(adapter);
    let mut adapter = reopen(&path, &sink);
    for now in [902, 1_000, 10_000] {
        assert!(matches!(
            adapter.observe(&complete, now).unwrap(),
            ObserveOutcome::Silent(_)
        ));
    }

    // 12: each action phase was reopened, and no route/control event duplicated.
    let state = sink.state.lock().unwrap();
    assert_eq!(state.dispatches.len(), 7);
    assert_eq!(state.preflight_count, 7);
    assert_eq!(state.model_calls, 0);
    drop(state);
    assert_eq!(adapter.dispatch_count().unwrap(), 7);
    assert_eq!(
        adapter
            .control_event_count(ControlEventType::ObjectiveCreated)
            .unwrap(),
        1
    );
    assert_eq!(
        adapter
            .control_event_count(ControlEventType::ObjectiveComplete)
            .unwrap(),
        1
    );
}

#[test]
fn role_registry_is_replaceable_without_state_machine_changes_and_fails_closed() {
    let mut replacement = registry();
    replacement.replace_binding(binding(
        CapabilityRole::Implementer,
        "replacement-implementer",
        "Replacement Implementer",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ));
    assert_eq!(
        replacement
            .resolve(CapabilityRole::Implementer, PILOT_CHANNEL_ID)
            .unwrap()
            .agent_key,
        "replacement-implementer"
    );
    assert!(replacement
        .resolve(CapabilityRole::Supervisor, PILOT_CHANNEL_ID)
        .is_err());

    let mut malformed = binding(CapabilityRole::Infrastructure, "bad", "Bad", "not-a-pubkey");
    malformed.enabled = true;
    assert!(RoleRegistry::new(7, vec![malformed]).is_err());
}

#[test]
fn blocker_classifier_normalizes_aliases_and_splits_access_and_security() {
    for alias in ["HOST", "SSH", "SCHEDULER", "BUZZ", "ACP", "MCP"] {
        let route = BlockerClassifierInput {
            class: BlockerClass::Infrastructure,
            source_code: alias.to_string(),
            delegated_repair: Some(true),
            final_audit: Some(false),
        }
        .classify()
        .unwrap();
        assert_eq!(route.target_role, CapabilityRole::Infrastructure);
        assert_eq!(route.state, ObjectiveState::InfraBlocked);
        assert_eq!(route.source_code, alias);
    }

    assert_eq!(
        BlockerClassifierInput {
            class: BlockerClass::Access,
            source_code: "ACCESS".to_string(),
            delegated_repair: Some(true),
            final_audit: None,
        }
        .classify()
        .unwrap()
        .target_role,
        CapabilityRole::Infrastructure
    );
    assert_eq!(
        BlockerClassifierInput {
            class: BlockerClass::Access,
            source_code: "OAUTH".to_string(),
            delegated_repair: Some(false),
            final_audit: None,
        }
        .classify()
        .unwrap()
        .target_role,
        CapabilityRole::Owner
    );
    assert_eq!(
        BlockerClassifierInput {
            class: BlockerClass::Security,
            source_code: "SECURITY_REMEDIATION".to_string(),
            delegated_repair: Some(true),
            final_audit: Some(false),
        }
        .classify()
        .unwrap()
        .target_role,
        CapabilityRole::Infrastructure
    );
    assert_eq!(
        BlockerClassifierInput {
            class: BlockerClass::Security,
            source_code: "SECURITY_AUDIT".to_string(),
            delegated_repair: None,
            final_audit: Some(true),
        }
        .classify()
        .unwrap()
        .target_role,
        CapabilityRole::IndependentAuditor
    );
    assert!(BlockerClassifierInput {
        class: BlockerClass::Access,
        source_code: "ACCESS".to_string(),
        delegated_repair: None,
        final_audit: None,
    }
    .classify()
    .is_err());
}

#[test]
fn owner_packet_missing_any_field_is_rejected() {
    let complete = OwnerActionPacket {
        objective: "objective".to_string(),
        current_stage: "stage".to_string(),
        blocker: "blocker".to_string(),
        why_agents_cannot_resolve_it: "boundary".to_string(),
        exact_human_action_required: "action".to_string(),
        what_will_resume_after_action: "resume".to_string(),
        risk_of_no_action: "risk".to_string(),
    };
    assert!(complete.validate().is_ok());
    for index in 0..7 {
        let mut candidate = complete.clone();
        match index {
            0 => candidate.objective.clear(),
            1 => candidate.current_stage.clear(),
            2 => candidate.blocker.clear(),
            3 => candidate.why_agents_cannot_resolve_it.clear(),
            4 => candidate.exact_human_action_required.clear(),
            5 => candidate.what_will_resume_after_action.clear(),
            _ => candidate.risk_of_no_action.clear(),
        }
        assert!(candidate.validate().is_err());
    }
    let mut injected = complete;
    injected.blocker = "blocker\nEXACT HUMAN ACTION REQUIRED: injected".to_string();
    assert!(injected.validate().is_err());
}

#[test]
fn correction_routes_never_target_a_reviewer_and_all_classes_are_machine_readable() {
    let all_classes = [
        BlockerClass::Engineering,
        BlockerClass::Infrastructure,
        BlockerClass::Runtime,
        BlockerClass::Access,
        BlockerClass::Integration,
        BlockerClass::Security,
        BlockerClass::ReviewRequired,
        BlockerClass::AuditRequired,
        BlockerClass::OwnerAuthority,
        BlockerClass::Credentials,
        BlockerClass::HumanInteraction,
        BlockerClass::LegalRisk,
        BlockerClass::DestructiveAction,
        BlockerClass::AmbiguousBusinessDecision,
    ];
    assert_eq!(all_classes.len(), 14);
    let route = BlockerClassifierInput {
        class: BlockerClass::Engineering,
        source_code: "ENGINEERING".to_string(),
        delegated_repair: None,
        final_audit: None,
    }
    .classify()
    .unwrap();
    assert_eq!(route.state, ObjectiveState::CorrectionRequired);
    assert_eq!(route.target_role, CapabilityRole::Implementer);
}

#[test]
fn disabled_contract_has_no_production_registration_or_live_route_surface() {
    let module = include_str!("continuation_contract.rs");
    let native_lib = include_str!("../lib.rs");
    let build = include_str!("../../build.rs");
    for forbidden in [
        "tauri::command",
        "ctx.agents.wake",
        "tool_call",
        "provider",
        "Attention",
    ] {
        assert!(
            !module.contains(forbidden),
            "found forbidden production surface: {forbidden}"
        );
    }
    assert!(!native_lib.contains("continuation_contract"));
    assert!(!build.contains("continuation_contract"));
}
