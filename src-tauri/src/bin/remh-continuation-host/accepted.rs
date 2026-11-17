pub mod store {
    pub type Result<T> = std::result::Result<T, String>;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SilentReason {
        Missing,
        Dormant,
        StaleGeneration,
        NotDue,
        AlreadyIntervened,
        ProviderCircuitOpen,
    }
}

#[path = "../../control_plane/continuation_contract.rs"]
mod continuation_contract;

use continuation_contract::{
    Blocker, BlockerClass, BlockerStatus, CapabilityRole, DisabledContinuationAdapter,
    GateEvidence, MaterialProof, ObjectiveSnapshot, ObjectiveState, PlannedDispatch, RoleBinding,
    RoleRegistry, SandboxEffectSink,
};
use serde::Serialize;
use std::path::PathBuf;

pub const PILOT_OBJECTIVE: &str = "REM-165-TREY-PILOT-001";
pub const PILOT_CHANNEL: &str = "2dcedaf8-3cb6-4ab3-ad6e-389248616972";
pub const PILOT_ROOT: &str = "513d637a6ed0e4782b091ecd47cd6476db42df379f6e81f82bcadbd650b0f5c1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Active,
    Implementing,
    InfraBlocked,
    ReviewReady,
    AwaitingOwner,
    Complete,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Implementing => "IMPLEMENTING",
            Self::InfraBlocked => "INFRA_BLOCKED",
            Self::ReviewReady => "REVIEW_READY",
            Self::AwaitingOwner => "AWAITING_OWNER",
            Self::Complete => "OBJECTIVE_COMPLETE",
        }
    }

    fn objective_state(self) -> ObjectiveState {
        match self {
            Self::Active => ObjectiveState::Active,
            Self::Implementing => ObjectiveState::Implementing,
            Self::InfraBlocked => ObjectiveState::InfraBlocked,
            Self::ReviewReady => ObjectiveState::ReviewReady,
            Self::AwaitingOwner => ObjectiveState::AwaitingOwner,
            Self::Complete => ObjectiveState::ObjectiveComplete,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlannerState {
    pub generation: i64,
    pub stage: Stage,
    pub label: String,
    pub next_review_at: Option<i64>,
    pub blocker_opened_generation: Option<i64>,
    pub blocker_resolved: bool,
    pub source_event_id: String,
    pub occurred_at: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct DispatchPlan {
    pub objective_id: String,
    pub generation: i64,
    pub state: String,
    pub target_role: String,
    pub target_agent_key: String,
    pub target_pubkey: String,
    pub channel_id: String,
    pub thread_root_id: String,
    pub route_identity: String,
    pub owner_action_message: Option<String>,
}

fn state_name(state: ObjectiveState) -> &'static str {
    match state {
        ObjectiveState::Active => "ACTIVE",
        ObjectiveState::Implementing => "IMPLEMENTING",
        ObjectiveState::SelfTest => "SELF_TEST",
        ObjectiveState::CorrectionRequired => "CORRECTION_REQUIRED",
        ObjectiveState::ReviewReady => "REVIEW_READY",
        ObjectiveState::FinalAuditReady => "FINAL_AUDIT_READY",
        ObjectiveState::InfraBlocked => "INFRA_BLOCKED",
        ObjectiveState::AwaitingOwner => "AWAITING_OWNER",
        ObjectiveState::OwnerPaused => "OWNER_PAUSED",
        ObjectiveState::OwnerTestReady => "OWNER_TEST_READY",
        ObjectiveState::ObjectiveComplete => "OBJECTIVE_COMPLETE",
    }
}

fn role_name(role: CapabilityRole) -> &'static str {
    match role {
        CapabilityRole::Implementer => "IMPLEMENTER",
        CapabilityRole::Infrastructure => "INFRASTRUCTURE",
        CapabilityRole::TechnicalReviewer => "TECHNICAL_REVIEWER",
        CapabilityRole::IndependentAuditor => "INDEPENDENT_AUDITOR",
        CapabilityRole::Supervisor => "SUPERVISOR",
        CapabilityRole::Owner => "OWNER",
    }
}

#[derive(Clone, Default)]
struct NoopSink;

impl SandboxEffectSink for NoopSink {
    fn preflight(&mut self, _dispatch: &PlannedDispatch) -> store::Result<()> {
        Ok(())
    }

    fn admit(&mut self, _dispatch: &PlannedDispatch) -> store::Result<()> {
        Ok(())
    }
}

fn binding(role: CapabilityRole, agent_key: &str, display_name: &str, pubkey: &str) -> RoleBinding {
    RoleBinding {
        role,
        agent_key: agent_key.to_string(),
        pubkey: pubkey.to_string(),
        display_name: display_name.to_string(),
        agent_revision: 1,
        registry_revision: 7,
        allowed_channel_ids: vec![PILOT_CHANNEL.to_string()],
        enabled: true,
    }
}

fn registry() -> RoleRegistry {
    RoleRegistry::new(
        7,
        vec![
            binding(
                CapabilityRole::Implementer,
                "kenny-remh",
                "Kenny | REMH - RE Management Holdings",
                "801987f5ed7ddd36bf855042b297ea7fa1882bf320580521bd7afa69e2aff5f9",
            ),
            binding(
                CapabilityRole::Infrastructure,
                "jimmy-wiring",
                "Jimmy | Wiring",
                "03f0cc2c1b1c292fec9d1813954b2b4986ff5d0f7a6d1fa2cd4e3f3188a05ab6",
            ),
            binding(
                CapabilityRole::TechnicalReviewer,
                "remh-grok-reviewer",
                "REMH Grok Build Specialist",
                "149a9f86ef67dad94202432aa889fd8075f82d6f2282083d1455e659c9f29fd5",
            ),
            binding(
                CapabilityRole::IndependentAuditor,
                "remh-independent-auditor",
                "Claude | REMH Audit",
                "116cfc840c6ee3a95cdb4bc91d525ec45dc6a37bc2987b0f6c3e18d04d0bce86",
            ),
            binding(
                CapabilityRole::Owner,
                "fgd-owner",
                "Matt",
                "f22e6b13c6e235caa2162ebc40ead515ba096ab1eeccc2aea87a55edefad4541",
            ),
        ],
    )
    .expect("fixed pilot role registry")
}

fn agent_key(role: CapabilityRole) -> store::Result<&'static str> {
    match role {
        CapabilityRole::Implementer => Ok("kenny-remh"),
        CapabilityRole::Infrastructure => Ok("jimmy-wiring"),
        CapabilityRole::TechnicalReviewer => Ok("remh-grok-reviewer"),
        CapabilityRole::IndependentAuditor => Ok("remh-independent-auditor"),
        CapabilityRole::Owner => Ok("fgd-owner"),
        CapabilityRole::Supervisor => Err("The first live pilot does not admit this role".into()),
    }
}

fn snapshot(state: &PlannerState) -> ObjectiveSnapshot {
    let blocker = if state.stage == Stage::AwaitingOwner {
        Some(Blocker {
            blocker_id: format!("pilot-owner-gate-{}", state.generation),
            class: BlockerClass::HumanInteraction,
            source_code: "OWNER_CONFIRMATION".to_string(),
            summary: "A harmless explicit owner confirmation is required".to_string(),
            exact_required_action:
                "Reply `REM-165 PILOT OWNER ACTION COMPLETE` in the Operations Control thread"
                    .to_string(),
            evidence_digest: format!("sha256:{}", state.label),
            opened_generation: state.generation,
            status: BlockerStatus::Open,
            resolved_proof: None,
        })
    } else {
        state
            .blocker_opened_generation
            .map(|opened_generation| Blocker {
                blocker_id: format!("pilot-blocker-{opened_generation}"),
                class: BlockerClass::Infrastructure,
                source_code: "AGENT_RUNTIME_FAILURE".to_string(),
                summary: "The deterministic continuation target requires infrastructure handling"
                    .to_string(),
                exact_required_action: "restore the bounded Buzz/ACP continuation path".to_string(),
                evidence_digest: format!("sha256:{}", state.label),
                opened_generation,
                status: if state.blocker_resolved {
                    BlockerStatus::Resolved
                } else {
                    BlockerStatus::Open
                },
                resolved_proof: state
                    .blocker_resolved
                    .then(|| "sha256:bounded-infrastructure-resolution".to_string()),
            })
    };
    let mut evidence = vec![GateEvidence {
        kind: "PILOT_ISOLATION".to_string(),
        reference: "folder-plugin-host-command".to_string(),
        digest: Some("sha256:fixed-remh-pilot".to_string()),
    }];
    if state.stage == Stage::Complete {
        evidence.push(GateEvidence {
            kind: "COMPLETION_EVIDENCE".to_string(),
            reference: "pilot:complete".to_string(),
            digest: Some("sha256:pilot-complete".to_string()),
        });
    }
    if state.label == "owner-action-received" {
        evidence.push(GateEvidence {
            kind: "OWNER_ACTION_RECEIVED".to_string(),
            reference: state.source_event_id.clone(),
            digest: Some("sha256:owner-action-received".to_string()),
        });
    }
    ObjectiveSnapshot {
        contract_version: 1,
        objective_id: PILOT_OBJECTIVE.to_string(),
        objective_title: "Harmless deterministic REMH continuation pilot".to_string(),
        objective_channel_id: PILOT_CHANNEL.to_string(),
        control_channel_id: PILOT_CHANNEL.to_string(),
        thread_root: PILOT_ROOT.to_string(),
        state: state.stage.objective_state(),
        generation: state.generation,
        implementing_role: CapabilityRole::Implementer,
        current_route_role: None,
        prior_implementation_state: if state.blocker_resolved {
            Some(ObjectiveState::Implementing)
        } else {
            None
        },
        blocker,
        material_proof: MaterialProof {
            kind: "PILOT_CHECKPOINT".to_string(),
            reference: format!("pilot:{}", state.label),
            digest: Some(format!("sha256:{}", state.label)),
            occurred_at: state.occurred_at,
        },
        gate_evidence: evidence,
        last_verified_material_progress_at: state.occurred_at,
        next_review_at: state.next_review_at,
        stale_generation: 0,
        route_registry_revision: 7,
        source_event_id: state.source_event_id.clone(),
        next_action: "perform only the fixed harmless continuation acknowledgement".to_string(),
        owner_action_packet: (state.stage == Stage::AwaitingOwner).then(|| {
            continuation_contract::OwnerActionPacket {
                objective: format!(
                    "{} — Harmless deterministic REMH continuation pilot",
                    PILOT_OBJECTIVE
                ),
                current_stage: "AWAITING_OWNER".to_string(),
                blocker: "HUMAN_INTERACTION — harmless owner confirmation required".to_string(),
                why_agents_cannot_resolve_it:
                    "The acceptance contract requires a real owner-authored confirmation event."
                        .to_string(),
                exact_human_action_required:
                    "Reply `REM-165 PILOT OWNER ACTION COMPLETE` in this thread.".to_string(),
                what_will_resume_after_action:
                    "The pilot will route once to IMPLEMENTER and continue acceptance.".to_string(),
                risk_of_no_action:
                    "The harmless pilot remains paused and Condition Report is not enrolled."
                        .to_string(),
            }
        }),
        created_at: 1,
        updated_at: state.occurred_at,
    }
}

pub fn evaluate(
    database: PathBuf,
    state: &PlannerState,
    trusted_now: i64,
) -> store::Result<Option<DispatchPlan>> {
    let registry = registry();
    let mut adapter = DisabledContinuationAdapter::open(database, registry, NoopSink)?;
    let outcome = adapter.observe(&snapshot(state), trusted_now)?;
    let Some(dispatch) = outcome.planned() else {
        return Ok(None);
    };
    Ok(Some(DispatchPlan {
        objective_id: dispatch.objective_id,
        generation: dispatch.generation,
        state: state_name(dispatch.state).to_string(),
        target_role: role_name(dispatch.target_role).to_string(),
        target_agent_key: agent_key(dispatch.target_role)?.to_string(),
        target_pubkey: dispatch.target_pubkey,
        channel_id: dispatch.channel_id,
        thread_root_id: dispatch.thread_root,
        route_identity: dispatch.route_identity,
        owner_action_message: dispatch.owner_action_message,
    }))
}
