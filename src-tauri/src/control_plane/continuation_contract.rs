use super::store::{Result, SilentReason};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum CapabilityRole {
    Implementer,
    Infrastructure,
    TechnicalReviewer,
    IndependentAuditor,
    Supervisor,
    Owner,
}

impl CapabilityRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Implementer => "IMPLEMENTER",
            Self::Infrastructure => "INFRASTRUCTURE",
            Self::TechnicalReviewer => "TECHNICAL_REVIEWER",
            Self::IndependentAuditor => "INDEPENDENT_AUDITOR",
            Self::Supervisor => "SUPERVISOR",
            Self::Owner => "OWNER",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObjectiveState {
    Active,
    Implementing,
    SelfTest,
    CorrectionRequired,
    ReviewReady,
    FinalAuditReady,
    InfraBlocked,
    AwaitingOwner,
    OwnerPaused,
    OwnerTestReady,
    ObjectiveComplete,
}

impl ObjectiveState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Implementing => "IMPLEMENTING",
            Self::SelfTest => "SELF_TEST",
            Self::CorrectionRequired => "CORRECTION_REQUIRED",
            Self::ReviewReady => "REVIEW_READY",
            Self::FinalAuditReady => "FINAL_AUDIT_READY",
            Self::InfraBlocked => "INFRA_BLOCKED",
            Self::AwaitingOwner => "AWAITING_OWNER",
            Self::OwnerPaused => "OWNER_PAUSED",
            Self::OwnerTestReady => "OWNER_TEST_READY",
            Self::ObjectiveComplete => "OBJECTIVE_COMPLETE",
        }
    }

    fn route_role(self) -> Option<CapabilityRole> {
        match self {
            Self::Active | Self::Implementing | Self::SelfTest | Self::CorrectionRequired => {
                Some(CapabilityRole::Implementer)
            }
            Self::ReviewReady => Some(CapabilityRole::TechnicalReviewer),
            Self::FinalAuditReady => Some(CapabilityRole::IndependentAuditor),
            Self::InfraBlocked => Some(CapabilityRole::Infrastructure),
            Self::AwaitingOwner | Self::OwnerTestReady => Some(CapabilityRole::Owner),
            Self::OwnerPaused | Self::ObjectiveComplete => None,
        }
    }

    fn stall_eligible(self) -> bool {
        matches!(
            self,
            Self::Active | Self::Implementing | Self::SelfTest | Self::CorrectionRequired
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BlockerClass {
    Engineering,
    Infrastructure,
    Runtime,
    Access,
    Integration,
    Security,
    ReviewRequired,
    AuditRequired,
    OwnerAuthority,
    Credentials,
    HumanInteraction,
    LegalRisk,
    DestructiveAction,
    AmbiguousBusinessDecision,
}

impl BlockerClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Engineering => "ENGINEERING",
            Self::Infrastructure => "INFRASTRUCTURE",
            Self::Runtime => "RUNTIME",
            Self::Access => "ACCESS",
            Self::Integration => "INTEGRATION",
            Self::Security => "SECURITY",
            Self::ReviewRequired => "REVIEW_REQUIRED",
            Self::AuditRequired => "AUDIT_REQUIRED",
            Self::OwnerAuthority => "OWNER_AUTHORITY",
            Self::Credentials => "CREDENTIALS",
            Self::HumanInteraction => "HUMAN_INTERACTION",
            Self::LegalRisk => "LEGAL_RISK",
            Self::DestructiveAction => "DESTRUCTIVE_ACTION",
            Self::AmbiguousBusinessDecision => "AMBIGUOUS_BUSINESS_DECISION",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BlockerStatus {
    Open,
    Resolved,
}

impl BlockerStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Resolved => "RESOLVED",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Blocker {
    pub blocker_id: String,
    pub class: BlockerClass,
    pub source_code: String,
    pub summary: String,
    pub exact_required_action: String,
    pub evidence_digest: String,
    pub opened_generation: i64,
    pub status: BlockerStatus,
    pub resolved_proof: Option<String>,
}

impl Blocker {
    fn validate(&self) -> Result<()> {
        if self.blocker_id.is_empty()
            || self.blocker_id.len() > 256
            || self.source_code.is_empty()
            || self.source_code.len() > 128
            || self.summary.is_empty()
            || self.summary.len() > 1_024
            || self.exact_required_action.is_empty()
            || self.exact_required_action.len() > 1_024
            || self.evidence_digest.is_empty()
            || self.evidence_digest.len() > 256
            || self.opened_generation < 1
            || (self.status == BlockerStatus::Resolved
                && self
                    .resolved_proof
                    .as_deref()
                    .is_none_or(|proof| proof.is_empty()))
            || (self.status == BlockerStatus::Open && self.resolved_proof.is_some())
        {
            return Err("Invalid REMH blocker record".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct BlockerRoute {
    pub target_role: CapabilityRole,
    pub state: ObjectiveState,
    pub source_code: String,
}

#[derive(Clone, Debug)]
pub(super) struct BlockerClassifierInput {
    pub class: BlockerClass,
    pub source_code: String,
    pub delegated_repair: Option<bool>,
    pub final_audit: Option<bool>,
}

impl BlockerClassifierInput {
    pub(super) fn classify(&self) -> Result<BlockerRoute> {
        if self.source_code.is_empty() || self.source_code.len() > 128 {
            return Err("Blocker source code is required".into());
        }
        let (target_role, state) = match self.class {
            BlockerClass::Engineering => (
                CapabilityRole::Implementer,
                ObjectiveState::CorrectionRequired,
            ),
            BlockerClass::Infrastructure | BlockerClass::Runtime | BlockerClass::Integration => {
                (CapabilityRole::Infrastructure, ObjectiveState::InfraBlocked)
            }
            BlockerClass::Access => match self.delegated_repair {
                Some(true) => (CapabilityRole::Infrastructure, ObjectiveState::InfraBlocked),
                Some(false) => (CapabilityRole::Owner, ObjectiveState::AwaitingOwner),
                None => return Err("ACCESS classification requires authority evidence".into()),
            },
            BlockerClass::Security => match (self.final_audit, self.delegated_repair) {
                (Some(true), _) => (
                    CapabilityRole::IndependentAuditor,
                    ObjectiveState::FinalAuditReady,
                ),
                (Some(false), Some(true)) => {
                    (CapabilityRole::Infrastructure, ObjectiveState::InfraBlocked)
                }
                _ => return Err("SECURITY classification requires bounded evidence".into()),
            },
            BlockerClass::ReviewRequired => (
                CapabilityRole::TechnicalReviewer,
                ObjectiveState::ReviewReady,
            ),
            BlockerClass::AuditRequired => (
                CapabilityRole::IndependentAuditor,
                ObjectiveState::FinalAuditReady,
            ),
            BlockerClass::OwnerAuthority
            | BlockerClass::Credentials
            | BlockerClass::HumanInteraction
            | BlockerClass::LegalRisk
            | BlockerClass::DestructiveAction
            | BlockerClass::AmbiguousBusinessDecision => {
                (CapabilityRole::Owner, ObjectiveState::AwaitingOwner)
            }
        };
        Ok(BlockerRoute {
            target_role,
            state,
            source_code: self.source_code.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RoleBinding {
    pub role: CapabilityRole,
    pub agent_key: String,
    pub pubkey: String,
    pub display_name: String,
    pub agent_revision: u32,
    pub registry_revision: u32,
    pub allowed_channel_ids: Vec<String>,
    pub enabled: bool,
}

impl RoleBinding {
    fn validate(&self, expected_revision: u32) -> Result<()> {
        if self.agent_key.is_empty()
            || self.agent_key.len() > 256
            || self.display_name.is_empty()
            || self.display_name.len() > 256
            || self.display_name.contains('\n')
            || self.display_name.contains('\r')
            || self.registry_revision != expected_revision
            || self.allowed_channel_ids.is_empty()
            || self
                .allowed_channel_ids
                .iter()
                .any(|channel| channel.is_empty() || channel.len() > 256)
        {
            return Err("Invalid or disabled role binding".into());
        }
        if self.enabled
            && (self.pubkey.len() != 64
                || !self.pubkey.bytes().all(|byte| byte.is_ascii_hexdigit())
                || self.agent_revision < 1)
        {
            return Err("Invalid or disabled role binding".into());
        }
        if !self.enabled && (!self.pubkey.is_empty() || self.agent_revision != 0) {
            return Err("Unresolved role binding must not claim an identity".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(super) struct RoleRegistry {
    revision: u32,
    bindings: HashMap<CapabilityRole, RoleBinding>,
}

impl RoleRegistry {
    pub(super) fn new(revision: u32, bindings: Vec<RoleBinding>) -> Result<Self> {
        if revision < 1 {
            return Err("Role registry revision is invalid".into());
        }
        let mut resolved = HashMap::new();
        for binding in bindings {
            binding.validate(revision)?;
            if resolved.insert(binding.role, binding).is_some() {
                return Err("Role registry contains a duplicate capability".into());
            }
        }
        Ok(Self {
            revision,
            bindings: resolved,
        })
    }

    pub(super) fn revision(&self) -> u32 {
        self.revision
    }

    fn fingerprint(&self) -> String {
        let mut bindings: Vec<_> = self.bindings.values().collect();
        bindings.sort_unstable_by_key(|binding| binding.role.as_str());
        let mut canonical = Vec::new();
        append_field(&mut canonical, &self.revision.to_string());
        for binding in bindings {
            let mut channels = binding.allowed_channel_ids.clone();
            channels.sort_unstable();
            for value in [
                binding.role.as_str(),
                binding.agent_key.as_str(),
                binding.pubkey.as_str(),
                binding.display_name.as_str(),
            ] {
                append_field(&mut canonical, value);
            }
            append_field(&mut canonical, &binding.agent_revision.to_string());
            append_field(&mut canonical, &binding.registry_revision.to_string());
            append_field(&mut canonical, if binding.enabled { "1" } else { "0" });
            for channel in channels {
                append_field(&mut canonical, &channel);
            }
        }
        format!("remh-role-registry:{:x}", Sha256::digest(canonical))
    }

    pub(super) fn resolve(&self, role: CapabilityRole, channel_id: &str) -> Result<&RoleBinding> {
        let binding = self
            .bindings
            .get(&role)
            .ok_or("Required capability role is unresolved")?;
        binding.validate(self.revision)?;
        if !binding.enabled {
            return Err("Required capability role is unresolved".into());
        }
        if !binding
            .allowed_channel_ids
            .iter()
            .any(|allowed| allowed == channel_id)
        {
            return Err("Role binding is not admitted to the target channel".into());
        }
        Ok(binding)
    }

    pub(super) fn replace_binding(&mut self, binding: RoleBinding) {
        self.bindings.insert(binding.role, binding);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OwnerActionPacket {
    pub objective: String,
    pub current_stage: String,
    pub blocker: String,
    pub why_agents_cannot_resolve_it: String,
    pub exact_human_action_required: String,
    pub what_will_resume_after_action: String,
    pub risk_of_no_action: String,
}

impl OwnerActionPacket {
    pub(super) fn validate(&self) -> Result<()> {
        let fields = [
            &self.objective,
            &self.current_stage,
            &self.blocker,
            &self.why_agents_cannot_resolve_it,
            &self.exact_human_action_required,
            &self.what_will_resume_after_action,
            &self.risk_of_no_action,
        ];
        if fields.iter().any(|value| {
            value.trim().is_empty()
                || value.len() > 1_024
                || value.contains('\n')
                || value.contains('\r')
        }) {
            return Err("Owner action packet requires all seven bounded fields".into());
        }
        Ok(())
    }

    fn render(&self, owner_display_name: &str) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "@{owner_display_name}\nOBJECTIVE: {}\nCURRENT STAGE: {}\nBLOCKER: {}\nWHY AGENTS CANNOT RESOLVE IT: {}\nEXACT HUMAN ACTION REQUIRED: {}\nWHAT WILL RESUME AFTER ACTION: {}\nRISK OF NO ACTION: {}",
            self.objective,
            self.current_stage,
            self.blocker,
            self.why_agents_cannot_resolve_it,
            self.exact_human_action_required,
            self.what_will_resume_after_action,
            self.risk_of_no_action
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MaterialProof {
    pub kind: String,
    pub reference: String,
    pub digest: Option<String>,
    pub occurred_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GateEvidence {
    pub kind: String,
    pub reference: String,
    pub digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ObjectiveSnapshot {
    pub contract_version: u32,
    pub objective_id: String,
    pub objective_title: String,
    pub objective_channel_id: String,
    pub control_channel_id: String,
    pub thread_root: String,
    pub state: ObjectiveState,
    pub generation: i64,
    pub implementing_role: CapabilityRole,
    pub current_route_role: Option<CapabilityRole>,
    pub prior_implementation_state: Option<ObjectiveState>,
    pub blocker: Option<Blocker>,
    pub material_proof: MaterialProof,
    pub gate_evidence: Vec<GateEvidence>,
    pub last_verified_material_progress_at: i64,
    pub next_review_at: Option<i64>,
    pub stale_generation: i64,
    pub route_registry_revision: u32,
    pub source_event_id: String,
    pub next_action: String,
    pub owner_action_packet: Option<OwnerActionPacket>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl ObjectiveSnapshot {
    pub(super) fn material_fingerprint(&self) -> Result<String> {
        self.validate_shape()?;
        let mut evidence: Vec<_> = self
            .gate_evidence
            .iter()
            .map(|item| {
                (
                    item.kind.as_str(),
                    item.reference.as_str(),
                    item.digest.as_deref().unwrap_or(""),
                )
            })
            .collect();
        evidence.sort_unstable();

        let mut canonical = Vec::new();
        for value in [
            self.contract_version.to_string(),
            self.objective_id.clone(),
            self.objective_title.clone(),
            self.objective_channel_id.clone(),
            self.control_channel_id.clone(),
            self.thread_root.clone(),
            self.state.as_str().to_string(),
            self.generation.to_string(),
            self.implementing_role.as_str().to_string(),
            self.current_route_role
                .map(CapabilityRole::as_str)
                .unwrap_or("")
                .to_string(),
            self.prior_implementation_state
                .map(ObjectiveState::as_str)
                .unwrap_or("")
                .to_string(),
            self.material_proof.kind.clone(),
            self.material_proof.reference.clone(),
            self.material_proof.digest.clone().unwrap_or_default(),
            self.material_proof.occurred_at.to_string(),
            self.route_registry_revision.to_string(),
            self.next_action.clone(),
        ] {
            append_field(&mut canonical, &value);
        }
        if let Some(blocker) = &self.blocker {
            for value in [
                blocker.blocker_id.as_str(),
                blocker.class.as_str(),
                blocker.source_code.as_str(),
                blocker.summary.as_str(),
                blocker.exact_required_action.as_str(),
                blocker.evidence_digest.as_str(),
                blocker.status.as_str(),
                blocker.resolved_proof.as_deref().unwrap_or(""),
            ] {
                append_field(&mut canonical, value);
            }
        }
        for (kind, reference, digest) in evidence {
            append_field(&mut canonical, kind);
            append_field(&mut canonical, reference);
            append_field(&mut canonical, digest);
        }
        if let Some(packet) = &self.owner_action_packet {
            packet.validate()?;
            for value in [
                packet.objective.as_str(),
                packet.current_stage.as_str(),
                packet.blocker.as_str(),
                packet.why_agents_cannot_resolve_it.as_str(),
                packet.exact_human_action_required.as_str(),
                packet.what_will_resume_after_action.as_str(),
                packet.risk_of_no_action.as_str(),
            ] {
                append_field(&mut canonical, value);
            }
        }
        Ok(format!(
            "remh-continuation:v{}:{:x}",
            self.contract_version,
            Sha256::digest(canonical)
        ))
    }

    fn validate_shape(&self) -> Result<()> {
        if self.contract_version != 1
            || self.objective_id.is_empty()
            || self.objective_id.len() > 256
            || self.objective_title.is_empty()
            || self.objective_title.len() > 512
            || self.objective_channel_id.is_empty()
            || self.objective_channel_id.len() > 256
            || self.control_channel_id.is_empty()
            || self.control_channel_id.len() > 256
            || self.thread_root.is_empty()
            || self.thread_root.len() > 256
            || self.generation < 1
            || self.implementing_role != CapabilityRole::Implementer
            || self.material_proof.kind.is_empty()
            || self.material_proof.kind.len() > 128
            || self.material_proof.reference.is_empty()
            || self.material_proof.reference.len() > 512
            || self
                .material_proof
                .digest
                .as_ref()
                .is_some_and(|value| value.len() > 256)
            || self.material_proof.occurred_at < 0
            || self.last_verified_material_progress_at < 0
            || self.next_review_at.is_some_and(|deadline| deadline < 0)
            || self.stale_generation < 0
            || self.route_registry_revision < 1
            || self.source_event_id.is_empty()
            || self.source_event_id.len() > 512
            || self.next_action.is_empty()
            || self.next_action.len() > 1_024
            || self.created_at < 0
            || self.updated_at < 0
            || self.gate_evidence.iter().any(|item| {
                item.kind.is_empty()
                    || item.kind.len() > 128
                    || item.reference.is_empty()
                    || item.reference.len() > 512
                    || item.digest.as_ref().is_some_and(|value| value.len() > 256)
            })
        {
            return Err("Invalid REMH continuation snapshot".into());
        }
        if let Some(blocker) = &self.blocker {
            blocker.validate()?;
        }
        match self.state {
            ObjectiveState::CorrectionRequired | ObjectiveState::InfraBlocked => {
                if self
                    .blocker
                    .as_ref()
                    .is_none_or(|value| value.status != BlockerStatus::Open)
                {
                    return Err("Blocked objective state requires one open blocker".into());
                }
            }
            ObjectiveState::AwaitingOwner => {
                if self
                    .blocker
                    .as_ref()
                    .is_none_or(|value| value.status != BlockerStatus::Open)
                    || self.owner_action_packet.is_none()
                {
                    return Err("Owner gate requires an open blocker and complete packet".into());
                }
            }
            ObjectiveState::ObjectiveComplete | ObjectiveState::OwnerPaused => {
                if self.next_review_at.is_some() {
                    return Err("Dormant objective state cannot carry a review deadline".into());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate(&self, trusted_now: i64, registry: &RoleRegistry) -> Result<()> {
        self.validate_shape()?;
        if trusted_now < 0
            || self.material_proof.occurred_at > trusted_now
            || self.last_verified_material_progress_at > trusted_now
            || self.created_at > trusted_now
        {
            return Err("Continuation evidence is outside trusted time".into());
        }
        if self.route_registry_revision != registry.revision() {
            return Err("Objective role registry revision is stale".into());
        }
        if let Some(role) = self.state.route_role() {
            let binding = registry.resolve(role, &self.objective_channel_id)?;
            if self
                .current_route_role
                .is_some_and(|current| current != role)
            {
                return Err("Objective route role conflicts with deterministic policy".into());
            }
            if role == CapabilityRole::Owner {
                self.owner_action_packet
                    .as_ref()
                    .ok_or("Owner route requires a structured action packet")?
                    .render(&binding.display_name)?;
            }
        }
        Ok(())
    }
}

fn append_field(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(value.as_bytes());
    output.push(0);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ControlEventType {
    ObjectiveCreated,
    StageChanged,
    BlockerRouted,
    BlockerResolved,
    ReviewRouted,
    OwnerActionRequired,
    OwnerActionReceived,
    OwnerTestReady,
    ObjectiveComplete,
}

impl ControlEventType {
    fn as_str(self) -> &'static str {
        match self {
            Self::ObjectiveCreated => "OBJECTIVE_CREATED",
            Self::StageChanged => "STAGE_CHANGED",
            Self::BlockerRouted => "BLOCKER_ROUTED",
            Self::BlockerResolved => "BLOCKER_RESOLVED",
            Self::ReviewRouted => "REVIEW_ROUTED",
            Self::OwnerActionRequired => "OWNER_ACTION_REQUIRED",
            Self::OwnerActionReceived => "OWNER_ACTION_RECEIVED",
            Self::OwnerTestReady => "OWNER_TEST_READY",
            Self::ObjectiveComplete => "OBJECTIVE_COMPLETE",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PlannedDispatch {
    pub objective_id: String,
    pub generation: i64,
    pub state: ObjectiveState,
    pub target_role: CapabilityRole,
    pub target_pubkey: String,
    pub registry_revision: u32,
    pub channel_id: String,
    pub thread_root: String,
    pub event_type: ControlEventType,
    pub material_fingerprint: String,
    pub route_identity: String,
    pub stale_generation: Option<i64>,
    pub owner_action_message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ObserveOutcome {
    Silent(SilentReason),
    Planned(PlannedDispatch),
}

impl ObserveOutcome {
    pub(super) fn planned(self) -> Option<PlannedDispatch> {
        match self {
            Self::Planned(dispatch) => Some(dispatch),
            Self::Silent(_) => None,
        }
    }
}

pub(super) trait SandboxEffectSink {
    fn preflight(&mut self, dispatch: &PlannedDispatch) -> Result<()>;
    fn admit(&mut self, dispatch: &PlannedDispatch) -> Result<()>;
}

pub(super) struct DisabledContinuationAdapter<S> {
    connection: Connection,
    registry: RoleRegistry,
    sink: S,
}

impl<S: SandboxEffectSink> DisabledContinuationAdapter<S> {
    pub(super) fn open(path: PathBuf, registry: RoleRegistry, sink: S) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or("Continuation sandbox database location unavailable")?;
        std::fs::create_dir_all(parent)
            .map_err(|_| "Continuation sandbox database directory unavailable")?;
        let connection = Connection::open(path)
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys=ON;
                 PRAGMA journal_mode=WAL;
                 CREATE TABLE IF NOT EXISTS continuation_snapshots(
                   objective_id TEXT PRIMARY KEY NOT NULL,
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   state TEXT NOT NULL,
                   material_fingerprint TEXT NOT NULL,
                   next_review_at INTEGER,
                   stale_generation INTEGER NOT NULL CHECK(stale_generation >= 0),
                   registry_revision INTEGER NOT NULL CHECK(registry_revision >= 1),
                   registry_fingerprint TEXT NOT NULL,
                   source_event_id TEXT UNIQUE NOT NULL,
                   accepted_at INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS continuation_source_events(
                   source_event_id TEXT PRIMARY KEY NOT NULL,
                   objective_id TEXT NOT NULL,
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   material_fingerprint TEXT NOT NULL,
                   accepted_at INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS continuation_dispatches(
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   objective_id TEXT NOT NULL,
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   state TEXT NOT NULL,
                   route_identity TEXT UNIQUE NOT NULL,
                   target_role TEXT NOT NULL,
                   target_pubkey TEXT NOT NULL,
                   registry_revision INTEGER NOT NULL,
                   channel_id TEXT NOT NULL,
                   thread_root TEXT NOT NULL,
                   event_type TEXT NOT NULL,
                   material_fingerprint TEXT NOT NULL,
                   stale_generation INTEGER,
                   created_at INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS continuation_control_events(
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   objective_id TEXT NOT NULL,
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   event_type TEXT NOT NULL,
                   event_identity TEXT UNIQUE NOT NULL,
                   material_fingerprint TEXT NOT NULL,
                   created_at INTEGER NOT NULL
                 ) STRICT;",
            )
            .map_err(|error| format!("Continuation sandbox schema failed: {error}"))?;
        Ok(Self {
            connection,
            registry,
            sink,
        })
    }

    pub(super) fn observe(
        &mut self,
        snapshot: &ObjectiveSnapshot,
        trusted_now: i64,
    ) -> Result<ObserveOutcome> {
        snapshot.validate(trusted_now, &self.registry)?;
        let fingerprint = snapshot.material_fingerprint()?;
        let registry = &self.registry;
        let sink = &mut self.sink;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        let prior: Option<(i64, String, String, Option<i64>, i64, String, String)> = tx
            .query_row(
                "SELECT generation,material_fingerprint,state,next_review_at,stale_generation,source_event_id,registry_fingerprint FROM continuation_snapshots WHERE objective_id=?1",
                [&snapshot.objective_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
            )
            .optional()
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;

        if let Some((
            _,
            prior_fingerprint,
            _,
            accepted_deadline,
            stale_generation,
            _,
            registry_fingerprint,
        )) = &prior
        {
            if registry_fingerprint != &registry.fingerprint() {
                return Err("Role registry revision was reused for different bindings".into());
            }
            if prior_fingerprint == &fingerprint {
                return Self::observe_unchanged(
                    registry,
                    sink,
                    tx,
                    snapshot,
                    trusted_now,
                    &fingerprint,
                    *accepted_deadline,
                    *stale_generation,
                );
            }
        }

        if let Some((prior_generation, _, _, _, _, _, _)) = &prior {
            if snapshot.generation != prior_generation + 1 {
                return Err(
                    "Material continuation change must advance generation exactly once".into(),
                );
            }
            let duplicate: Option<String> = tx
                .query_row(
                    "SELECT objective_id FROM continuation_source_events WHERE source_event_id=?1",
                    [&snapshot.source_event_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            if duplicate.is_some() {
                tx.commit()
                    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
                return Ok(ObserveOutcome::Silent(SilentReason::AlreadyIntervened));
            }
        }

        let prior_state = prior
            .as_ref()
            .map(|(_, _, state, _, _, _, _)| parse_state(state))
            .transpose()?;
        validate_transition(prior_state, snapshot)?;
        let event_type = transition_event(prior_state, snapshot)?;
        persist_snapshot(&tx, registry, snapshot, &fingerprint, trusted_now)?;
        persist_control_event(&tx, snapshot, event_type, &fingerprint, trusted_now)?;

        let progress_only_transition = matches!(
            (prior_state, snapshot.state),
            (Some(ObjectiveState::Active), ObjectiveState::Implementing)
                | (Some(ObjectiveState::Implementing), ObjectiveState::SelfTest)
        );
        let should_route = (prior_state.is_none() && snapshot.state.route_role().is_some())
            || (!progress_only_transition
                && prior_state != Some(snapshot.state)
                && snapshot.state.route_role().is_some());
        if !should_route {
            tx.commit()
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            return Ok(ObserveOutcome::Silent(SilentReason::Dormant));
        }

        let role = snapshot
            .state
            .route_role()
            .ok_or("Dormant objective state cannot route")?;
        let dispatch = plan_dispatch(registry, snapshot, role, event_type, &fingerprint, None)?;
        persist_dispatch(&tx, &dispatch, trusted_now)?;
        tx.commit()
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        sink.preflight(&dispatch)?;
        sink.admit(&dispatch)?;
        Ok(ObserveOutcome::Planned(dispatch))
    }

    fn observe_unchanged(
        registry: &RoleRegistry,
        sink: &mut S,
        tx: Transaction<'_>,
        snapshot: &ObjectiveSnapshot,
        trusted_now: i64,
        fingerprint: &str,
        accepted_deadline: Option<i64>,
        accepted_stale_generation: i64,
    ) -> Result<ObserveOutcome> {
        let Some(deadline) = accepted_deadline else {
            tx.commit()
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            return Ok(ObserveOutcome::Silent(SilentReason::Dormant));
        };
        if !snapshot.state.stall_eligible() || trusted_now < deadline {
            tx.commit()
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            return Ok(ObserveOutcome::Silent(SilentReason::NotDue));
        }
        let stale_generation = accepted_stale_generation + 1;
        let dispatch = plan_dispatch(
            registry,
            snapshot,
            CapabilityRole::Implementer,
            ControlEventType::StageChanged,
            fingerprint,
            Some(stale_generation),
        )?;
        let changed = persist_dispatch(&tx, &dispatch, trusted_now)?;
        if changed == 0 {
            tx.commit()
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            return Ok(ObserveOutcome::Silent(SilentReason::AlreadyIntervened));
        }
        tx.commit()
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        sink.preflight(&dispatch)?;
        sink.admit(&dispatch)?;
        Ok(ObserveOutcome::Planned(dispatch))
    }

    pub(super) fn dispatch_count(&self) -> Result<i64> {
        self.connection
            .query_row("SELECT COUNT(*) FROM continuation_dispatches", [], |row| {
                row.get(0)
            })
            .map_err(|error| format!("Continuation sandbox storage failed: {error}").into())
    }

    pub(super) fn control_event_count(&self, event_type: ControlEventType) -> Result<i64> {
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM continuation_control_events WHERE event_type=?1",
                [event_type.as_str()],
                |row| row.get(0),
            )
            .map_err(|error| format!("Continuation sandbox storage failed: {error}").into())
    }
}

fn parse_state(value: &str) -> Result<ObjectiveState> {
    match value {
        "ACTIVE" => Ok(ObjectiveState::Active),
        "IMPLEMENTING" => Ok(ObjectiveState::Implementing),
        "SELF_TEST" => Ok(ObjectiveState::SelfTest),
        "CORRECTION_REQUIRED" => Ok(ObjectiveState::CorrectionRequired),
        "REVIEW_READY" => Ok(ObjectiveState::ReviewReady),
        "FINAL_AUDIT_READY" => Ok(ObjectiveState::FinalAuditReady),
        "INFRA_BLOCKED" => Ok(ObjectiveState::InfraBlocked),
        "AWAITING_OWNER" => Ok(ObjectiveState::AwaitingOwner),
        "OWNER_PAUSED" => Ok(ObjectiveState::OwnerPaused),
        "OWNER_TEST_READY" => Ok(ObjectiveState::OwnerTestReady),
        "OBJECTIVE_COMPLETE" => Ok(ObjectiveState::ObjectiveComplete),
        _ => Err("Persisted objective state is unknown".into()),
    }
}

fn validate_transition(
    prior_state: Option<ObjectiveState>,
    snapshot: &ObjectiveSnapshot,
) -> Result<()> {
    if snapshot.state == ObjectiveState::ObjectiveComplete
        && !snapshot
            .gate_evidence
            .iter()
            .any(|evidence| evidence.kind == "COMPLETION_EVIDENCE")
    {
        return Err("Objective completion requires explicit evidence".into());
    }
    let Some(prior) = prior_state else {
        return Ok(());
    };
    if prior == ObjectiveState::ObjectiveComplete {
        return Err("Completed objective is terminal".into());
    }
    let allowed = prior == snapshot.state
        || matches!(
            (prior, snapshot.state),
            (ObjectiveState::Active, ObjectiveState::Implementing)
                | (ObjectiveState::Active, ObjectiveState::OwnerPaused)
                | (ObjectiveState::Implementing, ObjectiveState::SelfTest)
                | (
                    ObjectiveState::Implementing,
                    ObjectiveState::CorrectionRequired
                )
                | (ObjectiveState::Implementing, ObjectiveState::InfraBlocked)
                | (ObjectiveState::Implementing, ObjectiveState::ReviewReady)
                | (
                    ObjectiveState::Implementing,
                    ObjectiveState::FinalAuditReady
                )
                | (ObjectiveState::Implementing, ObjectiveState::AwaitingOwner)
                | (ObjectiveState::Implementing, ObjectiveState::OwnerPaused)
                | (ObjectiveState::Implementing, ObjectiveState::OwnerTestReady)
                | (
                    ObjectiveState::Implementing,
                    ObjectiveState::ObjectiveComplete
                )
                | (ObjectiveState::SelfTest, ObjectiveState::Implementing)
                | (ObjectiveState::SelfTest, ObjectiveState::CorrectionRequired)
                | (ObjectiveState::SelfTest, ObjectiveState::InfraBlocked)
                | (ObjectiveState::SelfTest, ObjectiveState::ReviewReady)
                | (ObjectiveState::SelfTest, ObjectiveState::FinalAuditReady)
                | (ObjectiveState::SelfTest, ObjectiveState::AwaitingOwner)
                | (ObjectiveState::SelfTest, ObjectiveState::OwnerPaused)
                | (ObjectiveState::SelfTest, ObjectiveState::ObjectiveComplete)
                | (
                    ObjectiveState::CorrectionRequired,
                    ObjectiveState::Implementing
                )
                | (
                    ObjectiveState::CorrectionRequired,
                    ObjectiveState::InfraBlocked
                )
                | (
                    ObjectiveState::CorrectionRequired,
                    ObjectiveState::ReviewReady
                )
                | (
                    ObjectiveState::CorrectionRequired,
                    ObjectiveState::AwaitingOwner
                )
                | (
                    ObjectiveState::CorrectionRequired,
                    ObjectiveState::OwnerPaused
                )
                | (ObjectiveState::InfraBlocked, ObjectiveState::Implementing)
                | (ObjectiveState::InfraBlocked, ObjectiveState::AwaitingOwner)
                | (ObjectiveState::InfraBlocked, ObjectiveState::OwnerPaused)
                | (
                    ObjectiveState::ReviewReady,
                    ObjectiveState::CorrectionRequired
                )
                | (ObjectiveState::ReviewReady, ObjectiveState::FinalAuditReady)
                | (ObjectiveState::ReviewReady, ObjectiveState::AwaitingOwner)
                | (ObjectiveState::ReviewReady, ObjectiveState::OwnerPaused)
                | (ObjectiveState::ReviewReady, ObjectiveState::OwnerTestReady)
                | (
                    ObjectiveState::FinalAuditReady,
                    ObjectiveState::CorrectionRequired
                )
                | (
                    ObjectiveState::FinalAuditReady,
                    ObjectiveState::AwaitingOwner
                )
                | (ObjectiveState::FinalAuditReady, ObjectiveState::OwnerPaused)
                | (
                    ObjectiveState::FinalAuditReady,
                    ObjectiveState::OwnerTestReady
                )
                | (ObjectiveState::AwaitingOwner, ObjectiveState::Implementing)
                | (ObjectiveState::AwaitingOwner, ObjectiveState::OwnerPaused)
                | (
                    ObjectiveState::AwaitingOwner,
                    ObjectiveState::OwnerTestReady
                )
                | (ObjectiveState::OwnerPaused, ObjectiveState::Active)
                | (ObjectiveState::OwnerPaused, ObjectiveState::Implementing)
                | (
                    ObjectiveState::OwnerTestReady,
                    ObjectiveState::CorrectionRequired
                )
                | (
                    ObjectiveState::OwnerTestReady,
                    ObjectiveState::AwaitingOwner
                )
                | (
                    ObjectiveState::OwnerTestReady,
                    ObjectiveState::ObjectiveComplete
                )
        );
    if !allowed {
        return Err("Objective state transition is not admitted".into());
    }
    Ok(())
}

fn transition_event(
    prior_state: Option<ObjectiveState>,
    snapshot: &ObjectiveSnapshot,
) -> Result<ControlEventType> {
    let event = match (prior_state, snapshot.state) {
        (None, _) => ControlEventType::ObjectiveCreated,
        (_, ObjectiveState::ObjectiveComplete) => ControlEventType::ObjectiveComplete,
        (Some(ObjectiveState::InfraBlocked), ObjectiveState::Implementing) => {
            let blocker = snapshot
                .blocker
                .as_ref()
                .ok_or("Infrastructure recovery requires resolved blocker evidence")?;
            if blocker.status != BlockerStatus::Resolved || blocker.resolved_proof.is_none() {
                return Err("Infrastructure recovery requires resolved blocker evidence".into());
            }
            ControlEventType::BlockerResolved
        }
        (Some(ObjectiveState::AwaitingOwner), ObjectiveState::Implementing) => {
            if !snapshot
                .gate_evidence
                .iter()
                .any(|evidence| evidence.kind == "OWNER_ACTION_RECEIVED")
            {
                return Err("Owner resume requires authorised action evidence".into());
            }
            ControlEventType::OwnerActionReceived
        }
        (_, ObjectiveState::CorrectionRequired | ObjectiveState::InfraBlocked) => {
            ControlEventType::BlockerRouted
        }
        (_, ObjectiveState::ReviewReady | ObjectiveState::FinalAuditReady) => {
            ControlEventType::ReviewRouted
        }
        (_, ObjectiveState::AwaitingOwner) => ControlEventType::OwnerActionRequired,
        (_, ObjectiveState::OwnerTestReady) => ControlEventType::OwnerTestReady,
        _ => ControlEventType::StageChanged,
    };
    Ok(event)
}

fn persist_snapshot(
    tx: &Transaction<'_>,
    registry: &RoleRegistry,
    snapshot: &ObjectiveSnapshot,
    fingerprint: &str,
    trusted_now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO continuation_snapshots(objective_id,generation,state,material_fingerprint,next_review_at,stale_generation,registry_revision,registry_fingerprint,source_event_id,accepted_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(objective_id) DO UPDATE SET generation=excluded.generation,state=excluded.state,material_fingerprint=excluded.material_fingerprint,next_review_at=excluded.next_review_at,stale_generation=excluded.stale_generation,registry_revision=excluded.registry_revision,registry_fingerprint=excluded.registry_fingerprint,source_event_id=excluded.source_event_id,accepted_at=excluded.accepted_at",
        params![snapshot.objective_id,snapshot.generation,snapshot.state.as_str(),fingerprint,snapshot.next_review_at,snapshot.stale_generation,snapshot.route_registry_revision,registry.fingerprint(),snapshot.source_event_id,trusted_now],
    )
    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
    tx.execute(
        "INSERT INTO continuation_source_events(source_event_id,objective_id,generation,material_fingerprint,accepted_at) VALUES(?1,?2,?3,?4,?5)",
        params![snapshot.source_event_id,snapshot.objective_id,snapshot.generation,fingerprint,trusted_now],
    )
    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
    Ok(())
}

fn persist_control_event(
    tx: &Transaction<'_>,
    snapshot: &ObjectiveSnapshot,
    event_type: ControlEventType,
    fingerprint: &str,
    trusted_now: i64,
) -> Result<()> {
    let event_identity = format!(
        "remh-control:{:x}",
        Sha256::digest(
            format!(
                "{}\0{}\0{}\0{}",
                snapshot.objective_id,
                snapshot.generation,
                event_type.as_str(),
                fingerprint
            )
            .as_bytes()
        )
    );
    tx.execute(
        "INSERT OR IGNORE INTO continuation_control_events(objective_id,generation,event_type,event_identity,material_fingerprint,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
        params![snapshot.objective_id,snapshot.generation,event_type.as_str(),event_identity,fingerprint,trusted_now],
    )
    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
    Ok(())
}

fn plan_dispatch(
    registry: &RoleRegistry,
    snapshot: &ObjectiveSnapshot,
    target_role: CapabilityRole,
    event_type: ControlEventType,
    fingerprint: &str,
    stale_generation: Option<i64>,
) -> Result<PlannedDispatch> {
    let binding = registry.resolve(target_role, &snapshot.objective_channel_id)?;
    let route_identity = format!(
        "remh-route:{:x}",
        Sha256::digest(
            format!(
                "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
                snapshot.contract_version,
                snapshot.objective_id,
                snapshot.generation,
                fingerprint,
                event_type.as_str(),
                target_role.as_str(),
                binding.pubkey,
                registry.revision(),
                snapshot.objective_channel_id,
                snapshot.thread_root,
                stale_generation
                    .map(|value| value.to_string())
                    .unwrap_or_default()
            )
            .as_bytes()
        )
    );
    let owner_action_message = if target_role == CapabilityRole::Owner {
        Some(
            snapshot
                .owner_action_packet
                .as_ref()
                .ok_or("Owner route requires a structured action packet")?
                .render(&binding.display_name)?,
        )
    } else {
        None
    };
    Ok(PlannedDispatch {
        objective_id: snapshot.objective_id.clone(),
        generation: snapshot.generation,
        state: snapshot.state,
        target_role,
        target_pubkey: binding.pubkey.clone(),
        registry_revision: registry.revision(),
        channel_id: snapshot.objective_channel_id.clone(),
        thread_root: snapshot.thread_root.clone(),
        event_type,
        material_fingerprint: fingerprint.to_string(),
        route_identity,
        stale_generation,
        owner_action_message,
    })
}

fn persist_dispatch(
    tx: &Transaction<'_>,
    dispatch: &PlannedDispatch,
    trusted_now: i64,
) -> Result<usize> {
    tx.execute(
        "INSERT OR IGNORE INTO continuation_dispatches(objective_id,generation,state,route_identity,target_role,target_pubkey,registry_revision,channel_id,thread_root,event_type,material_fingerprint,stale_generation,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
        params![dispatch.objective_id,dispatch.generation,dispatch.state.as_str(),dispatch.route_identity,dispatch.target_role.as_str(),dispatch.target_pubkey,dispatch.registry_revision,dispatch.channel_id,dispatch.thread_root,dispatch.event_type.as_str(),dispatch.material_fingerprint,dispatch.stale_generation,trusted_now],
    )
    .map_err(|error| format!("Continuation sandbox storage failed: {error}").into())
}

#[derive(Clone, Debug)]
pub(super) struct RemediationRequest {
    pub non_destructive: bool,
    pub reversible: bool,
    pub inside_objective: bool,
    pub credentials_available: bool,
    pub creates_external_commitment: bool,
    pub weakens_controls: bool,
    pub production_approval_required: bool,
    pub bounded_evidence: bool,
}

impl RemediationRequest {
    pub(super) fn autonomously_admissible(&self) -> bool {
        self.non_destructive
            && self.reversible
            && self.inside_objective
            && self.credentials_available
            && !self.creates_external_commitment
            && !self.weakens_controls
            && !self.production_approval_required
            && self.bounded_evidence
    }
}

#[derive(Clone, Debug)]
pub(super) struct AgentCreationEvidence {
    pub existing_identity_keys: HashSet<String>,
    pub proposed_identity_key: String,
    pub approved_template: Option<String>,
    pub bot_participant_only: bool,
    pub durable_channel_owner: bool,
}

impl AgentCreationEvidence {
    pub(super) fn validate(&self) -> Result<()> {
        if self.proposed_identity_key.is_empty()
            || self
                .existing_identity_keys
                .contains(&self.proposed_identity_key)
            || self
                .approved_template
                .as_deref()
                .is_none_or(|template| template.is_empty())
            || !self.bot_participant_only
            || self.durable_channel_owner
        {
            return Err("Agent creation policy is not satisfied".into());
        }
        Ok(())
    }
}
