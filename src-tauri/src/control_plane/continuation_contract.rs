use super::store::{Result, SilentReason};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lifecycle {
    Active,
    OwnerPaused,
    ObjectiveComplete,
}

impl Lifecycle {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::OwnerPaused => "OWNER_PAUSED",
            Self::ObjectiveComplete => "OBJECTIVE_COMPLETE",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stage {
    Implementing,
    SelfTest,
    ReviewReadyGrok,
    CorrectionRequired,
    FinalAuditReadyClaude,
    OwnerTestReady,
    InfraBlockedJimmy,
    AwaitingOwner,
    ObjectiveComplete,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Implementing => "IMPLEMENTING",
            Self::SelfTest => "SELF_TEST",
            Self::ReviewReadyGrok => "REVIEW_READY_GROK",
            Self::CorrectionRequired => "CORRECTION_REQUIRED",
            Self::FinalAuditReadyClaude => "FINAL_AUDIT_READY_CLAUDE",
            Self::OwnerTestReady => "OWNER_TEST_READY",
            Self::InfraBlockedJimmy => "INFRA_BLOCKED_JIMMY",
            Self::AwaitingOwner => "AWAITING_OWNER",
            Self::ObjectiveComplete => "OBJECTIVE_COMPLETE",
        }
    }

    pub(super) fn route(self) -> Option<Route> {
        Some(match self {
            Self::Implementing => Route {
                target: RouteTarget::Kenny,
                reason: RouteReason::ContinueImplementation,
            },
            Self::SelfTest => Route {
                target: RouteTarget::Kenny,
                reason: RouteReason::ContinueSelfTest,
            },
            Self::ReviewReadyGrok => Route {
                target: RouteTarget::Grok,
                reason: RouteReason::ReviewReady,
            },
            Self::CorrectionRequired => Route {
                target: RouteTarget::Kenny,
                reason: RouteReason::CorrectionRequired,
            },
            Self::FinalAuditReadyClaude => Route {
                target: RouteTarget::Claude,
                reason: RouteReason::FinalAuditReady,
            },
            Self::OwnerTestReady => Route {
                target: RouteTarget::Matt,
                reason: RouteReason::OwnerTestReady,
            },
            Self::InfraBlockedJimmy => Route {
                target: RouteTarget::Jimmy,
                reason: RouteReason::InfrastructureBlocked,
            },
            Self::AwaitingOwner => Route {
                target: RouteTarget::Matt,
                reason: RouteReason::AwaitingOwner,
            },
            Self::ObjectiveComplete => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RouteTarget {
    Kenny,
    Grok,
    Claude,
    Jimmy,
    Matt,
}

impl RouteTarget {
    fn as_str(self) -> &'static str {
        match self {
            Self::Kenny => "KENNY",
            Self::Grok => "GROK",
            Self::Claude => "CLAUDE",
            Self::Jimmy => "JIMMY",
            Self::Matt => "MATT",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RouteReason {
    ContinueImplementation,
    ContinueSelfTest,
    ReviewReady,
    CorrectionRequired,
    FinalAuditReady,
    OwnerTestReady,
    InfrastructureBlocked,
    AwaitingOwner,
}

impl RouteReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::ContinueImplementation => "CONTINUE_IMPLEMENTATION",
            Self::ContinueSelfTest => "CONTINUE_SELF_TEST",
            Self::ReviewReady => "REVIEW_READY_GROK",
            Self::CorrectionRequired => "CORRECTION_REQUIRED",
            Self::FinalAuditReady => "FINAL_AUDIT_READY_CLAUDE",
            Self::OwnerTestReady => "OWNER_TEST_READY",
            Self::InfrastructureBlocked => "INFRA_BLOCKED_JIMMY",
            Self::AwaitingOwner => "AWAITING_OWNER",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Route {
    pub target: RouteTarget,
    pub reason: RouteReason,
}

#[derive(Clone, Debug)]
pub(super) struct MaterialProof<'a> {
    pub kind: &'a str,
    pub reference: &'a str,
    pub digest: Option<&'a str>,
    pub occurred_at: i64,
}

#[derive(Clone, Debug)]
pub(super) struct GateEvidence<'a> {
    pub kind: &'a str,
    pub reference: &'a str,
    pub digest: Option<&'a str>,
}

#[derive(Clone, Debug)]
pub(super) struct ObjectiveSnapshot<'a> {
    pub contract_version: u32,
    pub objective_id: &'a str,
    pub generation: i64,
    pub lifecycle: Lifecycle,
    pub stage: Stage,
    pub stage_entered_at: i64,
    pub material_proof: MaterialProof<'a>,
    pub last_verified_material_progress_at: i64,
    pub next_review_at: i64,
    pub next_action: &'a str,
    pub gate_evidence: Vec<GateEvidence<'a>>,
    pub updated_at: i64,
}

impl ObjectiveSnapshot<'_> {
    pub(super) fn material_fingerprint(&self) -> Result<String> {
        self.validate()?;
        let mut evidence: Vec<_> = self
            .gate_evidence
            .iter()
            .map(|item| (item.kind, item.reference, item.digest.unwrap_or("")))
            .collect();
        evidence.sort_unstable();

        let mut canonical = Vec::new();
        for value in [
            self.contract_version.to_string(),
            self.objective_id.to_string(),
            self.generation.to_string(),
            self.lifecycle.as_str().to_string(),
            self.stage.as_str().to_string(),
            self.material_proof.kind.to_string(),
            self.material_proof.reference.to_string(),
            self.material_proof.digest.unwrap_or("").to_string(),
            self.material_proof.occurred_at.to_string(),
            self.next_action.to_string(),
        ] {
            append_field(&mut canonical, &value);
        }
        for (kind, reference, digest) in evidence {
            append_field(&mut canonical, kind);
            append_field(&mut canonical, reference);
            append_field(&mut canonical, digest);
        }
        Ok(format!(
            "remh-continuation:v{}:{:x}",
            self.contract_version,
            Sha256::digest(canonical)
        ))
    }

    fn validate(&self) -> Result<()> {
        if self.contract_version != 1
            || self.objective_id.is_empty()
            || self.objective_id.len() > 256
            || self.generation < 1
            || self.stage_entered_at < 0
            || self.material_proof.kind.is_empty()
            || self.material_proof.kind.len() > 128
            || self.material_proof.reference.is_empty()
            || self.material_proof.reference.len() > 512
            || self
                .material_proof
                .digest
                .is_some_and(|value| value.len() > 128)
            || self.material_proof.occurred_at < 0
            || self.last_verified_material_progress_at < 0
            || self.next_review_at < self.last_verified_material_progress_at
            || self.next_action.is_empty()
            || self.next_action.len() > 1_024
            || self.updated_at < 0
            || self.gate_evidence.iter().any(|item| {
                item.kind.is_empty()
                    || item.kind.len() > 128
                    || item.reference.is_empty()
                    || item.reference.len() > 512
                    || item.digest.is_some_and(|value| value.len() > 128)
            })
        {
            return Err("Invalid REMH continuation snapshot".into());
        }
        if (self.lifecycle == Lifecycle::ObjectiveComplete)
            != (self.stage == Stage::ObjectiveComplete)
        {
            return Err("REMH continuation lifecycle and stage are incoherent".into());
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PlannedDispatch {
    pub objective_id: String,
    pub generation: i64,
    pub stage: Stage,
    pub target: RouteTarget,
    pub reason: RouteReason,
    pub material_fingerprint: String,
    pub recovery_identity: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ObserveOutcome {
    Silent(SilentReason),
    Planned(PlannedDispatch),
}

#[allow(dead_code)]
pub(super) trait SandboxEffectSink {
    fn preflight(&mut self, dispatch: &PlannedDispatch) -> Result<()>;
    fn admit(&mut self, dispatch: &PlannedDispatch) -> Result<()>;
    fn status(&mut self, dispatch: &PlannedDispatch) -> Result<()>;
}

pub(super) struct DisabledContinuationAdapter<S> {
    connection: Connection,
    sink: S,
}

impl<S: SandboxEffectSink> DisabledContinuationAdapter<S> {
    pub(super) fn open(path: PathBuf, sink: S) -> Result<Self> {
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
                   lifecycle TEXT NOT NULL,
                   stage TEXT NOT NULL,
                   material_fingerprint TEXT NOT NULL,
                   last_verified_material_progress_at INTEGER NOT NULL,
                   next_review_at INTEGER NOT NULL,
                   accepted_at INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS continuation_dispatches(
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   objective_id TEXT NOT NULL,
                   generation INTEGER NOT NULL CHECK(generation >= 1),
                   stage TEXT NOT NULL,
                   dispatch_identity TEXT UNIQUE NOT NULL,
                   target TEXT NOT NULL,
                   reason TEXT NOT NULL,
                   material_fingerprint TEXT NOT NULL,
                   recovery_identity TEXT UNIQUE,
                   created_at INTEGER NOT NULL
                 ) STRICT;",
            )
            .map_err(|error| format!("Continuation sandbox schema failed: {error}"))?;
        Ok(Self { connection, sink })
    }

    pub(super) fn observe(
        &mut self,
        snapshot: &ObjectiveSnapshot<'_>,
        trusted_now: i64,
    ) -> Result<ObserveOutcome> {
        if trusted_now < 0 {
            return Err("Trusted continuation time is invalid".into());
        }
        let fingerprint = snapshot.material_fingerprint()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        let prior: Option<(i64, String, i64)> = tx
            .query_row(
                "SELECT generation,material_fingerprint,next_review_at FROM continuation_snapshots WHERE objective_id=?1",
                [snapshot.objective_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;

        let route = if prior
            .as_ref()
            .is_some_and(|(_, prior, _)| prior == &fingerprint)
        {
            let accepted_next_review_at = prior
                .as_ref()
                .map(|(_, _, next_review_at)| *next_review_at)
                .ok_or("Accepted continuation deadline disappeared")?;
            if snapshot.lifecycle != Lifecycle::Active
                || snapshot.stage == Stage::ObjectiveComplete
                || trusted_now < accepted_next_review_at
            {
                tx.commit()
                    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
                return Ok(ObserveOutcome::Silent(SilentReason::NotDue));
            }
            let route = snapshot
                .stage
                .route()
                .ok_or("Terminal continuation stage has no route")?;
            let recovery_identity =
                recovery_identity(snapshot, accepted_next_review_at, &fingerprint);
            let dispatch = PlannedDispatch {
                objective_id: snapshot.objective_id.to_string(),
                generation: snapshot.generation,
                stage: snapshot.stage,
                target: route.target,
                reason: route.reason,
                material_fingerprint: fingerprint.clone(),
                recovery_identity: Some(recovery_identity.clone()),
            };
            let changed = tx
                .execute(
                    "INSERT OR IGNORE INTO continuation_dispatches(objective_id,generation,stage,dispatch_identity,target,reason,material_fingerprint,recovery_identity,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![snapshot.objective_id,snapshot.generation,snapshot.stage.as_str(),recovery_identity,route.target.as_str(),route.reason.as_str(),fingerprint,dispatch.recovery_identity,trusted_now],
                )
                .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            if changed == 0 {
                tx.commit()
                    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
                return Ok(ObserveOutcome::Silent(SilentReason::AlreadyIntervened));
            }
            dispatch
        } else {
            if let Some((generation, _, _)) = prior {
                if snapshot.generation != generation + 1 {
                    return Err(
                        "Material continuation change must advance generation exactly once".into(),
                    );
                }
            }
            tx.execute(
                "INSERT INTO continuation_snapshots(objective_id,generation,lifecycle,stage,material_fingerprint,last_verified_material_progress_at,next_review_at,accepted_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(objective_id) DO UPDATE SET generation=excluded.generation,lifecycle=excluded.lifecycle,stage=excluded.stage,material_fingerprint=excluded.material_fingerprint,last_verified_material_progress_at=excluded.last_verified_material_progress_at,next_review_at=excluded.next_review_at,accepted_at=excluded.accepted_at",
                params![snapshot.objective_id,snapshot.generation,snapshot.lifecycle.as_str(),snapshot.stage.as_str(),fingerprint,snapshot.last_verified_material_progress_at,snapshot.next_review_at,trusted_now],
            )
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            if snapshot.lifecycle != Lifecycle::Active || snapshot.stage == Stage::ObjectiveComplete
            {
                tx.commit()
                    .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
                return Ok(ObserveOutcome::Silent(SilentReason::Dormant));
            }
            let route = snapshot
                .stage
                .route()
                .ok_or("Terminal continuation stage has no route")?;
            let dispatch_identity = format!("material:{fingerprint}");
            tx.execute(
                "INSERT INTO continuation_dispatches(objective_id,generation,stage,dispatch_identity,target,reason,material_fingerprint,recovery_identity,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,?8)",
                params![snapshot.objective_id,snapshot.generation,snapshot.stage.as_str(),dispatch_identity,route.target.as_str(),route.reason.as_str(),fingerprint,trusted_now],
            )
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
            PlannedDispatch {
                objective_id: snapshot.objective_id.to_string(),
                generation: snapshot.generation,
                stage: snapshot.stage,
                target: route.target,
                reason: route.reason,
                material_fingerprint: fingerprint,
                recovery_identity: None,
            }
        };
        tx.commit()
            .map_err(|error| format!("Continuation sandbox storage failed: {error}"))?;
        self.sink.preflight(&route)?;
        self.sink.admit(&route)?;
        Ok(ObserveOutcome::Planned(route))
    }
}

fn recovery_identity(
    snapshot: &ObjectiveSnapshot<'_>,
    accepted_next_review_at: i64,
    fingerprint: &str,
) -> String {
    let canonical = format!(
        "STALL\0{}\0{}\0{}\0{}\0{}",
        snapshot.objective_id,
        snapshot.generation,
        snapshot.stage.as_str(),
        accepted_next_review_at,
        fingerprint
    );
    format!("remh-stall:{:x}", Sha256::digest(canonical.as_bytes()))
}
