use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) type Result<T> = std::result::Result<T, String>;

const SMOKE_OBJECTIVE: &str = "FGD-SMOKE-PIP-NATIVE-1";
const SMOKE_PUBKEY: &str = "ec40342bf26a9c31fe647b0122de9defe94656cae804383e49b191541b77ce6b";
const SMOKE_CHANNEL: &str = "51d885db-c687-44a6-8f2c-01bd679e7bb0";
const SMOKE_ROOT: &str = "a5b8732af2784b1525f39f4fc5d1472835ced60cc8aa1ea43968bd6e92b32e3b";

fn database<T>(result: rusqlite::Result<T>) -> Result<T> {
    result.map_err(|error| format!("Control-plane storage failed: {error}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObjectiveState {
    Active,
    AwaitingOwner,
    OwnerPaused,
    NeedsSupervisor,
    ObjectiveComplete,
}

impl ObjectiveState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "ACTIVE" => Ok(Self::Active),
            "AWAITING_OWNER" => Ok(Self::AwaitingOwner),
            "OWNER_PAUSED" => Ok(Self::OwnerPaused),
            "NEEDS_SUPERVISOR" => Ok(Self::NeedsSupervisor),
            "OBJECTIVE_COMPLETE" => Ok(Self::ObjectiveComplete),
            _ => Err("Invalid control-plane objective state".into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Objective {
    pub objective_id: String,
    pub client_scope: String,
    pub lane: String,
    pub owner_agent: String,
    pub state: ObjectiveState,
    pub generation: i64,
    pub last_verified_progress_at: i64,
    pub next_review_at: Option<i64>,
    pub last_intervention_at: Option<i64>,
    pub recovery_policy: String,
    pub next_action: String,
    pub updated_at: i64,
}

impl Objective {
    pub(super) fn sentinel(&self) -> &'static str {
        match self.state {
            ObjectiveState::AwaitingOwner => "AWAITING OWNER",
            ObjectiveState::NeedsSupervisor => "NEEDS SUPERVISOR",
            ObjectiveState::ObjectiveComplete => "COMPLETE SENTINEL",
            ObjectiveState::Active | ObjectiveState::OwnerPaused => "",
        }
    }
}

pub(super) struct NewObjective<'a> {
    pub objective_id: &'a str,
    pub client_scope: &'a str,
    pub lane: &'a str,
    pub owner_agent: &'a str,
    pub last_verified_progress_at: i64,
    pub recovery_policy: &'a str,
    pub next_action: &'a str,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProofKind {
    SourceChange,
    TestStarted,
    TestResult,
    BlockerDiagnosis,
    Commit,
    Package,
    ReviewResult,
    DeploymentPreflight,
    OwnerTestPacket,
}

impl ProofKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceChange => "SOURCE_CHANGE",
            Self::TestStarted => "TEST_STARTED",
            Self::TestResult => "TEST_RESULT",
            Self::BlockerDiagnosis => "BLOCKER_DIAGNOSIS",
            Self::Commit => "COMMIT",
            Self::Package => "PACKAGE",
            Self::ReviewResult => "REVIEW_RESULT",
            Self::DeploymentPreflight => "DEPLOYMENT_PREFLIGHT",
            Self::OwnerTestPacket => "OWNER_TEST_PACKET",
        }
    }
}

pub(super) struct ProofInput<'a> {
    pub objective_id: &'a str,
    pub expected_generation: i64,
    pub kind: ProofKind,
    pub occurred_at: i64,
    pub observed_at: i64,
    pub reference: &'a str,
    pub digest: Option<&'a str>,
    pub result: Option<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProofRecord {
    pub id: i64,
    pub generation: i64,
    pub kind: String,
    pub occurred_at: i64,
    pub reference: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BlockerRoute {
    Specialist,
    Supervisor,
    Scrum,
    DependencyRetry,
    HumanOwner,
}

impl BlockerRoute {
    fn as_str(self) -> &'static str {
        match self {
            Self::Specialist => "SPECIALIST",
            Self::Supervisor => "SUPERVISOR",
            Self::Scrum => "SCRUM",
            Self::DependencyRetry => "DEPENDENCY_RETRY",
            Self::HumanOwner => "HUMAN_OWNER",
        }
    }

    fn next_action(self) -> &'static str {
        match self {
            Self::Specialist => "route:specialist",
            Self::Supervisor => "route:supervisor",
            Self::Scrum => "route:scrum",
            Self::DependencyRetry => "route:dependency-retry",
            Self::HumanOwner => "await owner instruction",
        }
    }
}

pub(super) struct BlockerInput<'a> {
    pub objective_id: &'a str,
    pub expected_generation: i64,
    pub route: BlockerRoute,
    pub diagnosed_at: i64,
    pub reference: &'a str,
    pub authority_admission_reference: Option<&'a str>,
}

pub(super) struct HumanAuthorityAdmissionInput<'a> {
    pub objective_id: &'a str,
    pub generation: i64,
    pub operation: &'a str,
    pub actor: &'a str,
    pub admission_reference: &'a str,
    pub admitted_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DispatchState {
    Pending,
    Claimed,
    WakeIntent,
    Sent,
    Acked,
    VerifyBeforeRetry,
    Failed,
}

impl DispatchState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "CLAIMED" => Ok(Self::Claimed),
            "WAKE_INTENT" => Ok(Self::WakeIntent),
            "SENT" => Ok(Self::Sent),
            "ACKED" => Ok(Self::Acked),
            "VERIFY_BEFORE_RETRY" => Ok(Self::VerifyBeforeRetry),
            "FAILED" => Ok(Self::Failed),
            _ => Err("Invalid control-plane dispatch state".into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Dispatch {
    pub id: i64,
    pub objective_id: String,
    pub objective_generation: i64,
    pub attempt: i64,
    pub state: DispatchState,
    pub target_agent_key: String,
    pub target_pubkey: Option<String>,
    pub channel_id: Option<String>,
    pub thread_root_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub ack_marker: Option<String>,
    pub wake_payload: Option<String>,
    pub claimed_at: Option<i64>,
    pub wake_intent_at: Option<i64>,
    pub sent_at: Option<i64>,
    pub ack_event_id: Option<String>,
    pub acknowledged_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub(super) struct WakeIntentInput<'a> {
    pub target_agent_key: &'a str,
    pub target_pubkey: &'a str,
    pub channel_id: &'a str,
    pub thread_root_id: &'a str,
    pub idempotency_key: &'a str,
    pub instruction: &'a str,
}

pub(super) struct SmokePrepareInput<'a> {
    pub objective_id: &'a str,
    pub target_agent_key: &'a str,
    pub target_pubkey: &'a str,
    pub channel_id: &'a str,
    pub thread_root_id: &'a str,
    pub now: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SmokeDispatch {
    pub id: i64,
    pub objective_id: String,
    pub objective_generation: i64,
    pub client_scope: String,
    pub lane: String,
    pub state: DispatchState,
    pub target_agent_key: String,
    pub target_pubkey: String,
    pub channel_id: String,
    pub thread_root_id: String,
    pub idempotency_key: String,
    pub ack_marker: String,
    pub wake_payload: String,
    pub wake_intent_at: Option<i64>,
    pub sent_at: Option<i64>,
    pub ack_event_id: Option<String>,
    pub acknowledged_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone)]
pub(super) struct SignedAckEvent<'a> {
    pub event_id: &'a str,
    pub author_pubkey: &'a str,
    pub channel_id: &'a str,
    pub thread_root_id: &'a str,
    pub content: &'a str,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AckOutcome {
    NoMatch,
    Acknowledged(i64),
    AlreadyAcknowledged(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StartupReconciliation {
    pub acknowledged: Vec<i64>,
    pub wake_dispatches: Vec<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SilentReason {
    Missing,
    Dormant,
    StaleGeneration,
    NotDue,
    AlreadyIntervened,
    ProviderCircuitOpen,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Evaluation {
    Silent(SilentReason),
    Dispatch(Dispatch),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DispatchException {
    pub id: i64,
    pub code: String,
    pub occurred_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ProviderCircuit {
    pub failure_count: i64,
    pub last_failure_at: i64,
    pub open_until: i64,
    pub last_error_code: String,
}

pub(super) struct Store {
    connection: Connection,
}

impl Store {
    pub(super) fn open(path: PathBuf) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or("Control-plane database location unavailable")?;
        std::fs::create_dir_all(parent)
            .map_err(|_| "Control-plane database directory unavailable")?;
        let connection = database(Connection::open(path))?;
        database(connection.busy_timeout(std::time::Duration::from_secs(2)))?;
        database(connection.execute_batch("PRAGMA foreign_keys=ON; BEGIN IMMEDIATE"))?;
        let version: u32 =
            database(connection.query_row("PRAGMA user_version", [], |row| row.get(0)))?;
        if version > 4 {
            let _ = connection.execute_batch("ROLLBACK");
            return Err("Control-plane database was created by a newer Buzz build".into());
        }
        if version == 0 {
            if let Err(error) = connection.execute_batch(include_str!(
                "../../../src/features/control-plane/schema.sql"
            )) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(format!("Control-plane migration failed: {error}"));
            }
        } else if version == 1 {
            if let Err(error) = connection.execute_batch(include_str!(
                "../../../src/features/control-plane/migration-v2.sql"
            )) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(format!("Control-plane migration failed: {error}"));
            }
        }
        if version == 1 || version == 2 {
            if let Err(error) = connection.execute_batch(include_str!(
                "../../../src/features/control-plane/migration-v3.sql"
            )) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(format!("Control-plane migration failed: {error}"));
            }
        }
        if version == 1 || version == 2 || version == 3 {
            if let Err(error) = connection.execute_batch(include_str!(
                "../../../src/features/control-plane/migration-v4.sql"
            )) {
                let _ = connection.execute_batch("ROLLBACK");
                return Err(format!("Control-plane migration failed: {error}"));
            }
        }
        database(connection.execute_batch(
            "PRAGMA user_version=4; COMMIT; PRAGMA journal_mode=WAL; PRAGMA secure_delete=ON;",
        ))?;
        Ok(Self { connection })
    }

    pub(super) fn create_active(&mut self, objective: NewObjective<'_>) -> Result<Objective> {
        if objective.client_scope != "REMH" || objective.lane != "OPERATIONS_CONTROL" {
            return Err("The Phase B control-plane pilot is REMH operations-control only".into());
        }
        let next_review_at = review_deadline(
            objective.recovery_policy,
            ProofKind::TestResult,
            objective.last_verified_progress_at,
        )?;
        database(self.connection.execute(
            "INSERT INTO objectives(objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at) VALUES (?1,?2,?3,?4,'ACTIVE',1,?5,?6,NULL,?7,?8,?9)",
            params![
                objective.objective_id,
                objective.client_scope,
                objective.lane,
                objective.owner_agent,
                objective.last_verified_progress_at,
                next_review_at,
                objective.recovery_policy,
                objective.next_action,
                objective.updated_at,
            ],
        ))?;
        self.objective(objective.objective_id)?
            .ok_or("Control-plane objective was not created".into())
    }

    pub(super) fn objective(&self, objective_id: &str) -> Result<Option<Objective>> {
        database(
            self.connection
                .query_row(
                    "SELECT objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at FROM objectives WHERE objective_id=?1",
                    [objective_id],
                    objective_row,
                )
                .optional(),
        )
    }

    pub(super) fn admit_human_authority(
        &mut self,
        admission: HumanAuthorityAdmissionInput<'_>,
    ) -> Result<()> {
        if admission.objective_id.is_empty()
            || admission.objective_id.len() > 256
            || admission.generation < 1
            || admission.operation.is_empty()
            || admission.operation.len() > 128
            || admission.actor.is_empty()
            || admission.actor.len() > 128
            || admission.admission_reference.is_empty()
            || admission.admission_reference.len() > 512
            || admission.admitted_at < 0
        {
            return Err("Invalid human authority admission".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective = objective_in(&tx, admission.objective_id)?
            .ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active || objective.generation != admission.generation
        {
            return Err("Human authority admission requires the current ACTIVE generation".into());
        }
        database(tx.execute(
            "INSERT INTO human_authority_admissions(admission_reference,objective_id,generation,operation,actor,admitted_at,consumed_at) VALUES (?1,?2,?3,?4,?5,?6,NULL)",
            params![admission.admission_reference,admission.objective_id,admission.generation,admission.operation,admission.actor,admission.admitted_at],
        ))?;
        database(tx.commit())
    }

    pub(super) fn record_verified_progress(&mut self, proof: ProofInput<'_>) -> Result<Objective> {
        self.record_qualified_proof(proof)
    }

    pub(super) fn record_qualified_proof(&mut self, proof: ProofInput<'_>) -> Result<Objective> {
        validate_proof(&proof)?;
        validate_proof_time(&proof, trusted_unix_time()?)?;
        let canonical_identity = canonical_proof_identity(&proof);
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective =
            objective_in(&tx, proof.objective_id)?.ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active {
            return Err("Verified progress requires an ACTIVE objective".into());
        }
        if objective.generation != proof.expected_generation {
            return Err("Proof generation is stale".into());
        }
        if duplicate_proof(&tx, &proof, &canonical_identity)? {
            return Err("Duplicate proof cannot advance an objective".into());
        }
        let advances = proof_advances(&proof)?;
        if advances && proof.occurred_at <= objective.last_verified_progress_at {
            return Err("Stale proof cannot advance an objective".into());
        }
        let generation = objective
            .generation
            .checked_add(i64::from(advances))
            .ok_or("Control-plane generation overflow")?;
        let classification = if advances {
            "ADVANCING"
        } else {
            "NON_ADVANCING"
        };
        database(tx.execute(
            "INSERT INTO proof_events(objective_id,source_generation,resulting_generation,proof_type,occurred_at,observed_at,classification,advances_generation,reference,new_event_identity,digest,result,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?6)",
            params![proof.objective_id,objective.generation,generation,proof.kind.as_str(),proof.occurred_at,proof.observed_at,classification,i64::from(advances),proof.reference,canonical_identity,proof.digest,proof.result],
        ))?;
        if advances {
            let next_review_at =
                review_deadline(&objective.recovery_policy, proof.kind, proof.occurred_at)?;
            database(tx.execute(
                "UPDATE objectives SET generation=?2,last_verified_progress_at=?3,next_review_at=?4,updated_at=?5 WHERE objective_id=?1 AND generation=?6",
                params![proof.objective_id,generation,proof.occurred_at,next_review_at,proof.observed_at,objective.generation],
            ))?;
        }
        database(tx.commit())?;
        self.objective(proof.objective_id)?
            .ok_or("Control-plane objective disappeared".into())
    }

    pub(super) fn record_status(
        &self,
        objective_id: &str,
        _status: &str,
        _observed_at: i64,
    ) -> Result<Objective> {
        self.objective(objective_id)?
            .ok_or("Control-plane objective not found".into())
    }

    pub(super) fn ready_for_owner_test(
        &mut self,
        proof: ProofInput<'_>,
        updated_at: i64,
        authority_admission_reference: &str,
    ) -> Result<Objective> {
        if proof.kind != ProofKind::OwnerTestPacket {
            return Err("READY FOR OWNER TEST requires an OWNER_TEST_PACKET proof".into());
        }
        if authority_admission_reference.is_empty() || authority_admission_reference.len() > 512 {
            return Err("AWAITING_OWNER requires admitted human authority".into());
        }
        validate_proof(&proof)?;
        validate_proof_time(&proof, trusted_unix_time()?)?;
        let canonical_identity = canonical_proof_identity(&proof);
        if proof.result != Some("READY FOR OWNER TEST") {
            return Err("Owner-test packet requires the exact READY FOR OWNER TEST result".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective =
            objective_in(&tx, proof.objective_id)?.ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active {
            return Err("READY FOR OWNER TEST requires an ACTIVE objective".into());
        }
        if objective.generation != proof.expected_generation {
            return Err("Owner-test packet generation is stale".into());
        }
        if duplicate_proof(&tx, &proof, &canonical_identity)? {
            return Err("Duplicate owner-test packet cannot advance an objective".into());
        }
        if proof.occurred_at <= objective.last_verified_progress_at {
            return Err("Stale owner-test packet cannot advance an objective".into());
        }
        consume_human_authority(
            &tx,
            proof.objective_id,
            objective.generation,
            authority_admission_reference,
            updated_at,
        )?;
        let generation = objective
            .generation
            .checked_add(1)
            .ok_or("Control-plane generation overflow")?;
        database(tx.execute(
            "INSERT INTO blocker_routes(objective_id,generation,route,reference,authority_admission_reference,diagnosed_at) VALUES (?1,?2,'HUMAN_OWNER',?3,?4,?5)",
            params![proof.objective_id,objective.generation,proof.reference,authority_admission_reference,updated_at],
        ))?;
        database(tx.execute(
            "INSERT INTO proof_events(objective_id,source_generation,resulting_generation,proof_type,occurred_at,observed_at,classification,advances_generation,reference,new_event_identity,digest,result,created_at) VALUES (?1,?2,?3,?4,?5,?6,'ADVANCING',1,?7,?8,?9,?10,?6)",
            params![proof.objective_id,objective.generation,generation,proof.kind.as_str(),proof.occurred_at,proof.observed_at,proof.reference,canonical_identity,proof.digest,proof.result],
        ))?;
        database(tx.execute(
            "UPDATE objectives SET state='AWAITING_OWNER',generation=?2,last_verified_progress_at=?3,next_review_at=NULL,next_action='await owner instruction',updated_at=?4 WHERE objective_id=?1 AND generation=?5",
            params![proof.objective_id,generation,proof.occurred_at,updated_at,objective.generation],
        ))?;
        database(tx.commit())?;
        self.objective(proof.objective_id)?
            .ok_or("Control-plane objective disappeared".into())
    }

    pub(super) fn reactivate(
        &mut self,
        objective_id: &str,
        next_action: &str,
        updated_at: i64,
    ) -> Result<Objective> {
        let objective = self
            .objective(objective_id)?
            .ok_or("Control-plane objective not found")?;
        let next_review_at = review_deadline(
            &objective.recovery_policy,
            ProofKind::TestResult,
            updated_at,
        )?;
        let changed = database(self.connection.execute(
            "UPDATE objectives SET state='ACTIVE',generation=generation+1,next_review_at=?2,next_action=?3,updated_at=?4 WHERE objective_id=?1 AND state IN ('AWAITING_OWNER','OWNER_PAUSED')",
            params![objective_id,next_review_at,next_action,updated_at],
        ))?;
        if changed != 1 {
            return Err("Only dormant owner-held objectives can be reactivated".into());
        }
        self.objective(objective_id)?
            .ok_or("Control-plane objective disappeared".into())
    }

    pub(super) fn route_blocker(&mut self, blocker: BlockerInput<'_>) -> Result<Objective> {
        if blocker.reference.is_empty()
            || blocker.reference.len() > 512
            || blocker
                .authority_admission_reference
                .is_some_and(|value| value.is_empty() || value.len() > 512)
        {
            return Err("Invalid blocker route".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective =
            objective_in(&tx, blocker.objective_id)?.ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active
            || objective.generation != blocker.expected_generation
        {
            return Err("Blocker route requires the current ACTIVE generation".into());
        }
        if blocker.route == BlockerRoute::HumanOwner
            && blocker.authority_admission_reference.is_none()
        {
            return Err("AWAITING_OWNER requires admitted human authority".into());
        }
        if blocker.route != BlockerRoute::HumanOwner
            && blocker.authority_admission_reference.is_some()
        {
            return Err("Non-human blocker routes cannot consume human authority".into());
        }
        if let Some(admission_reference) = blocker.authority_admission_reference {
            consume_human_authority(
                &tx,
                blocker.objective_id,
                blocker.expected_generation,
                admission_reference,
                blocker.diagnosed_at,
            )?;
        }
        database(tx.execute(
            "INSERT INTO blocker_routes(objective_id,generation,route,reference,authority_admission_reference,diagnosed_at) VALUES (?1,?2,?3,?4,?5,?6)",
            params![blocker.objective_id,blocker.expected_generation,blocker.route.as_str(),blocker.reference,blocker.authority_admission_reference,blocker.diagnosed_at],
        ))?;
        if blocker.route == BlockerRoute::HumanOwner {
            database(tx.execute(
                "UPDATE objectives SET state='AWAITING_OWNER',generation=generation+1,next_review_at=NULL,next_action=?2,updated_at=?3 WHERE objective_id=?1 AND generation=?4",
                params![blocker.objective_id,blocker.route.next_action(),blocker.diagnosed_at,blocker.expected_generation],
            ))?;
        } else {
            let interval = match blocker.route {
                BlockerRoute::DependencyRetry => 300,
                BlockerRoute::Supervisor => 600,
                BlockerRoute::Specialist | BlockerRoute::Scrum => 900,
                BlockerRoute::HumanOwner => unreachable!(),
            };
            let routed_review = blocker
                .diagnosed_at
                .checked_add(interval)
                .ok_or("Control-plane review deadline overflow")?;
            let next_review_at = objective
                .next_review_at
                .map_or(routed_review, |current| current.min(routed_review));
            database(tx.execute(
                "UPDATE objectives SET next_review_at=?2,next_action=?3,updated_at=?4 WHERE objective_id=?1 AND generation=?5",
                params![blocker.objective_id,next_review_at,blocker.route.next_action(),blocker.diagnosed_at,blocker.expected_generation],
            ))?;
        }
        database(tx.commit())?;
        self.objective(blocker.objective_id)?
            .ok_or("Control-plane objective disappeared".into())
    }

    pub(super) fn register_required_resource(
        &mut self,
        objective_id: &str,
        generation: i64,
        resource_key: &str,
        registered_at: i64,
    ) -> Result<()> {
        if resource_key.is_empty() || resource_key.len() > 256 {
            return Err("Invalid objective resource".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective =
            objective_in(&tx, objective_id)?.ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active || objective.generation != generation {
            return Err("Resource registration requires the current ACTIVE generation".into());
        }
        database(tx.execute(
            "INSERT INTO objective_resources(objective_id,generation,resource_key,registered_at) VALUES (?1,?2,?3,?4)",
            params![objective_id,generation,resource_key,registered_at],
        ))?;
        database(tx.commit())
    }

    pub(super) fn reconcile_resource(
        &mut self,
        objective_id: &str,
        generation: i64,
        resource_key: &str,
        reference: &str,
        reconciled_at: i64,
    ) -> Result<()> {
        if reference.is_empty() || reference.len() > 512 {
            return Err("Invalid resource reconciliation".into());
        }
        let changed = database(self.connection.execute(
            "UPDATE objective_resources SET reconciliation_reference=?4,reconciled_at=?5 WHERE objective_id=?1 AND generation=?2 AND resource_key=?3 AND reconciled_at IS NULL AND EXISTS(SELECT 1 FROM objectives WHERE objective_id=?1 AND state='ACTIVE' AND generation=?2)",
            params![objective_id,generation,resource_key,reference,reconciled_at],
        ))?;
        if changed != 1 {
            return Err("Resource is not registered for the current generation".into());
        }
        Ok(())
    }

    pub(super) fn complete_objective(&mut self, proof: ProofInput<'_>) -> Result<Objective> {
        validate_proof(&proof)?;
        validate_proof_time(&proof, trusted_unix_time()?)?;
        let canonical_identity = canonical_proof_identity(&proof);
        if proof.kind != ProofKind::ReviewResult || proof.result != Some("FINAL_PASS") {
            return Err("Objective completion requires a FINAL_PASS review proof".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let objective =
            objective_in(&tx, proof.objective_id)?.ok_or("Control-plane objective not found")?;
        if objective.state != ObjectiveState::Active
            || objective.generation != proof.expected_generation
        {
            return Err("Completion requires the current ACTIVE generation".into());
        }
        if proof.occurred_at <= objective.last_verified_progress_at {
            return Err("Final proof is stale or future-skewed".into());
        }
        let resource_counts: (i64, i64) = database(tx.query_row(
            "SELECT COUNT(*),SUM(CASE WHEN reconciled_at IS NOT NULL THEN 1 ELSE 0 END) FROM objective_resources WHERE objective_id=?1 AND generation=?2 AND required=1",
            params![proof.objective_id,proof.expected_generation],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
        ))?;
        if resource_counts.0 == 0 || resource_counts.0 != resource_counts.1 {
            return Err("All required resources must be reconciled before completion".into());
        }
        if duplicate_proof(&tx, &proof, &canonical_identity)? {
            return Err("Duplicate final proof cannot complete an objective".into());
        }
        let generation = objective
            .generation
            .checked_add(1)
            .ok_or("Control-plane generation overflow")?;
        database(tx.execute(
            "INSERT INTO proof_events(objective_id,source_generation,resulting_generation,proof_type,occurred_at,observed_at,classification,advances_generation,reference,new_event_identity,digest,result,created_at) VALUES (?1,?2,?3,?4,?5,?6,'FINAL',1,?7,?8,?9,?10,?6)",
            params![proof.objective_id,objective.generation,generation,proof.kind.as_str(),proof.occurred_at,proof.observed_at,proof.reference,canonical_identity,proof.digest,proof.result],
        ))?;
        let changed = database(tx.execute(
            "UPDATE objectives SET state='OBJECTIVE_COMPLETE',generation=?2,last_verified_progress_at=?3,next_review_at=NULL,next_action='objective complete',updated_at=?4 WHERE objective_id=?1 AND generation=?5 AND state='ACTIVE'",
            params![proof.objective_id,generation,proof.occurred_at,proof.observed_at,objective.generation],
        ))?;
        if changed != 1 {
            return Err("Objective completion lost its generation boundary".into());
        }
        database(tx.commit())?;
        self.objective(proof.objective_id)?
            .ok_or("Control-plane objective disappeared".into())
    }

    pub(super) fn evaluate(
        &mut self,
        objective_id: &str,
        expected_generation: i64,
        now: i64,
        agent_key: &str,
    ) -> Result<Evaluation> {
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let Some(objective) = objective_in(&tx, objective_id)? else {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::Missing));
        };
        if objective.generation != expected_generation {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::StaleGeneration));
        }
        if objective.state != ObjectiveState::Active {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::Dormant));
        }
        if objective
            .next_review_at
            .is_none_or(|deadline| now < deadline)
        {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::NotDue));
        }
        let circuit_open = database(
            tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM provider_circuits WHERE client_scope=?1 AND open_until>?2)",
                params![objective.client_scope, now],
                |row| row.get::<_, bool>(0),
            ),
        )?;
        if circuit_open {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::ProviderCircuitOpen));
        }
        let inserted = database(tx.execute(
            "INSERT OR IGNORE INTO dispatch_outbox(objective_id,objective_generation,attempt,state,target_agent_key,created_at,updated_at) VALUES (?1,?2,1,'PENDING',?3,?4,?4)",
            params![objective_id,expected_generation,agent_key,now],
        ))?;
        if inserted == 0 {
            database(tx.commit())?;
            return Ok(Evaluation::Silent(SilentReason::AlreadyIntervened));
        }
        let id = tx.last_insert_rowid();
        database(tx.execute(
            "UPDATE objectives SET last_intervention_at=?2,updated_at=?2 WHERE objective_id=?1 AND generation=?3",
            params![objective_id,now,expected_generation],
        ))?;
        database(tx.commit())?;
        Ok(Evaluation::Dispatch(
            self.dispatch(id)?
                .ok_or("Control-plane dispatch disappeared")?,
        ))
    }

    pub(super) fn proofs(&self, objective_id: &str) -> Result<Vec<ProofRecord>> {
        let mut statement = database(self.connection.prepare(
            "SELECT id,resulting_generation,proof_type,occurred_at,reference FROM proof_events WHERE objective_id=?1 ORDER BY id",
        ))?;
        let rows = database(statement.query_map([objective_id], |row| {
            Ok(ProofRecord {
                id: row.get(0)?,
                generation: row.get(1)?,
                kind: row.get(2)?,
                occurred_at: row.get(3)?,
                reference: row.get(4)?,
            })
        }))?;
        database(rows.collect())
    }

    pub(super) fn dispatch(&self, dispatch_id: i64) -> Result<Option<Dispatch>> {
        database(
            self.connection
                .query_row(
                    "SELECT id,objective_id,objective_generation,attempt,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,claimed_at,wake_intent_at,sent_at,ack_event_id,acknowledged_at,created_at,updated_at FROM dispatch_outbox WHERE id=?1",
                    [dispatch_id],
                    dispatch_row,
                )
                .optional(),
        )
    }

    pub(super) fn dispatches(&self, objective_id: &str) -> Result<Vec<Dispatch>> {
        let mut statement = database(self.connection.prepare(
            "SELECT id,objective_id,objective_generation,attempt,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,claimed_at,wake_intent_at,sent_at,ack_event_id,acknowledged_at,created_at,updated_at FROM dispatch_outbox WHERE objective_id=?1 ORDER BY id",
        ))?;
        let rows = database(statement.query_map([objective_id], dispatch_row))?;
        database(rows.collect())
    }

    pub(super) fn prepare_smoke_dispatch(
        &mut self,
        input: SmokePrepareInput<'_>,
    ) -> Result<SmokeDispatch> {
        if input.objective_id != SMOKE_OBJECTIVE
            || input.target_agent_key.is_empty()
            || input.target_agent_key.len() > 256
            || input.target_pubkey != SMOKE_PUBKEY
            || input.channel_id != SMOKE_CHANNEL
            || input.thread_root_id != SMOKE_ROOT
        {
            return Err("Smoke preparation is outside the approved FGD test contract".into());
        }
        if let Some(existing) = self.smoke_dispatch_by_objective(input.objective_id)? {
            if existing.target_agent_key != input.target_agent_key
                || existing.target_pubkey != input.target_pubkey
                || existing.channel_id != input.channel_id
                || existing.thread_root_id != input.thread_root_id
            {
                return Err("Existing smoke checkpoint does not match the requested target".into());
            }
            return Ok(existing);
        }

        let idempotency_key = uuid::Uuid::new_v4().to_string();
        let marker = format!("FGD_ACK:{}:1:{}", input.objective_id, idempotency_key);
        let payload = format!("Return exactly this marker as your final response:\n{marker}");
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        database(tx.execute(
            "INSERT INTO smoke_dispatch_outbox(objective_id,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,created_at,updated_at) VALUES (?1,'PENDING',?2,?3,?4,?5,?6,?7,?8,?9,?9)",
            params![input.objective_id,input.target_agent_key,input.target_pubkey,input.channel_id,input.thread_root_id,idempotency_key,marker,payload,input.now],
        ))?;
        let id = tx.last_insert_rowid();
        let changed = database(tx.execute(
            "UPDATE smoke_dispatch_outbox SET state='CLAIMED',updated_at=?2 WHERE id=?1 AND state='PENDING'",
            params![id,input.now],
        ))?;
        if changed != 1 {
            return Err("Smoke dispatch was not prepared".into());
        }
        database(tx.commit())?;
        self.smoke_dispatch(id)?
            .ok_or("Smoke dispatch disappeared".into())
    }

    pub(super) fn smoke_dispatch(&self, dispatch_id: i64) -> Result<Option<SmokeDispatch>> {
        database(
            self.connection
                .query_row(
                    "SELECT id,objective_id,objective_generation,client_scope,lane,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,wake_intent_at,sent_at,ack_event_id,acknowledged_at,created_at,updated_at FROM smoke_dispatch_outbox WHERE id=?1",
                    [dispatch_id],
                    smoke_dispatch_row,
                )
                .optional(),
        )
    }

    fn smoke_dispatch_by_objective(&self, objective_id: &str) -> Result<Option<SmokeDispatch>> {
        database(
            self.connection
                .query_row(
                    "SELECT id,objective_id,objective_generation,client_scope,lane,state,target_agent_key,target_pubkey,channel_id,thread_root_id,idempotency_key,ack_marker,wake_payload,wake_intent_at,sent_at,ack_event_id,acknowledged_at,created_at,updated_at FROM smoke_dispatch_outbox WHERE objective_id=?1",
                    [objective_id],
                    smoke_dispatch_row,
                )
                .optional(),
        )
    }

    pub(super) fn begin_smoke_release(
        &mut self,
        dispatch_id: i64,
        wake_intent_at: i64,
    ) -> Result<SmokeDispatch> {
        let changed = database(self.connection.execute(
            "UPDATE smoke_dispatch_outbox SET state='WAKE_INTENT',wake_intent_at=?2,updated_at=?2 WHERE id=?1 AND state='CLAIMED'",
            params![dispatch_id,wake_intent_at],
        ))?;
        if changed != 1 {
            return Err("Smoke dispatch is not eligible for release".into());
        }
        self.smoke_dispatch(dispatch_id)?
            .ok_or("Smoke dispatch disappeared".into())
    }

    pub(super) fn record_smoke_sent(
        &mut self,
        dispatch_id: i64,
        sent_at: i64,
    ) -> Result<SmokeDispatch> {
        let changed = database(self.connection.execute(
            "UPDATE smoke_dispatch_outbox SET state='SENT',sent_at=?2,updated_at=?2 WHERE id=?1 AND state='WAKE_INTENT' AND wake_intent_at<?2",
            params![dispatch_id,sent_at],
        ))?;
        if changed != 1 {
            return Err("Only an admitted smoke wake may be marked SENT".into());
        }
        self.smoke_dispatch(dispatch_id)?
            .ok_or("Smoke dispatch disappeared".into())
    }

    pub(super) fn reconcile_smoke_dispatch(
        &mut self,
        dispatch_id: i64,
        now: i64,
    ) -> Result<SmokeDispatch> {
        let existing = self
            .smoke_dispatch(dispatch_id)?
            .ok_or("Smoke dispatch not found")?;
        if existing.state == DispatchState::WakeIntent {
            let changed = database(self.connection.execute(
                "UPDATE smoke_dispatch_outbox SET state='VERIFY_BEFORE_RETRY',updated_at=?2 WHERE id=?1 AND state='WAKE_INTENT'",
                params![dispatch_id, now],
            ))?;
            if changed != 1 {
                return Err("Smoke dispatch reconciliation lost its state boundary".into());
            }
        }
        self.smoke_dispatch(dispatch_id)?
            .ok_or("Smoke dispatch disappeared".into())
    }

    pub(super) fn claim_dispatch(
        &mut self,
        dispatch_id: i64,
        holder: &str,
        claimed_at: i64,
        expires_at: i64,
    ) -> Result<Dispatch> {
        if holder.is_empty() || holder.len() > 256 || expires_at <= claimed_at {
            return Err("Invalid dispatch claim".into());
        }
        let changed = database(self.connection.execute(
            "UPDATE dispatch_outbox SET state='CLAIMED',lease_holder=?2,claimed_at=?3,claim_expires_at=?4,updated_at=?3 WHERE id=?1 AND state='PENDING'",
            params![dispatch_id,holder,claimed_at,expires_at],
        ))?;
        if changed != 1 {
            return Err("Dispatch is not available to claim".into());
        }
        self.dispatch(dispatch_id)?
            .ok_or("Control-plane dispatch disappeared".into())
    }

    pub(super) fn record_wake_intent(
        &mut self,
        dispatch_id: i64,
        holder: &str,
        input: WakeIntentInput<'_>,
        wake_intent_at: i64,
    ) -> Result<Dispatch> {
        if holder.is_empty()
            || holder.len() > 256
            || input.target_agent_key.is_empty()
            || input.target_agent_key.len() > 256
            || !hex64(input.target_pubkey)
            || input.channel_id.is_empty()
            || input.channel_id.len() > 256
            || !hex64(input.thread_root_id)
            || input.idempotency_key.is_empty()
            || input.idempotency_key.len() > 256
            || input.instruction.is_empty()
            || input.instruction.len() > 3_500
        {
            return Err("Invalid frozen wake intent".into());
        }
        let dispatch = self
            .dispatch(dispatch_id)?
            .ok_or("Control-plane dispatch not found")?;
        if dispatch.state != DispatchState::Claimed
            || dispatch.target_agent_key != input.target_agent_key
        {
            return Err("Only the claimed target may cross the wake boundary".into());
        }
        let marker = format!(
            "FGD_ACK:{}:{}:{}",
            dispatch.objective_id, dispatch.attempt, input.idempotency_key
        );
        let payload = format!(
            "{}\n\nAfter handling this wake, return exactly this marker as your final response:\n{}",
            input.instruction, marker
        );
        let changed = database(self.connection.execute(
            "UPDATE dispatch_outbox SET state='WAKE_INTENT',target_pubkey=?3,channel_id=?4,thread_root_id=?5,idempotency_key=?6,ack_marker=?7,wake_payload=?8,wake_intent_at=?9,recovery_state=NULL,error_code=NULL,updated_at=?9 WHERE id=?1 AND state='CLAIMED' AND lease_holder=?2 AND claim_expires_at>?9",
            params![dispatch_id,holder,input.target_pubkey,input.channel_id,input.thread_root_id,input.idempotency_key,marker,payload,wake_intent_at],
        ))?;
        if changed != 1 {
            return Err("Dispatch wake intent was not frozen".into());
        }
        self.dispatch(dispatch_id)?
            .ok_or("Control-plane dispatch disappeared".into())
    }

    pub(super) fn record_sent(&mut self, dispatch_id: i64, sent_at: i64) -> Result<Dispatch> {
        let changed = database(self.connection.execute(
            "UPDATE dispatch_outbox SET state='SENT',sent_at=?2,updated_at=?2 WHERE id=?1 AND state='WAKE_INTENT' AND wake_intent_at<?2",
            params![dispatch_id,sent_at],
        ))?;
        if changed != 1 {
            return Err("Only a frozen wake intent may be marked SENT".into());
        }
        self.dispatch(dispatch_id)?
            .ok_or("Control-plane dispatch disappeared".into())
    }

    pub(super) fn acknowledge_signed_event(
        &mut self,
        event: SignedAckEvent<'_>,
    ) -> Result<AckOutcome> {
        if event.event_id.is_empty() || event.event_id.len() > 128 {
            return Ok(AckOutcome::NoMatch);
        }
        if let Some(id) = database(
            self.connection
                .query_row(
                    "SELECT id FROM dispatch_outbox WHERE ack_event_id=?1",
                    [event.event_id],
                    |row| row.get(0),
                )
                .optional(),
        )? {
            return Ok(AckOutcome::AlreadyAcknowledged(id));
        }
        let matched: Option<i64> = database(
            self.connection
                .query_row(
                    "SELECT id FROM dispatch_outbox WHERE state IN ('WAKE_INTENT','SENT') AND target_pubkey=?1 AND channel_id=?2 AND thread_root_id=?3 AND ack_marker=?4 AND wake_intent_at<?5 ORDER BY id LIMIT 1",
                    params![event.author_pubkey,event.channel_id,event.thread_root_id,event.content,event.created_at],
                    |row| row.get(0),
                )
                .optional(),
        )?;
        if let Some(dispatch_id) = matched {
            let changed = database(self.connection.execute(
                "UPDATE dispatch_outbox SET state='ACKED',ack_event_id=?2,acknowledged_at=?3,updated_at=?3 WHERE id=?1 AND state IN ('WAKE_INTENT','SENT') AND ack_event_id IS NULL",
                params![dispatch_id,event.event_id,event.created_at],
            ))?;
            if changed == 1 {
                return Ok(AckOutcome::Acknowledged(dispatch_id));
            }
        }

        if let Some(id) = database(
            self.connection
                .query_row(
                    "SELECT id FROM smoke_dispatch_outbox WHERE ack_event_id=?1",
                    [event.event_id],
                    |row| row.get(0),
                )
                .optional(),
        )? {
            return Ok(AckOutcome::AlreadyAcknowledged(id));
        }
        let matched: Option<i64> = database(
            self.connection
                .query_row(
                    "SELECT id FROM smoke_dispatch_outbox WHERE state IN ('WAKE_INTENT','SENT') AND target_pubkey=?1 AND channel_id=?2 AND thread_root_id=?3 AND ack_marker=?4 AND wake_intent_at<?5 ORDER BY id LIMIT 1",
                    params![event.author_pubkey,event.channel_id,event.thread_root_id,event.content,event.created_at],
                    |row| row.get(0),
                )
                .optional(),
        )?;
        let Some(dispatch_id) = matched else {
            return Ok(AckOutcome::NoMatch);
        };
        let changed = database(self.connection.execute(
            "UPDATE smoke_dispatch_outbox SET state='ACKED',ack_event_id=?2,acknowledged_at=?3,updated_at=?3 WHERE id=?1 AND state IN ('WAKE_INTENT','SENT') AND ack_event_id IS NULL",
            params![dispatch_id,event.event_id,event.created_at],
        ))?;
        Ok(if changed == 1 {
            AckOutcome::Acknowledged(dispatch_id)
        } else {
            AckOutcome::NoMatch
        })
    }

    pub(super) fn reconcile_startup(
        &mut self,
        events: &[SignedAckEvent<'_>],
        now: i64,
        stale_claim_before: i64,
    ) -> Result<StartupReconciliation> {
        let mut acknowledged = Vec::new();
        for event in events {
            if let AckOutcome::Acknowledged(id) = self.acknowledge_signed_event(event.clone())? {
                acknowledged.push(id);
            }
        }
        database(self.connection.execute(
            "UPDATE dispatch_outbox SET state='PENDING',lease_holder=NULL,claimed_at=NULL,claim_expires_at=NULL,recovery_state='STALE_CLAIM_RECOVERED',updated_at=?1 WHERE state='CLAIMED' AND claim_expires_at<=?2",
            params![now,stale_claim_before],
        ))?;
        database(self.connection.execute(
            "UPDATE dispatch_outbox SET state='VERIFY_BEFORE_RETRY',recovery_state='UNCERTAIN_WAKE_BOUNDARY',updated_at=?1 WHERE state='WAKE_INTENT'",
            [now],
        ))?;
        database(self.connection.execute(
            "UPDATE smoke_dispatch_outbox SET state='VERIFY_BEFORE_RETRY',updated_at=?1 WHERE state='WAKE_INTENT'",
            [now],
        ))?;
        Ok(StartupReconciliation {
            acknowledged,
            wake_dispatches: Vec::new(),
        })
    }

    pub(super) fn record_dispatch_failure(
        &mut self,
        dispatch_id: i64,
        code: &str,
        occurred_at: i64,
        open_until: i64,
    ) -> Result<()> {
        if code.is_empty() || code.len() > 128 || open_until <= occurred_at {
            return Err("Invalid dispatch failure".into());
        }
        let tx = database(
            self.connection
                .transaction_with_behavior(TransactionBehavior::Immediate),
        )?;
        let (objective_id, client_scope): (String, String) = database(tx.query_row(
            "SELECT d.objective_id,o.client_scope FROM dispatch_outbox d JOIN objectives o USING(objective_id) WHERE d.id=?1",
            [dispatch_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ))?;
        let changed = database(tx.execute(
            "UPDATE dispatch_outbox SET state='FAILED',error_code=?3,updated_at=?2 WHERE id=?1 AND state IN ('PENDING','CLAIMED')",
            params![dispatch_id, occurred_at, code],
        ))?;
        if changed != 1 {
            return Err("Dispatch failure cannot overwrite a wake boundary or ACK".into());
        }
        database(tx.execute(
            "INSERT INTO dispatch_exceptions(objective_id,dispatch_id,code,occurred_at) VALUES (?1,?2,?3,?4)",
            params![objective_id,dispatch_id,code,occurred_at],
        ))?;
        database(tx.execute(
            "INSERT INTO provider_circuits(client_scope,failure_count,last_failure_at,open_until,last_error_code) VALUES (?1,1,?2,?3,?4) ON CONFLICT(client_scope) DO UPDATE SET failure_count=failure_count+1,last_failure_at=excluded.last_failure_at,open_until=MAX(open_until,excluded.open_until),last_error_code=excluded.last_error_code",
            params![client_scope,occurred_at,open_until,code],
        ))?;
        database(tx.commit())
    }

    pub(super) fn exceptions(&self, objective_id: &str) -> Result<Vec<DispatchException>> {
        let mut statement = database(self.connection.prepare(
            "SELECT id,code,occurred_at FROM dispatch_exceptions WHERE objective_id=?1 ORDER BY id",
        ))?;
        let rows = database(statement.query_map([objective_id], |row| {
            Ok(DispatchException {
                id: row.get(0)?,
                code: row.get(1)?,
                occurred_at: row.get(2)?,
            })
        }))?;
        database(rows.collect())
    }

    pub(super) fn provider_circuit(&self, client_scope: &str) -> Result<Option<ProviderCircuit>> {
        database(
            self.connection
                .query_row(
                    "SELECT failure_count,last_failure_at,open_until,last_error_code FROM provider_circuits WHERE client_scope=?1",
                    [client_scope],
                    |row| {
                        Ok(ProviderCircuit {
                            failure_count: row.get(0)?,
                            last_failure_at: row.get(1)?,
                            open_until: row.get(2)?,
                            last_error_code: row.get(3)?,
                        })
                    },
                )
                .optional(),
        )
    }
}

fn dispatch_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Dispatch> {
    let state: String = row.get(4)?;
    let state = DispatchState::parse(&state).map_err(|message| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message,
            )),
        )
    })?;
    Ok(Dispatch {
        id: row.get(0)?,
        objective_id: row.get(1)?,
        objective_generation: row.get(2)?,
        attempt: row.get(3)?,
        state,
        target_agent_key: row.get(5)?,
        target_pubkey: row.get(6)?,
        channel_id: row.get(7)?,
        thread_root_id: row.get(8)?,
        idempotency_key: row.get(9)?,
        ack_marker: row.get(10)?,
        wake_payload: row.get(11)?,
        claimed_at: row.get(12)?,
        wake_intent_at: row.get(13)?,
        sent_at: row.get(14)?,
        ack_event_id: row.get(15)?,
        acknowledged_at: row.get(16)?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
    })
}

fn smoke_dispatch_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SmokeDispatch> {
    let state: String = row.get(5)?;
    let state = DispatchState::parse(&state).map_err(|message| {
        rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message,
            )),
        )
    })?;
    Ok(SmokeDispatch {
        id: row.get(0)?,
        objective_id: row.get(1)?,
        objective_generation: row.get(2)?,
        client_scope: row.get(3)?,
        lane: row.get(4)?,
        state,
        target_agent_key: row.get(6)?,
        target_pubkey: row.get(7)?,
        channel_id: row.get(8)?,
        thread_root_id: row.get(9)?,
        idempotency_key: row.get(10)?,
        ack_marker: row.get(11)?,
        wake_payload: row.get(12)?,
        wake_intent_at: row.get(13)?,
        sent_at: row.get(14)?,
        ack_event_id: row.get(15)?,
        acknowledged_at: row.get(16)?,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
    })
}

fn hex64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn consume_human_authority(
    connection: &Connection,
    objective_id: &str,
    generation: i64,
    admission_reference: &str,
    consumed_at: i64,
) -> Result<()> {
    let changed = database(connection.execute(
        "UPDATE human_authority_admissions SET consumed_at=?4 WHERE admission_reference=?1 AND objective_id=?2 AND generation=?3 AND operation='REQUEST_OWNER_DECISION' AND consumed_at IS NULL AND admitted_at<=?4",
        params![admission_reference,objective_id,generation,consumed_at],
    ))?;
    if changed != 1 {
        return Err("Human authority admission is missing, stale, mismatched, or consumed".into());
    }
    Ok(())
}

fn validate_proof(proof: &ProofInput<'_>) -> Result<()> {
    if proof.reference.is_empty()
        || proof.reference.len() > 512
        || proof.digest.is_some_and(|value| {
            value.is_empty()
                || value.len() > 128
                || !value.bytes().all(|byte| byte.is_ascii_graphic())
        })
        || proof.result.is_some_and(|value| value.len() > 512)
        || proof.expected_generation < 1
        || proof.observed_at < 0
    {
        return Err("Invalid bounded proof record".into());
    }
    Ok(())
}

fn trusted_unix_time() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "Trusted native clock is before the Unix epoch")?
        .as_secs();
    i64::try_from(seconds).map_err(|_| "Trusted native clock exceeds supported range".into())
}

fn validate_proof_time(proof: &ProofInput<'_>, trusted_now: i64) -> Result<()> {
    let latest_from_observation = proof
        .observed_at
        .checked_add(300)
        .ok_or("Proof timestamp exceeds the allowed range")?;
    if proof.occurred_at > latest_from_observation
        || proof.occurred_at > trusted_now
        || proof.observed_at > trusted_now
    {
        return Err("Proof timestamp exceeds the trusted native clock".into());
    }
    Ok(())
}

fn canonical_proof_identity(proof: &ProofInput<'_>) -> String {
    proof
        .digest
        .map(|digest| format!("digest:{digest}"))
        .unwrap_or_else(|| proof.reference.to_string())
}

fn duplicate_proof(
    connection: &Connection,
    proof: &ProofInput<'_>,
    canonical_identity: &str,
) -> Result<bool> {
    database(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM proof_events WHERE objective_id=?1 AND (new_event_identity=?2 OR reference=?3 OR (?4 IS NOT NULL AND digest=?4)))",
        params![
            proof.objective_id,
            canonical_identity,
            proof.reference,
            proof.digest
        ],
        |row| row.get::<_, bool>(0),
    ))
}

fn proof_advances(proof: &ProofInput<'_>) -> Result<bool> {
    match proof.kind {
        ProofKind::TestStarted | ProofKind::BlockerDiagnosis => Ok(false),
        ProofKind::SourceChange | ProofKind::Commit | ProofKind::Package => {
            if proof.digest.is_none() {
                return Err("Advancing source proof requires a digest".into());
            }
            Ok(true)
        }
        ProofKind::TestResult | ProofKind::ReviewResult | ProofKind::DeploymentPreflight => {
            if proof.result != Some("PASS") {
                return Err("Advancing result proof requires PASS".into());
            }
            Ok(true)
        }
        ProofKind::OwnerTestPacket => {
            Err("OWNER_TEST_PACKET requires the explicit owner-test operation".into())
        }
    }
}

fn review_deadline(policy: &str, kind: ProofKind, occurred_at: i64) -> Result<i64> {
    let interval = match policy {
        "review-100" => 100,
        "review-86400" => 86_400,
        "wake-owner-once-per-generation" => match kind {
            ProofKind::TestStarted | ProofKind::BlockerDiagnosis => 0,
            ProofKind::TestResult | ProofKind::DeploymentPreflight => 900,
            ProofKind::SourceChange | ProofKind::Commit | ProofKind::ReviewResult => 1_800,
            ProofKind::Package => 3_600,
            ProofKind::OwnerTestPacket => 0,
        },
        _ => return Err("Unsupported persisted recovery policy".into()),
    };
    occurred_at
        .checked_add(interval)
        .ok_or("Control-plane review deadline overflow".into())
}

fn objective_in(connection: &Connection, objective_id: &str) -> Result<Option<Objective>> {
    database(
        connection
            .query_row(
                "SELECT objective_id,client_scope,lane,owner_agent,state,generation,last_verified_progress_at,next_review_at,last_intervention_at,recovery_policy,next_action,updated_at FROM objectives WHERE objective_id=?1",
                [objective_id],
                objective_row,
            )
            .optional(),
    )
}

fn objective_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Objective> {
    let state: String = row.get(4)?;
    let state = ObjectiveState::parse(&state).map_err(|message| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                message,
            )),
        )
    })?;
    Ok(Objective {
        objective_id: row.get(0)?,
        client_scope: row.get(1)?,
        lane: row.get(2)?,
        owner_agent: row.get(3)?,
        state,
        generation: row.get(5)?,
        last_verified_progress_at: row.get(6)?,
        next_review_at: row.get(7)?,
        last_intervention_at: row.get(8)?,
        recovery_policy: row.get(9)?,
        next_action: row.get(10)?,
        updated_at: row.get(11)?,
    })
}
