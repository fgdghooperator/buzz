use super::store::{
    ActivationFenceBinding, Dispatch, DispatchState, Evaluation, ObjectiveState, Result,
    SilentReason, Store, WakeIntentInput,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AgentRecord {
    pub agent_key: String,
    pub pubkey: String,
    pub active: bool,
    pub revision: String,
}

pub(super) trait AgentDirectory {
    fn records_for(&mut self, agent_key: &str) -> Result<Vec<AgentRecord>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CanonicalRoute {
    pub channel_id: String,
    pub thread_root_id: String,
    pub member_pubkeys: Vec<String>,
    pub revision: String,
}

pub(super) trait RouteDirectory {
    fn routes_for(&mut self, objective_id: &str) -> Result<Vec<CanonicalRoute>>;
}

pub(super) trait TrustedClock {
    fn now(&mut self) -> Result<i64>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AdapterFailure {
    pub code: String,
    pub open_for: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdapterResult {
    Admitted,
    Rejected,
    Error,
    Timeout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AdapterStatus {
    Confirmed { admitted_at: i64 },
    Absent,
    Uncertain,
    Rejected,
}

pub(super) struct ActivationPermit {
    binding: ActivationFenceBinding,
    authority_digest: String,
    admitted_at: i64,
}

impl ActivationPermit {
    pub(super) fn dispatch_id(&self) -> i64 {
        self.binding.dispatch_id
    }

    pub(super) fn idempotency_key(&self) -> &str {
        &self.binding.idempotency_key
    }

    pub(super) fn pubkey(&self) -> &str {
        &self.binding.target_pubkey
    }

    pub(super) fn channel_id(&self) -> &str {
        &self.binding.channel_id
    }

    pub(super) fn thread_root_id(&self) -> &str {
        &self.binding.thread_root_id
    }

    pub(super) fn authority_digest(&self) -> &str {
        &self.authority_digest
    }

    pub(super) fn admitted_at(&self) -> i64 {
        self.admitted_at
    }
}

pub(super) trait ActivationAdapter {
    fn preflight(&mut self, dispatch: &Dispatch) -> std::result::Result<(), AdapterFailure>;

    fn admit(&mut self, permit: ActivationPermit, dispatch: &Dispatch) -> AdapterResult;

    fn status(&mut self, dispatch: &Dispatch) -> Result<AdapterStatus>;
}

pub(super) fn admit_frozen<A: ActivationAdapter>(
    store: &mut Store,
    adapter: &mut A,
    frozen: &Dispatch,
    admission_at: i64,
    authority_digest: &str,
) -> Result<(Dispatch, Option<AdapterResult>)> {
    let binding = fence_binding(frozen)?;
    let Some(binding) = store.consume_activation_fence(&binding, admission_at)? else {
        store.block_activation_fence_if_invalid(&binding, admission_at, false)?;
        return Ok((fresh_dispatch(store, frozen.id)?, None));
    };
    let permit = ActivationPermit {
        binding,
        authority_digest: authority_digest.to_string(),
        admitted_at: admission_at,
    };
    let result = adapter.admit(permit, frozen);
    Ok((fresh_dispatch(store, frozen.id)?, Some(result)))
}

fn fresh_dispatch(store: &Store, dispatch_id: i64) -> Result<Dispatch> {
    store
        .dispatch(dispatch_id)?
        .ok_or("Control-plane dispatch disappeared".into())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ActivationOutcome {
    Silent(SilentReason),
    Rejected(Dispatch),
    Failed(Dispatch),
    Sent(Dispatch),
    Uncertain(Dispatch),
}

pub(super) struct ActivationBoundary<D, R, C, A> {
    store: Store,
    agents: D,
    routes: R,
    clock: C,
    adapter: A,
    holder: String,
}

impl<D, R, C, A> ActivationBoundary<D, R, C, A>
where
    D: AgentDirectory,
    R: RouteDirectory,
    C: TrustedClock,
    A: ActivationAdapter,
{
    pub(super) fn new(
        store: Store,
        agents: D,
        routes: R,
        clock: C,
        adapter: A,
        holder: &str,
    ) -> Self {
        Self {
            store,
            agents,
            routes,
            clock,
            adapter,
            holder: holder.to_string(),
        }
    }

    pub(super) fn store(&self) -> &Store {
        &self.store
    }

    pub(super) fn adapter_mut(&mut self) -> &mut A {
        &mut self.adapter
    }

    pub(super) fn recover_expired_claims(&mut self) -> Result<usize> {
        let now = self.trusted_time(None)?;
        self.store.recover_expired_activation_claims(now)
    }

    pub(super) fn reconcile_quarantined(&mut self, dispatch_id: i64) -> Result<Dispatch> {
        let dispatch = fresh_dispatch(&self.store, dispatch_id)?;
        if dispatch.state != DispatchState::VerifyBeforeRetry
            || dispatch.recovery_state.as_deref() != Some("UNCERTAIN_WAKE_BOUNDARY")
        {
            return Err("Activation dispatch is not quarantined for reconciliation".into());
        }
        let idempotency_key = dispatch
            .idempotency_key
            .clone()
            .ok_or("Quarantined activation dispatch has no idempotency key")?;
        let AdapterStatus::Confirmed { admitted_at } = self.adapter.status(&dispatch)? else {
            return Ok(dispatch);
        };
        self.store
            .record_reconciled_activation_sent(dispatch_id, &idempotency_key, admitted_at)
    }

    pub(super) fn coordinate(
        &mut self,
        objective_id: &str,
        expected_generation: i64,
    ) -> Result<ActivationOutcome> {
        let Some(objective) = self.store.objective(objective_id)? else {
            return Ok(ActivationOutcome::Silent(SilentReason::Missing));
        };
        if objective.generation != expected_generation {
            return Ok(ActivationOutcome::Silent(SilentReason::StaleGeneration));
        }
        if objective.state != ObjectiveState::Active {
            return Ok(ActivationOutcome::Silent(SilentReason::Dormant));
        }
        if self
            .store
            .has_unresolved_prior_activation(objective_id, expected_generation)?
        {
            return Ok(ActivationOutcome::Silent(SilentReason::AlreadyIntervened));
        }

        let pending = self
            .store
            .dispatch_for_generation(objective_id, expected_generation)?;
        if pending
            .as_ref()
            .is_some_and(|dispatch| dispatch.state != DispatchState::Pending)
        {
            return Ok(ActivationOutcome::Silent(SilentReason::AlreadyIntervened));
        }

        let first = match self.resolve_authority(objective_id, &objective.owner_agent) {
            Ok(authority) => authority,
            Err(_) => {
                return Ok(match pending {
                    Some(dispatch) => ActivationOutcome::Rejected(dispatch),
                    None => ActivationOutcome::Silent(SilentReason::NotDue),
                });
            }
        };
        let claim_at = match self.trusted_time(None) {
            Ok(now) => now,
            Err(_) => {
                return Ok(match pending {
                    Some(dispatch) => ActivationOutcome::Rejected(dispatch),
                    None => ActivationOutcome::Silent(SilentReason::NotDue),
                });
            }
        };
        let expires_at = match claim_at.checked_add(60) {
            Some(value) => value,
            None => {
                return Ok(match pending {
                    Some(dispatch) => ActivationOutcome::Rejected(dispatch),
                    None => ActivationOutcome::Silent(SilentReason::NotDue),
                });
            }
        };
        let dispatch = match pending {
            Some(dispatch) => dispatch,
            None => match self.store.evaluate(
                objective_id,
                expected_generation,
                claim_at,
                &first.agent.agent_key,
            )? {
                Evaluation::Silent(reason) => return Ok(ActivationOutcome::Silent(reason)),
                Evaluation::Dispatch(dispatch) => dispatch,
            },
        };
        let dispatch = self.store.claim_activation_dispatch(
            dispatch.id,
            &self.holder,
            claim_at,
            expires_at,
        )?;

        if let Err(failure) = self.adapter.preflight(&dispatch) {
            if failure.code.is_empty() || failure.code.len() > 128 || failure.open_for <= 0 {
                return Ok(ActivationOutcome::Rejected(dispatch));
            }
            let open_until = claim_at
                .checked_add(failure.open_for)
                .ok_or("Provider circuit deadline overflow")?;
            self.store
                .record_dispatch_failure(dispatch.id, &failure.code, claim_at, open_until)?;
            return Ok(ActivationOutcome::Failed(
                self.store
                    .dispatch(dispatch.id)?
                    .ok_or("Control-plane dispatch disappeared")?,
            ));
        }

        let freeze_at = match self.trusted_time(Some(claim_at)) {
            Ok(now) => now,
            Err(_) => return Ok(ActivationOutcome::Rejected(dispatch)),
        };
        let second = match self.resolve_authority(objective_id, &objective.owner_agent) {
            Ok(authority) if authority == first => authority,
            _ => {
                let released =
                    self.store
                        .release_activation_claim(dispatch.id, &self.holder, freeze_at)?;
                return Ok(ActivationOutcome::Rejected(released));
            }
        };
        let authority_digest = authority_revision(&second);
        let idempotency_key = deterministic_idempotency_key(&dispatch, &second);
        let frozen = match self.store.record_activation_wake_intent(
            dispatch.id,
            &self.holder,
            WakeIntentInput {
                target_agent_key: &second.agent.agent_key,
                target_pubkey: &second.agent.pubkey,
                channel_id: &second.route.channel_id,
                thread_root_id: &second.route.thread_root_id,
                idempotency_key: &idempotency_key,
                instruction: &objective.next_action,
            },
            freeze_at,
        ) {
            Ok(dispatch) => dispatch,
            Err(_) => return Ok(ActivationOutcome::Rejected(dispatch)),
        };

        let admission_at = match self.trusted_time(Some(freeze_at)) {
            Ok(now) => now,
            Err(_) => {
                return Ok(ActivationOutcome::Uncertain(fresh_dispatch(
                    &self.store,
                    frozen.id,
                )?));
            }
        };
        let (post_fence, adapter_result) = admit_frozen(
            &mut self.store,
            &mut self.adapter,
            &frozen,
            admission_at,
            &authority_digest,
        )?;
        match adapter_result {
            Some(AdapterResult::Admitted) => {
                let sent_at = match self.trusted_time(Some(admission_at)) {
                    Ok(now) => now,
                    Err(_) => {
                        return Ok(ActivationOutcome::Uncertain(fresh_dispatch(
                            &self.store,
                            frozen.id,
                        )?));
                    }
                };
                Ok(ActivationOutcome::Sent(self.store.record_activation_sent(
                    frozen.id,
                    frozen.idempotency_key.as_deref().unwrap_or_default(),
                    sent_at,
                )?))
            }
            Some(AdapterResult::Rejected | AdapterResult::Error | AdapterResult::Timeout)
            | None => Ok(ActivationOutcome::Uncertain(post_fence)),
        }
    }

    fn trusted_time(&mut self, previous: Option<i64>) -> Result<i64> {
        let now = self.clock.now()?;
        if now < 0 || previous.is_some_and(|value| now < value) {
            return Err("Trusted activation clock is negative or regressed".into());
        }
        Ok(now)
    }

    fn resolve_authority(&mut self, objective_id: &str, owner_agent: &str) -> Result<Authority> {
        let records = self.agents.records_for(owner_agent)?;
        if records.len() != 1 {
            return Err("Canonical activation agent is missing or ambiguous".into());
        }
        let agent = records.into_iter().next().unwrap();
        if !agent.active
            || agent.agent_key != owner_agent
            || !hex64(&agent.pubkey)
            || agent.revision.is_empty()
            || agent.revision.len() > 256
        {
            return Err("Canonical activation agent is inactive or mismatched".into());
        }
        let routes = self.routes.routes_for(objective_id)?;
        if routes.len() != 1 {
            return Err("Canonical activation route is missing or ambiguous".into());
        }
        let route = routes.into_iter().next().unwrap();
        if uuid::Uuid::parse_str(&route.channel_id).is_err()
            || !hex64(&route.thread_root_id)
            || route.revision.is_empty()
            || route.revision.len() > 256
            || !route
                .member_pubkeys
                .iter()
                .any(|pubkey| pubkey == &agent.pubkey)
        {
            return Err("Canonical activation route is invalid or lacks membership".into());
        }
        Ok(Authority { agent, route })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Authority {
    agent: AgentRecord,
    route: CanonicalRoute,
}

pub(super) fn fence_binding(dispatch: &Dispatch) -> Result<ActivationFenceBinding> {
    Ok(ActivationFenceBinding {
        dispatch_id: dispatch.id,
        objective_id: dispatch.objective_id.clone(),
        generation: dispatch.objective_generation,
        holder: dispatch
            .lease_holder
            .clone()
            .ok_or("Activation dispatch has no claim holder")?,
        target_agent_key: dispatch.target_agent_key.clone(),
        target_pubkey: dispatch
            .target_pubkey
            .clone()
            .ok_or("Activation dispatch has no pubkey")?,
        channel_id: dispatch
            .channel_id
            .clone()
            .ok_or("Activation dispatch has no channel")?,
        thread_root_id: dispatch
            .thread_root_id
            .clone()
            .ok_or("Activation dispatch has no thread root")?,
        idempotency_key: dispatch
            .idempotency_key
            .clone()
            .ok_or("Activation dispatch has no idempotency key")?,
    })
}

fn deterministic_idempotency_key(dispatch: &Dispatch, authority: &Authority) -> String {
    let authority_revision = authority_revision(authority);
    let mut digest = Sha256::new();
    for value in [
        dispatch.objective_id.as_str(),
        &dispatch.objective_generation.to_string(),
        &dispatch.attempt.to_string(),
        authority.agent.pubkey.as_str(),
        authority.route.channel_id.as_str(),
        authority.route.thread_root_id.as_str(),
        authority_revision.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    format!("remh-oi:{authority_revision}:{:x}", digest.finalize())
}

fn authority_revision(authority: &Authority) -> String {
    let mut members = authority.route.member_pubkeys.clone();
    members.sort();
    let mut digest = Sha256::new();
    for value in [
        authority.agent.agent_key.as_str(),
        authority.agent.pubkey.as_str(),
        authority.agent.revision.as_str(),
        authority.route.channel_id.as_str(),
        authority.route.thread_root_id.as_str(),
        authority.route.revision.as_str(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    for member in members {
        digest.update(member.as_bytes());
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

pub(super) fn authority_revision_for(agent: AgentRecord, route: CanonicalRoute) -> String {
    authority_revision(&Authority { agent, route })
}

fn hex64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
