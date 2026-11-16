use super::store::{
    AckOutcome, Dispatch, DispatchState, Evaluation, Objective, ProofInput, Result, SignedAckEvent,
    SilentReason, StartupReconciliation, Store, WakeIntentInput,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DispatchFailure {
    pub code: String,
    pub open_until: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WakeAdmission {
    Admitted,
    Uncertain,
}

pub(super) struct WakeTarget<'a> {
    pub agent_key: &'a str,
    pub pubkey: &'a str,
    pub channel_id: &'a str,
    pub thread_root_id: &'a str,
    pub instruction: &'a str,
}

pub(super) trait WakeAdapter {
    fn preflight(
        &mut self,
        dispatch: &Dispatch,
        target: &WakeTarget<'_>,
        idempotency_key: &str,
    ) -> std::result::Result<(), DispatchFailure>;

    fn before_admission_guard(&mut self, _dispatch: &Dispatch) {}

    fn admit(&mut self, dispatch: &Dispatch) -> WakeAdmission;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Coordination {
    Silent(SilentReason),
    Sent(Dispatch),
    Failed(Dispatch),
    Uncertain(Dispatch),
}

pub(super) struct Coordinator<A> {
    store: Store,
    adapter: A,
    holder: String,
}

impl<A: WakeAdapter> Coordinator<A> {
    pub(super) fn new(store: Store, adapter: A, holder: &str) -> Self {
        Self {
            store,
            adapter,
            holder: holder.to_string(),
        }
    }

    pub(super) fn store(&self) -> &Store {
        &self.store
    }

    pub(super) fn store_mut(&mut self) -> &mut Store {
        &mut self.store
    }

    pub(super) fn coordinate_due(
        &mut self,
        objective_id: &str,
        now: i64,
        target: WakeTarget<'_>,
    ) -> Result<Coordination> {
        let Some(objective) = self.store.objective(objective_id)? else {
            return Ok(Coordination::Silent(SilentReason::Missing));
        };
        let dispatch =
            match self
                .store
                .evaluate(objective_id, objective.generation, now, target.agent_key)?
            {
                Evaluation::Silent(SilentReason::AlreadyIntervened) => {
                    match self
                        .store
                        .dispatch_for_generation(objective_id, objective.generation)?
                    {
                        Some(dispatch) if dispatch.state == DispatchState::Pending => dispatch,
                        _ => {
                            return Ok(Coordination::Silent(SilentReason::AlreadyIntervened));
                        }
                    }
                }
                Evaluation::Silent(reason) => return Ok(Coordination::Silent(reason)),
                Evaluation::Dispatch(dispatch) => dispatch,
            };
        let expires_at = now
            .checked_add(60)
            .ok_or("Dispatch claim deadline overflow")?;
        let dispatch = self
            .store
            .claim_dispatch(dispatch.id, &self.holder, now, expires_at)?;
        let idempotency_key = deterministic_idempotency_key(&dispatch, &target);
        if let Err(failure) = self.adapter.preflight(&dispatch, &target, &idempotency_key) {
            self.store.record_dispatch_failure(
                dispatch.id,
                &failure.code,
                now,
                failure.open_until,
            )?;
            return Ok(Coordination::Failed(
                self.store
                    .dispatch(dispatch.id)?
                    .ok_or("Control-plane dispatch disappeared")?,
            ));
        }
        let wake_intent_at = now.checked_add(1).ok_or("Wake-intent timestamp overflow")?;
        let dispatch = self.store.record_wake_intent(
            dispatch.id,
            &self.holder,
            WakeIntentInput {
                target_agent_key: target.agent_key,
                target_pubkey: target.pubkey,
                channel_id: target.channel_id,
                thread_root_id: target.thread_root_id,
                idempotency_key: &idempotency_key,
                instruction: target.instruction,
            },
            wake_intent_at,
        )?;
        self.adapter.before_admission_guard(&dispatch);
        if !self.store.admission_ready(dispatch.id, wake_intent_at)? {
            return Ok(Coordination::Uncertain(dispatch));
        }
        match self.adapter.admit(&dispatch) {
            WakeAdmission::Admitted => {
                let sent_at = wake_intent_at
                    .checked_add(1)
                    .ok_or("Sent timestamp overflow")?;
                Ok(Coordination::Sent(
                    self.store.record_sent(dispatch.id, sent_at)?,
                ))
            }
            WakeAdmission::Uncertain => Ok(Coordination::Uncertain(dispatch)),
        }
    }

    pub(super) fn acknowledge(&mut self, event: SignedAckEvent<'_>) -> Result<AckOutcome> {
        self.store.acknowledge_signed_event(event)
    }

    pub(super) fn record_proof(&mut self, proof: ProofInput<'_>) -> Result<Objective> {
        self.store.record_qualified_proof(proof)
    }

    pub(super) fn reconcile_startup(
        &mut self,
        events: &[SignedAckEvent<'_>],
        now: i64,
        stale_claim_before: i64,
    ) -> Result<StartupReconciliation> {
        self.store
            .reconcile_startup(events, now, stale_claim_before)
    }
}

fn deterministic_idempotency_key(dispatch: &Dispatch, target: &WakeTarget<'_>) -> String {
    let mut digest = Sha256::new();
    for value in [
        dispatch.objective_id.as_str(),
        &dispatch.objective_generation.to_string(),
        &dispatch.attempt.to_string(),
        target.agent_key,
        target.pubkey,
        target.channel_id,
        target.thread_root_id,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(72);
    encoded.push_str("remh-oi:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
