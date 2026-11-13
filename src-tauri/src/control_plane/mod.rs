mod store;

use self::store::{
    canonical_database_path, AckOutcome, SignedAckEvent, SmokeDispatch, SmokePrepareInput, Store,
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Mutex};
use tauri::{AppHandle, Manager, State};

pub(crate) struct ControlPlaneHost {
    store: Mutex<Option<Store>>,
    path: Mutex<Option<PathBuf>>,
}

impl Default for ControlPlaneHost {
    fn default() -> Self {
        Self {
            store: Mutex::new(None),
            path: Mutex::new(None),
        }
    }
}

impl ControlPlaneHost {
    #[cfg(test)]
    fn open(path: PathBuf) -> store::Result<Self> {
        Ok(Self {
            store: Mutex::new(Some(Store::open(path.clone())?)),
            path: Mutex::new(Some(path)),
        })
    }

    fn observe_ack(&self, event: SignedAckEvent<'_>) -> store::Result<AckOutcome> {
        self.store
            .lock()
            .map_err(|_| "Control-plane store lock unavailable".to_string())?
            .as_mut()
            .ok_or("Control-plane store is not open".to_string())?
            .acknowledge_signed_event(event)
    }

    fn observe_ack_in_app<R: tauri::Runtime>(
        &self,
        app: &AppHandle<R>,
        event: SignedAckEvent<'_>,
    ) -> store::Result<AckOutcome> {
        self.with_store_in_app(app, |store| store.acknowledge_signed_event(event))
    }

    fn with_store_in_app<R: tauri::Runtime, T>(
        &self,
        app: &AppHandle<R>,
        operation: impl FnOnce(&mut Store) -> store::Result<T>,
    ) -> store::Result<T> {
        let app_data = app
            .path()
            .app_data_dir()
            .map_err(|_| "Control-plane app-data directory unavailable".to_string())?;
        let path = canonical_database_path(&app.config().identifier, &app_data)?;
        {
            let mut current = self
                .path
                .lock()
                .map_err(|_| "Control-plane path lock unavailable".to_string())?;
            let mut store = self
                .store
                .lock()
                .map_err(|_| "Control-plane store lock unavailable".to_string())?;
            if current.as_ref() != Some(&path) || store.is_none() {
                *store = Some(Store::open(path.clone())?);
                *current = Some(path);
            }
            let store = store
                .as_mut()
                .ok_or("Control-plane store is not open".to_string())?;
            operation(store)
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct SignedAckProjection {
    event_id: String,
    author_pubkey: String,
    channel_id: String,
    thread_root_id: String,
    content: String,
    created_at: i64,
}

#[tauri::command]
pub(crate) fn control_plane_observe_ack<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    event: SignedAckProjection,
) -> Result<&'static str, String> {
    let event = SignedAckEvent {
        event_id: &event.event_id,
        author_pubkey: &event.author_pubkey,
        channel_id: &event.channel_id,
        thread_root_id: &event.thread_root_id,
        content: &event.content,
        created_at: event.created_at,
    };
    Ok(match host.observe_ack_in_app(&app, event)? {
        AckOutcome::NoMatch => "NO_MATCH",
        AckOutcome::Acknowledged(_) => "ACKNOWLEDGED",
        AckOutcome::AlreadyAcknowledged(_) => "ALREADY_ACKNOWLEDGED",
    })
}

#[derive(Deserialize)]
pub(crate) struct SmokePrepareRequest {
    objective_id: String,
    target_agent_key: String,
    target_pubkey: String,
    channel_id: String,
    thread_root_id: String,
    now: i64,
}

#[derive(Serialize)]
pub(crate) struct SmokeSnapshot {
    dispatch_id: i64,
    objective_id: String,
    objective_generation: i64,
    client_scope: String,
    lane: String,
    state: &'static str,
    target_agent_key: String,
    target_pubkey: String,
    channel_id: String,
    thread_root_id: String,
    idempotency_key: String,
    ack_marker: String,
    wake_payload: String,
    wake_intent_at: Option<i64>,
    sent_at: Option<i64>,
    ack_event_id: Option<String>,
    acknowledged_at: Option<i64>,
    proof_count: usize,
}

fn smoke_snapshot(dispatch: SmokeDispatch) -> SmokeSnapshot {
    SmokeSnapshot {
        dispatch_id: dispatch.id,
        objective_id: dispatch.objective_id,
        objective_generation: dispatch.objective_generation,
        client_scope: dispatch.client_scope,
        lane: dispatch.lane,
        state: dispatch.state.as_str(),
        target_agent_key: dispatch.target_agent_key,
        target_pubkey: dispatch.target_pubkey,
        channel_id: dispatch.channel_id,
        thread_root_id: dispatch.thread_root_id,
        idempotency_key: dispatch.idempotency_key,
        ack_marker: dispatch.ack_marker,
        wake_payload: dispatch.wake_payload,
        wake_intent_at: dispatch.wake_intent_at,
        sent_at: dispatch.sent_at,
        ack_event_id: dispatch.ack_event_id,
        acknowledged_at: dispatch.acknowledged_at,
        proof_count: 0,
    }
}

#[tauri::command]
pub(crate) fn control_plane_smoke_prepare<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    request: SmokePrepareRequest,
) -> Result<SmokeSnapshot, String> {
    host.with_store_in_app(&app, |store| {
        let dispatch = store.prepare_smoke_dispatch(SmokePrepareInput {
            objective_id: &request.objective_id,
            target_agent_key: &request.target_agent_key,
            target_pubkey: &request.target_pubkey,
            channel_id: &request.channel_id,
            thread_root_id: &request.thread_root_id,
            now: request.now,
        })?;
        Ok(smoke_snapshot(dispatch))
    })
}

#[tauri::command]
pub(crate) fn control_plane_smoke_snapshot<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    dispatch_id: i64,
) -> Result<SmokeSnapshot, String> {
    host.with_store_in_app(&app, |store| {
        let dispatch = store
            .smoke_dispatch(dispatch_id)?
            .ok_or("Smoke dispatch not found")?;
        Ok(smoke_snapshot(dispatch))
    })
}

#[tauri::command]
pub(crate) fn control_plane_smoke_begin_release<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    dispatch_id: i64,
    wake_intent_at: i64,
) -> Result<SmokeSnapshot, String> {
    host.with_store_in_app(&app, |store| {
        let dispatch = store.begin_smoke_release(dispatch_id, wake_intent_at)?;
        Ok(smoke_snapshot(dispatch))
    })
}

#[tauri::command]
pub(crate) fn control_plane_smoke_record_sent<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    dispatch_id: i64,
    sent_at: i64,
) -> Result<SmokeSnapshot, String> {
    host.with_store_in_app(&app, |store| {
        let dispatch = store.record_smoke_sent(dispatch_id, sent_at)?;
        Ok(smoke_snapshot(dispatch))
    })
}

#[tauri::command]
pub(crate) fn control_plane_smoke_reconcile<R: tauri::Runtime>(
    app: AppHandle<R>,
    host: State<'_, ControlPlaneHost>,
    dispatch_id: i64,
    now: i64,
) -> Result<SmokeSnapshot, String> {
    host.with_store_in_app(&app, |store| {
        let dispatch = store.reconcile_smoke_dispatch(dispatch_id, now)?;
        Ok(smoke_snapshot(dispatch))
    })
}

#[cfg(test)]
mod tests;
