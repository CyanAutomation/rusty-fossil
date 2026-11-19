use fossil_core::contract::{OperationalEventType, SoyuzRunStatus};
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_core::lifecycle::LocalLifecycleState;
use fossil_core::queue::{CloudflareQueueMessage, QueueEnvelope};
use fossil_queue::{CloudflareQueueClient, QueueDisposition, retry_delay_seconds};
use fossil_soyuz::{
    CallbackDeliveryAuthorization, CancelledCallback, CompletedCallback, FailedCallback,
    OperationalEvent, SoyuzApi, SoyuzClientError, SoyuzRunRead, StartedAuthorization,
    StartedRequest, WorkerClaimRequest,
};
use fossil_storage::{
    CallbackOutboxRecord, CallbackPayload, ExecutionDescriptor, RecoveryAction, SqliteStore,
    callback_retry_deadline_ms,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::watch;

#[derive(Default)]
struct CoordinatorMetrics {
    message_validation_failures: AtomicU64,
    duplicate_deliveries: AtomicU64,
    claim_successes: AtomicU64,
    claim_conflicts: AtomicU64,
    start_successes: AtomicU64,
    start_failures: AtomicU64,
    recovery_operations: AtomicU64,
    callbacks_attempted: AtomicU64,
    callbacks_delivered: AtomicU64,
    callbacks_retried: AtomicU64,
    callbacks_blocked: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorMetricsSnapshot {
    pub message_validation_failures: u64,
    pub duplicate_deliveries: u64,
    pub claim_successes: u64,
    pub claim_conflicts: u64,
    pub start_successes: u64,
    pub start_failures: u64,
    pub recovery_operations: u64,
    pub callbacks_attempted: u64,
    pub callbacks_delivered: u64,
    pub callbacks_retried: u64,
    pub callbacks_blocked: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CallbackReplaySummary {
    pub attempted: u64,
    pub delivered: u64,
    pub retried: u64,
    pub blocked: u64,
}

/// Owns the single Soyuz authorization path from a validated Queue message to
/// a durable descriptor, claim, and start authorization. This coordinator is
/// intentionally not constructed by the production service yet.
pub struct AuthorizationCoordinator {
    soyuz: Arc<dyn SoyuzApi>,
    store: SqliteStore,
    worker_id: WorkerId,
    metrics: Arc<CoordinatorMetrics>,
}

impl AuthorizationCoordinator {
    pub fn new(soyuz: Arc<dyn SoyuzApi>, store: SqliteStore, worker_id: WorkerId) -> Self {
        Self {
            soyuz,
            store,
            worker_id,
            metrics: Arc::new(CoordinatorMetrics::default()),
        }
    }

    pub fn metrics(&self) -> CoordinatorMetricsSnapshot {
        CoordinatorMetricsSnapshot {
            message_validation_failures: self
                .metrics
                .message_validation_failures
                .load(Ordering::Relaxed),
            duplicate_deliveries: self.metrics.duplicate_deliveries.load(Ordering::Relaxed),
            claim_successes: self.metrics.claim_successes.load(Ordering::Relaxed),
            claim_conflicts: self.metrics.claim_conflicts.load(Ordering::Relaxed),
            start_successes: self.metrics.start_successes.load(Ordering::Relaxed),
            start_failures: self.metrics.start_failures.load(Ordering::Relaxed),
            recovery_operations: self.metrics.recovery_operations.load(Ordering::Relaxed),
            callbacks_attempted: self.metrics.callbacks_attempted.load(Ordering::Relaxed),
            callbacks_delivered: self.metrics.callbacks_delivered.load(Ordering::Relaxed),
            callbacks_retried: self.metrics.callbacks_retried.load(Ordering::Relaxed),
            callbacks_blocked: self.metrics.callbacks_blocked.load(Ordering::Relaxed),
        }
    }

    /// Replays durable callback intents independently from Queue intake and
    /// execution-slot capacity. IDs and payloads always come from SQLite.
    pub async fn replay_pending_callbacks(&self, limit: usize) -> CallbackReplaySummary {
        let mut summary = CallbackReplaySummary::default();
        let callbacks = match self.store.pending_callbacks() {
            Ok(callbacks) => callbacks,
            Err(_) => return summary,
        };
        for callback in callbacks.into_iter().take(limit) {
            let now = now_ms();
            let retry_at = callback_retry_deadline_ms(
                now,
                callback.attempts.saturating_add(1),
                callback.event_id,
            );
            if self
                .store
                .begin_callback_attempt(callback.event_id, now, retry_at)
                .is_err()
            {
                continue;
            }
            summary.attempted += 1;
            self.metrics
                .callbacks_attempted
                .fetch_add(1, Ordering::Relaxed);
            match self.deliver_callback(&callback).await {
                Ok(authorization) => {
                    if self.store.mark_callback_delivered(authorization).is_ok() {
                        summary.delivered += 1;
                        self.metrics
                            .callbacks_delivered
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(error) if error.is_retryable() => {
                    let class = callback_error_class(&error);
                    let _ = self
                        .store
                        .defer_callback(callback.event_id, class, retry_at);
                    summary.retried += 1;
                    self.metrics
                        .callbacks_retried
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    let class = callback_error_class(&error);
                    let _ = self.store.block_callback(callback.event_id, class);
                    summary.blocked += 1;
                    self.metrics
                        .callbacks_blocked
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        summary
    }

    async fn deliver_callback(
        &self,
        record: &CallbackOutboxRecord,
    ) -> Result<CallbackDeliveryAuthorization, SoyuzClientError> {
        let authorization = match &record.payload {
            CallbackPayload::Event {
                event_type,
                worker_id,
                claim_callback_id,
                stage,
                step,
                timestamp_ms,
                payload,
            } => {
                let event = OperationalEvent {
                    run_id: record.run_id,
                    event_id: record.event_id,
                    worker_id: worker_id.clone(),
                    claim_callback_id: Some(*claim_callback_id),
                    event_type: *event_type,
                    stage: stage.clone(),
                    step: step.clone(),
                    timestamp: Some(format_timestamp_ms(*timestamp_ms)?),
                    payload: Some(payload.clone()),
                };
                if *event_type == OperationalEventType::Heartbeat {
                    self.soyuz.heartbeat(event).await?
                } else {
                    self.soyuz.event(event).await?
                }
            }
            CallbackPayload::Heartbeat {
                worker_id,
                claim_callback_id,
                timestamp_ms,
            } => {
                self.soyuz
                    .heartbeat(OperationalEvent {
                        run_id: record.run_id,
                        event_id: record.event_id,
                        worker_id: worker_id.clone(),
                        claim_callback_id: Some(*claim_callback_id),
                        event_type: OperationalEventType::Heartbeat,
                        stage: None,
                        step: None,
                        timestamp: Some(format_timestamp_ms(*timestamp_ms)?),
                        payload: None,
                    })
                    .await?
            }
            CallbackPayload::Completed {
                worker_id,
                claim_callback_id,
                completed_at_ms,
                exit_code,
                summary,
                published_url,
                changed_file_count,
                outcome_summary,
            } => {
                self.soyuz
                    .completed(CompletedCallback {
                        run_id: record.run_id,
                        callback_id: record.event_id,
                        worker_id: worker_id.clone(),
                        claim_callback_id: Some(*claim_callback_id),
                        completed_at: Some(format_timestamp_ms(*completed_at_ms)?),
                        exit_code: *exit_code,
                        summary: summary.clone(),
                        published_url: published_url
                            .as_deref()
                            .map(url::Url::parse)
                            .transpose()
                            .map_err(|_| SoyuzClientError::InvalidRequest)?,
                        changed_file_count: *changed_file_count,
                        outcome_summary: outcome_summary.clone(),
                    })
                    .await?
            }
            CallbackPayload::Failed {
                worker_id,
                claim_callback_id,
                completed_at_ms,
                exit_code,
                failure_class,
                failure_message,
                outcome_summary,
            } => {
                self.soyuz
                    .failed(FailedCallback {
                        run_id: record.run_id,
                        callback_id: record.event_id,
                        worker_id: worker_id.clone(),
                        claim_callback_id: Some(*claim_callback_id),
                        completed_at: Some(format_timestamp_ms(*completed_at_ms)?),
                        exit_code: *exit_code,
                        failure_class: failure_class.clone(),
                        failure_message: failure_message.clone(),
                        outcome_summary: outcome_summary.clone(),
                    })
                    .await?
            }
            CallbackPayload::Cancelled {
                worker_id,
                claim_callback_id,
                completed_at_ms,
                exit_code,
                reason,
                outcome_summary,
            } => {
                self.soyuz
                    .cancelled(CancelledCallback {
                        run_id: record.run_id,
                        callback_id: record.event_id,
                        worker_id: worker_id.clone(),
                        claim_callback_id: Some(*claim_callback_id),
                        completed_at: Some(format_timestamp_ms(*completed_at_ms)?),
                        exit_code: *exit_code,
                        reason: reason.clone(),
                        outcome_summary: outcome_summary.clone(),
                    })
                    .await?
            }
        };
        Ok(authorization)
    }

    /// Polling remains opt-in. Capacity and unresolved local recovery state are
    /// checked before every one-message Cloudflare pull.
    pub async fn run(
        self: Arc<Self>,
        queue: &CloudflareQueueClient,
        poll_interval: Duration,
        shutdown: watch::Receiver<bool>,
    ) {
        let store = self.store.clone();
        let coordinator = Arc::clone(&self);
        queue
            .run(
                poll_interval,
                shutdown,
                move || store.execution_capacity_available().unwrap_or(false),
                move |message| {
                    let coordinator = Arc::clone(&coordinator);
                    async move { coordinator.process(message).await }
                },
            )
            .await;
    }

    /// Processes one leased delivery. The Queue layer performs the returned
    /// ack/retry operation; this method never treats Queue data as authorization.
    pub async fn process(&self, message: CloudflareQueueMessage) -> QueueDisposition {
        let envelope = match message.decode_envelope() {
            Ok(envelope) => envelope,
            Err(_) => {
                self.metrics
                    .message_validation_failures
                    .fetch_add(1, Ordering::Relaxed);
                return retry_for(message.attempts);
            }
        };
        let run_id = envelope.run_id;
        let mut record = match self.store.get_run(run_id) {
            Ok(record) => {
                self.metrics
                    .duplicate_deliveries
                    .fetch_add(1, Ordering::Relaxed);
                record
            }
            Err(fossil_storage::StoreError::RunNotFound) => {
                match self.store.observe_run(run_id, self.worker_id.clone()) {
                    Ok(record) => record,
                    Err(_) => return retry_for(message.attempts),
                }
            }
            Err(_) => return retry_for(message.attempts),
        };
        if record.worker_id != self.worker_id {
            return retry_for(message.attempts);
        }
        if record.lifecycle_state == LocalLifecycleState::ExecutionTerminal {
            return QueueDisposition::Acknowledge;
        }

        if record.lifecycle_state == LocalLifecycleState::QueueObserved {
            record = match self
                .store
                .transition(run_id, LocalLifecycleState::MessageValidated)
            {
                Ok(record) => record,
                Err(_) => return retry_for(message.attempts),
            };
        }
        if self
            .store
            .persist_execution_descriptor(ExecutionDescriptor::from(&envelope))
            .is_err()
        {
            let _ = self
                .store
                .record_recovery_action(run_id, RecoveryAction::ManualReview);
            return retry_for(message.attempts);
        }
        if self.store.reserve_intake_slot(run_id).is_err() {
            return self.recover_and_retry(run_id, message.attempts);
        }

        let canonical = match self.soyuz.get_run(run_id).await {
            Ok(run) if run.run_id == run_id => run,
            _ => return self.recover_and_retry(run_id, message.attempts),
        };

        if canonical.cancellation_requested() || canonical.status == SoyuzRunStatus::CancelRequested
        {
            return self.handle_cancellation(record, message.attempts);
        }
        if is_terminal(canonical.status) {
            return self.handle_terminal(record);
        }

        match canonical.status {
            SoyuzRunStatus::Admitting => self.recover_and_retry(run_id, message.attempts),
            SoyuzRunStatus::Queued => {
                if record.lifecycle_state == LocalLifecycleState::MessageValidated {
                    record = match self
                        .store
                        .transition(run_id, LocalLifecycleState::CanonicalRunChecked)
                    {
                        Ok(record) => record,
                        Err(_) => return retry_for(message.attempts),
                    };
                }
                if matches!(
                    record.lifecycle_state,
                    LocalLifecycleState::ClaimAuthorized
                        | LocalLifecycleState::StartAuthorizationPersisted
                        | LocalLifecycleState::ExecutionActive
                ) {
                    let _ = self
                        .store
                        .record_recovery_action(run_id, RecoveryAction::ManualReview);
                    return retry_for(message.attempts);
                }
                self.claim_and_authorize(envelope, record, message.attempts)
                    .await
            }
            SoyuzRunStatus::Claimed => {
                self.reconcile_claim_and_authorize(envelope, record, canonical, message.attempts)
                    .await
            }
            SoyuzRunStatus::Running => {
                if canonical.worker_id.as_ref() != Some(&self.worker_id) {
                    return self.handle_terminal(record);
                }
                if record.lifecycle_state == LocalLifecycleState::ExecutionActive {
                    // A restart must not launch a replacement for work that may
                    // already be running. The durable local state owns recovery.
                    return QueueDisposition::Acknowledge;
                }
                self.reconcile_started_and_handoff(envelope, record, canonical, message.attempts)
                    .await
            }
            SoyuzRunStatus::CancelRequested
            | SoyuzRunStatus::Completed
            | SoyuzRunStatus::Failed
            | SoyuzRunStatus::Cancelled
            | SoyuzRunStatus::AdmissionFailed => unreachable!("handled above"),
        }
    }

    async fn claim_and_authorize(
        &self,
        envelope: QueueEnvelope,
        record: fossil_storage::RunRecord,
        attempts: u32,
    ) -> QueueDisposition {
        let run_id = envelope.run_id;
        let claim_callback_id = match record.claim_callback_id {
            Some(callback_id) => callback_id,
            None => {
                let callback_id = ClaimCallbackId::new();
                if self.store.begin_claim_intent(run_id, callback_id).is_err() {
                    return retry_for(attempts);
                }
                callback_id
            }
        };
        let claim_proof = match self
            .soyuz
            .claim(WorkerClaimRequest {
                run_id,
                callback_id: claim_callback_id,
                worker_id: self.worker_id.clone(),
                lease_seconds: None,
            })
            .await
        {
            Ok(proof) => proof,
            Err(error) => {
                self.metrics.claim_conflicts.fetch_add(1, Ordering::Relaxed);
                match self
                    .soyuz
                    .reconcile_claim(run_id, self.worker_id.clone(), claim_callback_id)
                    .await
                {
                    Ok(proof) => proof,
                    Err(_) => {
                        let action = if error.is_retryable() {
                            RecoveryAction::ReconcileWithSoyuz
                        } else {
                            RecoveryAction::ManualReview
                        };
                        return self.recover_with_action(run_id, attempts, action);
                    }
                }
            }
        };
        self.metrics.claim_successes.fetch_add(1, Ordering::Relaxed);
        if self
            .store
            .record_claim_authorization(run_id, claim_proof)
            .is_err()
        {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        let canonical = match self.soyuz.get_run(run_id).await {
            Ok(run) if run.run_id == run_id => run,
            _ => return self.recover_and_retry(run_id, attempts),
        };
        if canonical.cancellation_requested()
            || canonical.status == SoyuzRunStatus::CancelRequested
            || is_terminal(canonical.status)
        {
            return self.handle_cancellation_or_terminal(run_id, canonical.status, attempts);
        }
        if canonical.status != SoyuzRunStatus::Claimed
            || canonical.worker_id.as_ref() != Some(&self.worker_id)
            || canonical.claim_callback_id != Some(claim_callback_id)
        {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        let record = match self.store.get_run(run_id) {
            Ok(record) => record,
            Err(_) => return retry_for(attempts),
        };
        self.start_and_handoff(envelope, record, attempts).await
    }

    async fn reconcile_claim_and_authorize(
        &self,
        envelope: QueueEnvelope,
        record: fossil_storage::RunRecord,
        canonical: SoyuzRunRead,
        attempts: u32,
    ) -> QueueDisposition {
        let run_id = envelope.run_id;
        if canonical.worker_id.as_ref() != Some(&self.worker_id) {
            return retry_for(attempts);
        }
        let Some(claim_callback_id) = record.claim_callback_id else {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        };
        if canonical.claim_callback_id != Some(claim_callback_id) {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        if record.lifecycle_state == LocalLifecycleState::ClaimIntentPersisted {
            let proof = match self
                .soyuz
                .reconcile_claim(run_id, self.worker_id.clone(), claim_callback_id)
                .await
            {
                Ok(proof) => proof,
                Err(_) => return self.recover_and_retry(run_id, attempts),
            };
            if self
                .store
                .record_claim_authorization(run_id, proof)
                .is_err()
            {
                return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
            }
        } else if record.lifecycle_state != LocalLifecycleState::ClaimAuthorized
            && record.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted
        {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        let record = match self.store.get_run(run_id) {
            Ok(record) => record,
            Err(_) => return retry_for(attempts),
        };
        self.start_and_handoff(envelope, record, attempts).await
    }

    async fn start_and_handoff(
        &self,
        envelope: QueueEnvelope,
        record: fossil_storage::RunRecord,
        attempts: u32,
    ) -> QueueDisposition {
        let run_id = envelope.run_id;
        let Some(claim_callback_id) = record.claim_callback_id else {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        };
        let start_callback_id = match record.start_callback_id {
            Some(callback_id) => callback_id,
            None => {
                let callback_id = EventId::new();
                if self.store.begin_start_intent(run_id, callback_id).is_err() {
                    return retry_for(attempts);
                }
                callback_id
            }
        };
        let started = match self
            .soyuz
            .started(StartedRequest {
                run_id,
                callback_id: start_callback_id,
                worker_id: self.worker_id.clone(),
                claim_callback_id: Some(claim_callback_id),
                started_at: None,
                stage: None,
            })
            .await
        {
            Ok(proof) => proof,
            Err(_) => {
                self.metrics.start_failures.fetch_add(1, Ordering::Relaxed);
                match self
                    .soyuz
                    .reconcile_started(
                        run_id,
                        start_callback_id,
                        self.worker_id.clone(),
                        claim_callback_id,
                    )
                    .await
                {
                    Ok(proof) => proof,
                    Err(_) => return self.recover_and_retry(run_id, attempts),
                }
            }
        };
        self.metrics.start_successes.fetch_add(1, Ordering::Relaxed);
        if self
            .store
            .record_start_authorization(run_id, started.clone())
            .is_err()
        {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        self.verify_and_reserve(envelope, started, attempts).await
    }

    async fn reconcile_started_and_handoff(
        &self,
        envelope: QueueEnvelope,
        record: fossil_storage::RunRecord,
        canonical: SoyuzRunRead,
        attempts: u32,
    ) -> QueueDisposition {
        let run_id = envelope.run_id;
        let Some(claim_callback_id) = record.claim_callback_id else {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        };
        let Some(start_callback_id) = record.start_callback_id else {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        };
        if canonical.claim_callback_id != Some(claim_callback_id)
            || canonical.worker_id.as_ref() != Some(&self.worker_id)
        {
            return self.handle_terminal(record);
        }
        let started = match self
            .soyuz
            .reconcile_started(
                run_id,
                start_callback_id,
                self.worker_id.clone(),
                claim_callback_id,
            )
            .await
        {
            Ok(proof) => proof,
            Err(_) => return self.recover_and_retry(run_id, attempts),
        };
        if record.lifecycle_state == LocalLifecycleState::ClaimAuthorized {
            if self
                .store
                .record_start_authorization(run_id, started.clone())
                .is_err()
            {
                return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
            }
        } else if record.lifecycle_state != LocalLifecycleState::StartAuthorizationPersisted {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        self.verify_and_reserve(envelope, started, attempts).await
    }

    async fn verify_and_reserve(
        &self,
        envelope: QueueEnvelope,
        started: StartedAuthorization,
        attempts: u32,
    ) -> QueueDisposition {
        let run_id = envelope.run_id;
        let gate = match self.soyuz.verify_execution_gate(&started).await {
            Ok(proof) => proof,
            Err(_) => {
                let canonical = match self.soyuz.get_run(run_id).await {
                    Ok(run) if run.run_id == run_id => run,
                    _ => return self.recover_and_retry(run_id, attempts),
                };
                if canonical.cancellation_requested()
                    || canonical.status == SoyuzRunStatus::CancelRequested
                    || is_terminal(canonical.status)
                {
                    return self.handle_cancellation_or_terminal(
                        run_id,
                        canonical.status,
                        attempts,
                    );
                }
                return self.recover_and_retry(run_id, attempts);
            }
        };
        if gate.run_id() != run_id
            || gate.worker_id() != &self.worker_id
            || self
                .store
                .execution_descriptor(run_id)
                .ok()
                .flatten()
                .is_none()
        {
            return self.recover_with_action(run_id, attempts, RecoveryAction::ManualReview);
        }
        match self.store.reserve_execution_slot(run_id) {
            Ok(_) => QueueDisposition::Acknowledge,
            Err(_) => self.recover_and_retry(run_id, attempts),
        }
    }

    fn handle_cancellation(
        &self,
        record: fossil_storage::RunRecord,
        attempts: u32,
    ) -> QueueDisposition {
        if record.lifecycle_state == LocalLifecycleState::ExecutionActive {
            let _ = self
                .store
                .record_recovery_action(record.run_id, RecoveryAction::ManualReview);
            return QueueDisposition::Acknowledge;
        }
        if let Some(claim_callback_id) = record.claim_callback_id {
            let callback = CallbackOutboxRecord::new(
                EventId::new(),
                record.run_id,
                CallbackPayload::Cancelled {
                    worker_id: record.worker_id.clone(),
                    claim_callback_id,
                    completed_at_ms: now_ms(),
                    exit_code: None,
                    reason: Some("cancelled before worker launch".to_owned()),
                    outcome_summary: None,
                },
            );
            if self
                .store
                .transition_terminal_with_callback(record.run_id, callback)
                .is_err()
            {
                return self.recover_with_action(
                    record.run_id,
                    attempts,
                    RecoveryAction::ManualReview,
                );
            }
            let _ = self.store.release_execution_slot(record.run_id);
            return QueueDisposition::Acknowledge;
        }
        self.handle_terminal(record)
    }

    fn handle_cancellation_or_terminal(
        &self,
        run_id: RunId,
        status: SoyuzRunStatus,
        attempts: u32,
    ) -> QueueDisposition {
        let record = match self.store.get_run(run_id) {
            Ok(record) => record,
            Err(_) => return retry_for(attempts),
        };
        if status == SoyuzRunStatus::CancelRequested
            || record.lifecycle_state == LocalLifecycleState::ExecutionActive
        {
            return self.handle_cancellation(record, attempts);
        }
        self.handle_terminal(record)
    }

    fn handle_terminal(&self, record: fossil_storage::RunRecord) -> QueueDisposition {
        if record.lifecycle_state != LocalLifecycleState::ExecutionTerminal
            && self
                .store
                .transition(record.run_id, LocalLifecycleState::ExecutionTerminal)
                .is_err()
        {
            return QueueDisposition::Retry {
                delay_seconds: retry_delay_seconds(1),
            };
        }
        let _ = self.store.release_execution_slot(record.run_id);
        QueueDisposition::Acknowledge
    }

    fn recover_and_retry(&self, run_id: RunId, attempts: u32) -> QueueDisposition {
        self.recover_with_action(run_id, attempts, RecoveryAction::ReconcileWithSoyuz)
    }

    fn recover_with_action(
        &self,
        run_id: RunId,
        attempts: u32,
        action: RecoveryAction,
    ) -> QueueDisposition {
        self.metrics
            .recovery_operations
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.store.record_recovery_action(run_id, action);
        retry_for(attempts)
    }
}

fn is_terminal(status: SoyuzRunStatus) -> bool {
    matches!(
        status,
        SoyuzRunStatus::Completed
            | SoyuzRunStatus::Failed
            | SoyuzRunStatus::Cancelled
            | SoyuzRunStatus::AdmissionFailed
    )
}

fn retry_for(attempts: u32) -> QueueDisposition {
    QueueDisposition::Retry {
        delay_seconds: retry_delay_seconds(attempts),
    }
}

fn format_timestamp_ms(timestamp_ms: i64) -> Result<String, SoyuzClientError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
        .map_err(|_| SoyuzClientError::InvalidRequest)?
        .format(&Rfc3339)
        .map_err(|_| SoyuzClientError::InvalidRequest)
}

fn callback_error_class(error: &SoyuzClientError) -> &'static str {
    match error {
        SoyuzClientError::Timeout | SoyuzClientError::Network | SoyuzClientError::Unavailable => {
            "soyuz_unavailable"
        }
        SoyuzClientError::RateLimited => "soyuz_rate_limited",
        SoyuzClientError::StaleClaim | SoyuzClientError::Conflict => "stale_claim",
        SoyuzClientError::Unauthorized | SoyuzClientError::Forbidden => "soyuz_auth_rejected",
        SoyuzClientError::NotFound => "soyuz_run_not_found",
        SoyuzClientError::InvalidRequest => "invalid_callback",
        SoyuzClientError::Protocol => "soyuz_protocol_error",
        SoyuzClientError::Remote { .. } => "soyuz_remote_error",
        SoyuzClientError::InvalidConfiguration => "invalid_soyuz_configuration",
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fossil_core::queue::{CloudflarePullResponse, CloudflareQueueMessage};
    use fossil_soyuz::{SoyuzClient, SoyuzClientConfig};
    use secrecy::SecretString;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const RUN_ID_TEXT: &str = "f8a0c4c5-d628-7b93-a7cf-4b4629a28853";

    struct RunReadSequence {
        reads: AtomicUsize,
        statuses: Vec<(&'static str, Option<&'static str>)>,
        claim_callback_id: Arc<Mutex<Option<String>>>,
    }

    impl Respond for RunReadSequence {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            let index = self.reads.fetch_add(1, Ordering::SeqCst);
            let (status, cancel_requested_at) = self
                .statuses
                .get(index)
                .or_else(|| self.statuses.last())
                .copied()
                .expect("test run read sequence is not empty");
            let claim_callback_id = self
                .claim_callback_id
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133".to_owned());
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {
                    "runId": RUN_ID_TEXT,
                    "displayId": "soyuz-213",
                    "status": status,
                    "cancelRequestedAt": cancel_requested_at,
                    "claimExpiresAt": "2026-10-02T12:02:00.000Z",
                    "workerId": "fossil-host-01",
                    "claimCallbackId": claim_callback_id,
                    "stage": "running",
                    "updatedAt": "2026-10-02T12:00:00.000Z",
                    "lastHeartbeatAt": "2026-10-02T12:00:00.000Z",
                    "operationalHealth": "healthy"
                },
                "requestId": format!("read-{index}")
            }))
        }
    }

    fn queue_message() -> CloudflareQueueMessage {
        let response: CloudflarePullResponse = serde_json::from_slice(include_bytes!(
            "../../fossil-core/tests/fixtures/cloudflare-pull-v1.json"
        ))
        .unwrap();
        response
            .result
            .unwrap()
            .messages
            .into_iter()
            .next()
            .unwrap()
    }

    fn soyuz_client(server: &MockServer) -> Arc<dyn SoyuzApi> {
        let config = SoyuzClientConfig::new(
            url::Url::parse(&server.uri()).unwrap(),
            SecretString::from("test-worker-token".to_owned()),
            Duration::from_secs(1),
        )
        .unwrap();
        Arc::new(SoyuzClient::new(config).unwrap())
    }

    async fn configure_success(server: &MockServer, final_status: &'static str) {
        let claim_callback_id = Arc::new(Mutex::new(None));
        let sequence = if final_status == "running" {
            vec![("queued", None), ("claimed", None), ("running", None)]
        } else {
            vec![
                ("queued", None),
                ("claimed", None),
                (final_status, Some("2026-10-02T12:00:01.000Z")),
            ]
        };
        Mock::given(method("GET"))
            .and(path(format!("/v1/worker/runs/{RUN_ID_TEXT}")))
            .respond_with(RunReadSequence {
                reads: AtomicUsize::new(0),
                statuses: sequence,
                claim_callback_id: Arc::clone(&claim_callback_id),
            })
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{RUN_ID_TEXT}/claim")))
            .respond_with(move |request: &Request| {
                if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&request.body)
                    && let Some(callback_id) =
                        body.get("callbackId").and_then(|value| value.as_str())
                {
                    *claim_callback_id.lock().unwrap() = Some(callback_id.to_owned());
                }
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "data": {"accepted": true},
                    "requestId": "claim-request"
                }))
            })
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{RUN_ID_TEXT}/started")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {},
                "requestId": "start-request"
            })))
            .expect(1)
            .mount(server)
            .await;
    }

    #[tokio::test]
    // Soyuz requirements: QUEUE-MESSAGE-01, WORKER-CLAIM-01, WORKER-START-01.
    async fn valid_delivery_becomes_a_durable_authorized_handoff_before_ack() {
        let server = MockServer::start().await;
        configure_success(&server, "running").await;
        let directory = tempdir().unwrap();
        let store =
            SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
        let coordinator = AuthorizationCoordinator::new(
            soyuz_client(&server),
            store.clone(),
            WorkerId::parse("fossil-host-01").unwrap(),
        );

        assert_eq!(
            coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        assert_eq!(
            coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        let record = store.get_run(RunId::parse(RUN_ID_TEXT).unwrap()).unwrap();
        assert_eq!(
            record.lifecycle_state,
            LocalLifecycleState::StartAuthorizationPersisted
        );
        assert!(record.start_authorized);
        assert!(store.execution_descriptor(record.run_id).unwrap().is_some());
        assert!(!store.execution_capacity_available().unwrap());
        assert_eq!(coordinator.metrics().claim_successes, 1);
        assert_eq!(coordinator.metrics().start_successes, 1);
        assert_eq!(coordinator.metrics().duplicate_deliveries, 1);
    }

    #[tokio::test]
    // Soyuz requirements: WORKER-START-01 and CANCEL-RUNNING-01.
    async fn canonical_cancellation_after_started_keeps_the_execution_gate_closed() {
        let server = MockServer::start().await;
        configure_success(&server, "cancel_requested").await;
        let directory = tempdir().unwrap();
        let store =
            SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
        let coordinator = AuthorizationCoordinator::new(
            soyuz_client(&server),
            store.clone(),
            WorkerId::parse("fossil-host-01").unwrap(),
        );

        assert_eq!(
            coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        assert_eq!(
            store
                .get_run(RunId::parse(RUN_ID_TEXT).unwrap())
                .unwrap()
                .lifecycle_state,
            LocalLifecycleState::ExecutionTerminal
        );
        let callbacks = store.pending_callbacks().unwrap();
        assert_eq!(callbacks.len(), 1);
        assert!(matches!(
            callbacks[0].payload,
            CallbackPayload::Cancelled { .. }
        ));
        assert!(store.execution_capacity_available().unwrap());
    }

    #[tokio::test]
    // Soyuz requirements: CALLBACK-IDEMPOTENCY-01 and WORKER-FAIL-01.
    async fn persisted_failure_callback_replays_the_same_fenced_request() {
        let server = MockServer::start().await;
        configure_success(&server, "running").await;
        let directory = tempdir().unwrap();
        let store =
            SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
        let worker = WorkerId::parse("fossil-host-01").unwrap();
        let coordinator =
            AuthorizationCoordinator::new(soyuz_client(&server), store.clone(), worker.clone());
        assert_eq!(
            coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );

        let run_id = RunId::parse(RUN_ID_TEXT).unwrap();
        let claim_callback_id = store.get_run(run_id).unwrap().claim_callback_id.unwrap();
        let event_id = EventId::parse("ea870068-6723-4b70-9e9f-53b8271e032d").unwrap();
        let callback = CallbackOutboxRecord::new(
            event_id,
            run_id,
            CallbackPayload::Failed {
                worker_id: worker,
                claim_callback_id,
                completed_at_ms: 1_798_000_000_000,
                exit_code: Some(17),
                failure_class: "worker_runtime".to_owned(),
                failure_message: "Worker exited unexpectedly".to_owned(),
                outcome_summary: None,
            },
        );
        store
            .transition_terminal_with_callback(run_id, callback)
            .unwrap();
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{RUN_ID_TEXT}/failed")))
            .and(wiremock::matchers::body_json(serde_json::json!({
                "contractVersion": "1",
                "callbackId": event_id.to_string(),
                "workerId": "fossil-host-01",
                "claimCallbackId": claim_callback_id.to_string(),
                "completedAt": "2026-12-23T04:26:40Z",
                "exitCode": 17,
                "failureClass": "worker_runtime",
                "failureMessage": "Worker exited unexpectedly"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": {"status": "failed"},
                "requestId": "failure-request"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let result = coordinator.replay_pending_callbacks(10).await;
        assert_eq!(result.attempted, 1);
        assert_eq!(result.delivered, 1);
        assert_eq!(result.retried, 0);
        assert_eq!(result.blocked, 0);
        assert!(store.pending_callbacks().unwrap().is_empty());
    }

    #[tokio::test]
    // Soyuz requirements: CALLBACK-IDEMPOTENCY-01 and WORKER-CLAIM-FENCE-01.
    async fn stale_claim_callback_is_blocked_for_operator_review() {
        let server = MockServer::start().await;
        configure_success(&server, "running").await;
        let directory = tempdir().unwrap();
        let store =
            SqliteStore::open(directory.path().join("private-state/state.sqlite3")).unwrap();
        let worker = WorkerId::parse("fossil-host-01").unwrap();
        let coordinator =
            AuthorizationCoordinator::new(soyuz_client(&server), store.clone(), worker.clone());
        assert_eq!(
            coordinator.process(queue_message()).await,
            QueueDisposition::Acknowledge
        );
        let run_id = RunId::parse(RUN_ID_TEXT).unwrap();
        let claim_callback_id = store.get_run(run_id).unwrap().claim_callback_id.unwrap();
        let event_id = EventId::parse("ec870068-6723-4b70-9e9f-53b8271e032d").unwrap();
        let callback = CallbackOutboxRecord::new(
            event_id,
            run_id,
            CallbackPayload::Cancelled {
                worker_id: worker,
                claim_callback_id,
                completed_at_ms: 1_798_000_000_000,
                exit_code: None,
                reason: Some("Cancellation confirmed".to_owned()),
                outcome_summary: None,
            },
        );
        store
            .transition_terminal_with_callback(run_id, callback)
            .unwrap();
        Mock::given(method("POST"))
            .and(path(format!("/v1/worker/runs/{RUN_ID_TEXT}/cancelled")))
            .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
                "error": {
                    "code": "STALE_CLAIM",
                    "message": "claim is stale",
                    "requestId": "stale-request"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let result = coordinator.replay_pending_callbacks(10).await;
        assert_eq!(result.attempted, 1);
        assert_eq!(result.blocked, 1);
        assert_eq!(store.callback_outbox_counts().unwrap(), (0, 1));
        assert_eq!(
            store.get_run(run_id).unwrap().callback_delivery,
            fossil_core::lifecycle::CallbackDeliveryState::Blocked
        );
    }
}
