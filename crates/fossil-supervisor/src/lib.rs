//! Proof-gated synthetic execution contracts.
//!
//! Proof-gated disposable Docker execution. Docker Engine access is isolated in
//! the `fossil-supervisord` process; callers communicate over a restricted
//! Unix-domain socket and the supervisor reloads durable authorization state.

use async_trait::async_trait;
use fossil_core::ids::RunId;
use fossil_soyuz::ExecutionGateAuthorization;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

mod docker;
mod docker_service;
pub mod ipc;
pub mod pi_events;

pub use docker::{
    DockerConfigError, DockerRuntimeConfig, SupervisorLimitSettings, SupervisorLimits,
};
pub use docker_service::{DockerSupervisor, DockerSupervisorMetricsSnapshot};
pub use fossil_core::execution::SyntheticScenario;
pub use ipc::{
    SupervisorIpcClient, SupervisorIpcError, SupervisorIpcMetricsSnapshot, SupervisorIpcServer,
    read_ipc_token_file,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedExecution {
    run_id: RunId,
    worker_id: String,
    claim_callback_id: String,
    start_callback_id: String,
    container_name: String,
    scenario: SyntheticScenario,
}

impl PreparedExecution {
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalReason {
    Completed,
    NonzeroExit,
    TimedOut,
    Cancelled,
    UnexpectedTermination,
    DockerFailure,
    ResourceLimitFailure,
    ExecutionUncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub run_id: RunId,
    pub exit_code: Option<i32>,
    pub reason: TerminalReason,
    pub diagnostic_summary: String,
    pub output_truncated: bool,
    pub workload_id: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub elapsed_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOutcome {
    Requested,
    AlreadyTerminal,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationOutcome {
    NeverPrepared,
    Prepared,
    Running,
    Terminal,
    Ambiguous,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SupervisorError {
    #[error("execution proof does not match the expected worker")]
    WrongWorker,
    #[error("run already has a conflicting execution identity")]
    IdentityConflict,
    #[error("run is already terminal")]
    AlreadyTerminal,
    #[error("execution state is poisoned and requires review")]
    StateUnavailable,
    #[error("execution lacks a matching durable Soyuz-authorized handoff")]
    AuthorizationRejected,
    #[error("canonical Soyuz authorization is unavailable")]
    SoyuzUnavailable,
    #[error("Docker Engine is unavailable or rejected an operation")]
    DockerUnavailable,
    #[error("container identity or effective security profile is ambiguous")]
    AmbiguousContainerState,
    #[error("container cleanup is pending")]
    CleanupPending,
}

#[async_trait]
pub trait ExecutionSupervisor: Send + Sync {
    async fn prepare(
        &self,
        authorization: ExecutionGateAuthorization,
        scenario: SyntheticScenario,
    ) -> Result<PreparedExecution, SupervisorError>;

    async fn execute(
        &self,
        prepared: PreparedExecution,
    ) -> Result<ExecutionOutcome, SupervisorError>;

    async fn cancel(&self, run_id: RunId) -> Result<CancellationOutcome, SupervisorError>;

    async fn reconcile(&self, run_id: RunId) -> Result<ReconciliationOutcome, SupervisorError>;
}

#[derive(Clone)]
pub struct MockSupervisor {
    worker_id: String,
    state: Arc<Mutex<HashMap<RunId, MockState>>>,
}

#[derive(Clone)]
enum MockState {
    Prepared(PreparedExecution),
    Running(PreparedExecution),
    Terminal(ExecutionOutcome),
    Cancelled,
}

impl MockSupervisor {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            state: Arc::default(),
        }
    }
}

#[async_trait]
impl ExecutionSupervisor for MockSupervisor {
    async fn prepare(
        &self,
        authorization: ExecutionGateAuthorization,
        scenario: SyntheticScenario,
    ) -> Result<PreparedExecution, SupervisorError> {
        if authorization.worker_id().as_str() != self.worker_id {
            return Err(SupervisorError::WrongWorker);
        }
        let run_id = authorization.run_id();
        let identity = PreparedExecution {
            run_id,
            worker_id: authorization.worker_id().to_string(),
            claim_callback_id: authorization.claim_callback_id().to_string(),
            start_callback_id: authorization.start_callback_id().to_string(),
            container_name: container_name(&authorization),
            scenario,
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        match state.get(&run_id) {
            Some(MockState::Prepared(existing) | MockState::Running(existing)) => {
                if existing.worker_id != identity.worker_id
                    || existing.claim_callback_id != identity.claim_callback_id
                    || existing.start_callback_id != identity.start_callback_id
                {
                    return Err(SupervisorError::IdentityConflict);
                }
                Ok(existing.clone())
            }
            Some(MockState::Terminal(_)) | Some(MockState::Cancelled) => {
                Err(SupervisorError::AlreadyTerminal)
            }
            None => {
                state.insert(run_id, MockState::Prepared(identity.clone()));
                Ok(identity)
            }
        }
    }

    async fn execute(
        &self,
        prepared: PreparedExecution,
    ) -> Result<ExecutionOutcome, SupervisorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        match state.get(&prepared.run_id) {
            Some(MockState::Terminal(outcome)) => return Ok(outcome.clone()),
            Some(MockState::Prepared(current)) if current == &prepared => {}
            Some(MockState::Cancelled) => {
                return Ok(outcome(
                    prepared.run_id,
                    None,
                    TerminalReason::Cancelled,
                    "cancelled",
                    false,
                ));
            }
            _ => return Err(SupervisorError::IdentityConflict),
        }
        state.insert(prepared.run_id, MockState::Running(prepared.clone()));
        let result = match prepared.scenario {
            SyntheticScenario::Success => outcome(
                prepared.run_id,
                Some(0),
                TerminalReason::Completed,
                "synthetic workload completed",
                false,
            ),
            SyntheticScenario::ControlledFailure => outcome(
                prepared.run_id,
                Some(17),
                TerminalReason::NonzeroExit,
                "synthetic workload failed",
                false,
            ),
            SyntheticScenario::Timeout => outcome(
                prepared.run_id,
                None,
                TerminalReason::TimedOut,
                "synthetic workload timed out",
                false,
            ),
            SyntheticScenario::Cancellation => outcome(
                prepared.run_id,
                None,
                TerminalReason::Cancelled,
                "synthetic workload cancelled",
                false,
            ),
            SyntheticScenario::BoundedOutput => outcome(
                prepared.run_id,
                Some(0),
                TerminalReason::Completed,
                "synthetic output bounded",
                true,
            ),
            SyntheticScenario::UnexpectedTermination => outcome(
                prepared.run_id,
                None,
                TerminalReason::UnexpectedTermination,
                "synthetic process terminated unexpectedly",
                false,
            ),
        };
        state.insert(prepared.run_id, MockState::Terminal(result.clone()));
        Ok(result)
    }

    async fn cancel(&self, run_id: RunId) -> Result<CancellationOutcome, SupervisorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        match state.get(&run_id) {
            None => Ok(CancellationOutcome::NotFound),
            Some(MockState::Terminal(_)) | Some(MockState::Cancelled) => {
                Ok(CancellationOutcome::AlreadyTerminal)
            }
            Some(MockState::Prepared(_) | MockState::Running(_)) => {
                state.insert(run_id, MockState::Cancelled);
                Ok(CancellationOutcome::Requested)
            }
        }
    }

    async fn reconcile(&self, run_id: RunId) -> Result<ReconciliationOutcome, SupervisorError> {
        let state = self
            .state
            .lock()
            .map_err(|_| SupervisorError::StateUnavailable)?;
        Ok(match state.get(&run_id) {
            None => ReconciliationOutcome::NeverPrepared,
            Some(MockState::Prepared(_)) => ReconciliationOutcome::Prepared,
            Some(MockState::Running(_)) => ReconciliationOutcome::Running,
            Some(MockState::Terminal(_)) | Some(MockState::Cancelled) => {
                ReconciliationOutcome::Terminal
            }
        })
    }
}

fn outcome(
    run_id: RunId,
    exit_code: Option<i32>,
    reason: TerminalReason,
    diagnostic_summary: &str,
    output_truncated: bool,
) -> ExecutionOutcome {
    ExecutionOutcome {
        run_id,
        exit_code,
        reason,
        diagnostic_summary: diagnostic_summary.chars().take(256).collect(),
        output_truncated,
        workload_id: "rusty-fossil-synthetic-v1".to_owned(),
        started_at: None,
        completed_at: None,
        elapsed_ms: None,
    }
}

fn container_name(authorization: &ExecutionGateAuthorization) -> String {
    let identity = format!(
        "{}:{}:{}:{}",
        authorization.run_id(),
        authorization.worker_id(),
        authorization.claim_callback_id(),
        authorization.start_callback_id()
    );
    let hash = identity.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("rusty-fossil-synth-{}-{hash:016x}", authorization.run_id())
}
