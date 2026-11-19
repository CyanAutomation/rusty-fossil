use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalLifecycleState {
    QueueObserved,
    MessageValidated,
    CanonicalRunChecked,
    ClaimIntentPersisted,
    ClaimAuthorized,
    StartAuthorizationPersisted,
    ExecutionActive,
    CancellationRequested,
    ExecutionTerminal,
}

impl LocalLifecycleState {
    pub fn transition(self, to: Self) -> Result<Self, LifecycleError> {
        let valid = matches!(
            (self, to),
            (Self::QueueObserved, Self::MessageValidated)
                | (Self::MessageValidated, Self::CanonicalRunChecked)
                | (Self::CanonicalRunChecked, Self::ClaimIntentPersisted)
                | (Self::ClaimIntentPersisted, Self::ClaimAuthorized)
                | (Self::ClaimAuthorized, Self::StartAuthorizationPersisted)
                | (Self::StartAuthorizationPersisted, Self::ExecutionActive)
                | (Self::ExecutionActive, Self::CancellationRequested)
                | (Self::ExecutionActive, Self::ExecutionTerminal)
                | (Self::CancellationRequested, Self::ExecutionTerminal)
                | (Self::ClaimIntentPersisted, Self::ExecutionTerminal)
                | (Self::ClaimAuthorized, Self::ExecutionTerminal)
        );
        if valid {
            Ok(to)
        } else {
            Err(LifecycleError::InvalidTransition { from: self, to })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackDeliveryState {
    NotRequired,
    Pending,
    Delivered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LifecycleError {
    #[error("invalid local lifecycle transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: LocalLifecycleState,
        to: LocalLifecycleState,
    },
}
