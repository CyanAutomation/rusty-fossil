use serde::{Deserialize, Serialize};

/// A fixed synthetic behaviour selected only by controlled runtime or test
/// configuration. Queue payloads never select a workload scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticScenario {
    Success,
    ControlledFailure,
    Timeout,
    Cancellation,
    BoundedOutput,
    UnexpectedTermination,
}

impl SyntheticScenario {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ControlledFailure => "controlled_failure",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::BoundedOutput => "bounded_output",
            Self::UnexpectedTermination => "unexpected_termination",
        }
    }
}
