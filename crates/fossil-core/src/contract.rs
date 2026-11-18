use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContractVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoyuzRunStatus {
    Admitting,
    Queued,
    Claimed,
    Running,
    CancelRequested,
    Completed,
    Failed,
    Cancelled,
    AdmissionFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalEventType {
    StageChanged,
    StepChanged,
    ProgressUpdate,
    Heartbeat,
    ValidationCompleted,
    PublicationCompleted,
    ArtifactsUploaded,
}

impl SoyuzRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitting => "admitting",
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::AdmissionFailed => "admission_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiSuccess<T> {
    pub data: T,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiErrorEnvelope {
    pub error: ApiErrorBody,
}
