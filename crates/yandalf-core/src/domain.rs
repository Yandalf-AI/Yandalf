use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: String,
    pub source: String,
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub text: String,
    #[serde(default)]
    pub context: Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Preparing,
    Submitting,
    Running,
    Uncertain,
    Archiving,
    Finalizing,
    Succeeded,
    Skipped,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Preparing => "preparing",
            Self::Submitting => "submitting",
            Self::Running => "running",
            Self::Uncertain => "uncertain",
            Self::Archiving => "archiving",
            Self::Finalizing => "finalizing",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Skipped | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub event: Event,
    pub status: JobStatus,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub cancel_requested: bool,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub subject: String,
    pub cwd: PathBuf,
    pub thread_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionCreation {
    pub id: String,
    pub subject: String,
    pub cwd: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThreadInspection {
    Missing,
    Found { thread_id: String },
    Ambiguous { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TurnInput {
    pub text: String,
    #[serde(default)]
    pub context: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitInfo {
    pub sha: String,
    pub branch: Option<String>,
    pub origin_url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSpec {
    pub cwd: PathBuf,
    pub title: Option<String>,
    pub git: Option<GitInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PreparedTask {
    pub session: SessionSpec,
    pub input: TurnInput,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TurnResult {
    pub status: TurnStatus,
    pub text: String,
    #[serde(default)]
    pub usage: Value,
}

#[derive(Clone, Debug)]
pub enum Inspection {
    Missing,
    Active { turn_id: String },
    Finished { turn_id: String, result: TurnResult },
    Ambiguous { reason: String },
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}
