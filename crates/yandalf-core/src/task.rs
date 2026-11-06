use crate::{
    decision::Decision,
    domain::{Job, PreparedTask, Session, SessionSpec, TurnResult},
    plugin::{Notification, SerialEvent},
};

pub struct TaskDeciding {
    pub job: Job,
    pub decision: Decision,
}

pub struct TaskPreparing {
    pub job: Job,
    pub existing_session: Option<Session>,
    pub task: PreparedTask,
}

pub struct SessionPreparing {
    pub job: Job,
    pub session: SessionSpec,
}

pub struct TaskCompleted {
    pub job: Job,
    pub result: TurnResult,
}

impl SerialEvent for TaskDeciding {}
impl SerialEvent for TaskPreparing {}
impl SerialEvent for SessionPreparing {}
impl Notification for TaskCompleted {}
