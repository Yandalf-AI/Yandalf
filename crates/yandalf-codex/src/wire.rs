use std::collections::{HashMap, HashSet};

use anyhow::{Result, bail};
use serde_json::{Value, json};
use yandalf_core::domain::{TurnResult, TurnStatus};

type TurnKey = (String, String);

#[derive(Default)]
struct TurnState {
    messages: Vec<Value>,
    usage: Value,
    result: Option<TurnResult>,
}

#[derive(Default)]
pub(super) struct Events {
    turns: HashMap<TurnKey, TurnState>,
    threads: HashSet<String>,
    settled: HashMap<String, String>,
}

impl Events {
    pub(super) fn track_thread(&mut self, thread_id: &str) {
        self.threads.insert(thread_id.into());
    }

    pub(super) fn forget_thread(&mut self, thread_id: &str) {
        self.threads.remove(thread_id);
        self.settled.remove(thread_id);
        self.turns.retain(|(thread, _), _| thread != thread_id);
    }

    pub(super) fn settle_turn(&mut self, thread_id: &str, turn_id: &str) {
        self.turns.remove(&(thread_id.into(), turn_id.into()));
        self.settled.insert(thread_id.into(), turn_id.into());
    }

    pub(super) fn take_result(&mut self, thread_id: &str, turn_id: &str) -> Option<TurnResult> {
        let result = self
            .turns
            .get(&(thread_id.into(), turn_id.into()))?
            .result
            .clone()?;
        self.settle_turn(thread_id, turn_id);
        Some(result)
    }

    pub(super) fn notification(&mut self, method: &str, params: &Value) -> Result<bool> {
        if !matches!(
            method,
            "item/completed" | "thread/tokenUsage/updated" | "turn/completed"
        ) {
            return Ok(false);
        }
        let thread_id = params["threadId"].as_str();
        let turn_id = params["turnId"]
            .as_str()
            .or_else(|| params["turn"]["id"].as_str());
        let (Some(thread_id), Some(turn_id)) = (thread_id, turn_id) else {
            return Ok(false);
        };
        if !self.threads.contains(thread_id)
            || self.settled.get(thread_id).is_some_and(|id| id == turn_id)
        {
            return Ok(false);
        }
        let turn = self
            .turns
            .entry((thread_id.into(), turn_id.into()))
            .or_default();
        match method {
            "item/completed" if params["item"]["type"] == "agentMessage" => {
                let item = &params["item"];
                if item["phase"] == "final_answer"
                    || (item["phase"].is_null()
                        && !turn
                            .messages
                            .iter()
                            .any(|message| message["phase"] == "final_answer"))
                {
                    turn.messages.clear();
                    turn.messages.push(item.clone());
                }
            }
            "thread/tokenUsage/updated" => turn.usage = params["tokenUsage"].clone(),
            "turn/completed" => {
                turn.result = Some(turn_result(
                    &params["turn"],
                    &turn.messages,
                    turn.usage.clone(),
                )?);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

pub(super) fn server_request_response(id: Value, method: &str) -> Value {
    let result = match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            json!({"decision":"decline"})
        }
        "item/permissions/requestApproval" => json!({"permissions":{},"scope":"turn"}),
        _ => {
            return json!({"id":id,"error":{"code":-32601,"message":"Yandalf has no interactive handler for this server request"}});
        }
    };
    json!({"id":id,"result":result})
}

pub(super) fn turn_result(turn: &Value, observed: &[Value], usage: Value) -> Result<TurnResult> {
    let status = match turn["status"].as_str() {
        Some("completed") => TurnStatus::Completed,
        Some("failed") => TurnStatus::Failed,
        Some("interrupted") => TurnStatus::Interrupted,
        other => bail!("unexpected terminal Codex status: {other:?}"),
    };
    let items = turn["items"].as_array().map_or(observed, Vec::as_slice);
    let items = if items.iter().any(|item| item["type"] == "agentMessage") {
        items
    } else {
        observed
    };
    let final_message = items
        .iter()
        .rev()
        .find(|item| item["type"] == "agentMessage" && item["phase"] == "final_answer")
        .or_else(|| {
            items
                .iter()
                .rev()
                .find(|item| item["type"] == "agentMessage" && item["phase"].is_null())
        });
    let text = final_message
        .and_then(|item| item["text"].as_str())
        .or_else(|| turn["error"]["message"].as_str())
        .unwrap_or_default()
        .to_owned();
    Ok(TurnResult {
        status,
        text,
        usage,
    })
}
