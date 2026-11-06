use std::{collections::HashSet, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use yandalf_core::{
    domain::{Inspection, SessionSpec, ThreadInspection, TurnInput, TurnResult},
    executor::Executor,
};

use crate::{CodexConfig, transport::Transport, wire::turn_result};

pub struct Codex {
    transport: Transport,
    loaded_threads: Mutex<HashSet<String>>,
}

impl Codex {
    pub async fn connect(config: &CodexConfig) -> Result<Self> {
        Ok(Self {
            transport: Transport::connect(config).await?,
            loaded_threads: Mutex::new(HashSet::new()),
        })
    }

    pub async fn wait_closed(&self) -> String {
        self.transport.wait_closed().await
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        self.transport.request(method, params).await
    }

    async fn read_thread(&self, thread_id: &str) -> Result<Value> {
        let response = self
            .rpc(
                "thread/read",
                json!({"threadId":thread_id,"includeTurns":false}),
            )
            .await?;
        response
            .get("thread")
            .filter(|thread| thread.is_object())
            .cloned()
            .context("thread/read omitted thread metadata")
    }

    async fn list_threads(&self, cursor: Option<&str>, archived: bool) -> Result<Value> {
        self.rpc(
            "thread/list",
            json!({
                "cursor":cursor,
                "limit":100,
                "archived":archived,
                "modelProviders":[],
                "sourceKinds":["cli","vscode","exec","appServer","unknown"]
            }),
        )
        .await
    }

    async fn ensure_loaded(&self, thread_id: &str) -> Result<()> {
        self.ensure_loaded_at(thread_id, None).await?;
        Ok(())
    }

    async fn ensure_loaded_at(&self, thread_id: &str, cwd: Option<&Path>) -> Result<bool> {
        let mut loaded = self.loaded_threads.lock().await;
        if loaded.contains(thread_id) {
            return Ok(false);
        }
        if self.thread_is_archived(thread_id).await? {
            self.rpc("thread/unarchive", json!({"threadId": thread_id}))
                .await?;
        }
        self.transport.track_thread(thread_id);
        let mut params = json!({"threadId": thread_id, "excludeTurns": true});
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd);
        }
        self.rpc("thread/resume", params).await?;
        loaded.insert(thread_id.into());
        Ok(true)
    }

    async fn thread_is_archived(&self, thread_id: &str) -> Result<bool> {
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        loop {
            let page = self.list_threads(cursor.as_deref(), true).await?;
            let threads = page["data"]
                .as_array()
                .context("thread/list omitted data")?;
            if threads.iter().any(|thread| thread["id"] == thread_id) {
                return Ok(true);
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                return Ok(false);
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated archived-thread cursor");
            }
        }
    }
}

#[async_trait]
impl Executor for Codex {
    async fn create_thread(&self, cwd: &Path, creation_id: &str) -> Result<String> {
        let params = json!({
            "cwd": cwd.to_str().context("cwd must be UTF-8")?,
            "threadSource":format!("yandalf:{creation_id}")
        });
        let response = self.rpc("thread/start", params).await?;
        let id = response["thread"]["id"]
            .as_str()
            .context("thread/start omitted thread ID")?
            .to_owned();
        self.loaded_threads.lock().await.insert(id.clone());
        self.transport.track_thread(&id);
        Ok(id)
    }

    async fn inspect_thread(&self, creation_id: &str) -> Result<ThreadInspection> {
        let marker = format!("yandalf:{creation_id}");
        let mut matches = HashSet::new();
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        loop {
            let page = self
                .rpc("thread/loaded/list", json!({"cursor":cursor,"limit":100}))
                .await?;
            for thread_id in page["data"]
                .as_array()
                .context("thread/loaded/list omitted data")?
            {
                let thread_id = thread_id
                    .as_str()
                    .context("loaded thread id must be a string")?;
                let thread = self.read_thread(thread_id).await?;
                if thread["threadSource"] == marker {
                    matches.insert(thread_id.to_owned());
                }
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated loaded-thread cursor");
            }
        }
        for archived in [false, true] {
            let mut cursor = None::<String>;
            let mut cursors = HashSet::new();
            loop {
                let page = self.list_threads(cursor.as_deref(), archived).await?;
                for thread in page["data"]
                    .as_array()
                    .context("thread/list omitted data")?
                {
                    if thread["threadSource"] == marker {
                        matches.insert(
                            thread["id"]
                                .as_str()
                                .context("thread/list omitted ID")?
                                .to_owned(),
                        );
                    }
                }
                cursor = page["nextCursor"].as_str().map(str::to_owned);
                let Some(next) = &cursor else {
                    break;
                };
                if !cursors.insert(next.clone()) {
                    bail!("Codex returned a repeated thread cursor");
                }
            }
        }
        match matches.len() {
            0 => Ok(ThreadInspection::Missing),
            1 => Ok(ThreadInspection::Found {
                thread_id: matches.into_iter().next().unwrap(),
            }),
            _ => Ok(ThreadInspection::Ambiguous {
                reason: "multiple Codex threads match the persisted creation intent".into(),
            }),
        }
    }

    async fn thread_cwd(&self, thread_id: &str) -> Result<std::path::PathBuf> {
        let thread = self.read_thread(thread_id).await?;
        ensure_idle(&thread)?;
        let cwd = thread["cwd"].as_str().context("thread/read omitted cwd")?;
        let cwd = std::path::PathBuf::from(cwd);
        anyhow::ensure!(
            cwd.is_absolute(),
            "executor returned a relative working directory"
        );
        Ok(cwd)
    }

    async fn archive_thread(&self, thread_id: &str) -> Result<()> {
        if !self.thread_is_archived(thread_id).await? {
            let current = self.read_thread(thread_id).await?;
            ensure_idle(&current)?;
            self.loaded_threads.lock().await.remove(thread_id);
            self.rpc("thread/archive", json!({"threadId":thread_id}))
                .await?;
        }
        self.loaded_threads.lock().await.remove(thread_id);
        self.transport.forget_thread(thread_id);
        Ok(())
    }

    async fn configure_thread(&self, thread_id: &str, spec: &SessionSpec) -> Result<()> {
        let mut current = self.read_thread(thread_id).await?;
        ensure_idle(&current)?;
        if self.ensure_loaded_at(thread_id, Some(&spec.cwd)).await? {
            current = self.read_thread(thread_id).await?;
            ensure_idle(&current)?;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        if current["cwd"] != spec.cwd.to_string_lossy().as_ref() {
            self.rpc(
                "thread/settings/update",
                json!({"threadId":thread_id,"cwd":spec.cwd}),
            )
            .await?;
            loop {
                current = self.read_thread(thread_id).await?;
                ensure_idle(&current)?;
                if current["cwd"] == spec.cwd.to_string_lossy().as_ref() {
                    break;
                }
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline,
                    "Codex did not confirm prepared cwd"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        if let Some(title) = &spec.title {
            self.rpc(
                "thread/name/set",
                json!({"threadId":thread_id,"name":title}),
            )
            .await?;
        }
        let git = spec
            .git
            .as_ref()
            .map(|git| json!({"sha":git.sha,"branch":git.branch,"originUrl":git.origin_url}));
        if let Some(git) = &git {
            self.rpc(
                "thread/metadata/update",
                json!({"threadId":thread_id,"gitInfo":git}),
            )
            .await?;
        }
        let mut reapplied = false;
        loop {
            let current = self.read_thread(thread_id).await?;
            ensure_idle(&current)?;
            let cwd_matches = current["cwd"] == spec.cwd.to_string_lossy().as_ref();
            let title_matches = spec
                .title
                .as_ref()
                .is_none_or(|title| current["name"] == *title);
            let git_matches = git.as_ref().is_none_or(|git| {
                ["sha", "branch", "originUrl"]
                    .iter()
                    .all(|key| current["gitInfo"][*key] == git[*key])
            });
            if cwd_matches && title_matches && git_matches {
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "Codex did not confirm prepared session metadata"
            );
            if cwd_matches && !git_matches && !reapplied {
                self.rpc(
                    "thread/metadata/update",
                    json!({"threadId":thread_id,"gitInfo":git}),
                )
                .await?;
                reapplied = true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn start_turn(
        &self,
        thread_id: &str,
        client_message_id: &str,
        input: &TurnInput,
    ) -> Result<String> {
        self.ensure_loaded(thread_id).await?;
        let mut content = vec![json!({"type":"text","text":input.text,"text_elements":[]})];
        let has_context = match &input.context {
            Value::Null => false,
            Value::Array(value) => !value.is_empty(),
            Value::Object(value) => !value.is_empty(),
            Value::String(value) => !value.is_empty(),
            _ => true,
        };
        if has_context {
            content.push(json!({
                "type":"text",
                "text":format!("Event context (data):\n{}", serde_json::to_string(&input.context)?),
                "text_elements":[]
            }));
        }
        let params = json!({
            "threadId":thread_id,
            "clientUserMessageId":client_message_id,
            "input":content
        });
        let response = self.rpc("turn/start", params).await?;
        Ok(response["turn"]["id"]
            .as_str()
            .context("turn/start omitted turn ID")?
            .into())
    }

    async fn wait_turn(&self, thread_id: &str, turn_id: &str) -> Result<TurnResult> {
        self.transport.wait_turn(thread_id, turn_id).await
    }

    async fn inspect(
        &self,
        thread_id: &str,
        client_message_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Inspection> {
        self.ensure_loaded(thread_id).await?;
        let mut cursor = None::<String>;
        let mut cursors = HashSet::new();
        let mut matches = Vec::new();
        loop {
            let page = self
                .rpc(
                    "thread/turns/list",
                    json!({
                        "threadId":thread_id,"limit":100,"itemsView":"full","cursor":cursor
                    }),
                )
                .await?;
            for turn in page["data"]
                .as_array()
                .context("thread/turns/list omitted data")?
            {
                if turn["itemsView"]
                    .as_str()
                    .is_some_and(|view| view != "full")
                {
                    bail!("Codex returned incomplete items while reconciling a turn");
                }
                let matches_id = turn_id.is_some_and(|id| turn["id"] == id);
                let matches_client = turn["items"].as_array().is_some_and(|items| {
                    items.iter().any(|item| {
                        item["type"] == "userMessage" && item["clientId"] == client_message_id
                    })
                });
                if matches_id || matches_client {
                    matches.push(turn.clone());
                }
            }
            cursor = page["nextCursor"].as_str().map(str::to_owned);
            let Some(next) = &cursor else {
                break;
            };
            if !cursors.insert(next.clone()) {
                bail!("Codex returned a repeated history cursor");
            }
        }
        match matches.as_slice() {
            [] => Ok(Inspection::Missing),
            [turn] => {
                let id = turn["id"]
                    .as_str()
                    .context("history turn omitted ID")?
                    .to_owned();
                if turn["status"] == "inProgress" {
                    Ok(Inspection::Active { turn_id: id })
                } else {
                    let result = turn_result(turn, &[], Value::Null)?;
                    self.transport.settle_turn(thread_id, &id);
                    Ok(Inspection::Finished {
                        turn_id: id,
                        result,
                    })
                }
            }
            _ => Ok(Inspection::Ambiguous {
                reason:
                    "multiple Codex turns match the persisted request; refusing automatic replay"
                        .into(),
            }),
        }
    }

    async fn interrupt(&self, thread_id: &str, turn_id: &str) -> Result<()> {
        self.ensure_loaded(thread_id).await?;
        self.rpc(
            "turn/interrupt",
            json!({"threadId":thread_id,"turnId":turn_id}),
        )
        .await?;
        Ok(())
    }
}

fn ensure_idle(thread: &Value) -> Result<()> {
    let status = thread["status"]["type"]
        .as_str()
        .context("thread/read omitted runtime status")?;
    anyhow::ensure!(
        matches!(status, "idle" | "notLoaded"),
        "Codex thread is {status}; workspace preparation must wait"
    );
    Ok(())
}
