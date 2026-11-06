use serde_json::json;
use yandalf_core::{
    decision::Decision, domain::Event, plugin::App, store::Store, task::TaskDeciding,
};
use yandalf_github::Github;
use yandalf_sqlite::Sqlite;

fn event(id: &str, head: &str, state: &str, draft: bool) -> Event {
    Event {
        id: id.into(),
        source: "github:owner/repo".into(),
        subject: "github:owner/repo/pr/1".into(),
        title: Some("PR #1".into()),
        text: "Review".into(),
        context: json!({"base_sha":"a".repeat(40),"head_sha":head,"pull":{"state":state,"draft":draft,"head_ref":"feature"},"changes":[{"kind":"revision"}],"viewer":"bot"}),
    }
}

#[tokio::test]
async fn newer_revision_state_or_branch_supersedes_queued_review() {
    for (new_head, state, draft, branch) in [
        ("c".repeat(40), "open", false, "feature"),
        ("b".repeat(40), "closed", false, "feature"),
        ("b".repeat(40), "open", true, "feature"),
        ("b".repeat(40), "open", false, "renamed-feature"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let app = App::builder()
            .plugin(Github::new("owner/repo", Some("30s"), None).unwrap())
            .plugin(Sqlite(temp.path().join("state.sqlite")))
            .build()
            .await
            .unwrap();
        let services = app.services();
        let store = services.get::<dyn Store>().unwrap();
        let old = store
            .enqueue(&event("old", &"b".repeat(40), "open", false))
            .await
            .unwrap();
        let mut latest = event("new", &new_head, state, draft);
        latest.context["pull"]["head_ref"] = branch.into();
        store.enqueue(&latest).await.unwrap();
        let mut deciding = TaskDeciding {
            job: old,
            decision: Decision::Run,
        };
        services.dispatch(&mut deciding).await.unwrap();
        assert!(matches!(deciding.decision, Decision::Skip { .. }));
        app.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn later_observation_with_same_open_revision_does_not_discard_the_task() {
    let temp = tempfile::tempdir().unwrap();
    let app = App::builder()
        .plugin(Github::new("owner/repo", Some("30s"), None).unwrap())
        .plugin(Sqlite(temp.path().join("state.sqlite")))
        .build()
        .await
        .unwrap();
    let services = app.services();
    let store = services.get::<dyn Store>().unwrap();
    let old = store
        .enqueue(&event("old", &"b".repeat(40), "open", false))
        .await
        .unwrap();
    store
        .enqueue(&event("new", &"b".repeat(40), "open", false))
        .await
        .unwrap();
    let mut deciding = TaskDeciding {
        job: old,
        decision: Decision::Run,
    };
    services.dispatch(&mut deciding).await.unwrap();
    assert_eq!(deciding.decision, Decision::Run);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn reopened_pr_supersedes_queued_archive_even_at_the_same_revision() {
    let temp = tempfile::tempdir().unwrap();
    let app = App::builder()
        .plugin(Github::new("owner/repo", Some("30s"), None).unwrap())
        .plugin(Sqlite(temp.path().join("state.sqlite")))
        .build()
        .await
        .unwrap();
    let services = app.services();
    let store = services.get::<dyn Store>().unwrap();
    let closed = store
        .enqueue(&event("closed", &"b".repeat(40), "closed", false))
        .await
        .unwrap();
    let mut deciding = TaskDeciding {
        job: closed,
        decision: Decision::Run,
    };
    services.dispatch(&mut deciding).await.unwrap();
    assert_eq!(deciding.decision, Decision::Archive);
    store
        .enqueue(&event("reopened", &"b".repeat(40), "open", false))
        .await
        .unwrap();
    deciding.decision = Decision::Run;
    services.dispatch(&mut deciding).await.unwrap();
    assert!(matches!(deciding.decision, Decision::Skip { .. }));
    app.shutdown().await.unwrap();
}
