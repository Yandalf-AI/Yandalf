use crate::{
    ChangeKind, GithubDecisions,
    api::{canonical_repository, decode_pages, parse_activities, parse_pull, parse_pulls},
    model::{Activity, Change, GithubContext, PullRequest},
    observation::{changed_event, closed_event},
    policy::supersedes,
};
use serde_json::{Value, json};
use yandalf_core::{
    decision::{Decision, DecisionProvider},
    domain::{Event, now_ms},
};

const REPOSITORY: &str = "ydb-platform/ydb-js-sdk";
const VIEWER: &str = "YandalfRed";
const START: &str = "2026-11-04T00:00:00Z";

fn watch_started() -> i64 {
    chrono::DateTime::parse_from_rfc3339(START)
        .unwrap()
        .timestamp_millis()
}

fn raw_pull(number: u64) -> Value {
    json!({
        "number": number,
        "html_url": format!("https://github.com/{REPOSITORY}/pull/{number}"),
        "title": "Handle cancellation",
        "body": "Preserve the caller's cancellation reason.",
        "state": "open",
        "draft": false,
        "created_at": "2026-11-03T10:00:00Z",
        "updated_at": "2026-11-03T12:00:00Z",
        "base": {
            "sha": "a".repeat(40),
            "ref": "main",
            "repo": {"url": format!("https://api.github.com/repos/{REPOSITORY}")}
        },
        "head": {
            "sha": "b".repeat(40),
            "ref": "fix-cancellation",
            "repo": null
        }
    })
}

fn pull(number: u64) -> PullRequest {
    parse_pull(REPOSITORY, raw_pull(number)).unwrap()
}

fn observation(pull: &PullRequest, activity: &[Activity], previous: Option<&Event>) -> Event {
    changed_event(
        REPOSITORY,
        pull,
        activity,
        previous,
        VIEWER,
        watch_started(),
        std::slice::from_ref(pull),
    )
    .unwrap()
    .unwrap()
}

fn changes(event: &Event) -> Vec<ChangeKind> {
    serde_json::from_value::<Vec<Change>>(event.context["changes"].clone())
        .unwrap()
        .into_iter()
        .map(|change| change.kind)
        .collect()
}

#[test]
fn decodes_paginated_json_without_requiring_gh_slurp_support() {
    assert_eq!(
        decode_pages(
            br#"[{"number":1}]
[{"number":2}]"#
        )
        .unwrap(),
        vec![json!([{"number":1}]), json!([{"number":2}])]
    );
    assert_eq!(
        decode_pages(br#"{"login":"bot"}"#).unwrap(),
        vec![json!({"login":"bot"})]
    );
    assert_eq!(decode_pages(b"[]").unwrap(), vec![json!([])]);
    assert!(decode_pages(b" ").is_err());
    assert!(decode_pages(b"[] [{").is_err());
}

fn policy(on: Vec<ChangeKind>) -> GithubDecisions {
    GithubDecisions::new(REPOSITORY, on).unwrap()
}

async fn should_run(event: &Event) -> bool {
    policy(ChangeKind::defaults()).decide(event).await.unwrap() == Decision::Run
}

fn activity(event: &str, actor: &str, actor_type: &str, target: &str) -> Vec<Activity> {
    parse_activities(
        vec![json!([{
            "id": 42,
            "event": event,
            "actor": {"login": actor, "type": actor_type},
            "requested_reviewer": {"login": target},
            "body": "Please recheck cancellation.",
            "created_at": "2026-11-04T01:00:00Z",
            "html_url": "https://github.com/example/activity/42"
        }])],
        vec![json!([])],
    )
    .unwrap()
}

#[test]
fn new_pr_message_starts_with_its_identity_and_revision() {
    let mut pull = pull(800);
    pull.created_at = "2026-11-04T00:01:00Z".into();
    let event = observation(&pull, &[], None);
    assert_eq!(
        event.title.as_deref(),
        Some("PR #800 — Handle cancellation")
    );
    assert!(event.text.starts_with(
        "PR #800 — Handle cancellation\nhttps://github.com/ydb-platform/ydb-js-sdk/pull/800\n"
    ));
    assert!(event.text.contains("Репозиторий: ydb-platform/ydb-js-sdk"));
    assert!(
        event
            .text
            .contains("Ветка PR: fix-cancellation (bbbbbbbbbbbb)")
    );
    assert!(event.text.contains("База: main (aaaaaaaaaaaa)"));
    assert!(event.text.ends_with("Изменения: открыт новый PR."));
    assert_eq!(event.context["pull"]["body"], pull.body);
}

#[test]
fn updated_pr_message_identifies_code_changes_and_discussion() {
    let original = pull(715);
    let previous = observation(&original, &[], None);
    let mut pull = original;
    pull.head_sha = "c".repeat(40);
    pull.title = "Keep the cancellation reason".into();
    let comments = activity("commented", "polRk", "User", "");
    let event = observation(&pull, &comments, Some(&previous));
    assert_eq!(
        event.title.as_deref(),
        Some("PR #715 — Keep the cancellation reason")
    );
    assert!(
        event
            .text
            .starts_with("PR #715 — Keep the cancellation reason\n")
    );
    assert!(
        event
            .text
            .contains("Ветка PR: fix-cancellation (cccccccccccc)")
    );
    assert!(event.text.contains("обновлены коммиты PR или его базы"));
    assert!(event.text.contains("добавлен или изменён комментарий"));
    assert!(!event.text.contains("открыт новый PR"));
}

#[tokio::test]
async fn existing_pr_is_baselined_but_new_pr_triggers_opened() {
    let old = pull(715);
    let mut old_comments = activity("commented", "polRk", "User", "");
    old_comments[0].updated_at = "2026-11-03T23:00:00Z".into();
    let initial = observation(&old, &old_comments, None);
    assert!(changes(&initial).is_empty());
    assert!(!should_run(&initial).await);

    let fresh_comment = activity("commented", "polRk", "User", "");
    let during_bootstrap = observation(&old, &fresh_comment, None);
    assert_eq!(changes(&during_bootstrap), [ChangeKind::Commented]);
    assert!(should_run(&during_bootstrap).await);

    let mut created = pull(800);
    created.created_at = "2026-11-04T00:01:00Z".into();
    let initial = observation(&created, &[], None);
    assert_eq!(changes(&initial), [ChangeKind::Opened]);
    assert!(should_run(&initial).await);
}

#[tokio::test]
async fn initial_events_in_watch_start_second_are_not_lost_to_timestamp_precision() {
    let mut created = pull(800);
    created.created_at = START.into();
    let event = changed_event(
        REPOSITORY,
        &created,
        &[],
        None,
        VIEWER,
        watch_started() + 500,
        std::slice::from_ref(&created),
    )
    .unwrap()
    .unwrap();
    assert_eq!(changes(&event), [ChangeKind::Opened]);
    assert!(should_run(&event).await);

    let old = pull(715);
    let mut comment = activity("commented", "polRk", "User", "");
    comment[0].updated_at = START.into();
    let event = changed_event(
        REPOSITORY,
        &old,
        &comment,
        None,
        VIEWER,
        watch_started() + 500,
        std::slice::from_ref(&old),
    )
    .unwrap()
    .unwrap();
    assert_eq!(changes(&event), [ChangeKind::Commented]);
    assert!(should_run(&event).await);
}

#[tokio::test]
async fn changed_revision_runs_and_identical_snapshot_does_not_emit_again() {
    let initial_pull = pull(715);
    let initial = observation(&initial_pull, &[], None);
    let mut updated = initial_pull.clone();
    updated.head_sha = "c".repeat(40);
    let event = observation(&updated, &[], Some(&initial));
    assert_eq!(changes(&event), [ChangeKind::Revision]);
    assert!(should_run(&event).await);
    assert_eq!(event.subject, initial.subject);
    assert_eq!(
        event.context["previous_revision"]["head_sha"],
        initial_pull.head_sha
    );
    assert!(
        changed_event(
            REPOSITORY,
            &updated,
            &[],
            Some(&event),
            VIEWER,
            watch_started(),
            &[updated.clone()]
        )
        .unwrap()
        .is_none()
    );

    let returned = observation(&initial_pull, &[], Some(&event));
    assert!(should_run(&returned).await);
    assert_ne!(returned.id, initial.id);
    assert_eq!(
        returned.id,
        observation(&initial_pull, &[], Some(&event)).id
    );
}

#[tokio::test]
async fn base_revision_and_title_or_body_edits_trigger_independently() {
    let original = pull(715);
    let initial = observation(&original, &[], None);
    let mut rebase = original.clone();
    rebase.base_sha = "d".repeat(40);
    assert_eq!(
        changes(&observation(&rebase, &[], Some(&initial))),
        [ChangeKind::Revision]
    );
    for field in ["title", "body", "base_ref", "head_ref"] {
        let mut edited = original.clone();
        match field {
            "title" => edited.title = "Changed cancellation contract".into(),
            "body" => edited.body = "The caller now retains the original reason.".into(),
            "base_ref" => edited.base_ref = "release".into(),
            _ => edited.head_ref = "renamed-cancellation".into(),
        }
        let event = observation(&edited, &[], Some(&initial));
        assert_eq!(changes(&event), [ChangeKind::Edited]);
        assert!(should_run(&event).await);
    }
}

#[tokio::test]
async fn drafts_skip_and_closed_prs_archive_but_ready_and_reopened_prs_run() {
    let original = pull(715);
    let initial = observation(&original, &[], None);
    let mut draft = original.clone();
    draft.draft = true;
    draft.head_sha = "c".repeat(40);
    let draft_event = observation(&draft, &[], Some(&initial));
    assert!(!should_run(&draft_event).await);
    draft.draft = false;
    let ready = observation(&draft, &[], Some(&draft_event));
    assert_eq!(changes(&ready), [ChangeKind::ReadyForReview]);
    assert!(should_run(&ready).await);

    let mut closed = original.clone();
    closed.state = "closed".into();
    closed.head_sha = "e".repeat(40);
    let closed_event = observation(&closed, &[], Some(&initial));
    assert!(!should_run(&closed_event).await);
    closed.state = "open".into();
    let reopened = observation(&closed, &[], Some(&closed_event));
    assert_eq!(changes(&reopened), [ChangeKind::Reopened]);
    assert!(should_run(&reopened).await);
}

#[tokio::test]
async fn closure_archives_without_review_triggers_or_activity_context() {
    let mut closed = pull(715);
    closed.state = "closed".into();
    closed.draft = true;
    let event = closed_event(REPOSITORY, &closed).unwrap();
    assert_eq!(
        policy(Vec::new()).decide(&event).await.unwrap(),
        Decision::Archive
    );
}

#[test]
fn closure_identity_survives_restart_and_reclosing_creates_another_event() {
    let mut closed = pull(715);
    closed.state = "closed".into();
    let mut legacy = closed_event(REPOSITORY, &closed).unwrap();
    legacy.id = "old-skipped-observation".into();
    legacy.context["viewer"] = VIEWER.into();
    let upgraded = observation(&closed, &[], Some(&legacy));
    let restored: Event = serde_json::from_str(&serde_json::to_string(&upgraded).unwrap()).unwrap();
    let restored_pull = serde_json::from_value(restored.context["pull"].clone()).unwrap();
    assert!(
        changed_event(
            REPOSITORY,
            &restored_pull,
            &[],
            Some(&restored),
            "another-account",
            now_ms(),
            &[]
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        serde_json::to_value(closed_event(REPOSITORY, &restored_pull).unwrap()).unwrap(),
        serde_json::to_value(&restored).unwrap()
    );
    closed.updated_at = "2026-11-05T00:00:00Z".into();
    assert_ne!(closed_event(REPOSITORY, &closed).unwrap().id, restored.id);
}

#[tokio::test]
async fn review_requests_require_human_actor_and_target_current_bot() {
    let pull = pull(715);
    let initial = observation(&pull, &[], None);
    for (actor, actor_type, target, expected) in [
        ("polRk", "User", VIEWER, true),
        ("polRk", "User", "another-reviewer", false),
        ("yandalfred", "User", VIEWER, false),
        ("helper", "Bot", VIEWER, false),
        ("helper[bot]", "User", VIEWER, false),
        ("", "User", VIEWER, false),
    ] {
        let events = activity("review_requested", actor, actor_type, target);
        let event = observation(&pull, &events, Some(&initial));
        assert_eq!(changes(&event), [ChangeKind::ReviewRequested]);
        assert_eq!(
            should_run(&event).await,
            expected,
            "actor={actor}, target={target}"
        );
    }
}

#[tokio::test]
async fn human_reviews_comments_and_edited_inline_replies_trigger() {
    let pull = pull(715);
    let initial = observation(&pull, &[], None);
    for kind in ["reviewed", "commented"] {
        let events = activity(kind, "polRk", "User", "");
        assert!(should_run(&observation(&pull, &events, Some(&initial))).await);
    }
    let inline = json!({
        "id": 15,
        "user": {"login": "polRk", "type": "User"},
        "body": "Fixed the cancellation path.",
        "path": "src/query.ts",
        "line": 12,
        "in_reply_to_id": 8,
        "created_at": "2026-11-04T01:00:00Z",
        "updated_at": "2026-11-04T01:00:00Z",
        "html_url": "https://github.com/example/pull/715#discussion_r15"
    });
    let activities = parse_activities(vec![json!([])], vec![json!([inline.clone()])]).unwrap();
    let reply = observation(&pull, &activities, Some(&initial));
    assert!(should_run(&reply).await);
    assert_eq!(reply.context["changes"][0]["activity"]["in_reply_to_id"], 8);
    assert_eq!(
        reply.context["changes"][0]["activity"]["path"],
        "src/query.ts"
    );

    let mut edited = inline;
    edited["body"] = "The fix is in the next commit; please check it.".into();
    edited["updated_at"] = "2026-11-04T02:00:00Z".into();
    let activities = parse_activities(vec![json!([])], vec![json!([edited])]).unwrap();
    assert!(should_run(&observation(&pull, &activities, Some(&reply))).await);
}

#[tokio::test]
async fn own_activity_and_timestamp_only_updates_are_persisted_without_feedback_turns() {
    let mut pull = pull(715);
    let initial = observation(&pull, &[], None);
    for kind in ["reviewed", "commented"] {
        let own_activity = activity(kind, VIEWER, "User", "");
        pull.updated_at = "2026-11-04T02:00:00Z".into();
        let ignored = observation(&pull, &own_activity, Some(&initial));
        assert!(!should_run(&ignored).await);
        assert!(
            changed_event(
                REPOSITORY,
                &pull,
                &own_activity,
                Some(&ignored),
                VIEWER,
                watch_started(),
                &[pull.clone()]
            )
            .unwrap()
            .is_none()
        );
    }
    let timestamp = observation(&pull, &[], Some(&initial));
    assert!(changes(&timestamp).is_empty());
    assert!(!should_run(&timestamp).await);
}

#[tokio::test]
async fn configured_trigger_set_filters_changes_but_does_not_affect_other_sources() {
    let pull = pull(715);
    let initial = observation(&pull, &[], None);
    let activities = activity("commented", "polRk", "User", "");
    let comment = observation(&pull, &activities, Some(&initial));
    let revisions_only = policy(vec![ChangeKind::Revision]);
    assert!(matches!(
        revisions_only.decide(&comment).await.unwrap(),
        Decision::Skip { .. }
    ));
    let mut external = comment;
    external.source = "schedule:example".into();
    assert_eq!(
        revisions_only.decide(&external).await.unwrap(),
        Decision::Run
    );
}

#[tokio::test]
async fn stack_metadata_is_context_not_a_trigger_or_subject_partition() {
    let original = pull(715);
    let initial = observation(&original, &[], None);
    let mut stacked = original;
    stacked.stack = json!({"number": 730, "position": 2, "size": 2, "base": "main"});
    let mut lower = pull(721);
    lower.stack = json!({"number": 730, "position": 1, "size": 2, "base": "main"});
    let event = changed_event(
        REPOSITORY,
        &stacked,
        &[],
        Some(&initial),
        VIEWER,
        watch_started(),
        &[stacked.clone(), lower],
    )
    .unwrap()
    .unwrap();
    assert_eq!(event.subject, initial.subject);
    assert!(changes(&event).is_empty());
    assert!(!should_run(&event).await);
    assert_eq!(event.context["stack"]["pull_requests"][0]["number"], 721);
    assert_eq!(event.context["stack"]["pull_requests"][1]["number"], 715);
}

#[test]
fn repository_pages_are_complete_and_reject_duplicate_or_foreign_prs() {
    let parsed = parse_pulls(
        REPOSITORY,
        vec![json!([raw_pull(715)]), json!([raw_pull(800)])],
    )
    .unwrap();
    assert_eq!(
        parsed.iter().map(|pull| pull.number).collect::<Vec<_>>(),
        [715, 800]
    );
    assert!(
        parse_pulls(
            REPOSITORY,
            vec![json!([raw_pull(715)]), json!([raw_pull(715)])]
        )
        .is_err()
    );
    let mut foreign = raw_pull(715);
    foreign["base"]["repo"]["url"] = "https://api.github.com/elsewhere/another".into();
    assert!(parse_pull(REPOSITORY, foreign).is_err());
    let mut foreign_url = raw_pull(715);
    foreign_url["html_url"] = "https://github.com/elsewhere/another/pull/715".into();
    assert!(parse_pull(REPOSITORY, foreign_url).is_err());
    let mut truncated = raw_pull(715);
    truncated["head"]["sha"] = "1234567".into();
    assert!(parse_pull(REPOSITORY, truncated).is_err());
    assert_eq!(
        canonical_repository("YDB-Platform/YDB-JS-SDK").unwrap(),
        REPOSITORY
    );
    for invalid in [
        "../repo",
        "owner/repo?state=all",
        "owner/../repo",
        "https://github.com/owner/repo",
    ] {
        assert!(canonical_repository(invalid).is_err());
    }
}

#[test]
fn activity_pages_preserve_inline_and_timeline_ids_without_collision() {
    let timeline = vec![
        json!([{
            "id": 7,
            "event": "commented",
            "user": {"login": "author", "type": "User"},
            "body": "Question"
        }]),
        json!([{
            "id": 8,
            "event": "reviewed",
            "user": {"login": "reviewer", "type": "User"},
            "body": "Found a bug",
            "state": "changes_requested"
        }]),
    ];
    let inline = vec![json!([{
        "id": 7,
        "user": {"login": "author", "type": "User"},
        "body": "Fixed",
        "in_reply_to_id": 3
    }])];
    let activities = parse_activities(timeline, inline).unwrap();
    assert_eq!(activities.len(), 3);
    assert!(
        activities
            .iter()
            .any(|entry| entry.id == "commented:7" && entry.actor == "author")
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.id == "inline:7" && entry.in_reply_to_id == 3)
    );
    assert!(
        activities
            .iter()
            .any(|entry| entry.id == "reviewed:8" && entry.state == "changes_requested")
    );
}

#[test]
fn typed_context_retains_closed_snapshot_without_review_only_fields() {
    let mut pull = pull(715);
    pull.state = "closed".into();
    pull.stack = json!({"number": 730, "position": 2, "size": 3, "base": "main"});
    let event = closed_event(REPOSITORY, &pull).unwrap();

    let context = GithubContext::decode(&event).unwrap();

    assert_eq!(context.pr, Some(pull.number));
    assert!(context.viewer.is_none());
    assert!(context.changes.is_none());
    assert!(context.matches_pull(&pull).unwrap());
    assert_eq!(
        serde_json::to_value(context.pull.unwrap().complete().unwrap()).unwrap(),
        serde_json::to_value(pull).unwrap()
    );
}

#[test]
fn legacy_revision_context_without_pull_can_resume_observation_and_supersession() {
    let original = pull(715);
    let mut legacy = observation(&original, &[], None);
    legacy.context = json!({
        "pr": original.number,
        "base_sha": original.base_sha,
        "head_sha": original.head_sha
    });
    let older = GithubContext::decode(&legacy).unwrap();
    let unchanged = observation(&original, &[], Some(&legacy));
    let current = GithubContext::decode(&unchanged).unwrap();
    assert!(changes(&unchanged).is_empty());
    assert!(!supersedes(&older, &current));

    let mut updated = original;
    updated.head_sha = "c".repeat(40);
    let event = observation(&updated, &[], Some(&legacy));
    let current = GithubContext::decode(&event).unwrap();
    assert_eq!(changes(&event), [ChangeKind::Revision]);
    assert!(supersedes(&older, &current));
    assert_eq!(
        event.context["previous_revision"]["head_sha"],
        "b".repeat(40)
    );
}
