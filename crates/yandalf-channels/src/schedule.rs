use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use croner::{
    Cron,
    parser::{CronParser, Seconds, Year},
};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Clone)]
pub struct Schedule {
    source: String,
    subject: String,
    timing: Timing,
    text: String,
    context: Value,
}

#[derive(Clone)]
pub enum Timing {
    Interval(i64),
    Cron(Box<Cron>),
}

impl Schedule {
    pub fn new(
        interval: Option<&str>,
        cron: Option<&str>,
        text: &str,
        context: &Value,
    ) -> Result<Self> {
        ensure!(!text.trim().is_empty(), "schedule text must not be empty");
        let timing = Timing::new(interval, cron)?;
        let timing_value = match &timing {
            Timing::Interval(milliseconds) => json!({"interval_ms": milliseconds}),
            Timing::Cron(cron) => json!({"cron": cron.to_string(), "timezone": "UTC"}),
        };
        let action = json!({"text": text, "context": context});
        let subject = identity("subject", action.clone())?;
        let source = identity("source", json!({"timing": timing_value, "action": action}))?;
        Ok(Self {
            source,
            subject,
            timing,
            text: text.into(),
            context: context.clone(),
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }
    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn first_tick(&self, now: i64) -> Result<i64> {
        self.timing.first_tick(now)
    }

    pub fn next_tick(&self, previous: i64) -> Result<i64> {
        self.timing.next_tick(previous)
    }
}

impl Timing {
    pub fn new(interval: Option<&str>, cron: Option<&str>) -> Result<Self> {
        Ok(match (interval, cron) {
            (Some(interval), None) => {
                let duration =
                    humantime::parse_duration(interval).context("invalid schedule interval")?;
                ensure!(
                    !duration.is_zero() && duration.subsec_nanos().is_multiple_of(1_000_000),
                    "schedule interval must be positive with millisecond precision"
                );
                Timing::Interval(
                    i64::try_from(duration.as_millis())
                        .context("schedule interval is too large")?,
                )
            }
            (None, Some(pattern)) => {
                ensure!(
                    pattern.split_whitespace().count() == 5,
                    "cron must contain five fields (UTC)"
                );
                let pattern = pattern.split_whitespace().collect::<Vec<_>>().join(" ");
                let cron = CronParser::builder()
                    .seconds(Seconds::Disallowed)
                    .year(Year::Disallowed)
                    .build()
                    .parse(&pattern)
                    .context("invalid cron expression")?;
                Timing::Cron(Box::new(cron))
            }
            _ => bail!("schedule requires exactly one of interval or cron"),
        })
    }

    pub fn first_tick(&self, now: i64) -> Result<i64> {
        match self {
            Timing::Interval(_) => Ok(now),
            Timing::Cron(_) => self.next_tick(now),
        }
    }

    pub fn next_tick(&self, previous: i64) -> Result<i64> {
        match self {
            Timing::Interval(milliseconds) => previous
                .checked_add(*milliseconds)
                .context("schedule timestamp overflow"),
            Timing::Cron(cron) => {
                let previous = DateTime::<Utc>::from_timestamp_millis(previous)
                    .context("invalid schedule timestamp")?;
                let next = cron
                    .find_next_occurrence(&previous, false)
                    .context("cron has no next occurrence")?;
                Ok(next.timestamp_millis())
            }
        }
    }
}

fn identity(kind: &str, value: Value) -> Result<String> {
    let mut canonical = json!({"type": format!("yandalf.schedule.{kind}.v1"), "value": value});
    canonical.sort_all_objects();
    Ok(format!(
        "schedule:{}",
        Uuid::new_v5(&Uuid::NAMESPACE_URL, &serde_json::to_vec(&canonical)?)
    ))
}

#[async_trait::async_trait]
impl yandalf_core::channel::Channel for Schedule {
    async fn run(
        &self,
        inbox: yandalf_core::channel::Inbox,
        stop: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        use yandalf_core::domain::{Event, now_ms};
        let initial = self.first_tick(now_ms())?;
        let mut tick = inbox.source_cursor(self.source(), initial).await?;
        loop {
            let wait = tick.saturating_sub(now_ms()).max(0) as u64;
            tokio::select! {_=stop.cancelled()=>return Ok(()),_=tokio::time::sleep(std::time::Duration::from_millis(wait.min(60_000)))=>{}}
            if tick > now_ms() {
                continue;
            }
            let event = Event {
                id: tick.to_string(),
                source: self.source().into(),
                subject: self.subject().into(),
                title: None,
                text: self.text.clone(),
                context: self.context.clone(),
            };
            let next = self.next_tick(tick)?;
            inbox.accept_scheduled(&event, next).await?;
            tick = next;
            tokio::task::yield_now().await;
        }
    }
}
impl yandalf_core::plugin::Plugin for Schedule {
    fn name(&self) -> &str {
        self.source()
    }
    fn register(&self, ctx: &mut yandalf_core::plugin::PluginContext) -> Result<()> {
        yandalf_core::channel::ChannelPlugin::new(self.source(), self.clone()).register(ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn interval(value: &str) -> Result<Schedule> {
        Schedule::new(Some(value), None, "Inspect pending tasks", &Value::Null)
    }
    fn cron(value: &str) -> Result<Schedule> {
        Schedule::new(None, Some(value), "Inspect pending tasks", &Value::Null)
    }

    #[test]
    fn validates_interval_precision_and_advances_from_the_scheduled_tick() {
        for invalid in ["0s", "1ns", "1.5ms", "-1s", "later"] {
            assert!(interval(invalid).is_err(), "{invalid}");
        }
        let schedule = interval("1.5s").unwrap();
        assert_eq!(schedule.first_tick(10_000).unwrap(), 10_000);
        assert_eq!(schedule.next_tick(10_000).unwrap(), 11_500);
        assert!(schedule.next_tick(i64::MAX).is_err());
    }

    #[test]
    fn requires_one_timing_and_five_cron_fields() {
        assert!(Schedule::new(None, None, "task", &Value::Null).is_err());
        assert!(Schedule::new(Some("15s"), Some("* * * * *"), "task", &Value::Null).is_err());
        for invalid in [
            "* * * *",
            "* * * * * *",
            "0 0 * * * 2026",
            "70 * * * *",
            "@daily",
        ] {
            assert!(cron(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn cron_uses_utc_and_excludes_the_previous_match() {
        let schedule = cron("*/15 * * * *").unwrap();
        let before = Utc
            .with_ymd_and_hms(2026, 11, 3, 12, 7, 30)
            .unwrap()
            .timestamp_millis();
        let first = Utc
            .with_ymd_and_hms(2026, 11, 3, 12, 15, 0)
            .unwrap()
            .timestamp_millis();
        assert_eq!(schedule.first_tick(before).unwrap(), first);
        assert_eq!(schedule.next_tick(first).unwrap(), first + 15 * 60_000);
    }

    #[test]
    fn cron_preserves_standard_day_of_month_or_weekday_semantics() {
        let schedule = cron("0 9 1 * MON").unwrap();
        let before = Utc
            .with_ymd_and_hms(2026, 11, 3, 12, 0, 0)
            .unwrap()
            .timestamp_millis();
        let monday = Utc
            .with_ymd_and_hms(2026, 11, 9, 9, 0, 0)
            .unwrap()
            .timestamp_millis();
        assert_eq!(schedule.next_tick(before).unwrap(), monday);
    }

    #[test]
    fn identities_are_stable_and_timing_only_changes_the_source() {
        let context: Value =
            serde_json::from_str(r#"{"project":"example","options":{"b":2,"a":1}}"#).unwrap();
        let reordered: Value =
            serde_json::from_str(r#"{"options":{"a":1,"b":2},"project":"example"}"#).unwrap();
        let first = Schedule::new(Some("1m"), None, "task", &context).unwrap();
        let same = Schedule::new(Some("60s"), None, "task", &reordered).unwrap();
        let changed = Schedule::new(Some("2m"), None, "task", &context).unwrap();
        let different_action = Schedule::new(Some("1m"), None, "another task", &context).unwrap();
        assert_eq!(first.source(), same.source());
        assert_eq!(first.subject(), same.subject());
        assert_ne!(first.source(), changed.source());
        assert_eq!(first.subject(), changed.subject());
        assert_ne!(first.subject(), different_action.subject());
    }
}
