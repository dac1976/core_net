use miette::{IntoDiagnostic, Result};
use std::{
    fmt::{self, Write as _},
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use time::{format_description::FormatItem, macros::format_description, OffsetDateTime, UtcOffset};
use tracing::{Event, Subscriber};
use tracing_rolling_file::{RollingConditionBase, RollingFileAppender};
use tracing_subscriber::{
    fmt::{
        format::{FormatEvent, FormatFields, Writer},
        FmtContext, Layer as FmtLayer,
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter, Registry,
};

#[derive(Debug, Clone, Copy)]
pub enum LogTimeMode {
    Utc,
    Local,
    LocalWithOffset,
}

#[derive(Debug, Clone)]
pub struct LoggingConfig<'a> {
    pub directory: &'a str,
    pub file_name: &'a str,
    pub max_bytes: u64,
    pub keep_files: usize,
    pub level_filter: &'a str,
    pub time_mode: LogTimeMode,

    /// Mirror logs to stderr too, useful for demos and debugging.
    pub also_stderr: bool,
}

impl<'a> Default for LoggingConfig<'a> {
    fn default() -> Self {
        Self {
            directory: "logs",
            file_name: "application.log",
            max_bytes: 10 * 1024 * 1024,
            keep_files: 10,
            level_filter: "info",
            time_mode: LogTimeMode::Utc,
            also_stderr: true,
        }
    }
}

/// Writer handed to tracing-subscriber.
/// Internally locks the shared rolling appender and flushes on each write.
#[derive(Clone)]
struct SharedRollingWriter {
    inner: Arc<Mutex<RollingFileAppender<RollingConditionBase>>>,
}

impl io::Write for SharedRollingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("rolling log writer mutex poisoned"))?;

        let written = guard.write(buf)?;
        guard.flush()?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("rolling log writer mutex poisoned"))?;
        guard.flush()
    }
}

pub fn init_tracing(cfg: &LoggingConfig<'_>) -> Result<()> {
    std::fs::create_dir_all(cfg.directory).into_diagnostic()?;

    let log_path: PathBuf = PathBuf::from(cfg.directory).join(cfg.file_name);

    let appender = RollingFileAppender::new(
        &log_path,
        RollingConditionBase::new().max_size(cfg.max_bytes),
        cfg.keep_files,
    )
    .into_diagnostic()?;

    let shared = Arc::new(Mutex::new(appender));

    let file_layer = FmtLayer::default()
        .with_ansi(false)
        .with_writer({
            let shared = Arc::clone(&shared);
            move || SharedRollingWriter {
                inner: Arc::clone(&shared),
            }
        })
        .event_format(CustomEventFormatter::new(cfg.time_mode));

    let subscriber = Registry::default()
        .with(EnvFilter::try_new(cfg.level_filter).into_diagnostic()?)
        .with(file_layer);

    if cfg.also_stderr {
        let stderr_layer = FmtLayer::default()
            .with_ansi(true)
            .with_writer(std::io::stderr)
            .event_format(CustomEventFormatter::new(cfg.time_mode));

        subscriber.with(stderr_layer).init();
    } else {
        subscriber.init();
    }

    Ok(())
}

#[derive(Clone)]
pub struct CustomEventFormatter {
    time_mode: LogTimeMode,
}

impl CustomEventFormatter {
    pub fn new(time_mode: LogTimeMode) -> Self {
        Self { time_mode }
    }

    fn timestamp_string(&self) -> String {
        const UTC_FMT: &[FormatItem<'static>] = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3] UTC"
        );

        const LOCAL_FMT: &[FormatItem<'static>] = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]"
        );

        const LOCAL_OFFSET_FMT: &[FormatItem<'static>] = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3] [offset_hour sign:mandatory]:[offset_minute]"
        );

        match self.time_mode {
            LogTimeMode::Utc => OffsetDateTime::now_utc()
                .format(UTC_FMT)
                .unwrap_or_else(|_| "0000-00-00 00:00:00.000 UTC".to_string()),

            LogTimeMode::Local => {
                let now = match UtcOffset::current_local_offset() {
                    Ok(offset) => OffsetDateTime::now_utc().to_offset(offset),
                    Err(_) => OffsetDateTime::now_utc(),
                };

                now.format(LOCAL_FMT)
                    .unwrap_or_else(|_| "0000-00-00 00:00:00.000".to_string())
            }

            LogTimeMode::LocalWithOffset => {
                let now = match UtcOffset::current_local_offset() {
                    Ok(offset) => OffsetDateTime::now_utc().to_offset(offset),
                    Err(_) => OffsetDateTime::now_utc(),
                };

                now.format(LOCAL_OFFSET_FMT)
                    .unwrap_or_else(|_| "0000-00-00 00:00:00.000 +00:00".to_string())
            }
        }
    }
}

struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(&mut self.message, "{value:?}");
            if self.message.starts_with('"')
                && self.message.ends_with('"')
                && self.message.len() >= 2
            {
                self.message.remove(0);
                self.message.pop();
            }
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message.clear();
            self.message.push_str(value);
        }
    }
}

impl<S, N> FormatEvent<S, N> for CustomEventFormatter
where
    S: Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let timestamp = self.timestamp_string();
        let level = meta.level();
        let file = meta.file().unwrap_or("unknown");
        let line = meta
            .line()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let function_name = if let Some(span) = ctx.lookup_current() {
            span.name().to_string()
        } else {
            meta.module_path().unwrap_or("unknown").to_string()
        };

        let thread_id = format!("{:?}", std::thread::current().id());

        writeln!(
            writer,
            "{} | {} | {} | {} | {} | {} | {}",
            timestamp, level, visitor.message, file, function_name, line, thread_id
        )
    }
}
