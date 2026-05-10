// This file is part of core-net Rust crate containing useful reusable
// networking utilities.
//
// Copyright (C) 2026 to present, Duncan Crutchley
// Contact <15799155+dac1976@users.noreply.github.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License and GNU Lesser General Public License
// for more details.
//
// You should have received a copy of the GNU General Public License
// and GNU Lesser General Public License along with this program. If
// not, see <http://www.gnu.org/licenses/>.

use miette::{IntoDiagnostic, Result};

use std::{
    fmt::{self, Write as _},
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use time::{OffsetDateTime, UtcOffset, format_description::FormatItem, macros::format_description};

use tracing::{Event, Subscriber};

use tracing_rolling_file::{RollingConditionBase, RollingFileAppender};

use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::{
        FmtContext, Layer as FmtLayer,
        format::{FormatEvent, FormatFields, Writer},
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
};

/// Controls how timestamps are emitted in log output.
///
/// The logging subsystem supports UTC and local-time variants so applications
/// can choose the most appropriate representation for operational use.
#[derive(Debug, Clone, Copy)]
pub enum LogTimeMode {
    /// Log timestamps in UTC.
    ///
    /// Recommended for distributed systems and multi-site deployments because
    /// timestamps are globally comparable.
    Utc,

    /// Log timestamps in local time without displaying the UTC offset.
    ///
    /// Useful for simple local demos, but less ideal for distributed logs.
    Local,

    /// Log timestamps in local time including the UTC offset.
    ///
    /// Useful when human readability is desired but the offset must still be
    /// visible for correlation.
    LocalWithOffset,
}

/// Configuration for the core_net tracing/logging subsystem.
///
/// This configures:
///
/// - log directory
/// - log file name
/// - size-based log rotation
/// - retained file count
/// - filtering level
/// - timestamp mode
/// - optional stderr mirroring
#[derive(Debug, Clone)]
pub struct LoggingConfig<'a> {
    /// Directory where log files are written.
    pub directory: &'a str,

    /// Active log file name inside `directory`.
    pub file_name: &'a str,

    /// Maximum size in bytes before the rolling file appender rotates.
    pub max_bytes: u64,

    /// Number of rolled log files to retain.
    pub keep_files: usize,

    /// Tracing filter expression.
    ///
    /// Examples:
    ///
    /// - `"info"`
    /// - `"debug"`
    /// - `"core_net=debug,my_app=trace"`
    pub level_filter: &'a str,

    /// Timestamp formatting mode.
    pub time_mode: LogTimeMode,

    /// Mirror logs to stderr too.
    ///
    /// Useful for demos, local development and services running in containers.
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

/// Writer adapter handed to `tracing-subscriber`.
///
/// The underlying rolling file appender is shared through:
///
/// ```text
/// Arc<Mutex<RollingFileAppender<_>>>
/// ```
///
/// This makes the writer clonable, which is required by tracing-subscriber,
/// while still serialising actual file writes through a mutex.
///
/// The writer flushes on each write so log messages are durable and visible
/// promptly during debugging and demos.
#[derive(Clone)]
struct SharedRollingWriter {
    inner: Arc<Mutex<RollingFileAppender<RollingConditionBase>>>,
}

impl io::Write for SharedRollingWriter {
    /// Writes log bytes to the rolling file appender.
    ///
    /// The mutex protects the underlying file appender from concurrent writes.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("rolling log writer mutex poisoned"))?;

        let written = guard.write(buf)?;

        // Flush immediately so logs are not lost if the process exits or
        // crashes shortly after writing.
        guard.flush()?;

        Ok(written)
    }

    /// Flushes the underlying rolling file appender.
    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("rolling log writer mutex poisoned"))?;

        guard.flush()
    }
}

/// Initialises global tracing/logging for an application.
///
/// This installs a tracing subscriber with:
///
/// - size-based rolling file output
/// - optional stderr output
/// - custom compact event formatting
///
/// This should normally be called once near application startup.
///
/// Returns a `miette::Result` so binaries can use ergonomic `.wrap_err(...)`
/// context at call sites.
pub fn init_tracing(cfg: &LoggingConfig<'_>) -> Result<()> {
    // Ensure log directory exists before creating the rolling appender.
    std::fs::create_dir_all(cfg.directory).into_diagnostic()?;

    let log_path: PathBuf = PathBuf::from(cfg.directory).join(cfg.file_name);

    // Create size-based rolling file appender.
    let appender = RollingFileAppender::new(
        &log_path,
        RollingConditionBase::new().max_size(cfg.max_bytes),
        cfg.keep_files,
    )
    .into_diagnostic()?;

    let shared = Arc::new(Mutex::new(appender));

    // File logging layer.
    //
    // ANSI colour is disabled for file output.
    let file_layer = FmtLayer::default()
        .with_ansi(false)
        .with_writer({
            let shared = Arc::clone(&shared);

            move || SharedRollingWriter {
                inner: Arc::clone(&shared),
            }
        })
        .event_format(CustomEventFormatter::new(cfg.time_mode));

    // Base subscriber with environment/filter configuration and file output.
    let subscriber = Registry::default()
        .with(EnvFilter::try_new(cfg.level_filter).into_diagnostic()?)
        .with(file_layer);

    // Optionally mirror logs to stderr.
    //
    // ANSI colour is enabled for stderr output.
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

/// Custom event formatter used by core_net logging.
///
/// Output format:
///
/// ```text
/// timestamp | level | message | source_file | function/span | line | thread_id
/// ```
///
/// Example:
///
/// ```text
/// 2026-05-10 12:34:56.789 UTC | INFO | server started | src/main.rs | main | 42 | ThreadId(1)
/// ```
#[derive(Clone)]
pub struct CustomEventFormatter {
    time_mode: LogTimeMode,
}

impl CustomEventFormatter {
    /// Creates a new formatter with the requested timestamp mode.
    pub fn new(time_mode: LogTimeMode) -> Self {
        Self { time_mode }
    }

    /// Builds the formatted timestamp string for the current event.
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

/// Visitor used to extract the main tracing event message.
///
/// tracing events store fields generically. The human-readable log message is
/// normally stored in the special `"message"` field created by macros like:
///
/// ```rust
/// info!("server started");
/// error!(error = %err, "operation failed");
/// ```
///
/// This visitor extracts only that message text for compact log formatting.
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
    /// Captures debug-formatted message fields.
    ///
    /// String values recorded through debug formatting often include quotes,
    /// so this removes surrounding quotes for cleaner output.
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

    /// Captures string message fields directly.
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
    /// Formats one tracing event into one log line.
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        // Extract message field.
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let timestamp = self.timestamp_string();

        let level = meta.level();

        let file = meta.file().unwrap_or("unknown");

        let line = meta
            .line()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Prefer the current span name if one exists.
        //
        // For #[instrument] functions this usually gives the function name.
        // Otherwise fall back to module path.
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
