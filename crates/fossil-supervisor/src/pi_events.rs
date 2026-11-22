use serde_json::Value;
use std::fmt;
use thiserror::Error;

const MAX_PI_EVENT_STREAM_BYTES: usize = 16 * 1024 * 1024;
const MAX_PI_EVENT_COUNT: usize = 4_096;
const MAX_PI_EVENT_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PiCompletion {
    Completed,
    ProviderError,
    Aborted,
    Incomplete,
}

/// A transcript-free summary of a Pi JSONL stream. It deliberately omits
/// model text, tool arguments, tool output, session IDs, and paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PiEventSummary {
    pub completion: PiCompletion,
    pub event_count: usize,
    pub tool_call_count: usize,
    pub output_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PiStopReason {
    /// Persisted cancellation intent was committed before the observed exit.
    Cancelled,
    /// Persisted timeout intent was committed before the observed exit.
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PiExecutionClass {
    Completed,
    ProviderError,
    PiProcessFailure,
    Cancelled,
    TimedOut,
    InvalidEventStream,
    OutputTruncated,
    ExecutionUncertain,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PiStreamError {
    #[error("Pi JSONL stream exceeds the configured bound")]
    StreamTooLarge,
    #[error("Pi JSONL record exceeds the configured bound")]
    RecordTooLarge,
    #[error("Pi JSONL stream contains too many records")]
    TooManyEvents,
    #[error("Pi JSONL stream contains a malformed record")]
    MalformedRecord,
    #[error("Pi JSONL stream contains an invalid event sequence")]
    InvalidSequence,
}

/// Bounded incremental parser for the pinned Pi CLI JSON event protocol.
/// It retains only the current line and transcript-free counters; prompts,
/// assistant text, tool arguments, and tool results are discarded per event.
#[derive(Default)]
pub struct PiEventParser {
    stream_bytes: usize,
    pending_line: Vec<u8>,
    event_count: usize,
    tool_call_count: usize,
    final_stop_reason: Option<&'static str>,
    settled: Option<bool>,
}

impl fmt::Debug for PiEventParser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PiEventParser")
            .field("stream_bytes", &self.stream_bytes)
            .field("buffered_line_bytes", &self.pending_line.len())
            .field("event_count", &self.event_count)
            .field("tool_call_count", &self.tool_call_count)
            .field("has_final_stop_reason", &self.final_stop_reason.is_some())
            .field("settled", &self.settled)
            .finish()
    }
}

impl PiEventParser {
    /// Add an arbitrary stdout chunk. The total observed stream, unfinished
    /// line, and event count each have independent bounds.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), PiStreamError> {
        self.stream_bytes = self
            .stream_bytes
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_PI_EVENT_STREAM_BYTES)
            .ok_or(PiStreamError::StreamTooLarge)?;

        for byte in bytes {
            if *byte == b'\n' {
                let mut line = std::mem::take(&mut self.pending_line);
                let result = self.record_line(&line);
                line.clear();
                self.pending_line = line;
                result?;
            } else {
                if self.pending_line.len() >= MAX_PI_EVENT_LINE_BYTES {
                    return Err(PiStreamError::RecordTooLarge);
                }
                self.pending_line.push(*byte);
            }
        }
        Ok(())
    }

    /// Finish parsing. An untruncated final record need not have a trailing
    /// newline. When Docker reports capture truncation, any partial final line
    /// is discarded and completion is forced to `Incomplete`.
    pub fn finish(mut self, output_truncated: bool) -> Result<PiEventSummary, PiStreamError> {
        if !output_truncated && !self.pending_line.is_empty() {
            let line = std::mem::take(&mut self.pending_line);
            self.record_line(&line)?;
        }

        let completion = if output_truncated {
            PiCompletion::Incomplete
        } else {
            match (self.settled, self.final_stop_reason) {
                (Some(true), _) => PiCompletion::Aborted,
                (Some(false), Some("error")) => PiCompletion::ProviderError,
                (Some(false), Some("aborted")) => PiCompletion::Aborted,
                (Some(false), Some("stop")) => PiCompletion::Completed,
                _ => PiCompletion::Incomplete,
            }
        };

        Ok(PiEventSummary {
            completion,
            event_count: self.event_count,
            tool_call_count: self.tool_call_count,
            output_truncated,
        })
    }

    fn record_line(&mut self, raw_line: &[u8]) -> Result<(), PiStreamError> {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            return Ok(());
        }
        if self.settled.is_some() {
            return Err(PiStreamError::InvalidSequence);
        }

        let record: Value =
            serde_json::from_slice(line).map_err(|_| PiStreamError::MalformedRecord)?;
        let event = record
            .as_object()
            .and_then(|object| object.get("type"))
            .and_then(Value::as_str)
            .ok_or(PiStreamError::MalformedRecord)?;

        self.event_count += 1;
        if self.event_count > MAX_PI_EVENT_COUNT {
            return Err(PiStreamError::TooManyEvents);
        }

        match event {
            "message_end" => {
                let message = record
                    .get("message")
                    .and_then(Value::as_object)
                    .ok_or(PiStreamError::MalformedRecord)?;
                if message.get("role").and_then(Value::as_str) == Some("assistant") {
                    let reason = message
                        .get("stopReason")
                        .and_then(Value::as_str)
                        .ok_or(PiStreamError::MalformedRecord)?;
                    self.final_stop_reason = Some(match reason {
                        "error" => "error",
                        "aborted" => "aborted",
                        "stop" => "stop",
                        _ => "other",
                    });
                }
            }
            "tool_execution_start" => {
                self.tool_call_count = self.tool_call_count.saturating_add(1);
            }
            "agent_settled" => {
                self.settled = Some(
                    record
                        .get("aborted")
                        .and_then(Value::as_bool)
                        .ok_or(PiStreamError::MalformedRecord)?,
                );
            }
            _ => {}
        }
        Ok(())
    }
}

/// Classify the Pi stream together with the actual container process result.
/// A caller supplies a stop reason only when its durable intent predates the
/// observed process exit, preserving the existing cancellation race rule.
pub fn classify_pi_execution(
    stream: Result<PiEventSummary, PiStreamError>,
    process_exit_code: Option<i32>,
    stop_reason: Option<PiStopReason>,
) -> PiExecutionClass {
    match stop_reason {
        Some(PiStopReason::Cancelled) => return PiExecutionClass::Cancelled,
        Some(PiStopReason::TimedOut) => return PiExecutionClass::TimedOut,
        None => {}
    }

    let summary = match stream {
        Ok(summary) => summary,
        Err(PiStreamError::StreamTooLarge | PiStreamError::TooManyEvents) => {
            return PiExecutionClass::OutputTruncated;
        }
        Err(_) => return PiExecutionClass::InvalidEventStream,
    };
    if summary.output_truncated {
        return PiExecutionClass::OutputTruncated;
    }

    match summary.completion {
        PiCompletion::ProviderError => PiExecutionClass::ProviderError,
        PiCompletion::Aborted => PiExecutionClass::PiProcessFailure,
        PiCompletion::Incomplete => PiExecutionClass::ExecutionUncertain,
        PiCompletion::Completed => match process_exit_code {
            Some(0) => PiExecutionClass::Completed,
            Some(_) => PiExecutionClass::PiProcessFailure,
            None => PiExecutionClass::ExecutionUncertain,
        },
    }
}

/// Summarize one bounded Pi JSONL stream without retaining or exposing task
/// content. Streaming Docker callers should use [`PiEventParser`] directly.
pub fn summarize_pi_jsonl(
    bytes: &[u8],
    output_truncated: bool,
) -> Result<PiEventSummary, PiStreamError> {
    let mut parser = PiEventParser::default();
    parser.push(bytes)?;
    parser.finish(output_truncated)
}

#[cfg(test)]
mod tests {
    use super::{
        PiCompletion, PiEventParser, PiExecutionClass, PiStopReason, PiStreamError,
        classify_pi_execution, summarize_pi_jsonl,
    };

    const SUCCESS: &str = concat!(
        r#"{"type":"session","version":3,"id":"test","timestamp":"2026-11-22T00:00:00Z","cwd":"/workspace"}"#,
        "\n",
        r#"{"type":"agent_start"}"#,
        "\n",
        r#"{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[]}}"#,
        "\n",
        r#"{"type":"agent_settled","aborted":false}"#,
        "\n"
    );

    #[test]
    fn requires_a_settled_assistant_response_for_completion() {
        let result = summarize_pi_jsonl(SUCCESS.as_bytes(), false).unwrap();
        assert_eq!(result.completion, PiCompletion::Completed);
        assert_eq!(result.event_count, 4);
        assert!(!result.output_truncated);
    }

    #[test]
    fn classifies_provider_errors_and_aborts_without_reading_assistant_text() {
        let provider_error = concat!(
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"private diagnostic","content":[]}}"#,
            "\n",
            r#"{"type":"agent_settled","aborted":false}"#,
            "\n"
        );
        let aborted = concat!(
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"aborted","content":[]}}"#,
            "\n",
            r#"{"type":"agent_settled","aborted":true}"#,
            "\n"
        );

        assert_eq!(
            summarize_pi_jsonl(provider_error.as_bytes(), false)
                .unwrap()
                .completion,
            PiCompletion::ProviderError
        );
        assert_eq!(
            summarize_pi_jsonl(aborted.as_bytes(), false)
                .unwrap()
                .completion,
            PiCompletion::Aborted
        );
    }

    #[test]
    fn marks_partial_or_truncated_streams_incomplete() {
        let no_settled =
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"stop"}}"#;
        let successful_but_truncated = SUCCESS;
        let reached_token_limit = concat!(
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"length"}}"#,
            "\n",
            r#"{"type":"agent_settled","aborted":false}"#,
            "\n"
        );

        assert_eq!(
            summarize_pi_jsonl(no_settled.as_bytes(), false)
                .unwrap()
                .completion,
            PiCompletion::Incomplete
        );
        assert_eq!(
            summarize_pi_jsonl(successful_but_truncated.as_bytes(), true)
                .unwrap()
                .completion,
            PiCompletion::Incomplete
        );
        assert_eq!(
            summarize_pi_jsonl(reached_token_limit.as_bytes(), false)
                .unwrap()
                .completion,
            PiCompletion::Incomplete
        );
    }

    #[test]
    fn rejects_malformed_and_oversized_records() {
        assert_eq!(
            summarize_pi_jsonl(b"{not-json}\n", false).unwrap_err(),
            PiStreamError::MalformedRecord
        );
        let oversized = vec![b'x'; super::MAX_PI_EVENT_LINE_BYTES + 1];
        assert_eq!(
            summarize_pi_jsonl(&oversized, false).unwrap_err(),
            PiStreamError::RecordTooLarge
        );
    }

    #[test]
    fn enforces_total_stream_and_event_count_bounds() {
        let oversized_stream = vec![b'\n'; super::MAX_PI_EVENT_STREAM_BYTES + 1];
        assert_eq!(
            summarize_pi_jsonl(&oversized_stream, false).unwrap_err(),
            PiStreamError::StreamTooLarge
        );

        let max_events = "{\"type\":\"x\"}\n".repeat(super::MAX_PI_EVENT_COUNT);
        let summary = summarize_pi_jsonl(max_events.as_bytes(), false).unwrap();
        assert_eq!(summary.event_count, super::MAX_PI_EVENT_COUNT);

        let too_many_events = "{\"type\":\"x\"}\n".repeat(super::MAX_PI_EVENT_COUNT + 1);
        assert_eq!(
            summarize_pi_jsonl(too_many_events.as_bytes(), false).unwrap_err(),
            PiStreamError::TooManyEvents
        );
    }

    #[test]
    fn counts_tool_calls_without_retaining_tool_arguments_or_results() {
        let stream = concat!(
            r#"{"type":"tool_execution_start","toolCallId":"call-1","toolName":"bash","args":{"command":"sensitive command"}}"#,
            "\n",
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[]}}"#,
            "\n",
            r#"{"type":"agent_settled","aborted":false}"#,
            "\n"
        );

        let result = summarize_pi_jsonl(stream.as_bytes(), false).unwrap();
        assert_eq!(result.tool_call_count, 1);
        assert!(!format!("{result:?}").contains("sensitive command"));
    }

    #[test]
    fn incremental_parser_accepts_arbitrary_chunk_boundaries_and_unterminated_final_line() {
        let stream = SUCCESS.trim_end().as_bytes();
        let mut parser = PiEventParser::default();
        for chunk in stream.chunks(7) {
            parser.push(chunk).unwrap();
        }

        let summary = parser.finish(false).unwrap();
        assert_eq!(summary.completion, PiCompletion::Completed);
        assert_eq!(summary.event_count, 4);
    }

    #[test]
    fn incremental_parser_accepts_streams_larger_than_legacy_cap() {
        let stream = "{\"type\":\"x\",\"padding\":\"0123456789abcdef\"}\n".repeat(4_096);
        assert!(stream.len() > 64 * 1024);

        let mut parser = PiEventParser::default();
        for chunk in stream.as_bytes().chunks(733) {
            parser.push(chunk).unwrap();
        }

        let summary = parser.finish(false).unwrap();
        assert_eq!(summary.event_count, 4_096);
        assert_eq!(summary.completion, PiCompletion::Incomplete);
    }

    #[test]
    fn successful_result_requires_both_pi_completion_and_zero_process_exit() {
        let completed = summarize_pi_jsonl(SUCCESS.as_bytes(), false).unwrap();
        assert_eq!(
            classify_pi_execution(Ok(completed), Some(0), None),
            PiExecutionClass::Completed
        );
        assert_eq!(
            classify_pi_execution(Ok(completed), Some(1), None),
            PiExecutionClass::PiProcessFailure
        );
        assert_eq!(
            classify_pi_execution(Ok(completed), None, None),
            PiExecutionClass::ExecutionUncertain
        );
    }

    #[test]
    fn provider_errors_and_capture_failures_never_become_success() {
        let provider_error = concat!(
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"error","content":[]}}"#,
            "\n",
            r#"{"type":"agent_settled","aborted":false}"#,
            "\n"
        );
        let summary = summarize_pi_jsonl(provider_error.as_bytes(), false).unwrap();
        assert_eq!(
            classify_pi_execution(Ok(summary), Some(0), None),
            PiExecutionClass::ProviderError
        );
        assert_eq!(
            classify_pi_execution(Err(PiStreamError::StreamTooLarge), Some(0), None),
            PiExecutionClass::OutputTruncated
        );
        assert_eq!(
            classify_pi_execution(Err(PiStreamError::MalformedRecord), Some(0), None),
            PiExecutionClass::InvalidEventStream
        );
    }

    #[test]
    fn durable_stop_reason_wins_over_the_pi_exit_race() {
        let completed = summarize_pi_jsonl(SUCCESS.as_bytes(), false).unwrap();
        assert_eq!(
            classify_pi_execution(Ok(completed), Some(0), Some(PiStopReason::Cancelled)),
            PiExecutionClass::Cancelled
        );
        assert_eq!(
            classify_pi_execution(Ok(completed), Some(137), Some(PiStopReason::TimedOut)),
            PiExecutionClass::TimedOut
        );
    }

    #[test]
    fn output_truncation_cannot_be_success_even_with_a_final_settled_event() {
        let mut parser = PiEventParser::default();
        parser.push(SUCCESS.as_bytes()).unwrap();

        assert_eq!(
            parser.finish(true).unwrap().completion,
            PiCompletion::Incomplete
        );
    }

    #[test]
    fn incremental_parser_bounds_partial_line_memory() {
        let mut parser = PiEventParser::default();
        let first = vec![b'x'; super::MAX_PI_EVENT_LINE_BYTES];
        parser.push(&first).unwrap();
        assert_eq!(
            parser.push(b"x").unwrap_err(),
            PiStreamError::RecordTooLarge
        );
    }

    #[test]
    fn parser_debug_reports_only_buffer_length_not_pending_json() {
        let mut parser = PiEventParser::default();
        let record = br#"{"type":"message_end","message":{"role":"assistant","content":"private assistant text"}}"#;
        parser.push(record).unwrap();

        let debug = format!("{parser:?}");
        assert!(debug.contains("buffered_line_bytes"));
        assert!(!debug.contains("pending_line"));
        assert!(!debug.contains("private assistant text"));
    }
}
