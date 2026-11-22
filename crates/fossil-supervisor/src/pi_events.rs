use serde_json::Value;
use thiserror::Error;

const MAX_PI_EVENT_STREAM_BYTES: usize = 64 * 1024;
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

/// Summarize the pinned Pi CLI JSON event protocol without retaining or
/// exposing prompt, assistant text, tool arguments, or tool results.
///
/// Pi JSON mode is successful only when the stream ends with a non-aborted
/// `agent_settled` event and its final assistant message has `stopReason=stop`.
/// A process exit code alone is insufficient because JSON mode represents
/// assistant errors in events and can still exit with status zero.
pub fn summarize_pi_jsonl(
    bytes: &[u8],
    output_truncated: bool,
) -> Result<PiEventSummary, PiStreamError> {
    if bytes.len() > MAX_PI_EVENT_STREAM_BYTES {
        return Err(PiStreamError::StreamTooLarge);
    }

    let mut event_count = 0_usize;
    let mut tool_call_count = 0_usize;
    let mut final_stop_reason = None;
    let mut settled = None;

    for raw_line in bytes.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() {
            continue;
        }
        if line.len() > MAX_PI_EVENT_LINE_BYTES {
            return Err(PiStreamError::RecordTooLarge);
        }
        if settled.is_some() {
            return Err(PiStreamError::InvalidSequence);
        }

        let record: Value =
            serde_json::from_slice(line).map_err(|_| PiStreamError::MalformedRecord)?;
        let event = record
            .as_object()
            .and_then(|object| object.get("type"))
            .and_then(Value::as_str)
            .ok_or(PiStreamError::MalformedRecord)?;

        event_count += 1;
        if event_count > MAX_PI_EVENT_COUNT {
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
                    final_stop_reason = Some(match reason {
                        "error" => "error",
                        "aborted" => "aborted",
                        "stop" => "stop",
                        _ => "other",
                    });
                }
            }
            "tool_execution_start" => {
                tool_call_count = tool_call_count.saturating_add(1);
            }
            "agent_settled" => {
                settled = Some(
                    record
                        .get("aborted")
                        .and_then(Value::as_bool)
                        .ok_or(PiStreamError::MalformedRecord)?,
                );
            }
            _ => {}
        }
    }

    let completion = if output_truncated {
        PiCompletion::Incomplete
    } else {
        match (settled, final_stop_reason) {
            (Some(true), _) => PiCompletion::Aborted,
            (Some(false), Some("error")) => PiCompletion::ProviderError,
            (Some(false), Some("aborted")) => PiCompletion::Aborted,
            (Some(false), Some("stop")) => PiCompletion::Completed,
            _ => PiCompletion::Incomplete,
        }
    };

    Ok(PiEventSummary {
        completion,
        event_count,
        tool_call_count,
        output_truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::{PiCompletion, PiStreamError, summarize_pi_jsonl};

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
}
