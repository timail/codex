//! Regression coverage for retry advice carried by failed Responses events.

use super::process_responses_event;
use super::spawn_response_stream;
use crate::api_bridge::map_api_error;
use crate::common::ResponseEvent;
use crate::error::ApiError;
use bytes::Bytes;
use codex_client::StreamResponse;
use codex_http_client::RetryAfter;
use codex_http_client::TransportError;
use codex_protocol::protocol::CodexErrorInfo;
use futures::StreamExt;
use futures::stream;
use http::HeaderMap;
use http::StatusCode;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;

#[test]
fn audio_validation_codes_map_to_non_retryable_audio_input_errors_on_all_transports() {
    for code in [
        "audio_too_large",
        "audio_too_short",
        "invalid_audio",
        "invalid_audio_data",
        "invalid_audio_format",
        "missing_audio_sample_rate",
    ] {
        let detail = json!({"code": code, "message": "Invalid recording."});
        let http_error = map_api_error(crate::error::ApiError::Transport(
            crate::TransportError::Http {
                status: StatusCode::BAD_REQUEST,
                url: None,
                headers: None,
                body: Some(json!({"error": detail}).to_string()),
                retry_after: None,
            },
        ));
        let mut errors = vec![http_error];
        for failure in [
            json!({"type": "error", "error": detail}),
            json!({"type": "response.failed", "response": {"error": detail}}),
        ] {
            let event = serde_json::from_value(failure).unwrap();
            errors.push(map_api_error(
                process_responses_event(event).unwrap_err().into_api_error(),
            ));
        }
        for error in errors {
            assert_eq!(error.retry_delay(/*retry_count*/ 1), None);
            assert!(matches!(
                error.details(),
                codex_protocol::error::CodexErrorDetails::InvalidAudioInput { .. }
            ));
            assert_eq!(error.to_string(), "Invalid recording.");
        }
    }
}

/// Only the explicit policy capacity code changes overload attribution.
#[tokio::test(start_paused = true)]
async fn policy_capacity_classification_preserves_overload_retry_behavior() {
    for (code, error_type, expected) in [
        (
            "policy_capacity_unavailable",
            None,
            CodexErrorInfo::PolicyCapacityUnavailable,
        ),
        (
            "server_is_overloaded",
            Some("policy_error"),
            CodexErrorInfo::ServerOverloaded,
        ),
        (
            "server_is_overloaded",
            None,
            CodexErrorInfo::ServerOverloaded,
        ),
        (
            "server_is_overloaded",
            Some("rate_limit_error"),
            CodexErrorInfo::ServerOverloaded,
        ),
        (
            "policy_capacity_unavailable",
            Some("throttling_error"),
            CodexErrorInfo::PolicyCapacityUnavailable,
        ),
    ] {
        for retry_after in [Some("3"), None] {
            let event = serde_json::from_value(json!({
                "type": "response.failed",
                "response": {"error": {
                    "type": error_type,
                    "code": code,
                    "headers": {"Retry-After": retry_after},
                }},
            }))
            .unwrap();
            let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
            assert_eq!(
                (
                    error.to_codex_protocol_error(),
                    error.retry_delay(/*retry_count*/ 1),
                    error.to_string()
                ),
                (
                    expected.clone(),
                    retry_after.map(|_| Duration::from_secs(3)),
                    if expected == CodexErrorInfo::PolicyCapacityUnavailable {
                        "Policy-restricted capacity is unavailable. Please try again later."
                    } else {
                        "Selected model is at capacity. Please try a different model."
                    }
                    .to_string()
                )
            );
        }
    }
}

/// Terminal provider failures ignore retry headers and stop before a later completed event.
#[tokio::test(start_paused = true)]
async fn terminal_failure_with_retry_header_ends_stream_immediately() {
    for (code, expected) in [
        ("flex_unavailable", CodexErrorInfo::FlexUnavailable),
        ("budget_reached", CodexErrorInfo::SessionBudgetExceeded),
        ("inference_hook_denied", CodexErrorInfo::InferenceHookDenied),
        (
            "context_length_exceeded",
            CodexErrorInfo::ContextWindowExceeded,
        ),
        ("invalid_access_program", CodexErrorInfo::Other),
        ("unsupported_access_program", CodexErrorInfo::Other),
        ("content_policy_violation", CodexErrorInfo::InvalidPrompt),
        (
            "access_program_not_enabled",
            CodexErrorInfo::HttpConnectionFailed {
                http_status_code: None,
            },
        ),
    ] {
        let error = json!({
            "code": code,
            "plan_type": 42,
            "headers": {"retry-after": "300"}
        });
        for failure in [
            json!({"type": "error", "error": error}),
            json!({"type": "response.failed", "response": {"error": error}}),
        ] {
            let completed = json!({"type": "response.completed", "response": {"id": "later"}});
            let bytes = stream::iter([Ok(Bytes::from(format!(
                "data: {failure}\n\ndata: {completed}\n\n"
            )))])
            .chain(stream::pending());
            let mut stream = spawn_response_stream(
                StreamResponse {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    bytes: Box::pin(bytes),
                },
                Duration::from_secs(60),
                /*telemetry*/ None,
                /*turn_state*/ None,
            );
            let error = loop {
                match stream.next().await {
                    Some(Ok(ResponseEvent::RateLimits(_))) => {}
                    Some(Err(error)) => break map_api_error(error),
                    event => panic!("expected a terminal error for {code}, got {event:?}"),
                }
            };
            assert_eq!(
                (
                    error.to_codex_protocol_error(),
                    error.retry_delay(/*retry_count*/ 1)
                ),
                (expected.clone(), None)
            );
            assert!(stream.next().await.is_none());
        }
    }
}

#[test]
fn statusless_model_errors_have_no_http_status() {
    let error = json!({"code": "model_not_found"});
    for event in [
        json!({"type": "error", "error": error}),
        json!({"type": "response.failed", "response": {"error": error}}),
    ] {
        let event = serde_json::from_value(event).unwrap();
        let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
        assert_eq!(
            (
                error.to_codex_protocol_error(),
                error.http_status_code_value()
            ),
            (CodexErrorInfo::ModelNotFound, None)
        );
    }
}

/// Error headers beat rate-limit message advice; invalid ones fall back.
#[tokio::test(start_paused = true)]
async fn rate_limit_plaintext_loses_to_valid_error_header() {
    for code in ["rate_limit_exceeded", "slow_down"] {
        for (error_header, expected_seconds) in [
            (Some("2"), 2),
            (Some("0"), 0),
            (Some("Wed, 21 Oct 2015 07:28:00 GMT"), 0),
            (Some("bad"), 35),
            (None, 35),
        ] {
            let event = serde_json::from_value(json!({
                "type": "response.failed",
                "response": {
                    "error": {
                        "code": code,
                        "message": "Try again in 35 seconds.",
                        "headers": {"Retry-After": error_header}
                    }
                }
            }))
            .unwrap();
            let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
            assert_eq!(
                error.retry_after(),
                RetryAfter::from_delay(Duration::from_secs(expected_seconds))
            );
        }
    }
}

/// Retry advice uses the same HTTP value validation as other event headers.
#[tokio::test(start_paused = true)]
async fn streamed_retry_after_uses_http_header_validation() {
    for (error_headers, expected_seconds) in [
        (json!({"retry-after": "\n5\n"}), 12),
        (json!({"retry-after": "\t5\t"}), 5),
        (json!({"Retry-After": "5", "retry-after": "30"}), 30),
    ] {
        let event = serde_json::from_value(json!({
            "type": "response.failed",
            "response": {"error": {"code": "rate_limit_exceeded", "message": "Try again in 12 seconds.", "headers": error_headers}}
        }))
        .unwrap();
        let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
        assert_eq!(
            error.retry_after(),
            RetryAfter::from_delay(Duration::from_secs(expected_seconds))
        );
    }
}

/// Event headers retain their deadlines for overload and generic failures.
#[tokio::test(start_paused = true)]
async fn retry_header_survives_other_failed_responses() {
    for (code, expected_code) in [
        ("server_is_overloaded", CodexErrorInfo::ServerOverloaded),
        ("safety_violation", CodexErrorInfo::Other),
        ("unknown_failure", CodexErrorInfo::Other),
    ] {
        let event = serde_json::from_value(json!({
            "type": "response.failed",
            "response": {"error": {"code": code, "headers": {"retry-after": "7"}}},
        }))
        .unwrap();
        let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
        assert_eq!(
            (
                error.to_codex_protocol_error(),
                error.retry_after(),
                error.retry_delay(/*retry_count*/ 1),
            ),
            (
                expected_code,
                RetryAfter::from_delay(Duration::from_secs(7)),
                Some(Duration::from_secs(7)),
            )
        );
    }
}

/// An event header cannot make a quota or policy failure retryable.
#[tokio::test(start_paused = true)]
async fn retry_header_does_not_override_terminal_errors() {
    for code in ["insufficient_quota", "cyber_policy", "bio_policy"] {
        let event = serde_json::from_value(json!({
            "type": "response.failed",
            "response": {"error": {"code": code, "headers": {"retry-after": "1"}}}
        }))
        .unwrap();
        let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
        assert_eq!(error.retry_delay(/*retry_count*/ 1), None, "{code}");
    }
}

#[tokio::test(start_paused = true)]
async fn streamed_terminal_limit_markers_override_rate_limit_status_and_headers() {
    for field in ["code", "type", "litellm_error_type"] {
        for marker in [
            "insufficient_quota",
            "credit_balance_exhausted",
            "organization_spend_limit_exceeded",
            "project_spend_limit_exceeded",
            "organization_usage_limit_exceeded",
            "budget_exceeded",
            "usage_limit_reached",
            "usage_not_included",
            "budget_reached",
        ] {
            let mut detail = json!({
                "type": "throttling_error",
                "code": 429,
                "plan_type": 42,
                "headers": {"retry-after": "30"},
            });
            detail[field] = marker.into();
            for payload in [
                json!({"type": "error", "status": 429, "error": detail}),
                json!({"type": "response.failed", "response": {"error": detail}}),
            ] {
                let event = serde_json::from_value(payload).unwrap();
                let error =
                    map_api_error(process_responses_event(event).unwrap_err().into_api_error());
                assert_eq!(
                    error.retry_delay(/*retry_count*/ 1),
                    None,
                    "{field}={marker}"
                );
                let expected = if marker == "budget_reached" {
                    CodexErrorInfo::SessionBudgetExceeded
                } else {
                    CodexErrorInfo::UsageLimitExceeded
                };
                assert_eq!(error.to_codex_protocol_error(), expected);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn streamed_rate_limits_preserve_nested_and_outer_retry_advice() {
    for detail in [
        json!({"code": "rate_limit_exceeded"}),
        json!({"code": "slow_down"}),
        json!({"type": "throttling_error", "code": "429"}),
        json!({"type": "rate_limit_error"}),
        json!({"code": 429}),
        json!({"status": 429}),
        json!({"status_code": "429"}),
    ] {
        for (headers, seconds) in [
            (json!({"retry-after": "7"}), 7),
            (json!({"retry-after": "invalid"}), 11),
        ] {
            let mut detail = detail.clone();
            detail["headers"] = headers;
            detail["message"] = "Try again in 13 seconds.".into();
            for payload in [
                json!({"type": "error", "headers": {"retry-after": "11"}, "error": detail}),
                json!({"type": "response.failed", "response": {"error": detail}}),
            ] {
                let expected_seconds = if payload["type"] == "response.failed" && seconds == 11 {
                    13
                } else {
                    seconds
                };
                let event = serde_json::from_value(payload).unwrap();
                let error =
                    map_api_error(process_responses_event(event).unwrap_err().into_api_error());
                assert_eq!(
                    error.to_codex_protocol_error(),
                    CodexErrorInfo::RateLimitExceeded
                );
                assert_eq!(
                    error.retry_after(),
                    RetryAfter::from_delay(Duration::from_secs(expected_seconds))
                );
            }
        }
    }
    for field in ["status", "status_code"] {
        for status in [json!(429), json!("429")] {
            let mut payload = json!({"type": "error", "error": {"message": "try later"}});
            payload[field] = status;
            let event = serde_json::from_value(payload).unwrap();
            let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
            assert_eq!(
                error.to_codex_protocol_error(),
                CodexErrorInfo::RateLimitExceeded
            );
            assert_eq!(error.retry_after(), None);
        }
    }
}

#[test]
fn streamed_rate_limit_classification_does_not_guess_from_message_text() {
    let detail = json!({"code": "unknown_error", "message": "401 invalid_api_key authentication_error 429 rate_limit_exceeded budget_exceeded 503"});
    let event = serde_json::from_value(json!({"type": "error", "error": detail})).unwrap();
    assert!(process_responses_event(event).unwrap().is_none());
    let event =
        serde_json::from_value(json!({"type": "response.failed", "response": {"error": detail}}))
            .unwrap();
    let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
    assert_eq!(error.to_codex_protocol_error(), CodexErrorInfo::Other);
}

#[test]
fn streamed_rate_limit_classification_respects_explicit_nonrate_status_and_code() {
    for status in [json!(401), json!(503), json!("503")] {
        let event = serde_json::from_value(json!({
            "type": "error", "status": status,
            "error": {"type": "throttling_error", "code": "rate_limit_exceeded"},
        }))
        .unwrap();
        let error = process_responses_event(event).unwrap_err().into_api_error();
        assert!(matches!(
            error,
            ApiError::Transport(TransportError::Http { .. })
        ));
    }
    for code in [json!("server_is_overloaded"), json!("503"), json!(503)] {
        let event = serde_json::from_value(json!({
            "type": "error", "status": 429,
            "error": {"type": "throttling_error", "code": code},
        }))
        .unwrap();
        let result = process_responses_event(event);
        if code == "server_is_overloaded" {
            assert!(result.unwrap().is_none());
        } else {
            assert!(matches!(
                result.unwrap_err().into_api_error(),
                ApiError::Transport(TransportError::Http {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    ..
                })
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn streamed_explicit_http_errors_preserve_status_headers_and_retry_advice() {
    for status in [400, 401, 403, 404, 408, 422, 500, 503] {
        for value in [json!(status), json!(status.to_string())] {
            for field in ["code", "status", "status_code"] {
                let mut detail = json!({
                    "message": "provider rejected this request",
                    "headers": {"Retry-After": "3600", "x-request-id": "sse-request"},
                });
                detail[field] = value.clone();
                for payload in [
                    json!({"type": "error", "error": detail}),
                    json!({"type": "response.failed", "response": {"error": detail}}),
                ] {
                    let event = serde_json::from_value(payload).unwrap();
                    let error = process_responses_event(event).unwrap_err().into_api_error();
                    let ApiError::Transport(TransportError::Http {
                        status: actual_status,
                        headers: Some(headers),
                        body: Some(body),
                        retry_after,
                        ..
                    }) = &error
                    else {
                        panic!("expected structured HTTP error, got {error:?}");
                    };
                    assert_eq!(actual_status.as_u16(), status);
                    assert_eq!(headers["x-request-id"], "sse-request");
                    assert!(body.contains("provider rejected this request"));
                    assert_eq!(
                        *retry_after,
                        RetryAfter::from_delay(Duration::from_secs(3600))
                    );
                    let mapped = map_api_error(error);
                    assert_eq!(
                        mapped.retry_after(),
                        RetryAfter::from_delay(Duration::from_secs(3600))
                    );
                    assert!(!matches!(
                        mapped.details(),
                        codex_protocol::error::CodexErrorDetails::Stream(_)
                            | codex_protocol::error::CodexErrorDetails::RateLimitExceeded(_)
                    ));
                }
            }
        }
    }
}

#[test]
fn streamed_auth_markers_override_rate_limit_status_and_retry_advice() {
    for (marker, status) in [
        ("invalid_api_key", 401),
        ("authentication_error", 401),
        ("auth_error", 401),
        ("token_not_found_in_db", 401),
        ("expired_key", 401),
        ("permission_error", 403),
        ("permission_denied", 403),
        ("key_model_access_denied", 403),
    ] {
        for field in ["code", "type", "litellm_error_type"] {
            let mut detail = json!({
                "code": "429", "type": "throttling_error",
                "message": "credential rejected; try again in 1 second",
                "headers": {"Retry-After": "3600"},
            });
            detail[field] = marker.into();
            for payload in [
                json!({"type": "error", "status": 429, "error": detail}),
                json!({"type": "response.failed", "response": {"error": detail}}),
            ] {
                let event = serde_json::from_value(payload).unwrap();
                let error =
                    map_api_error(process_responses_event(event).unwrap_err().into_api_error());
                assert_eq!(error.http_status_code_value(), Some(status), "{marker}");
                assert!(error.retry_after().is_some());
                assert!(matches!(
                    error.details(),
                    codex_protocol::error::CodexErrorDetails::UnexpectedStatus(_)
                ));
            }
        }
    }
}

#[test]
fn streamed_http_status_classification_rejects_non_http_codes() {
    for code in [
        json!("401 invalid"),
        json!("0401"),
        json!(401.5),
        json!(600),
        json!(-401),
    ] {
        let event = serde_json::from_value(json!({
            "type": "error", "error": {"code": code, "message": "401 authentication_error"},
        }))
        .unwrap();
        assert!(process_responses_event(event).unwrap().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn streamed_http_errors_end_before_later_completion_or_idle_timeout() {
    for payload in [
        json!({"type": "error", "status": 401, "error": {"message": "expired"}}),
        json!({"type": "error", "status_code": "403", "error": {"message": "denied"}}),
        json!({"type": "error", "error": {"code": 500, "headers": {"Retry-After": "3600"}}}),
        json!({"type": "response.failed", "response": {"error": {"code": "invalid_api_key", "type": "authentication_error"}}}),
    ] {
        let completed = json!({"type": "response.completed", "response": {"id": "later"}});
        let bytes = stream::iter([Ok(Bytes::from(format!(
            "data: {payload}\n\ndata: {completed}\n\n"
        )))])
        .chain(stream::pending());
        let mut stream = spawn_response_stream(
            StreamResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                bytes: Box::pin(bytes),
            },
            Duration::from_secs(60),
            /*telemetry*/ None,
            /*turn_state*/ None,
        );
        let started = tokio::time::Instant::now();
        loop {
            match stream.next().await {
                Some(Ok(ResponseEvent::RateLimits(_))) => {}
                Some(Err(ApiError::Transport(TransportError::Http { .. }))) => break,
                event => panic!("expected structured HTTP error, got {event:?}"),
            }
        }
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(stream.next().await.is_none());
    }
}

#[test]
fn streamed_terminal_policy_codes_override_transient_rate_limit_fields() {
    for code in [
        "cyber_policy",
        "bio_policy",
        "misalignment_policy_violation",
        "invalid_prompt",
    ] {
        let detail = json!({"code": code, "type": "throttling_error"});
        for payload in [
            json!({"type": "error", "status": 429, "error": detail}),
            json!({"type": "response.failed", "response": {"error": detail}}),
        ] {
            let event = serde_json::from_value(payload).unwrap();
            let error = map_api_error(process_responses_event(event).unwrap_err().into_api_error());
            assert_eq!(error.retry_delay(/*retry_count*/ 1), None, "{code}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn streamed_limit_errors_end_the_stream_before_later_output_or_idle_timeout() {
    for error_type in ["budget_exceeded", "usage_not_included", "throttling_error"] {
        for payload in [
            json!({"type": "error", "status": 429, "error": {"type": error_type, "code": "429"}}),
            json!({"type": "response.failed", "response": {"error": {"type": error_type, "code": "429"}}}),
        ] {
            let completed = json!({"type": "response.completed", "response": {"id": "later"}});
            let bytes = stream::iter([Ok(Bytes::from(format!(
                "data: {payload}\n\ndata: {completed}\n\n"
            )))])
            .chain(stream::pending());
            let mut stream = spawn_response_stream(
                StreamResponse {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    bytes: Box::pin(bytes),
                },
                Duration::from_secs(60),
                /*telemetry*/ None,
                /*turn_state*/ None,
            );
            let started = tokio::time::Instant::now();
            let error = loop {
                match stream.next().await {
                    Some(Ok(ResponseEvent::RateLimits(_))) => {}
                    Some(Err(error)) => break map_api_error(error),
                    event => panic!("expected a limit error for {error_type}, got {event:?}"),
                }
            };
            let expected = if error_type == "throttling_error" {
                CodexErrorInfo::RateLimitExceeded
            } else {
                CodexErrorInfo::UsageLimitExceeded
            };
            assert_eq!(error.to_codex_protocol_error(), expected);
            assert_eq!(started.elapsed(), Duration::ZERO);
            assert!(stream.next().await.is_none());
        }
    }
}
