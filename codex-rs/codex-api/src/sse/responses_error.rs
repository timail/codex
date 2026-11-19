//! Classifies failed Responses events and their retry advice.
//! Only rate-limit errors derive retry delays from plaintext messages.

use crate::error::ApiError;
use crate::error::parse_terminal_provider_error;
use crate::error::provider_error_matches;
use crate::responses_headers::json_headers_to_http_headers;
use codex_http_client::RetryAfter;
use codex_http_client::TransportError;
use http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

const RATE_LIMIT_ERROR_KINDS: &[&str] = &[
    "rate_limit_exceeded",
    "slow_down",
    "throttling_error",
    "rate_limit_error",
];

// For unclassified errors, keep the existing validation of optional diagnostic fields.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Error {
    r#type: Option<String>,
    code: Option<String>,
    message: Option<String>,
    plan_type: Option<String>,
    resets_at: Option<i64>,
    #[serde(default)]
    misalignment: Option<Value>,
}

fn retry_after_header(headers: Option<&Value>) -> Option<RetryAfter> {
    headers
        .and_then(Value::as_object)
        .and_then(|headers| RetryAfter::from_headers(&json_headers_to_http_headers(headers)))
}

pub(super) fn parse_failed_response(response: Option<Value>) -> ApiError {
    let error = response.as_ref().and_then(|response| response.get("error"));
    if let Some(error) = parse_error_event(error, /*headers*/ None, /*status*/ None) {
        return error;
    }
    let retry_after_header = error
        .and_then(|error| error.get("headers"))
        .and_then(Value::as_object)
        .and_then(|headers| RetryAfter::from_headers(&json_headers_to_http_headers(headers)));
    let Some(error) = error.and_then(|error| serde_json::from_value::<Error>(error.clone()).ok())
    else {
        let message = "response.failed event received".to_string();
        return match retry_after_header {
            Some(_) => ApiError::Retryable {
                message,
                retry_after: retry_after_header,
            },
            None => ApiError::Stream(message),
        };
    };

    match error.code.as_deref() {
        Some("policy_capacity_unavailable") => ApiError::PolicyCapacityUnavailable {
            retry_after: retry_after_header,
        },
        Some("server_is_overloaded") => ApiError::ServerOverloaded {
            retry_after: retry_after_header,
        },
        _ => ApiError::Retryable {
            message: error.message.unwrap_or_default(),
            retry_after: retry_after_header,
        },
    }
}

/// Normalizes status-bearing and statusless SSE errors without inferring status from prose.
/// Terminal quota markers take precedence over rate-limit status or retry advice.
pub(super) fn parse_error_event(
    error: Option<&Value>,
    headers: Option<&Value>,
    status: Option<&Value>,
) -> Option<ApiError> {
    if let Some(error) = parse_terminal_provider_error(error, /*status*/ None) {
        return Some(error);
    }
    let statuses = [
        status,
        error.and_then(|error| error.get("status")),
        error.and_then(|error| error.get("status_code")),
        error.and_then(|error| error.get("code")),
    ]
    .map(|value| value.and_then(http_error_status));
    let http_status = statuses
        .into_iter()
        .flatten()
        .find(|status| matches!(*status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN))
        .or_else(|| error.and_then(auth_error_status))
        .or_else(|| {
            statuses
                .into_iter()
                .flatten()
                .find(|status| *status != StatusCode::TOO_MANY_REQUESTS)
        });
    if let Some(status) = http_status {
        // Preserve structured status and advice instead of hiding hard failures in Stream errors.
        // Keep this in the SSE parser so HTTP handshake auth recovery and diagnostics are unchanged.
        let nested_headers = error.and_then(|error| error.get("headers"));
        let retry_after = [nested_headers, headers]
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .find_map(|headers| RetryAfter::from_headers(&json_headers_to_http_headers(headers)));
        let mut response_headers = headers
            .and_then(Value::as_object)
            .map(json_headers_to_http_headers)
            .unwrap_or_default();
        if let Some(headers) = nested_headers.and_then(Value::as_object) {
            response_headers.extend(json_headers_to_http_headers(headers));
        }
        return Some(ApiError::Transport(TransportError::Http {
            status,
            url: None,
            headers: Some(response_headers),
            body: Some(serde_json::json!({ "error": error }).to_string()),
            retry_after,
        }));
    }
    if let Some(code @ ("policy_capacity_unavailable" | "server_is_overloaded")) = error
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
    {
        let retry_after = retry_after_header(error.and_then(|error| error.get("headers")))
            .or_else(|| retry_after_header(headers));
        return Some(if code == "policy_capacity_unavailable" {
            ApiError::PolicyCapacityUnavailable { retry_after }
        } else {
            ApiError::ServerOverloaded { retry_after }
        });
    }
    let code = error
        .and_then(|error| error.get("code"))
        .filter(|code| !code.is_null());
    // A coarse error type must not override an explicit non-rate status or provider code.
    if status.is_some_and(|status| !is_rate_limit_status(status))
        || code.is_some_and(|code| {
            !is_rate_limit_status(code)
                && !code
                    .as_str()
                    .is_some_and(|code| RATE_LIMIT_ERROR_KINDS.contains(&code))
        })
    {
        return None;
    }
    let is_rate_limit = statuses.contains(&Some(StatusCode::TOO_MANY_REQUESTS))
        || error.is_some_and(|error| {
            provider_error_matches(error, RATE_LIMIT_ERROR_KINDS)
                || error.get("code").is_some_and(is_rate_limit_status)
        });
    if !is_rate_limit {
        return None;
    }
    let message = error
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let retry_after = [error.and_then(|error| error.get("headers")), headers]
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .find_map(|headers| RetryAfter::from_headers(&json_headers_to_http_headers(headers)))
        .or_else(|| try_parse_retry_delay(message).and_then(RetryAfter::from_delay));
    Some(ApiError::RateLimitExceeded {
        message: message.to_owned(),
        retry_after,
    })
}

fn http_error_status(value: &Value) -> Option<StatusCode> {
    let status = value
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .and_then(|value| StatusCode::from_u16(value).ok())
        .or_else(|| {
            value
                .as_str()
                .and_then(|value| StatusCode::from_bytes(value.as_bytes()).ok())
        })?;
    (status.is_client_error() || status.is_server_error()).then_some(status)
}

fn auth_error_status(error: &Value) -> Option<StatusCode> {
    if provider_error_matches(
        error,
        &[
            "invalid_api_key",
            "authentication_error",
            "auth_error",
            "token_not_found_in_db",
            "expired_key",
        ],
    ) {
        Some(StatusCode::UNAUTHORIZED)
    } else if provider_error_matches(
        error,
        &[
            "permission_error",
            "permission_denied",
            "key_model_access_denied",
            "team_model_access_denied",
            "user_model_access_denied",
            "org_model_access_denied",
            "project_model_access_denied",
            "tool_access_denied",
        ],
    ) {
        Some(StatusCode::FORBIDDEN)
    } else {
        None
    }
}

fn is_rate_limit_status(value: &Value) -> bool {
    value.as_u64() == Some(429) || value.as_str() == Some("429")
}

fn try_parse_retry_delay(message: &str) -> Option<Duration> {
    let re = rate_limit_regex();
    if let Some(captures) = re.captures(message) {
        let seconds = captures.get(1);
        let unit = captures.get(2);

        if let (Some(value), Some(unit)) = (seconds, unit) {
            let value = value.as_str().parse::<f64>().ok()?;
            let unit = unit.as_str().to_ascii_lowercase();

            if unit == "s" || unit.starts_with("second") {
                return Duration::try_from_secs_f64(value).ok();
            } else if unit == "ms" {
                return Some(Duration::from_millis(value as u64));
            }
        }
    }
    None
}

fn rate_limit_regex() -> &'static regex_lite::Regex {
    static RE: std::sync::OnceLock<regex_lite::Regex> = std::sync::OnceLock::new();
    #[expect(clippy::unwrap_used)]
    RE.get_or_init(|| {
        regex_lite::Regex::new(r"(?i)try again in\s*(\d+(?:\.\d+)?)\s*(s|ms|seconds?)").unwrap()
    })
}

#[cfg(test)]
#[path = "responses_error_parser_tests.rs"]
mod tests;
