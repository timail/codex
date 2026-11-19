use crate::rate_limits::RateLimitError;
use codex_client::TransportError;
use codex_http_client::RetryAfter;
use codex_protocol::error::ResponseIncompleteError;
use codex_protocol::error::SpendLimitScope;
use codex_protocol::protocol::MisalignmentErrorDetails;
use http::StatusCode;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("api error {status}: {message}")]
    Api { status: StatusCode, message: String },
    #[error("stream error: {0}")]
    Stream(String),
    #[error("websocket request exceeds the peer's message size limit")]
    WebsocketMessageTooLarge,
    #[error("stream error: Incomplete response returned, reason: content_filter")]
    ContentFilter,
    #[error(transparent)]
    ResponseIncomplete(Box<ResponseIncompleteError>),
    #[error("context window exceeded")]
    ContextWindowExceeded,
    #[error("quota exceeded")]
    QuotaExceeded,
    #[error("spend limit exceeded")]
    SpendLimitExceeded(SpendLimitScope),
    #[error("usage not included")]
    UsageNotIncluded,
    #[error("session budget exceeded")]
    SessionBudgetExceeded,
    #[error("retryable error: {message}")]
    Retryable {
        message: String,
        retry_after: Option<RetryAfter>,
    },
    #[error("rate limit exceeded: {message}")]
    RateLimitExceeded {
        message: String,
        retry_after: Option<RetryAfter>,
    },
    #[error("rate limit: {0}")]
    RateLimit(String),
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("{message}")]
    AccessProgramNotEnabled { message: String },
    #[error("{message}")]
    InvalidAudioInput {
        message: String,
        /// True when the provider rejected an individual clip, independent of request size.
        per_recording: bool,
    },
    #[error("{message}")]
    ModelNotFound {
        message: String,
        status: Option<StatusCode>,
    },
    #[error("invalid prompt: {message}")]
    InvalidPrompt { message: String },
    #[error("{message}")]
    InferenceHookDenied { message: String },
    #[error("{message}")]
    ContentPolicyViolation {
        message: String,
        status: Option<StatusCode>,
    },
    #[error("cyber policy: {message}")]
    CyberPolicy { message: String },
    #[error("bio policy: {message}")]
    BioPolicy { message: String },
    #[error("misalignment policy violation: {message}")]
    MisalignmentPolicyViolation {
        message: String,
        misalignment: Option<MisalignmentErrorDetails>,
    },
    #[error("Flex capacity unavailable.")]
    FlexUnavailable,
    /// Server-confirmed capacity failure on a policy-restricted route.
    #[error("policy-restricted capacity unavailable")]
    PolicyCapacityUnavailable { retry_after: Option<RetryAfter> },
    #[error("server overloaded")]
    ServerOverloaded { retry_after: Option<RetryAfter> },
}

impl From<RateLimitError> for ApiError {
    fn from(err: RateLimitError) -> Self {
        Self::RateLimit(err.to_string())
    }
}

pub(crate) fn parse_terminal_provider_error(
    error: Option<&Value>,
    status: Option<StatusCode>,
) -> Option<ApiError> {
    let error = error?;
    // Inspect the identifying fields before deserializing optional diagnostics. LiteLLM
    // often puts the classification in `type` and the HTTP status string in `code`.
    if provider_error_matches(error, &["usage_not_included"]) {
        return Some(ApiError::UsageNotIncluded);
    }
    if provider_error_matches(error, &["budget_reached"]) {
        return Some(ApiError::SessionBudgetExceeded);
    }
    for (kind, scope) in [
        ("api_key_spend_limit_exceeded", SpendLimitScope::ApiKey),
        ("project_spend_limit_exceeded", SpendLimitScope::Project),
        (
            "organization_spend_limit_exceeded",
            SpendLimitScope::Organization,
        ),
    ] {
        if provider_error_matches(error, &[kind]) {
            return Some(ApiError::SpendLimitExceeded(scope));
        }
    }
    if provider_error_matches(
        error,
        &[
            "insufficient_quota",
            "credit_balance_exhausted",
            "organization_usage_limit_exceeded",
            "usage_limit_reached",
            "budget_exceeded",
        ],
    ) {
        return Some(ApiError::QuotaExceeded);
    }
    // A proxy may label a policy rejection as 429 or put its code in the type field.
    let policy_code = if matches!(status, None | Some(StatusCode::TOO_MANY_REQUESTS)) {
        provider_error_kinds(error).find(|kind| {
            matches!(
                *kind,
                "invalid_prompt"
                    | "misalignment_policy_violation"
                    | "inference_hook_denied"
                    | "content_policy_violation"
                    | "cyber_policy"
                    | "bio_policy"
                    | "access_program_not_enabled"
            )
        })
    } else {
        None
    };
    let code = policy_code.or_else(|| error.get("code")?.as_str())?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty());
    match (code, status.filter(|_| policy_code.is_none())) {
        ("invalid_prompt", None) => Some(ApiError::InvalidPrompt {
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Invalid request.")
                .to_owned(),
        }),
        ("misalignment_policy_violation", None) => Some(ApiError::MisalignmentPolicyViolation {
            message: message
                .unwrap_or("This request was blocked due to a misalignment policy violation.")
                .to_owned(),
            misalignment: error.get("misalignment").cloned().and_then(|details| {
                serde_json::from_value::<MisalignmentErrorDetails>(details).ok()
            }),
        }),
        ("inference_hook_denied", None | Some(StatusCode::FORBIDDEN)) => {
            Some(ApiError::InferenceHookDenied {
                message: message
                    .unwrap_or("Your workspace inference hook blocked this request.")
                    .to_owned(),
            })
        }
        ("context_length_exceeded", None | Some(StatusCode::BAD_REQUEST)) => {
            Some(ApiError::ContextWindowExceeded)
        }
        // A non-streaming 400 already follows the ordinary invalid-request path.
        ("invalid_access_program" | "unsupported_access_program", None) => {
            Some(ApiError::InvalidRequest {
                message: message
                    .unwrap_or("The requested access program cannot be used with this model.")
                    .to_owned(),
            })
        }
        ("content_policy_violation", None | Some(StatusCode::BAD_REQUEST)) => {
            Some(ApiError::ContentPolicyViolation {
                message: message
                    .unwrap_or("The input was rejected by the content policy.")
                    .to_owned(),
                status,
            })
        }
        ("cyber_policy", None) => Some(ApiError::CyberPolicy {
            message: message
                .unwrap_or("This request has been flagged for possible cybersecurity risk.")
                .to_owned(),
        }),
        ("bio_policy", None) => Some(ApiError::BioPolicy {
            message: message
                .unwrap_or("This content was flagged for possible biological risk.")
                .to_owned(),
        }),
        // HTTP 403s retain their request ID and headers in the API bridge.
        ("access_program_not_enabled", None) => Some(ApiError::AccessProgramNotEnabled {
            message: message
                .unwrap_or("The requested access program is not enabled.")
                .to_owned(),
        }),
        (
            "audio_too_large"
            | "audio_too_short"
            | "invalid_audio"
            | "invalid_audio_data"
            | "invalid_audio_format"
            | "missing_audio_sample_rate",
            None | Some(StatusCode::BAD_REQUEST),
        ) => Some(ApiError::InvalidAudioInput {
            per_recording: code != "audio_too_large",
            message: message.unwrap_or("Invalid audio input.").to_owned(),
        }),
        ("flex_unavailable", None | Some(StatusCode::TOO_MANY_REQUESTS)) => {
            Some(ApiError::FlexUnavailable)
        }
        (
            "model_not_found",
            None | Some(StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND),
        ) => Some(ApiError::ModelNotFound {
            message: message.unwrap_or(code).to_owned(),
            status,
        }),
        _ => None,
    }
}

fn provider_error_kinds(error: &Value) -> impl Iterator<Item = &str> {
    ["code", "type", "litellm_error_type"]
        .into_iter()
        .filter_map(|field| error.get(field).and_then(Value::as_str))
}

pub(crate) fn provider_error_matches(error: &Value, kinds: &[&str]) -> bool {
    provider_error_kinds(error).any(|kind| kinds.contains(&kind))
}

pub(crate) fn auth_error_status(error: &Value) -> Option<StatusCode> {
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

impl From<crate::AuthError> for ApiError {
    fn from(error: crate::AuthError) -> Self {
        Self::Transport(error.into())
    }
}
