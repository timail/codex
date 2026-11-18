//! Shared retry and transport fallback decisions for Responses requests.
//! Content-filter guidance is recorded for sampling requests before retry decisions.
//! Server advice controls timing. Optional long rate-limit recovery has a separate bounded window.

use std::sync::Mutex;
use std::time::Duration;

use crate::client::ModelClientSession;
use crate::context::ContentFilterGuidance;
use crate::context::ContextualUserFragment;
use crate::session::session::Session;
use crate::session::step_context::StepContext;
use crate::session::turn_context::TurnContext;
use codex_client::RetryOperation;
use codex_features::Feature;
use codex_http_client::HttpTransport;
use codex_http_client::Request;
use codex_http_client::Response;
use codex_http_client::RetryAfter;
use codex_http_client::StreamResponse;
use codex_http_client::TransportError;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use tokio::time::Instant;
use tracing::warn;

const INITIAL_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_CONNECTION_RETRY_DELAY: Duration = Duration::from_secs(60);
const LONG_RATE_LIMIT_RETRY_INTERVAL: Duration = Duration::from_secs(30 * 60);
const LONG_RATE_LIMIT_RETRY_WINDOW: Duration = Duration::from_secs(12 * 60 * 60);

/// Recovery state for one continuous outage. A fully successful foreground response resets it.
/// Keeping the gate on the client session also prevents same-turn steering from skipping a wait.
#[derive(Default)]
pub(crate) struct LongRateLimitRetryState {
    deadline: Option<Instant>,
    not_before: Mutex<Option<Instant>>,
    last_error: Option<String>,
}

impl LongRateLimitRetryState {
    pub(crate) fn is_active(&self) -> bool {
        self.deadline.is_some()
    }

    pub(crate) fn schedule_if_needed(
        &mut self,
        err: &CodexErr,
        enabled: bool,
        normal_retry_available: bool,
    ) -> Option<Instant> {
        if (self.is_active()
            || (enabled && is_temporary_rate_limit(err) && !normal_retry_available))
            && can_retry_during_rate_limit_recovery(err)
        {
            Some(self.schedule_retry(err))
        } else {
            None
        }
    }

    fn schedule_retry(&mut self, err: &CodexErr) -> Instant {
        let now = Instant::now();
        let deadline = *self
            .deadline
            .get_or_insert(now + LONG_RATE_LIMIT_RETRY_WINDOW);
        let not_before = self
            .not_before
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retry_at = (now + LONG_RATE_LIMIT_RETRY_INTERVAL)
            .max(not_before.unwrap_or(now))
            .max(err.retry_after().map(RetryAfter::deadline).unwrap_or(now));
        *not_before = Some(retry_at);
        self.last_error = Some(err.to_string());
        // A longer Retry-After exhausts the window without sleeping past it or retrying early.
        retry_at.min(deadline)
    }

    pub(crate) async fn wait_for_retry(&self) -> Result<(), CodexErr> {
        let Some(retry_at) = *self
            .not_before
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        else {
            return Ok(());
        };
        let deadline = self.deadline.expect("active recovery has a deadline");
        if Instant::now() > deadline {
            return Err(self.exhausted());
        }
        let wake_at = retry_at.min(deadline);
        if Instant::now() < wake_at {
            tokio::time::sleep_until(wake_at).await;
        }
        // An attempt exactly on the horizon is permitted; no request may start after it.
        if retry_at > deadline || Instant::now() > deadline {
            return Err(self.exhausted());
        }
        Ok(())
    }

    /// Reserves one request slot before dispatch. A preempted request cannot free its slot.
    async fn before_attempt(&self) -> Result<(), CodexErr> {
        loop {
            self.wait_for_retry().await?;
            let mut not_before = self
                .not_before
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            if self.deadline.is_some_and(|deadline| now > deadline) {
                return Err(self.exhausted());
            }
            if not_before.is_some_and(|retry_at| retry_at > now) {
                continue;
            }
            if self.is_active() {
                *not_before = Some(now + LONG_RATE_LIMIT_RETRY_INTERVAL);
            }
            return Ok(());
        }
    }

    pub(crate) async fn wait_after_error(
        &self,
        sess: &Session,
        turn_context: &TurnContext,
        err: CodexErr,
        retry_at: Instant,
    ) -> Result<(), CodexErr> {
        let delay = retry_at.saturating_duration_since(Instant::now());
        sess.notify_stream_error(
            turn_context,
            format!(
                "Rate-limit recovery: waiting {}s (30-minute minimum, 12-hour window)",
                delay.as_secs()
            ),
            err,
        )
        .await;
        self.wait_for_retry().await
    }

    pub(crate) fn finish_response(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn exhausted(&self) -> CodexErr {
        CodexErrorDetails::RateLimitRecoveryExhausted(format!(
            "Long rate-limit retry window exhausted after 12 hours. Last error: {}",
            self.last_error.as_deref().unwrap_or("rate limit exceeded")
        ))
        .into()
    }
}

/// Gate after async credential preparation, immediately before transport dispatch.
pub(crate) struct RateLimitRecoveryTransport<'a, T> {
    pub(crate) inner: T,
    pub(crate) recovery: Option<&'a LongRateLimitRetryState>,
}

impl<T: HttpTransport> HttpTransport for RateLimitRecoveryTransport<'_, T> {
    async fn execute(&self, req: Request) -> Result<Response, TransportError> {
        if let Some(recovery) = self.recovery {
            recovery
                .before_attempt()
                .await
                .map_err(|_| TransportError::RetryLimit)?;
        }
        self.inner.execute(req).await
    }

    async fn stream(&self, req: Request) -> Result<StreamResponse, TransportError> {
        if let Some(recovery) = self.recovery {
            recovery
                .before_attempt()
                .await
                .map_err(|_| TransportError::RetryLimit)?;
        }
        self.inner.stream(req).await
    }
}

fn is_temporary_rate_limit(err: &CodexErr) -> bool {
    matches!(err.details(), CodexErrorDetails::RateLimitExceeded(_))
        || matches!(err.details(), CodexErrorDetails::RetryLimit(error) if error.status.as_u16() == 429)
}

pub(crate) fn can_retry_during_rate_limit_recovery(err: &CodexErr) -> bool {
    if is_temporary_rate_limit(err) {
        return true;
    }
    match err.details() {
        CodexErrorDetails::Stream(_)
        | CodexErrorDetails::ConnectionFailed(_)
        | CodexErrorDetails::ResponseStreamFailed(_)
        | CodexErrorDetails::RequestTimeout
        | CodexErrorDetails::Timeout
        | CodexErrorDetails::InternalServerError => err
            .http_status_code_value()
            .is_none_or(|status| (500..600).contains(&status)),
        CodexErrorDetails::UnexpectedStatus(error) => error.status.is_server_error(),
        CodexErrorDetails::ServerOverloaded | CodexErrorDetails::PolicyCapacityUnavailable => {
            err.retry_after().is_some()
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponsesStreamRequest {
    Sampling,
    RemoteCompactionV2,
}

/// A new generation cannot repair a terminal provider response or settle the
/// previous attempt's bill. Preserve these outcomes across transport retries,
/// same-turn steering, and compaction's fallback to another model.
pub(crate) fn is_terminal_sampling_outcome(error: &CodexErr) -> bool {
    matches!(
        error.details(),
        CodexErrorDetails::ResponseIncomplete(_)
            | CodexErrorDetails::UsageAccountingFailed(_)
            | CodexErrorDetails::SessionBudgetExceeded
            | CodexErrorDetails::APISpendLimitExceeded(_)
            | CodexErrorDetails::QuotaExceeded
            | CodexErrorDetails::UsageNotIncluded
            | CodexErrorDetails::UsageLimitReached(_)
            | CodexErrorDetails::RateLimitRecoveryExhausted(_)
            | CodexErrorDetails::InferenceHookDenied { .. }
    )
}

pub(crate) struct ResponsesStreamRetryState {
    retries: u64,
    connection_retries: u64,
    connection_retry_delay: Duration,
}

impl Default for ResponsesStreamRetryState {
    fn default() -> Self {
        Self {
            retries: 0,
            connection_retries: 0,
            connection_retry_delay: INITIAL_CONNECTION_RETRY_DELAY,
        }
    }
}

/// Server retry advice retained after stream retries are exhausted. The turn ID
/// prevents a reused Guardian session from applying advice from an earlier review.
pub(crate) struct ExhaustedResponseRetry {
    pub(crate) turn_id: String,
    pub(crate) retry_at: Option<tokio::time::Instant>,
}

/// Returns `Ok(())` when the caller should retry the request loop, or the original error when
/// it is terminal or the retry budget is exhausted.
#[tracing::instrument(level = "trace", skip_all, fields(request = ?request))]
pub(crate) fn handle_response_stream_error(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    step_context: &StepContext,
    request: ResponsesStreamRequest,
) -> impl std::future::Future<Output = Result<(), CodexErr>> {
    let turn_context = &step_context.turn;
    let retry_count = retry_state.retries.saturating_add(1);
    // Latch the deadline before the cancellable future is polled: an already queued steer may
    // otherwise cancel the wait before its first poll and immediately submit another request.
    let long_retry_at = client_session.long_rate_limit_retry.schedule_if_needed(
        &err,
        turn_context
            .config
            .features
            .enabled(Feature::LongRateLimitRetries),
        retry_state.retries < max_retries && err.retry_delay(retry_count).is_some(),
    );
    async move {
        // Recovery owns every later retry, before fallback or unbounded connection retries can
        // restore a fast loop. Background compaction does not use this handler.
        if client_session.long_rate_limit_retry.is_active() {
            let Some(retry_at) = long_retry_at else {
                return Err(err);
            };
            let operation = match request {
                ResponsesStreamRequest::Sampling => RetryOperation::Sampling,
                ResponsesStreamRequest::RemoteCompactionV2 => RetryOperation::RemoteCompactionV2,
            };
            let delay = retry_at.saturating_duration_since(Instant::now());
            codex_client::record_retry!(retry_count, delay, operation);
            return client_session
                .long_rate_limit_retry
                .wait_after_error(sess, turn_context, err, retry_at)
                .await;
        }
        handle_standard_response_stream_error(
            retry_state,
            max_retries,
            err,
            client_session,
            sess,
            step_context,
            request,
        )
        .await
    }
}

async fn handle_standard_response_stream_error(
    retry_state: &mut ResponsesStreamRetryState,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    step_context: &StepContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    let turn_context = &step_context.turn;
    if matches!(request, ResponsesStreamRequest::Sampling)
        && matches!(err.details(), CodexErrorDetails::ContentFilter)
    {
        let model_info = &step_context.settings.model_info;
        let guidance = ContentFilterGuidance {
            text: codex_prompts::ResolvedModelMessages::from_model(model_info)
                .content_filter_guidance()
                .to_string(),
        };
        sess.record_conversation_items(
            turn_context,
            model_info,
            &[ContextualUserFragment::into(guidance)],
        )
        .await;
    }
    let operation = match request {
        ResponsesStreamRequest::Sampling => RetryOperation::Sampling,
        ResponsesStreamRequest::RemoteCompactionV2 => RetryOperation::RemoteCompactionV2,
    };
    let retry_count = retry_state.retries.saturating_add(1);
    let Some(delay) = err.retry_delay(retry_count) else {
        return Err(err);
    };
    let retry_after = err.retry_after();

    if turn_context
        .config
        .features
        .enabled(Feature::UnboundedConnectionRetries)
        && matches!(request, ResponsesStreamRequest::Sampling)
        && matches!(err.details(), CodexErrorDetails::ConnectionFailed(_))
        && !turn_context.session_source.is_internal()
        && !turn_context.provider.info().is_amazon_bedrock()
    {
        let retry_delay = retry_state.connection_retry_delay;
        warn!(
            turn_id = %turn_context.sub_id,
            error = %err,
            ?retry_delay,
            "stream connection failed; waiting to retry"
        );
        sess.notify_stream_error(turn_context, "Reconnecting... waiting for network", err)
            .await;
        retry_state.connection_retries = retry_state.connection_retries.saturating_add(1);
        codex_client::record_retry!(retry_state.connection_retries, retry_delay, operation);
        tokio::time::sleep(retry_delay).await;
        retry_state.connection_retry_delay = retry_delay
            .saturating_mul(2)
            .min(MAX_CONNECTION_RETRY_DELAY);
        return Ok(());
    }

    let requires_http_fallback = err.requires_http_fallback();
    let switched = (requires_http_fallback || retry_state.retries >= max_retries)
        && client_session.try_switch_fallback_transport(
            &turn_context.session_telemetry,
            turn_context.model_info(),
        );
    if switched {
        // Changing transport must not bypass the server's retry deadline.
        if let Some(retry_after) = retry_after {
            tokio::time::sleep_until(retry_after.deadline()).await;
        }
        sess.send_event(
            turn_context,
            EventMsg::Warning(WarningEvent {
                message: format!("Falling back from WebSockets to HTTPS transport. {err:#}"),
            }),
        )
        .await;
    }
    // Another request can already have switched this session to HTTP.
    if switched || requires_http_fallback {
        retry_state.retries = 0;
        return Ok(());
    }

    if retry_state.retries < max_retries {
        retry_state.retries = retry_count;
        log_retry(request, turn_context, &err, retry_count, max_retries, delay);

        // In release builds, hide the first websocket retry notification to reduce noisy
        // transient reconnect messages. In debug builds, keep full visibility for diagnosis.
        let report_error = retry_count > 1
            || cfg!(debug_assertions)
            || !sess.services.model_client.responses_websocket_enabled();
        if report_error {
            // Surface retry information to any UI/front-end so the user understands what is
            // happening instead of staring at a seemingly frozen screen.
            sess.notify_stream_error(
                turn_context,
                format!("Reconnecting... {retry_count}/{max_retries}"),
                err,
            )
            .await;
        }
        // Use one clock sample so local backoff telemetry retains the selected delay.
        let now = Instant::now();
        let retry_at = retry_after.map(RetryAfter::deadline).unwrap_or(now + delay);
        let delay = retry_at.saturating_duration_since(now);
        codex_client::record_retry!(retry_count, delay, operation);
        tokio::time::sleep_until(retry_at).await;
        return Ok(());
    }

    sess.services
        .thread_extension_data
        .insert(ExhaustedResponseRetry {
            turn_id: turn_context.sub_id.clone(),
            retry_at: retry_after.map(RetryAfter::deadline),
        });
    Err(err)
}

fn log_retry(
    request: ResponsesStreamRequest,
    turn_context: &TurnContext,
    err: &CodexErr,
    retries: u64,
    max_retries: u64,
    delay: Duration,
) {
    match request {
        ResponsesStreamRequest::Sampling => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                sampling_error = %err,
                "stream disconnected - retrying sampling request ({retries}/{max_retries} in {delay:?})...",
            );
        }
        ResponsesStreamRequest::RemoteCompactionV2 => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "remote compaction v2 stream failed; retrying request after delay"
            );
        }
    }
}

#[cfg(test)]
#[path = "responses_retry_tests.rs"]
mod tests;
