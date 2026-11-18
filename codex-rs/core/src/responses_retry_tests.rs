use super::LONG_RATE_LIMIT_RETRY_INTERVAL;
use super::LONG_RATE_LIMIT_RETRY_WINDOW;
use super::LongRateLimitRetryState;
use super::RateLimitRecoveryTransport;
use super::ResponsesStreamRequest;
use super::ResponsesStreamRetryState;
use super::can_retry_during_rate_limit_recovery;
use super::handle_response_stream_error;
use super::log_retry;
use crate::realtime_history::RealtimeHistoryState;
use crate::session::step_context::StepContext;
use crate::session::tests::make_session_and_context;
use codex_async_utils::OrCancelExt;
use codex_features::Feature;
use codex_http_client::HttpTransport;
use codex_http_client::Request;
use codex_http_client::Response;
use codex_http_client::RetryAfter;
use codex_http_client::StreamResponse;
use codex_http_client::TransportError;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::RetryLimitReachedError;
use http::Method;
use http::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing_test::internal::MockWriter;

#[tokio::test]
async fn sampling_retry_logs_stream_error_context() {
    let (_session, turn_context) = make_session_and_context().await;
    let buffer: &'static std::sync::Mutex<Vec<u8>> =
        Box::leak(Box::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(MockWriter::new(buffer))
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    log_retry(
        ResponsesStreamRequest::Sampling,
        &turn_context,
        &CodexErr::Stream("websocket closed by server before response.completed".to_string()),
        /*retries*/ 2,
        /*max_retries*/ 5,
        Duration::from_secs(1),
    );

    let logs = String::from_utf8(
        buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("retry log should be valid utf-8");
    assert!(logs.contains("stream disconnected - retrying sampling request"));
    assert!(logs.contains(&format!("turn_id={}", turn_context.sub_id)));
    assert!(logs.contains("retries=2"));
    assert!(logs.contains("max_retries=5"));
    assert!(logs.contains(
        "sampling_error=stream disconnected before completion: websocket closed by server before response.completed"
    ));
}

/// Time spent reporting a retry must count toward the server's original deadline.
#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "test holds the event-delivery lock to delay notification while virtual time advances"
)]
async fn stream_retry_preserves_deadline_across_delayed_notification() {
    let (mut session, turn_context) = make_session_and_context().await;
    let step_context = StepContext::for_test(Arc::new(turn_context));
    session.realtime_history = Some(Mutex::new(RealtimeHistoryState::default()));
    let mut client_session = session.services.model_client.new_session();
    // The second retry emits a notification even when release builds hide the first.
    let mut retry_state = ResponsesStreamRetryState {
        retries: 1,
        ..Default::default()
    };

    tokio::time::pause();
    let advice = RetryAfter::from_delay(Duration::from_secs(10)).expect("retry deadline");
    tokio::time::advance(Duration::from_secs(4)).await;

    // Hold the event-delivery lock so reporting the error consumes part of the deadline.
    let history_guard = session.realtime_history.as_ref().unwrap().lock().await;
    let retry = handle_response_stream_error(
        &mut retry_state,
        /*max_retries*/ 2,
        CodexErr::InternalServerError.with_retry_after(advice),
        &mut client_session,
        &session,
        &step_context,
        ResponsesStreamRequest::Sampling,
    );
    tokio::pin!(retry);
    assert!(futures::poll!(&mut retry).is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;
    drop(history_guard);

    assert!(futures::poll!(&mut retry).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(futures::poll!(&mut retry).is_pending());
    retry.await.expect("retry should be allowed");
    // Tokio rounds timer deadlines up to the next millisecond.
    let resumed_at = Instant::now();
    assert!(
        (advice.deadline()..=advice.deadline() + Duration::from_millis(1)).contains(&resumed_at),
        "retry resumed at {resumed_at:?}, expected {advice:?}"
    );
}

fn temporary_rate_limit() -> CodexErr {
    CodexErr::RetryLimit(RetryLimitReachedError {
        status: StatusCode::TOO_MANY_REQUESTS,
        request_id: Some("request-to-preserve".to_string()),
    })
}

#[tokio::test(start_paused = true)]
async fn long_retry_allows_at_most_twenty_four_spaced_attempts_in_one_outage() {
    let mut recovery = LongRateLimitRetryState::default();
    let started = Instant::now();
    for attempt in 1..=24 {
        let scheduled = recovery.schedule_retry(&temporary_rate_limit());
        assert_eq!(
            scheduled,
            started + LONG_RATE_LIMIT_RETRY_INTERVAL * attempt
        );
        tokio::time::advance(LONG_RATE_LIMIT_RETRY_INTERVAL).await;
        recovery.before_attempt().await.expect("legal retry slot");
        assert_eq!(Instant::now(), scheduled);
    }
    assert_eq!(Instant::now(), started + LONG_RATE_LIMIT_RETRY_WINDOW);
    recovery.schedule_retry(&temporary_rate_limit());
    let error = recovery.before_attempt().await.unwrap_err();
    assert!(matches!(
        error.details(),
        CodexErrorDetails::RateLimitRecoveryExhausted(_)
    ));
    assert!(error.to_string().contains("12 hours"));
    assert!(error.to_string().contains("429 Too Many Requests"));
    assert!(error.to_string().contains("request-to-preserve"));
    assert_eq!(Instant::now(), started + LONG_RATE_LIMIT_RETRY_WINDOW);
}

#[tokio::test(start_paused = true)]
async fn long_retry_honors_original_server_deadline_and_caps_wait_at_twelve_hours() {
    let mut recovery = LongRateLimitRetryState::default();
    let started = Instant::now();
    let advice = RetryAfter::from_delay(Duration::from_secs(60 * 60)).unwrap();
    tokio::time::advance(Duration::from_secs(10 * 60)).await;
    assert_eq!(
        recovery.schedule_retry(&temporary_rate_limit().with_retry_after(advice)),
        started + Duration::from_secs(60 * 60)
    );
    let horizon = recovery.deadline.unwrap();
    let too_late = RetryAfter::from_delay(LONG_RATE_LIMIT_RETRY_WINDOW * 2).unwrap();
    assert_eq!(
        recovery.schedule_retry(&temporary_rate_limit().with_retry_after(too_late)),
        horizon
    );
    let wait = recovery.wait_for_retry();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(LONG_RATE_LIMIT_RETRY_WINDOW - Duration::from_secs(1)).await;
    assert!(futures::poll!(&mut wait).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(wait.await.is_err());
    assert_eq!(Instant::now(), horizon);
}

#[tokio::test(start_paused = true)]
async fn long_retry_cancellation_keeps_the_gate_for_same_turn_steering() {
    let mut recovery = LongRateLimitRetryState::default();
    recovery.schedule_retry(&temporary_rate_limit());
    let cancel = CancellationToken::new();
    {
        let wait = recovery.wait_for_retry().or_cancel(&cancel);
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        tokio::time::advance(Duration::from_secs(20 * 60)).await;
        cancel.cancel();
        assert!(wait.await.is_err());
    }
    assert!(recovery.is_active());
    let retry = recovery.before_attempt();
    tokio::pin!(retry);
    assert!(futures::poll!(&mut retry).is_pending());
    tokio::time::advance(Duration::from_secs(10 * 60)).await;
    retry.await.expect("steer must use the original retry slot");
}

#[tokio::test(start_paused = true)]
async fn long_retry_success_allows_a_fresh_budget_for_a_later_outage() {
    let mut recovery = LongRateLimitRetryState::default();
    recovery.schedule_retry(&temporary_rate_limit());
    tokio::time::advance(LONG_RATE_LIMIT_RETRY_WINDOW).await;
    recovery.finish_response();
    assert!(!recovery.is_active());
    tokio::time::advance(LONG_RATE_LIMIT_RETRY_WINDOW).await;
    let started = Instant::now();
    assert_eq!(
        recovery.schedule_retry(&temporary_rate_limit()),
        started + LONG_RATE_LIMIT_RETRY_INTERVAL
    );
    assert_eq!(
        recovery.deadline,
        Some(started + LONG_RATE_LIMIT_RETRY_WINDOW)
    );
}

#[tokio::test]
async fn long_retry_exhaustion_stops_compaction_model_fallback() {
    let (_session, mut previous) = make_session_and_context().await;
    let (_session, current) = make_session_and_context().await;
    Arc::make_mut(&mut Arc::make_mut(&mut previous.initial_settings).model_info).slug =
        "previous-model".to_string();
    assert!(
        crate::compact_model_fallback::should_retry_with_current_model(
            &CodexErr::InternalServerError,
            &previous,
            &current,
        )
    );
    assert!(
        !crate::compact_model_fallback::should_retry_with_current_model(
            &LongRateLimitRetryState::default().exhausted(),
            &previous,
            &current,
        )
    );
}

#[derive(Clone, Default)]
struct RecordingTransport(Arc<std::sync::Mutex<Vec<Instant>>>);

impl HttpTransport for RecordingTransport {
    async fn execute(&self, _req: Request) -> Result<Response, TransportError> {
        self.0.lock().unwrap().push(Instant::now());
        Err(TransportError::Timeout)
    }

    async fn stream(&self, _req: Request) -> Result<StreamResponse, TransportError> {
        self.0.lock().unwrap().push(Instant::now());
        Err(TransportError::Timeout)
    }
}

#[tokio::test(start_paused = true)]
async fn long_retry_gate_measures_actual_dispatch_after_slow_auth_or_setup() {
    let mut recovery = LongRateLimitRetryState::default();
    recovery.schedule_retry(&temporary_rate_limit());
    let inner = RecordingTransport::default();
    let transport = RateLimitRecoveryTransport {
        inner: inner.clone(),
        recovery: Some(&recovery),
    };
    let started = Instant::now();
    // Setup/auth completes five minutes after the first allowed retry slot.
    tokio::time::advance(Duration::from_secs(35 * 60)).await;
    let request = || Request::new(Method::POST, "http://localhost/responses".to_string());
    assert!(transport.stream(request()).await.is_err());
    tokio::time::advance(Duration::from_secs(25 * 60)).await;
    let second = transport.stream(request());
    tokio::pin!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(inner.0.lock().unwrap().len(), 1);
    tokio::time::advance(Duration::from_secs(5 * 60)).await;
    assert!(second.await.is_err());
    assert_eq!(
        *inner.0.lock().unwrap(),
        vec![
            started + Duration::from_secs(35 * 60),
            started + Duration::from_secs(65 * 60)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn long_retry_gate_blocks_dispatch_if_auth_finishes_after_the_horizon() {
    let mut recovery = LongRateLimitRetryState::default();
    recovery.schedule_retry(&temporary_rate_limit());
    let inner = RecordingTransport::default();
    let transport = RateLimitRecoveryTransport {
        inner: inner.clone(),
        recovery: Some(&recovery),
    };
    tokio::time::advance(LONG_RATE_LIMIT_RETRY_WINDOW + Duration::from_millis(1)).await;
    let error = transport
        .stream(Request::new(
            Method::POST,
            "http://localhost/responses".to_string(),
        ))
        .await;
    assert!(matches!(error, Err(TransportError::RetryLimit)));
    assert!(inner.0.lock().unwrap().is_empty());
}

#[test_case::test_case(ResponsesStreamRequest::Sampling; "sampling")]
#[test_case::test_case(ResponsesStreamRequest::RemoteCompactionV2; "remote_compaction")]
#[tokio::test]
async fn long_retry_is_opt_in_and_preserves_the_normal_retry_budget(
    request: ResponsesStreamRequest,
) {
    let (session, turn_context) = make_session_and_context().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = ResponsesStreamRetryState::default();
    let disabled_context = StepContext::for_test(Arc::new(turn_context));
    assert!(
        handle_response_stream_error(
            &mut retry_state,
            1,
            temporary_rate_limit(),
            &mut client_session,
            &session,
            &disabled_context,
            request,
        )
        .await
        .is_err()
    );
    assert!(!client_session.long_rate_limit_retry.is_active());
    let (session, mut turn_context) = make_session_and_context().await;
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = ResponsesStreamRetryState::default();
    Arc::make_mut(&mut turn_context.config)
        .features
        .enable(Feature::LongRateLimitRetries)
        .expect("enable long retries");
    let step_context = StepContext::for_test(Arc::new(turn_context));
    tokio::time::pause();
    let started = Instant::now();
    let stream_rate_limit =
        || CodexErr::new(CodexErrorDetails::RateLimitExceeded("throttled".into()));
    handle_response_stream_error(
        &mut retry_state,
        1,
        stream_rate_limit(),
        &mut client_session,
        &session,
        &step_context,
        request,
    )
    .await
    .unwrap();
    assert!(Instant::now() - started < Duration::from_secs(1));
    assert!(!client_session.long_rate_limit_retry.is_active());
    assert_eq!(retry_state.retries, 1);
    let retry = handle_response_stream_error(
        &mut retry_state,
        1,
        stream_rate_limit(),
        &mut client_session,
        &session,
        &step_context,
        request,
    );
    tokio::pin!(retry);
    assert!(futures::poll!(&mut retry).is_pending());
    tokio::time::advance(LONG_RATE_LIMIT_RETRY_INTERVAL - Duration::from_secs(1)).await;
    assert!(futures::poll!(&mut retry).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    retry.await.unwrap();
}

#[tokio::test]
async fn long_retry_is_latched_even_when_steering_drops_the_unpolled_wait() {
    let (session, mut turn_context) = make_session_and_context().await;
    Arc::make_mut(&mut turn_context.config)
        .features
        .enable(Feature::LongRateLimitRetries)
        .expect("enable long retries");
    let step_context = StepContext::for_test(Arc::new(turn_context));
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = ResponsesStreamRetryState::default();
    tokio::time::pause();
    let started = Instant::now();
    // The caller's already-cancelled preempt token is allowed to drop this without polling it.
    drop(handle_response_stream_error(
        &mut retry_state,
        0,
        temporary_rate_limit(),
        &mut client_session,
        &session,
        &step_context,
        ResponsesStreamRequest::Sampling,
    ));
    assert!(client_session.long_rate_limit_retry.is_active());
    assert_eq!(
        *client_session
            .long_rate_limit_retry
            .not_before
            .lock()
            .unwrap(),
        Some(started + LONG_RATE_LIMIT_RETRY_INTERVAL)
    );
    let wait = client_session.long_rate_limit_retry.before_attempt();
    tokio::pin!(wait);
    assert!(futures::poll!(&mut wait).is_pending());
}

#[tokio::test]
async fn long_retry_keeps_server_errors_slow_and_hard_errors_terminal() {
    let (session, mut turn_context) = make_session_and_context().await;
    Arc::make_mut(&mut turn_context.config)
        .features
        .enable(Feature::LongRateLimitRetries)
        .expect("enable long retries");
    let step_context = StepContext::for_test(Arc::new(turn_context));
    let mut client_session = session.services.model_client.new_session();
    let mut retry_state = ResponsesStreamRetryState::default();
    tokio::time::pause();
    client_session
        .long_rate_limit_retry
        .schedule_retry(&temporary_rate_limit());
    let deadline = client_session.long_rate_limit_retry.deadline;
    let started = Instant::now();
    {
        let retry = handle_response_stream_error(
            &mut retry_state,
            100,
            CodexErr::InternalServerError,
            &mut client_session,
            &session,
            &step_context,
            ResponsesStreamRequest::Sampling,
        );
        tokio::pin!(retry);
        assert!(futures::poll!(&mut retry).is_pending());
        tokio::time::advance(LONG_RATE_LIMIT_RETRY_INTERVAL).await;
        retry.await.unwrap();
    }
    let retry_at = started + LONG_RATE_LIMIT_RETRY_INTERVAL;
    assert!((retry_at..=retry_at + Duration::from_millis(1)).contains(&Instant::now()));
    assert_eq!(client_session.long_rate_limit_retry.deadline, deadline);
    let after_wait = Instant::now();
    for error in [
        CodexErr::QuotaExceeded,
        CodexErr::UsageNotIncluded,
        codex_api::map_api_error(codex_api::ApiError::Api {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid API key".into(),
        }),
        codex_api::map_api_error(codex_api::ApiError::Api {
            status: StatusCode::FORBIDDEN,
            message: "permission denied".into(),
        }),
        CodexErr::Fatal("auth required".into()),
    ] {
        assert!(!can_retry_during_rate_limit_recovery(&error));
        assert!(
            handle_response_stream_error(
                &mut retry_state,
                100,
                error,
                &mut client_session,
                &session,
                &step_context,
                ResponsesStreamRequest::Sampling,
            )
            .await
            .is_err()
        );
        assert_eq!(Instant::now(), after_wait);
    }
}
