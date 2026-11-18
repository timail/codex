use anyhow::Result;
use codex_config::config_toml::ResponseProcessMode;
use codex_core::CodexThread;
use codex_core::StartThreadOptions;
use codex_core::TurnInputSubmission;
use codex_features::Feature;
use codex_model_provider_info::ModelProviderCapabilities;
use codex_model_provider_info::RemoteCompactionSupport;
use codex_protocol::dynamic_tools::DynamicToolCallOutputContentItem;
use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
use codex_protocol::dynamic_tools::DynamicToolResponse;
use codex_protocol::dynamic_tools::DynamicToolSpec;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::turn_input::TurnInputRequest;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::time::Duration;
use tokio::sync::oneshot;
use wiremock::ResponseTemplate;

const RETRY_INTERVAL: Duration = Duration::from_secs(30 * 60);

fn enable_long_retries(config: &mut codex_core::config::Config) {
    config
        .features
        .enable(Feature::LongRateLimitRetries)
        .unwrap();
    config.features.enable(Feature::InstantInterrupt).unwrap();
    config.model_provider.request_max_retries = Some(4);
    config.model_provider.stream_max_retries = Some(0);
    config.model_provider.supports_websockets = false;
    config.response_process_mode = ResponseProcessMode::Streaming;
}

fn input(text: &str) -> TurnInputRequest {
    TurnInputRequest::user_input(vec![UserInput::Text {
        text: text.to_owned(),
        text_elements: Vec::new(),
    }])
}

async fn wait_for_recovery(codex: &CodexThread) {
    wait_for_event(codex, |event| {
        assert!(!matches!(event, EventMsg::TurnComplete(_) | EventMsg::Error(_)));
        matches!(event, EventMsg::StreamError(error) if error.message.starts_with("Rate-limit recovery:"))
    })
    .await;
}

// Keep real network I/O on the ordinary clock; advance only the long wait virtually.
async fn advance_retry_clock(duration: Duration) {
    tokio::time::pause();
    tokio::time::advance(duration).await;
    tokio::time::resume();
}

fn failed_rate_limit() -> serde_json::Value {
    json!({
        "type": "response.failed",
        "response": {"error": {"code": "rate_limit_exceeded", "message": "temporary throttle"}}
    })
}

fn set_compaction_protocol(config: &mut codex_core::config::Config, remote: bool) {
    enable_long_retries(config);
    config.features.disable(Feature::AsyncCompaction).unwrap();
    config.model_provider.capabilities = Some(ModelProviderCapabilities {
        remote_compaction: Some(if remote {
            RemoteCompactionSupport::V2
        } else {
            RemoteCompactionSupport::Unsupported
        }),
        ..Default::default()
    });
}

#[test_case::test_case(false; "local_compaction")]
#[test_case::test_case(true; "remote_compaction")]
#[tokio::test]
async fn successful_inline_compaction_releases_the_same_turn_recovery_gate(
    remote: bool,
) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let compacted_item = if remote {
        json!({
            "type": "response.output_item.done",
            "item": {"type": "compaction", "encrypted_content": "summary-for-test"}
        })
    } else {
        responses::ev_assistant_message("summary", "retained context summary")
    };
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse(vec![
                responses::ev_assistant_message("initial", "large earlier context"),
                responses::ev_completed_with_tokens("before-compact", 250_000),
            ])),
            ResponseTemplate::new(429),
            responses::sse_response(responses::sse(vec![
                compacted_item,
                responses::ev_completed_with_tokens("compacted", 200),
            ])),
            responses::sse_response(responses::sse_completed("follow-up")),
        ],
    )
    .await;
    let test = test_codex()
        .with_model("gpt-5.4")
        .with_config(move |config| {
            set_compaction_protocol(config, remote);
            config.model_auto_compact_token_limit = Some(200_000);
        })
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("retain this context").await?;
    assert_eq!(mock.requests().len(), 1);
    let TurnInputSubmission::Started { turn_id, .. } = test
        .codex
        .start_or_steer_turn(input("continue after compaction"))
        .await?
    else {
        panic!("expected a new turn");
    };
    wait_for_recovery(&test.codex).await;
    assert_eq!(mock.requests().len(), 2);
    assert_eq!(
        !mock.requests()[1]
            .inputs_of_type("compaction_trigger")
            .is_empty(),
        remote,
    );
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    // A complete compaction ends the outage: sampling must proceed without another 30-minute wait.
    wait_for_event(&test.codex, |event| {
        assert!(!matches!(event, EventMsg::Error(_)));
        matches!(event, EventMsg::TurnComplete(completed) if completed.error.is_none())
    })
    .await;
    let requests = mock.requests();
    assert_eq!(requests.len(), 4);
    responses::assert_root_turn(&requests[3].body_json(), Some(&turn_id))?;
    assert!(requests[3].body_contains_text("continue after compaction"));
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[test_case::test_case(false; "local_compaction")]
#[test_case::test_case(true; "remote_compaction")]
#[tokio::test]
async fn foreground_compaction_stays_slow_after_500_and_cancels(remote: bool) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_completed("before-compact")),
            ResponseTemplate::new(429),
            ResponseTemplate::new(500),
            responses::sse_response(responses::sse_completed("unexpected-after-cancel")),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(move |config| set_compaction_protocol(config, remote))
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("retain this context").await?;
    test.codex.submit(Op::Compact).await?;
    wait_for_recovery(&test.codex).await;
    assert_eq!(mock.requests().len(), 2);
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    wait_for_recovery(&test.codex).await;
    assert_eq!(
        mock.requests().len(),
        3,
        "500 must not restore transport retries"
    );
    test.codex.submit(Op::Interrupt).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    assert_eq!(mock.requests().len(), 3);
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[test_case::test_case(false; "local_compaction")]
#[test_case::test_case(true; "remote_compaction")]
#[tokio::test]
async fn foreground_compaction_caps_retry_after_at_the_outage_deadline(remote: bool) -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            responses::sse_response(responses::sse_completed("before-compact")),
            ResponseTemplate::new(429).insert_header("Retry-After", "46800"),
            responses::sse_response(responses::sse_completed("unexpected-after-expiry")),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(move |config| set_compaction_protocol(config, remote))
        .build_with_auto_env(&server)
        .await?;
    test.submit_turn("retain this context").await?;
    test.codex.submit(Op::Compact).await?;
    wait_for_recovery(&test.codex).await;
    advance_retry_clock(Duration::from_secs(12 * 60 * 60 + 1)).await;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::Error(error) if error.message.contains("retry window exhausted after 12 hours"))
    }).await;
    assert_eq!(
        mock.requests().len(),
        2,
        "expiry must not start another request or fallback"
    );
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test]
async fn http_and_sse_recovery_never_burst_and_interrupt_cancels_the_wait() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_response_sequence(
        &server,
        vec![
            ResponseTemplate::new(429)
                .set_body_json(json!({"error": {"type": "throttling_error", "code": "429"}})),
            ResponseTemplate::new(500),
            responses::sse_response(responses::sse(vec![failed_rate_limit()])),
            responses::sse_response(responses::sse_completed("unexpected-after-cancel")),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(enable_long_retries)
        .build_with_auto_env(&server)
        .await?;
    test.codex
        .start_or_steer_turn(input("continue through a temporary throttle"))
        .await?;
    for expected in 1..=3 {
        wait_for_recovery(&test.codex).await;
        assert_eq!(
            mock.requests().len(),
            expected,
            "one physical request per recovery slot"
        );
        if expected < 3 {
            advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
        }
    }
    test.codex.submit(Op::Interrupt).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    assert_eq!(mock.requests().len(), 3);
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test]
async fn completed_tool_output_survives_failed_stream_without_reexecuting_the_tool() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let server = responses::start_mock_server().await;
    let mock = responses::mount_sse_sequence(
        &server,
        vec![
            responses::sse(vec![
                responses::ev_response_created("before-throttle"),
                responses::ev_function_call("once-call", "side_effect", "{}"),
                failed_rate_limit(),
            ]),
            responses::sse_completed("recovered"),
        ],
    )
    .await;
    let test = test_codex()
        .with_model("gpt-5.4")
        .with_config(enable_long_retries)
        .build_with_auto_env(&server)
        .await?;
    let codex = test
        .thread_manager
        .start_thread(StartThreadOptions {
            dynamic_tools: vec![DynamicToolSpec::Function(DynamicToolFunctionSpec {
                name: "side_effect".to_string(),
                description: "A side effect recorded once by this test.".to_string(),
                input_schema: json!({"type": "object", "properties": {}}),
                defer_loading: false,
            })],
            ..StartThreadOptions::new(test.config.clone())
        })
        .await?
        .thread;
    let TurnInputSubmission::Started { turn_id, .. } = codex
        .start_or_steer_turn(input("perform the side effect once"))
        .await?
    else {
        panic!("expected a new turn");
    };
    let EventMsg::DynamicToolCallRequest(call) = wait_for_event(&codex, |event| {
        matches!(event, EventMsg::DynamicToolCallRequest(_))
    })
    .await
    else {
        unreachable!("tool request predicate");
    };
    assert_eq!(call.call_id, "once-call");
    codex
        .submit(Op::DynamicToolResponse {
            id: call.call_id,
            response: DynamicToolResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText {
                    text: "side effect completed once".to_string(),
                }],
                success: true,
            },
        })
        .await?;
    wait_for_recovery(&codex).await;
    assert_eq!(mock.requests().len(), 1);
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    wait_for_event(&codex, |event| {
        assert!(
            !matches!(event, EventMsg::DynamicToolCallRequest(_)),
            "completed tool must not execute again"
        );
        matches!(event, EventMsg::TurnComplete(completed) if completed.error.is_none())
    })
    .await;
    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    let second = requests[1].body_json();
    responses::assert_root_turn(&second, Some(&turn_id))?;
    assert!(
        requests[1].function_call_output("once-call")["output"]
            .to_string()
            .contains("side effect completed once")
    );
    assert_eq!(
        second["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["type"] == "function_call_output" && item["call_id"] == "once-call")
            .count(),
        1
    );
    codex.shutdown_and_wait().await?;
    test.codex.shutdown_and_wait().await?;
    Ok(())
}

#[tokio::test]
async fn steering_an_open_partial_recovery_stream_keeps_the_original_gate() -> Result<()> {
    skip_if_no_network!(Ok(()));
    let (release_partial, hold_partial) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![failed_rate_limit()]),
        }],
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created("partial"),
                    responses::ev_message_item_added("partial-message", ""),
                    responses::ev_output_text_delta("partial reply"),
                ]),
            },
            StreamingSseChunk {
                gate: Some(hold_partial),
                body: String::new(),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse_completed("after-steer"),
        }],
    ])
    .await;
    let test = test_codex()
        .with_model("gpt-5.4")
        .with_config(enable_long_retries)
        .build_with_streaming_server(&server)
        .await?;
    let TurnInputSubmission::Started { turn_id, .. } =
        test.codex.start_or_steer_turn(input("first input")).await?
    else {
        panic!("expected a new turn");
    };
    wait_for_recovery(&test.codex).await;
    advance_retry_clock(RETRY_INTERVAL + Duration::from_secs(1)).await;
    wait_for_event(&test.codex, |event| matches!(event, EventMsg::AgentMessageContentDelta(delta) if delta.delta.contains("partial reply"))).await;
    assert_eq!(server.requests().await.len(), 2);
    let submission = test
        .codex
        .start_or_steer_turn(input("steered input"))
        .await?;
    assert!(matches!(submission, TurnInputSubmission::Steered { .. }));
    wait_for_event(&test.codex, |event| matches!(event, EventMsg::UserMessage(message) if message.message == "steered input")).await;
    // A partial response's Ok(needs_follow_up) must not reset the recovery gate.
    advance_retry_clock(Duration::from_secs(29 * 60)).await;
    tokio::task::yield_now().await;
    assert_eq!(server.requests().await.len(), 2);
    advance_retry_clock(Duration::from_secs(61)).await;
    wait_for_event(
        &test.codex,
        |event| matches!(event, EventMsg::TurnComplete(completed) if completed.error.is_none()),
    )
    .await;
    let requests = server.requests().await;
    assert_eq!(requests.len(), 3);
    let final_request: serde_json::Value = serde_json::from_slice(&requests[2])?;
    responses::assert_root_turn(&final_request, Some(&turn_id))?;
    assert!(final_request["input"].to_string().contains("steered input"));
    drop(release_partial);
    test.codex.shutdown_and_wait().await?;
    server.shutdown().await;
    Ok(())
}
