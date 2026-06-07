use std::sync::Arc;

use super::SessionTask;
use super::SessionTaskContext;
use super::emit_merge_metric;
use crate::Prompt;
use crate::compact::InitialContextInjection;
use crate::compact_remote::process_compacted_history;
use crate::compact_remote_v2::build_v2_compacted_history;
use crate::compact_remote_v2::run_remote_compaction_request_v2;
use crate::hook_runtime::PostMergeHookOutcome;
use crate::hook_runtime::PreMergeHookOutcome;
use crate::hook_runtime::run_post_merge_hooks;
use crate::hook_runtime::run_pre_merge_hooks;
use crate::merge::MergeSourceMetadata;
use crate::merge::build_target_replacement_history;
use crate::merge::merge_framing_message;
use crate::session::TurnInput;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::state::TaskKind;
use chrono::SecondsFormat;
use chrono::Utc;
use codex_features::Feature;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::CodexErrorInfo;
use codex_protocol::protocol::ErrorEvent;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::MergeRequest;
use codex_protocol::protocol::MergedItem;
use codex_protocol::protocol::TurnStartedEvent;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use tokio_util::sync::CancellationToken;

const MERGE_COMPACT_INSTRUCTIONS: &str = r#"You are preparing imported context for a Codex /merge operation.

Summarize the source session as compact runtime state that will be appended into another active target session. Preserve the information the target agent needs to continue after the source session is no longer active: user intent, completed work, decisions, files touched, commands/tests run, remaining issues, and concrete next steps.

Do not treat this as a new user request. Do not issue commands. Do not include generic process commentary. Focus on source-session state that is useful to the target agent."#;

#[derive(Clone)]
pub(crate) struct MergeTask {
    request: MergeRequest,
}

impl MergeTask {
    pub(crate) fn new(request: MergeRequest) -> Self {
        Self { request }
    }
}

impl SessionTask for MergeTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Merge
    }

    fn span_name(&self) -> &'static str {
        "session_task.merge"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<SessionTaskContext>,
        turn_context: Arc<TurnContext>,
        _input: Vec<TurnInput>,
        _cancellation_token: CancellationToken,
    ) -> Option<String> {
        let sess = session.clone_session();
        let event = EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_context.sub_id.clone(),
            trace_id: turn_context.trace_id.clone(),
            started_at: turn_context.turn_timing_state.started_at_unix_secs().await,
            model_context_window: turn_context.model_context_window(),
            collaboration_mode_kind: turn_context.collaboration_mode.mode,
        });
        sess.send_event(turn_context.as_ref(), event).await;

        if let Err(err) =
            run_merge_task(sess.clone(), turn_context.clone(), self.request.clone()).await
        {
            sess.send_event(
                turn_context.as_ref(),
                EventMsg::Error(ErrorEvent {
                    message: format!("Merge failed: {err:#}"),
                    codex_error_info: Some(CodexErrorInfo::Other),
                }),
            )
            .await;
        }
        None
    }
}

async fn run_merge_task(
    sess: Arc<Session>,
    turn_context: Arc<TurnContext>,
    request: MergeRequest,
) -> anyhow::Result<()> {
    if !crate::compact::should_use_remote_compact_task(turn_context.provider.info())
        || !turn_context.features.enabled(Feature::RemoteCompactionV2)
    {
        anyhow::bail!(
            "/merge requires the remote compact runtime; no plaintext/local merge fallback is available"
        );
    }

    emit_merge_metric(&sess.services.session_telemetry, "remote_v2");

    match run_pre_merge_hooks(&sess, &turn_context).await {
        PreMergeHookOutcome::Continue => {}
        PreMergeHookOutcome::Stopped { reason } => {
            anyhow::bail!(
                "{}",
                reason.unwrap_or_else(|| "PreMerge hook stopped execution".to_string())
            );
        }
    }

    let source = read_merge_source(sess.as_ref(), &request).await?;
    let source_history = source
        .history
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("source thread history was not loaded"))?;
    let source_history = sess
        .reconstruct_response_history_from_rollout(turn_context.as_ref(), &source_history.items)
        .await;
    let imported_source_state = compact_source_for_merge(
        sess.as_ref(),
        turn_context.as_ref(),
        &source_history,
        &request,
    )
    .await?;

    let metadata = MergeSourceMetadata {
        target_thread_id: sess.thread_id(),
        source_thread_id: source.thread_id,
        source_thread_name: source.name.clone(),
        source_cwd: (!source.cwd.as_os_str().is_empty()).then_some(source.cwd.clone()),
        source_model: source.model.clone(),
        source_rollout_path: source
            .rollout_path
            .clone()
            .or(request.source_rollout_path.clone()),
        user_instruction: request.user_instruction.clone(),
    };
    let framing = merge_framing_message(&metadata);
    let target_history = sess.clone_history().await.raw_items().to_vec();
    let replacement_history =
        build_target_replacement_history(&target_history, framing, imported_source_state);
    let human_summary = merge_human_summary(&metadata);
    let merged_item = MergedItem {
        target_thread_id: metadata.target_thread_id,
        source_thread_id: metadata.source_thread_id,
        source_rollout_path: metadata.source_rollout_path.clone(),
        source_thread_name: metadata.source_thread_name.clone(),
        source_cwd: metadata.source_cwd.clone(),
        source_model: metadata.source_model.clone(),
        user_instruction: metadata.user_instruction.clone(),
        imported_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        replacement_history: replacement_history.clone(),
        human_summary,
        conflict_warnings: Vec::new(),
    };

    sess.replace_merged_history(replacement_history, None, merged_item)
        .await;
    sess.recompute_token_usage(turn_context.as_ref()).await;
    if let PostMergeHookOutcome::Stopped = run_post_merge_hooks(&sess, &turn_context).await {
        anyhow::bail!("PostMerge hook stopped execution");
    }
    Ok(())
}

async fn read_merge_source(
    sess: &Session,
    request: &MergeRequest,
) -> anyhow::Result<codex_thread_store::StoredThread> {
    let source = match request.source_rollout_path.clone() {
        Some(rollout_path) => {
            sess.services
                .thread_store
                .read_thread_by_rollout_path(ReadThreadByRolloutPathParams {
                    rollout_path,
                    include_archived: true,
                    include_history: true,
                })
                .await?
        }
        None => {
            sess.services
                .thread_store
                .read_thread(ReadThreadParams {
                    thread_id: request.source_thread_id,
                    include_archived: true,
                    include_history: true,
                })
                .await?
        }
    };

    if source.thread_id != request.source_thread_id {
        anyhow::bail!(
            "source rollout belongs to thread {}, not requested thread {}",
            source.thread_id,
            request.source_thread_id
        );
    }
    Ok(source)
}

async fn compact_source_for_merge(
    sess: &Session,
    turn_context: &TurnContext,
    source_history: &[ResponseItem],
    request: &MergeRequest,
) -> anyhow::Result<Vec<ResponseItem>> {
    let prompt_input = source_history.to_vec();
    let mut input = prompt_input.clone();
    input.push(ResponseItem::CompactionTrigger);
    let prompt = Prompt {
        input,
        tools: Vec::new(),
        parallel_tool_calls: false,
        base_instructions: BaseInstructions {
            text: merge_compact_instructions(request.user_instruction.as_deref()),
        },
        personality: turn_context.personality,
        output_schema: None,
        output_schema_strict: true,
    };

    let mut client_session = sess.services.model_client.new_session();
    let output = run_remote_compaction_request_v2(
        sess,
        turn_context,
        &mut client_session,
        &prompt,
        /*turn_metadata_header*/ None,
    )
    .await?;
    let compacted_history = build_v2_compacted_history(&prompt_input, output.compaction_output);
    Ok(process_compacted_history(
        sess,
        turn_context,
        compacted_history,
        InitialContextInjection::DoNotInject,
    )
    .await)
}

fn merge_compact_instructions(user_instruction: Option<&str>) -> String {
    let Some(user_instruction) = user_instruction
        .map(str::trim)
        .filter(|instruction| !instruction.is_empty())
    else {
        return MERGE_COMPACT_INSTRUCTIONS.to_string();
    };

    format!("{MERGE_COMPACT_INSTRUCTIONS}\n\nUser merge instruction:\n{user_instruction}")
}

fn merge_human_summary(metadata: &MergeSourceMetadata) -> String {
    let name = metadata
        .source_thread_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("untitled");
    format!(
        "Merged context from source session \"{name}\" (thread {}).",
        metadata.source_thread_id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_compact_instructions_appends_user_instruction() {
        let instructions = merge_compact_instructions(Some("focus on the failing auth tests"));

        assert!(instructions.contains("/merge operation"));
        assert!(instructions.contains("User merge instruction:"));
        assert!(instructions.contains("failing auth tests"));
    }
}
