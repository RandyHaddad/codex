use std::path::PathBuf;

use codex_protocol::ThreadId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;

pub(crate) struct MergeSourceMetadata {
    pub(crate) target_thread_id: ThreadId,
    pub(crate) source_thread_id: ThreadId,
    pub(crate) source_thread_name: Option<String>,
    pub(crate) source_cwd: Option<PathBuf>,
    pub(crate) source_model: Option<String>,
    pub(crate) source_rollout_path: Option<PathBuf>,
    pub(crate) user_instruction: Option<String>,
}

pub(crate) fn merge_framing_message(meta: &MergeSourceMetadata) -> ResponseItem {
    let mut text = format!(
        "Merged context from source session \"{}\" (thread {}).\n\n\
         Target thread: {}.\n\n\
         This is imported background context, not a new user request. Use it to preserve completed work, decisions, files touched, unresolved issues, and next steps from the source session.",
        meta.source_thread_name.as_deref().unwrap_or("untitled"),
        meta.source_thread_id,
        meta.target_thread_id,
    );
    if let Some(source_cwd) = meta.source_cwd.as_ref() {
        text.push_str("\n\nSource cwd: ");
        text.push_str(&source_cwd.display().to_string());
    }
    if let Some(source_model) = meta.source_model.as_deref() {
        text.push_str("\nSource model: ");
        text.push_str(source_model);
    }
    if let Some(source_rollout_path) = meta.source_rollout_path.as_ref() {
        text.push_str("\nSource transcript: ");
        text.push_str(&source_rollout_path.display().to_string());
    }
    if let Some(instruction) = meta
        .user_instruction
        .as_deref()
        .filter(|s| !s.trim().is_empty())
    {
        text.push_str("\n\nUser merge instruction: ");
        text.push_str(instruction.trim());
    }
    text.push_str("\n\nImported source state follows.");

    ResponseItem::Message {
        id: None,
        role: "developer".to_string(),
        content: vec![ContentItem::InputText { text }],
        phase: None,
    }
}

pub(crate) fn build_target_replacement_history(
    target_history: &[ResponseItem],
    merge_framing: ResponseItem,
    imported_source_state: Vec<ResponseItem>,
) -> Vec<ResponseItem> {
    let mut replacement =
        Vec::with_capacity(target_history.len() + 1 + imported_source_state.len());
    replacement.extend_from_slice(target_history);
    replacement.push(merge_framing);
    replacement.extend(imported_source_state);
    replacement
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(role: &str, text: &str) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: role.to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
        }
    }

    fn message_text(item: &ResponseItem) -> Option<&str> {
        let ResponseItem::Message { content, .. } = item else {
            return None;
        };
        let [ContentItem::InputText { text }] = content.as_slice() else {
            return None;
        };
        Some(text.as_str())
    }

    #[test]
    fn merge_framing_marks_import_as_background_context() {
        let meta = MergeSourceMetadata {
            target_thread_id: ThreadId::new(),
            source_thread_id: ThreadId::new(),
            source_thread_name: Some("fix-auth-tests".to_string()),
            source_cwd: Some(PathBuf::from("/tmp/repo")),
            source_model: Some("gpt-5.4".to_string()),
            source_rollout_path: Some(PathBuf::from("/tmp/source.jsonl")),
            user_instruction: Some("focus on failing tests".to_string()),
        };

        let framing = merge_framing_message(&meta);

        let ResponseItem::Message { role, .. } = &framing else {
            panic!("merge framing should be a message");
        };
        assert_eq!(role, "developer");
        let text = message_text(&framing).unwrap_or_default();
        assert!(text.contains("imported background context, not a new user request"));
        assert!(text.contains("focus on failing tests"));
        assert!(text.contains("fix-auth-tests"));
    }

    #[test]
    fn target_replacement_history_preserves_target_then_imports_source() {
        let target = vec![message("user", "target")];
        let framing = message("developer", "merge framing");
        let source = vec![ResponseItem::Compaction {
            encrypted_content: "opaque".to_string(),
        }];

        let history = build_target_replacement_history(&target, framing.clone(), source.clone());

        assert_eq!(history.len(), 3);
        assert_eq!(history[0], target[0]);
        assert_eq!(history[1], framing);
        assert_eq!(history[2], source[0]);
    }
}
