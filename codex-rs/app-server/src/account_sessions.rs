use base64::Engine;
use chrono::Utc;
use codex_app_server_protocol::AccountSession;
use codex_app_server_protocol::AccountSessionWorkspace;
use codex_app_server_protocol::AccountSessionWorkspaceKind;
use codex_app_server_protocol::AccountSessionWorkspaceStatus;
use codex_app_server_protocol::AccountSessionsResponse;
use codex_backend_client::AccountEntry;
use codex_backend_client::Client as BackendClient;
use codex_config::types::AuthCredentialsStoreMode;
use codex_login::AuthDotJson;
use codex_login::CodexAuth;
use codex_login::load_auth_dot_json;
use codex_login::logout;
use codex_login::revoke_auth_tokens;
use codex_login::save_auth;
use serde::Deserialize;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

const ACCOUNT_SESSIONS_FILE: &str = "account-sessions.json";

pub(crate) struct AccountSessionsStore<'a> {
    codex_home: &'a Path,
    auth_credentials_store_mode: AuthCredentialsStoreMode,
    chatgpt_base_url: &'a str,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredAccountSessions {
    active_session_id: Option<String>,
    sessions: Vec<StoredAccountSession>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredAccountSession {
    session_id: String,
    auth_json: AuthDotJson,
    email: Option<String>,
    user_id: Option<String>,
    display_name: Option<String>,
    image_url: Option<String>,
    plan: Option<String>,
    last_used_at: i64,
    selected_workspace_account_id: Option<String>,
    workspaces: Vec<AccountSessionWorkspace>,
}

#[derive(Default, Deserialize)]
struct AccessTokenClaims {
    #[serde(rename = "https://api.openai.com/auth", default)]
    auth: AccessTokenAuthClaims,
    #[serde(rename = "https://api.openai.com/profile", default)]
    profile: AccessTokenProfileClaims,
}

#[derive(Default, Deserialize)]
struct AccessTokenAuthClaims {
    #[serde(default)]
    chatgpt_account_id: Option<String>,
    #[serde(default)]
    chatgpt_plan_type: Option<String>,
    #[serde(default)]
    chatgpt_user_id: Option<String>,
}

#[derive(Default, Deserialize)]
struct AccessTokenProfileClaims {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    picture: Option<String>,
}

impl<'a> AccountSessionsStore<'a> {
    pub(crate) fn new(
        codex_home: &'a Path,
        auth_credentials_store_mode: AuthCredentialsStoreMode,
        chatgpt_base_url: &'a str,
    ) -> Self {
        Self {
            codex_home,
            auth_credentials_store_mode,
            chatgpt_base_url,
        }
    }

    pub(crate) async fn add(
        &self,
        switch_to_added_account: bool,
    ) -> std::io::Result<AccountSessionsResponse> {
        let mut stored = self.load()?;
        let auth_json = load_auth_dot_json(self.codex_home, self.auth_credentials_store_mode)?
            .ok_or_else(|| std::io::Error::other("No active ChatGPT auth session to add"))?;
        let mut session = Self::session_from_auth_json(auth_json)?;
        self.refresh_workspace_metadata(&mut session).await;

        let existing_index = stored.sessions.iter().position(|saved| {
            session
                .email
                .as_ref()
                .is_some_and(|email| saved.email.as_ref() == Some(email))
                || session
                    .user_id
                    .as_ref()
                    .is_some_and(|user_id| saved.user_id.as_ref() == Some(user_id))
        });
        if let Some(index) = existing_index {
            session
                .session_id
                .clone_from(&stored.sessions[index].session_id);
        }
        let added_session_id = session.session_id.clone();
        let added_auth_json = session.auth_json.clone();
        if let Some(index) = existing_index {
            stored.sessions[index] = session;
        } else {
            stored.sessions.push(session);
        }

        if switch_to_added_account {
            stored.active_session_id = Some(added_session_id);
            save_auth(
                self.codex_home,
                &added_auth_json,
                self.auth_credentials_store_mode,
            )?;
        }

        self.save(&stored)?;
        Ok(Self::response(stored))
    }

    pub(crate) async fn list(
        &self,
        refresh_workspace_metadata: bool,
    ) -> std::io::Result<AccountSessionsResponse> {
        let mut stored = self.load()?;
        if refresh_workspace_metadata {
            for session in &mut stored.sessions {
                self.refresh_workspace_metadata(session).await;
            }
            self.save(&stored)?;
        }
        Ok(Self::response(stored))
    }

    pub(crate) async fn logout(
        &self,
        session_id: &str,
    ) -> std::io::Result<AccountSessionsResponse> {
        let mut stored = self.load()?;
        let index = stored
            .sessions
            .iter()
            .position(|session| session.session_id == session_id)
            .ok_or_else(|| std::io::Error::other("Saved ChatGPT account session not found"))?;
        let removed = stored.sessions.remove(index);
        if let Err(err) = revoke_auth_tokens(Some(&removed.auth_json)).await {
            tracing::warn!("failed to revoke saved account session during logout: {err}");
        }

        if stored.active_session_id.as_deref() == Some(session_id) {
            let newest = stored
                .sessions
                .iter()
                .max_by_key(|session| session.last_used_at);
            stored.active_session_id = newest.map(|session| session.session_id.clone());
            match newest {
                Some(session) => save_auth(
                    self.codex_home,
                    &session.auth_json,
                    self.auth_credentials_store_mode,
                )?,
                None => {
                    logout(self.codex_home, self.auth_credentials_store_mode)?;
                }
            }
        }

        self.save(&stored)?;
        Ok(Self::response(stored))
    }

    pub(crate) fn switch(
        &self,
        session_id: &str,
        account_id: &str,
    ) -> std::io::Result<AccountSessionsResponse> {
        let mut stored = self.load()?;
        let session = stored
            .sessions
            .iter_mut()
            .find(|session| session.session_id == session_id)
            .ok_or_else(|| std::io::Error::other("Saved ChatGPT account session not found"))?;
        let tokens =
            session.auth_json.tokens.as_mut().ok_or_else(|| {
                std::io::Error::other("Saved ChatGPT account session has no tokens")
            })?;
        tokens.account_id = Some(account_id.to_string());
        session.selected_workspace_account_id = Some(account_id.to_string());
        session.last_used_at = Utc::now().timestamp();
        stored.active_session_id = Some(session_id.to_string());
        save_auth(
            self.codex_home,
            &session.auth_json,
            self.auth_credentials_store_mode,
        )?;
        self.save(&stored)?;
        Ok(Self::response(stored))
    }

    pub(crate) fn clear(&self) -> std::io::Result<()> {
        match std::fs::remove_file(self.path()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn load(&self) -> std::io::Result<StoredAccountSessions> {
        let path = self.path();
        match std::fs::read_to_string(path) {
            Ok(payload) => serde_json::from_str(&payload).map_err(std::io::Error::other),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let auth_json =
                    load_auth_dot_json(self.codex_home, self.auth_credentials_store_mode)?;
                let Some(auth_json) = auth_json else {
                    return Ok(StoredAccountSessions::default());
                };
                let session = Self::session_from_auth_json(auth_json)?;
                let stored = StoredAccountSessions {
                    active_session_id: Some(session.session_id.clone()),
                    sessions: vec![session],
                };
                self.save(&stored)?;
                Ok(stored)
            }
            Err(err) => Err(err),
        }
    }

    fn save(&self, sessions: &StoredAccountSessions) -> std::io::Result<()> {
        let path = self.path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = OpenOptions::new();
        options.truncate(true).write(true).create(true);
        #[cfg(unix)]
        {
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(serde_json::to_string_pretty(sessions)?.as_bytes())?;
        file.flush()
    }

    async fn refresh_workspace_metadata(&self, session: &mut StoredAccountSession) {
        let Ok(auth) = CodexAuth::from_auth_dot_json(
            self.codex_home,
            session.auth_json.clone(),
            self.auth_credentials_store_mode,
            Some(self.chatgpt_base_url),
        )
        .await
        else {
            return;
        };
        let Ok(client) = BackendClient::from_auth(self.chatgpt_base_url, &auth) else {
            return;
        };
        let Ok(accounts) = client.get_accounts_check().await else {
            return;
        };
        session.selected_workspace_account_id = session
            .selected_workspace_account_id
            .clone()
            .or(accounts.default_account_id)
            .or_else(|| accounts.account_ordering.first().cloned());
        if let Some(account_id) = session.selected_workspace_account_id.as_ref()
            && let Some(tokens) = session.auth_json.tokens.as_mut()
        {
            tokens.account_id = Some(account_id.clone());
        }
        session.workspaces = accounts
            .accounts
            .into_iter()
            .map(Self::workspace_from_account)
            .collect();
    }

    fn session_from_auth_json(auth_json: AuthDotJson) -> std::io::Result<StoredAccountSession> {
        let tokens = auth_json
            .tokens
            .as_ref()
            .ok_or_else(|| std::io::Error::other("No active ChatGPT auth session to add"))?;
        let claims = Self::access_token_claims(&tokens.access_token);
        let selected_workspace_account_id = tokens
            .account_id
            .clone()
            .or_else(|| claims.auth.chatgpt_account_id.clone());
        let workspaces = selected_workspace_account_id
            .as_ref()
            .map(|account_id| {
                vec![AccountSessionWorkspace {
                    account_id: account_id.clone(),
                    name: None,
                    image_url: None,
                    kind: None,
                    status: AccountSessionWorkspaceStatus::Active,
                }]
            })
            .unwrap_or_default();
        Ok(StoredAccountSession {
            session_id: Uuid::now_v7().to_string(),
            auth_json,
            email: claims.profile.email,
            user_id: claims.auth.chatgpt_user_id,
            display_name: claims.profile.name,
            image_url: claims.profile.picture.or(claims.profile.image),
            plan: claims.auth.chatgpt_plan_type,
            last_used_at: Utc::now().timestamp(),
            selected_workspace_account_id,
            workspaces,
        })
    }

    fn access_token_claims(access_token: &str) -> AccessTokenClaims {
        let Some(payload) = access_token.split('.').nth(1) else {
            return AccessTokenClaims::default();
        };
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .ok()
            .and_then(|payload| serde_json::from_slice(&payload).ok())
            .unwrap_or_default()
    }

    fn workspace_from_account(account: AccountEntry) -> AccountSessionWorkspace {
        let kind = match account.structure.as_str() {
            "personal" => Some(AccountSessionWorkspaceKind::Personal),
            "workspace" => Some(AccountSessionWorkspaceKind::Workspace),
            _ => None,
        };
        AccountSessionWorkspace {
            account_id: account.id,
            name: account.name,
            image_url: account.profile_picture_url,
            kind,
            status: AccountSessionWorkspaceStatus::Active,
        }
    }

    fn response(stored: StoredAccountSessions) -> AccountSessionsResponse {
        let active_session_id = stored.active_session_id;
        let mut sessions = stored
            .sessions
            .into_iter()
            .map(|session| AccountSession {
                is_active: Some(&session.session_id) == active_session_id.as_ref(),
                session_id: session.session_id,
                email: session.email,
                user_id: session.user_id,
                display_name: session.display_name,
                image_url: session.image_url,
                plan: session.plan,
                last_used_at: session.last_used_at,
                selected_workspace_account_id: session.selected_workspace_account_id,
                workspaces: session.workspaces,
            })
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| std::cmp::Reverse(session.last_used_at));
        AccountSessionsResponse {
            active_session_id,
            sessions,
        }
    }

    fn path(&self) -> PathBuf {
        self.codex_home.join(ACCOUNT_SESSIONS_FILE)
    }
}
