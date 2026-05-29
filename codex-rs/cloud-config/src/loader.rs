//! Cloud config bundle loading, refresh, and backend transport orchestration.
//!
//! Startup loads a single shared bundle from cache or backend, and a background
//! refresher keeps the cache warm for future app starts without changing the
//! already-snapshotted runtime config.

use crate::cache::CloudConfigBundleCache;
use codex_backend_client::Client as BackendClient;
use codex_backend_client::ConfigBundleResponse;
use codex_backend_client::DeliveredTomlFragment;
use codex_config::AbsolutePathBuf;
use codex_config::CloudConfigBundle;
use codex_config::CloudConfigBundleLayers;
use codex_config::CloudConfigBundleLoadError;
use codex_config::CloudConfigBundleLoadErrorCode;
use codex_config::CloudConfigBundleLoader;
use codex_config::CloudConfigFragment;
use codex_config::CloudConfigTomlBundle;
use codex_config::CloudRequirementsFragment;
use codex_config::CloudRequirementsTomlBundle;
use codex_config::compose_requirements;
use codex_config::types::AuthCredentialsStoreMode;
use codex_core::util::backoff;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_login::RefreshTokenError;
use codex_protocol::account::PlanType;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::timeout;

const CLOUD_CONFIG_BUNDLE_TIMEOUT: Duration = Duration::from_secs(15);
const CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS: usize = 5;
const CLOUD_CONFIG_BUNDLE_CACHE_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const CLOUD_CONFIG_BUNDLE_FETCH_ATTEMPT_METRIC: &str = "codex.cloud_config_bundle.fetch_attempt";
const CLOUD_CONFIG_BUNDLE_FETCH_FINAL_METRIC: &str = "codex.cloud_config_bundle.fetch_final";
const CLOUD_CONFIG_BUNDLE_LOAD_METRIC: &str = "codex.cloud_config_bundle.load";
const CLOUD_CONFIG_BUNDLE_LOAD_FAILED_MESSAGE: &str =
    "Failed to load cloud config bundle (workspace-managed policies).";
const CLOUD_CONFIG_BUNDLE_AUTH_RECOVERY_FAILED_MESSAGE: &str = concat!(
    "Your authentication session could not be refreshed automatically. ",
    "Please log out and sign in again."
);
fn refresher_task_slot() -> &'static Mutex<Option<JoinHandle<()>>> {
    static REFRESHER_TASK: OnceLock<Mutex<Option<JoinHandle<()>>>> = OnceLock::new();
    REFRESHER_TASK.get_or_init(|| Mutex::new(None))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetryableFailureKind {
    BackendClientInit,
    Request { status_code: Option<u16> },
}

impl RetryableFailureKind {
    fn status_code(self) -> Option<u16> {
        match self {
            Self::BackendClientInit => None,
            Self::Request { status_code } => status_code,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FetchAttemptError {
    Retryable(RetryableFailureKind),
    Unauthorized {
        status_code: Option<u16>,
        message: String,
    },
}

fn auth_identity(auth: &CodexAuth) -> (Option<String>, Option<String>) {
    (auth.get_chatgpt_user_id(), auth.get_account_id())
}

fn cloud_config_eligible_auth(auth: &CodexAuth) -> bool {
    let Some(plan_type) = auth.account_plan_type() else {
        return false;
    };
    auth.uses_codex_backend()
        && (plan_type.is_business_like() || matches!(plan_type, PlanType::Enterprise))
}

fn optional_bundle(bundle: CloudConfigBundle) -> Option<CloudConfigBundle> {
    if bundle.is_empty() {
        None
    } else {
        Some(bundle)
    }
}

fn validate_bundle(
    bundle: &CloudConfigBundle,
    codex_home: &std::path::Path,
) -> Result<(), CloudConfigBundleLoadError> {
    let base_dir = AbsolutePathBuf::from_absolute_path(codex_home).map_err(|err| {
        CloudConfigBundleLoadError::new(
            CloudConfigBundleLoadErrorCode::Internal,
            /*status_code*/ None,
            format!("failed to validate cloud config bundle base path: {err}"),
        )
    })?;
    let bundle_layers =
        CloudConfigBundleLayers::from_bundle(bundle.clone(), &base_dir).map_err(|err| {
            CloudConfigBundleLoadError::new(
                CloudConfigBundleLoadErrorCode::InvalidBundle,
                /*status_code*/ None,
                format!("invalid cloud config bundle: {err}"),
            )
        })?;
    let CloudConfigBundleLayers {
        enterprise_managed_config: _,
        enterprise_managed_requirements,
    } = bundle_layers;

    compose_requirements(enterprise_managed_requirements).map_err(|err| {
        CloudConfigBundleLoadError::new(
            CloudConfigBundleLoadErrorCode::InvalidBundle,
            /*status_code*/ None,
            format!("invalid cloud config bundle: {err}"),
        )
    })?;

    Ok(())
}

/// Fetches the raw cloud config bundle for an authenticated Codex account.
///
/// Implementations should return the backend-selected bundle exactly as delivered and leave
/// config and requirements TOML parsing to the config crate's layer handling.
trait BundleFetcher: Send + Sync {
    fn fetch_bundle(
        &self,
        auth: &CodexAuth,
    ) -> impl Future<Output = Result<CloudConfigBundle, FetchAttemptError>> + Send;
}

struct BackendBundleFetcher {
    base_url: String,
}

impl BackendBundleFetcher {
    fn new(base_url: String) -> Self {
        Self { base_url }
    }
}

impl BundleFetcher for BackendBundleFetcher {
    async fn fetch_bundle(&self, auth: &CodexAuth) -> Result<CloudConfigBundle, FetchAttemptError> {
        let client = BackendClient::from_auth(self.base_url.clone(), auth)
            .inspect_err(|err| {
                tracing::warn!(
                    error = %err,
                    "Failed to construct backend client for cloud config bundle"
                );
            })
            .map_err(|_| FetchAttemptError::Retryable(RetryableFailureKind::BackendClientInit))?;

        let response = client
            .get_config_bundle()
            .await
            .inspect_err(|err| {
                tracing::warn!(error = %err, "Failed to fetch cloud config bundle");
            })
            .map_err(|err| {
                let status_code = err.status().map(|status| status.as_u16());
                if err.is_unauthorized() {
                    FetchAttemptError::Unauthorized {
                        status_code,
                        message: err.to_string(),
                    }
                } else {
                    FetchAttemptError::Retryable(RetryableFailureKind::Request { status_code })
                }
            })?;

        Ok(bundle_from_response(response))
    }
}

struct CloudConfigBundleService<F> {
    auth_manager: Arc<AuthManager>,
    fetcher: Arc<F>,
    cache: CloudConfigBundleCache,
    codex_home: PathBuf,
    timeout: Duration,
}

impl<F> Clone for CloudConfigBundleService<F> {
    fn clone(&self) -> Self {
        Self {
            auth_manager: Arc::clone(&self.auth_manager),
            fetcher: Arc::clone(&self.fetcher),
            cache: self.cache.clone(),
            codex_home: self.codex_home.clone(),
            timeout: self.timeout,
        }
    }
}

impl<F> CloudConfigBundleService<F>
where
    F: BundleFetcher + 'static,
{
    fn new(
        auth_manager: Arc<AuthManager>,
        fetcher: Arc<F>,
        codex_home: PathBuf,
        timeout: Duration,
    ) -> Self {
        Self {
            auth_manager,
            fetcher,
            cache: CloudConfigBundleCache::new(codex_home.clone()),
            codex_home,
            timeout,
        }
    }

    async fn fetch_with_timeout(
        &self,
    ) -> Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError> {
        let _timer =
            codex_otel::start_global_timer("codex.cloud_config_bundle.fetch.duration_ms", &[]);
        let started_at = Instant::now();
        let fetch_result = timeout(self.timeout, self.fetch())
            .await
            .inspect_err(|_| {
                let message = format!(
                    "Timed out waiting for cloud config bundle after {}s",
                    self.timeout.as_secs()
                );
                tracing::error!("{message}");
                emit_load_metric("startup", "error", /*bundle*/ None);
            })
            .map_err(|_| {
                CloudConfigBundleLoadError::new(
                    CloudConfigBundleLoadErrorCode::Timeout,
                    /*status_code*/ None,
                    format!(
                        "timed out waiting for cloud config bundle after {}s",
                        self.timeout.as_secs()
                    ),
                )
            })?;

        let result = match fetch_result {
            Ok(result) => result,
            Err(err) => {
                emit_load_metric("startup", "error", /*bundle*/ None);
                return Err(err);
            }
        };

        match result.as_ref() {
            Some(bundle) => {
                tracing::info!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    config_fragments = bundle.config_toml.enterprise_managed.len(),
                    requirements_fragments = bundle.requirements_toml.enterprise_managed.len(),
                    "Cloud config bundle load completed"
                );
                emit_load_metric("startup", "success", Some(bundle));
            }
            None => {
                tracing::info!(
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "Cloud config bundle load completed (none)"
                );
                emit_load_metric("startup", "success", /*bundle*/ None);
            }
        }

        Ok(result)
    }

    async fn fetch(&self) -> Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError> {
        let Some(auth) = self.auth_manager.auth().await else {
            return Ok(None);
        };
        if !cloud_config_eligible_auth(&auth) {
            return Ok(None);
        }
        let (chatgpt_user_id, account_id) = auth_identity(&auth);

        match self
            .cache
            .load(chatgpt_user_id.as_deref(), account_id.as_deref())
            .await
        {
            Ok(signed_payload) => {
                if let Err(err) = validate_bundle(&signed_payload.bundle, &self.codex_home) {
                    tracing::warn!(
                        path = %self.cache.path().display(),
                        error = %err,
                        "Ignoring invalid cached cloud config bundle"
                    );
                    self.cache
                        .log_load_status(&crate::cache::CacheLoadStatus::CacheInvalidBundle);
                } else {
                    tracing::info!(
                        path = %self.cache.path().display(),
                        "Using cached cloud config bundle"
                    );
                    return Ok(optional_bundle(signed_payload.bundle));
                }
            }
            Err(cache_load_status) => {
                self.cache.log_load_status(&cache_load_status);
            }
        }

        self.fetch_with_retries(auth, "startup").await
    }

    async fn fetch_with_retries(
        &self,
        mut auth: CodexAuth,
        trigger: &'static str,
    ) -> Result<Option<CloudConfigBundle>, CloudConfigBundleLoadError> {
        let mut attempt = 1;
        let mut last_status_code: Option<u16> = None;
        let mut auth_recovery = self.auth_manager.unauthorized_recovery();

        while attempt <= CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS {
            let bundle = match self.fetcher.fetch_bundle(&auth).await {
                Ok(bundle) => {
                    emit_fetch_attempt_metric(
                        trigger, attempt, "success", /*status_code*/ None,
                    );
                    if let Err(err) = validate_bundle(&bundle, &self.codex_home) {
                        emit_fetch_final_metric(
                            trigger,
                            "error",
                            "invalid_bundle",
                            attempt,
                            /*status_code*/ None,
                            /*bundle*/ None,
                        );
                        return Err(err);
                    }
                    bundle
                }
                Err(FetchAttemptError::Retryable(status)) => {
                    let status_code = status.status_code();
                    last_status_code = status_code;
                    emit_fetch_attempt_metric(trigger, attempt, "error", status_code);
                    if attempt < CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS {
                        tracing::warn!(
                            status = ?status,
                            attempt,
                            max_attempts = CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS,
                            "Failed to fetch cloud config bundle; retrying"
                        );
                        sleep(backoff(attempt as u64)).await;
                    }
                    attempt += 1;
                    continue;
                }
                Err(FetchAttemptError::Unauthorized {
                    status_code,
                    message,
                }) => {
                    last_status_code = status_code;
                    emit_fetch_attempt_metric(trigger, attempt, "unauthorized", status_code);
                    if auth_recovery.has_next() {
                        tracing::warn!(
                            attempt,
                            max_attempts = CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS,
                            "Cloud config bundle request was unauthorized; attempting auth recovery"
                        );
                        match auth_recovery.next().await {
                            Ok(_) => {
                                let Some(refreshed_auth) = self.auth_manager.auth().await else {
                                    tracing::error!(
                                        "Auth recovery succeeded but no auth is available for cloud config bundle"
                                    );
                                    emit_fetch_final_metric(
                                        trigger,
                                        "error",
                                        "auth_recovery_missing_auth",
                                        attempt,
                                        status_code,
                                        /*bundle*/ None,
                                    );
                                    return Err(CloudConfigBundleLoadError::new(
                                        CloudConfigBundleLoadErrorCode::Auth,
                                        status_code,
                                        CLOUD_CONFIG_BUNDLE_AUTH_RECOVERY_FAILED_MESSAGE,
                                    ));
                                };
                                auth = refreshed_auth;
                                continue;
                            }
                            Err(RefreshTokenError::Permanent(failed)) => {
                                tracing::warn!(
                                    error = %failed,
                                    "Failed to recover from unauthorized cloud config bundle request"
                                );
                                emit_fetch_final_metric(
                                    trigger,
                                    "error",
                                    "auth_recovery_unrecoverable",
                                    attempt,
                                    status_code,
                                    /*bundle*/ None,
                                );
                                return Err(CloudConfigBundleLoadError::new(
                                    CloudConfigBundleLoadErrorCode::Auth,
                                    status_code,
                                    failed.message,
                                ));
                            }
                            Err(RefreshTokenError::Transient(recovery_err)) => {
                                if attempt < CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS {
                                    tracing::warn!(
                                        error = %recovery_err,
                                        attempt,
                                        max_attempts = CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS,
                                        "Failed to recover from unauthorized cloud config bundle request; retrying"
                                    );
                                    sleep(backoff(attempt as u64)).await;
                                }
                                attempt += 1;
                                continue;
                            }
                        }
                    }

                    tracing::warn!(
                        error = %message,
                        "Cloud config bundle request was unauthorized and no auth recovery is available"
                    );
                    emit_fetch_final_metric(
                        trigger,
                        "error",
                        "auth_recovery_unavailable",
                        attempt,
                        status_code,
                        /*bundle*/ None,
                    );
                    return Err(CloudConfigBundleLoadError::new(
                        CloudConfigBundleLoadErrorCode::Auth,
                        status_code,
                        CLOUD_CONFIG_BUNDLE_AUTH_RECOVERY_FAILED_MESSAGE,
                    ));
                }
            };

            let (chatgpt_user_id, account_id) = auth_identity(&auth);
            if let Err(err) = self
                .cache
                .save(chatgpt_user_id, account_id, bundle.clone())
                .await
            {
                tracing::warn!(
                    error = %err,
                    "Failed to write cloud config bundle cache"
                );
            }

            emit_fetch_final_metric(
                trigger,
                "success",
                "none",
                attempt,
                /*status_code*/ None,
                Some(&bundle),
            );
            return Ok(optional_bundle(bundle));
        }

        emit_fetch_final_metric(
            trigger,
            "error",
            "request_retry_exhausted",
            CLOUD_CONFIG_BUNDLE_MAX_ATTEMPTS,
            last_status_code,
            /*bundle*/ None,
        );
        tracing::error!(
            path = %self.cache.path().display(),
            "{CLOUD_CONFIG_BUNDLE_LOAD_FAILED_MESSAGE}"
        );
        Err(CloudConfigBundleLoadError::new(
            CloudConfigBundleLoadErrorCode::RequestFailed,
            last_status_code,
            CLOUD_CONFIG_BUNDLE_LOAD_FAILED_MESSAGE,
        ))
    }

    async fn refresh_cache_in_background(&self) {
        loop {
            sleep(CLOUD_CONFIG_BUNDLE_CACHE_REFRESH_INTERVAL).await;
            match timeout(self.timeout, self.refresh_cache()).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => {
                    tracing::error!(
                        "Timed out refreshing cloud config bundle cache from remote; keeping existing cache"
                    );
                    emit_load_metric("refresh", "error", /*bundle*/ None);
                }
            }
        }
    }

    async fn refresh_cache(&self) -> bool {
        let Some(auth) = self.auth_manager.auth().await else {
            return false;
        };
        if !cloud_config_eligible_auth(&auth) {
            return false;
        }

        match self.fetch_with_retries(auth, "refresh").await {
            Ok(bundle) => emit_load_metric("refresh", "success", bundle.as_ref()),
            Err(err) => {
                tracing::error!(
                    path = %self.cache.path().display(),
                    error = %err,
                    "Failed to refresh cloud config bundle cache from remote"
                );
                emit_load_metric("refresh", "error", /*bundle*/ None);
            }
        }
        true
    }
}

pub fn cloud_config_bundle_loader(
    auth_manager: Arc<AuthManager>,
    chatgpt_base_url: String,
    codex_home: PathBuf,
) -> CloudConfigBundleLoader {
    let service = CloudConfigBundleService::new(
        auth_manager,
        Arc::new(BackendBundleFetcher::new(chatgpt_base_url)),
        codex_home,
        CLOUD_CONFIG_BUNDLE_TIMEOUT,
    );
    let refresh_service = service.clone();
    let task = tokio::spawn(async move { service.fetch_with_timeout().await });
    let refresh_task =
        tokio::spawn(async move { refresh_service.refresh_cache_in_background().await });
    let mut refresher_guard = refresher_task_slot().lock().unwrap_or_else(|err| {
        tracing::warn!("cloud config bundle refresher task slot was poisoned");
        err.into_inner()
    });
    if let Some(existing_task) = refresher_guard.replace(refresh_task) {
        existing_task.abort();
    }
    CloudConfigBundleLoader::new(async move {
        task.await.map_err(|err| {
            tracing::error!(error = %err, "Cloud config bundle task failed");
            CloudConfigBundleLoadError::new(
                CloudConfigBundleLoadErrorCode::Internal,
                /*status_code*/ None,
                format!("cloud config bundle load failed: {err}"),
            )
        })?
    })
}

pub async fn cloud_config_bundle_loader_for_storage(
    codex_home: PathBuf,
    enable_codex_api_key_env: bool,
    credentials_store_mode: AuthCredentialsStoreMode,
    chatgpt_base_url: String,
) -> CloudConfigBundleLoader {
    let auth_manager = AuthManager::shared(
        codex_home.clone(),
        enable_codex_api_key_env,
        credentials_store_mode,
        Some(chatgpt_base_url.clone()),
    )
    .await;
    cloud_config_bundle_loader(auth_manager, chatgpt_base_url, codex_home)
}

fn bundle_from_response(response: ConfigBundleResponse) -> CloudConfigBundle {
    let config_toml = response
        .config_toml
        .flatten()
        .map(|config_toml| *config_toml)
        .and_then(|config_toml| config_toml.enterprise_managed.flatten())
        .unwrap_or_default()
        .into_iter()
        .map(config_fragment_from_delivered)
        .collect();
    let requirements_toml = response
        .requirements_toml
        .flatten()
        .map(|requirements_toml| *requirements_toml)
        .and_then(|requirements_toml| requirements_toml.enterprise_managed.flatten())
        .unwrap_or_default()
        .into_iter()
        .map(requirements_fragment_from_delivered)
        .collect();

    CloudConfigBundle {
        config_toml: CloudConfigTomlBundle {
            enterprise_managed: config_toml,
        },
        requirements_toml: CloudRequirementsTomlBundle {
            enterprise_managed: requirements_toml,
        },
    }
}

fn config_fragment_from_delivered(fragment: DeliveredTomlFragment) -> CloudConfigFragment {
    CloudConfigFragment {
        id: fragment.id,
        name: fragment.name,
        contents: fragment.contents,
    }
}

fn requirements_fragment_from_delivered(
    fragment: DeliveredTomlFragment,
) -> CloudRequirementsFragment {
    CloudRequirementsFragment {
        id: fragment.id,
        name: fragment.name,
        contents: fragment.contents,
    }
}

fn emit_fetch_attempt_metric(
    trigger: &str,
    attempt: usize,
    outcome: &str,
    status_code: Option<u16>,
) {
    let attempt_tag = attempt.to_string();
    let status_code_tag = status_code_tag(status_code);
    emit_metric(
        CLOUD_CONFIG_BUNDLE_FETCH_ATTEMPT_METRIC,
        vec![
            ("trigger", trigger.to_string()),
            ("attempt", attempt_tag),
            ("outcome", outcome.to_string()),
            ("status_code", status_code_tag),
        ],
    );
}

fn emit_fetch_final_metric(
    trigger: &str,
    outcome: &str,
    reason: &str,
    attempt_count: usize,
    status_code: Option<u16>,
    bundle: Option<&CloudConfigBundle>,
) {
    let attempt_count_tag = attempt_count.to_string();
    let status_code_tag = status_code_tag(status_code);
    emit_metric(
        CLOUD_CONFIG_BUNDLE_FETCH_FINAL_METRIC,
        vec![
            ("trigger", trigger.to_string()),
            ("outcome", outcome.to_string()),
            ("reason", reason.to_string()),
            ("attempt_count", attempt_count_tag),
            ("status_code", status_code_tag),
            ("bundle_shape", bundle_shape_tag(bundle)),
        ],
    );
}

fn emit_load_metric(trigger: &str, outcome: &str, bundle: Option<&CloudConfigBundle>) {
    emit_metric(
        CLOUD_CONFIG_BUNDLE_LOAD_METRIC,
        vec![
            ("trigger", trigger.to_string()),
            ("outcome", outcome.to_string()),
            ("bundle_shape", bundle_shape_tag(bundle)),
        ],
    );
}

fn bundle_shape_tag(bundle: Option<&CloudConfigBundle>) -> String {
    let Some(bundle) = bundle else {
        return "none".to_string();
    };

    let mut sources = Vec::new();
    if !bundle.config_toml.enterprise_managed.is_empty() {
        sources.push("enterprise_config");
    }
    if !bundle.requirements_toml.enterprise_managed.is_empty() {
        sources.push("enterprise_requirements");
    }

    if sources.is_empty() {
        "empty".to_string()
    } else {
        sources.sort_unstable();
        sources.join(",")
    }
}

fn status_code_tag(status_code: Option<u16>) -> String {
    status_code
        .map(|status_code| status_code.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn emit_metric(metric_name: &str, tags: Vec<(&str, String)>) {
    if let Some(metrics) = codex_otel::global() {
        let tag_refs = tags
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();
        let _ = metrics.counter(metric_name, /*inc*/ 1, &tag_refs);
    }
}

#[cfg(test)]
#[path = "loader_tests.rs"]
mod tests;
