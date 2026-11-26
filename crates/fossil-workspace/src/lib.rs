//! Restricted repository preparation for the Phase 2C-A pilot.
//!
//! This crate has no Docker dependency. It accepts only an operator allowlist,
//! disables Git helpers and hooks, and routes HTTPS Git traffic through a
//! short-lived CONNECT proxy that permits one hostname and public IPs only.
//! The caller remains responsible for persisting the resolved commit and
//! workspace identity before checkout, and for delaying cleanup until the
//! durable supervisor lifecycle permits it.

use fossil_core::queue::{PublishMode, SoyuzRunRequest, TaskMode};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::process::Command;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

pub const PILOT_REPOSITORY_URL: &str = "https://github.com/CyanAutomation/somatic";

const GIT_OPERATION_TIMEOUT: Duration = Duration::from_secs(180);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PROXY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROXY_HEADER_BYTES: usize = 8 * 1024;
const MAX_PROXY_CONNECTIONS: usize = 16;
const MAX_GIT_STDOUT_BYTES: usize = 64 * 1024;
const MAX_GIT_TREE_BYTES: usize = 16 * 1024 * 1024;
const MAX_WORKSPACE_FILES: usize = 10_000;
const MAX_WORKSPACE_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_WORKSPACE_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_GIT_TRANSFER_BYTES_PER_DIRECTION: usize = 128 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("workspace root must be an existing private absolute directory")]
    InvalidWorkspaceRoot,
    #[error("workspace root permissions expose repository contents")]
    UnsafeWorkspaceRootPermissions,
    #[error("repository is not in the trusted allowlist")]
    RepositoryNotApproved,
    #[error("task mode is not supported by repository preparation")]
    TaskModeNotAllowed,
    #[error("repository preparation requires publishMode none")]
    PublishModeNotAllowed,
    #[error("repository preparation requires a task prompt")]
    MissingTaskPrompt,
    #[error("Git reference is invalid or unsupported")]
    InvalidGitReference,
    #[error("resolved Git commit is invalid")]
    InvalidCommit,
    #[error("workspace identity is invalid")]
    InvalidWorkspaceIdentity,
    #[error("workspace identity already exists with a different checkout")]
    WorkspaceIdentityConflict,
    #[error("workspace cleanup target is not a safe direct child")]
    UnsafeCleanupTarget,
    #[error("repository tree contains symlinks, submodules, or unsafe paths")]
    UnsafeRepositoryTree,
    #[error("repository exceeds file-count, file-size, or checkout-size limits")]
    RepositoryTooLarge,
    #[error("authorized Git reference changed before checkout")]
    ReferenceChanged,
    #[error("restricted Git transport refused the destination")]
    GitEgressDenied,
    #[error("Git operation failed")]
    GitFailed,
    #[error("Git operation exceeded its time limit")]
    GitTimedOut,
    #[error("Git output exceeded its bound")]
    GitOutputTooLarge,
    #[error("Git output was not valid UTF-8")]
    InvalidGitOutput,
    #[error("workspace filesystem operation failed")]
    Io(#[from] io::Error),
}

/// An opaque, path-safe identifier persisted with the durable launch row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkspaceIdentity(String);

impl WorkspaceIdentity {
    pub fn new() -> Self {
        Self(format!("wsp_{}", Uuid::new_v4()))
    }

    pub fn parse(value: &str) -> Result<Self, WorkspaceError> {
        let Some(uuid) = value.strip_prefix("wsp_") else {
            return Err(WorkspaceError::InvalidWorkspaceIdentity);
        };
        let parsed = Uuid::parse_str(uuid).map_err(|_| WorkspaceError::InvalidWorkspaceIdentity)?;
        if parsed.to_string() != uuid {
            return Err(WorkspaceError::InvalidWorkspaceIdentity);
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkspaceIdentity {
    fn default() -> Self {
        Self::new()
    }
}

/// A request authorized by the trusted repository allowlist and Soyuz fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedRepository {
    repository_url: Url,
    authorized_git_ref: String,
    fetch_ref: String,
}

impl AuthorizedRepository {
    pub fn repository_url(&self) -> &str {
        self.repository_url.as_str()
    }

    pub fn authorized_git_ref(&self) -> &str {
        &self.authorized_git_ref
    }

    pub fn fetch_ref(&self) -> &str {
        &self.fetch_ref
    }
}

/// The exact repository, canonical branch ref, and commit resolved from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRepository {
    approved: AuthorizedRepository,
    resolved_commit: String,
}

impl ResolvedRepository {
    pub fn repository_url(&self) -> &str {
        self.approved.repository_url()
    }

    pub fn authorized_git_ref(&self) -> &str {
        self.approved.authorized_git_ref()
    }

    pub fn resolved_commit(&self) -> &str {
        &self.resolved_commit
    }
}

/// A successful checkout. `path` is a trusted host-side value and must never
/// be serialized into Docker labels, task input, or public diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedWorkspace {
    identity: WorkspaceIdentity,
    path: PathBuf,
    repository_url: String,
    authorized_git_ref: String,
    resolved_commit: String,
}

impl PreparedWorkspace {
    pub fn identity(&self) -> &WorkspaceIdentity {
        &self.identity
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn repository_url(&self) -> &str {
        &self.repository_url
    }

    pub fn authorized_git_ref(&self) -> &str {
        &self.authorized_git_ref
    }

    pub fn resolved_commit(&self) -> &str {
        &self.resolved_commit
    }
}

/// Trusted repository allowlist and isolated workspace root.
#[derive(Debug, Clone)]
pub struct RepositoryPolicy {
    workspace_root: PathBuf,
    allowed_repositories: HashSet<String>,
}

impl RepositoryPolicy {
    /// Create the initial single-repository pilot policy.
    pub fn pilot(workspace_root: impl AsRef<Path>) -> Result<Self, WorkspaceError> {
        Self::new(workspace_root, [PILOT_REPOSITORY_URL])
    }

    /// Create a policy from operator-owned exact HTTPS repository URLs.
    pub fn new<I, S>(
        workspace_root: impl AsRef<Path>,
        allowed_repositories: I,
    ) -> Result<Self, WorkspaceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let requested_root = workspace_root.as_ref();
        if !requested_root.is_absolute()
            || fs::symlink_metadata(requested_root)
                .map(|metadata| metadata.file_type().is_symlink() || !metadata.is_dir())
                .unwrap_or(true)
        {
            return Err(WorkspaceError::InvalidWorkspaceRoot);
        }
        let workspace_root =
            fs::canonicalize(requested_root).map_err(|_| WorkspaceError::InvalidWorkspaceRoot)?;
        if workspace_root.to_string_lossy().contains(':')
            || path_exists_no_follow(&workspace_root.join(".git"))?
        {
            return Err(WorkspaceError::InvalidWorkspaceRoot);
        }
        ensure_private_directory(&workspace_root)?;

        let mut allowed = HashSet::new();
        for repository in allowed_repositories {
            let repository = repository.as_ref();
            validate_repository_url(repository)?;
            allowed.insert(repository.to_owned());
        }
        if allowed.is_empty() {
            return Err(WorkspaceError::RepositoryNotApproved);
        }
        Ok(Self {
            workspace_root,
            allowed_repositories: allowed,
        })
    }

    /// Validate the canonical request before performing any Git network call.
    /// The task prompt is checked for presence only; it is never copied here.
    pub fn authorize_request(
        &self,
        request: &SoyuzRunRequest,
    ) -> Result<AuthorizedRepository, WorkspaceError> {
        if request.task_mode != TaskMode::Patch {
            return Err(WorkspaceError::TaskModeNotAllowed);
        }
        if request.publish_mode != PublishMode::None {
            return Err(WorkspaceError::PublishModeNotAllowed);
        }
        if request
            .task_prompt
            .as_deref()
            .is_none_or(|prompt| prompt.trim().is_empty())
        {
            return Err(WorkspaceError::MissingTaskPrompt);
        }
        self.authorize_repository(request.repo_url.as_str(), &request.git_ref)
    }

    /// Reconstruct a persisted repository identity during supervisor recovery.
    /// `prepare_checkout` still re-fetches the exact ref and fails if its commit
    /// no longer matches the persisted commit.
    pub fn restore_for_request(
        &self,
        request: &SoyuzRunRequest,
        repository_url: &str,
        authorized_git_ref: &str,
        resolved_commit: &str,
    ) -> Result<ResolvedRepository, WorkspaceError> {
        let approved = self.authorize_request(request)?;
        if approved.repository_url() != repository_url
            || approved.authorized_git_ref() != authorized_git_ref
        {
            return Err(WorkspaceError::RepositoryNotApproved);
        }
        validate_commit(resolved_commit)?;
        Ok(ResolvedRepository {
            approved,
            resolved_commit: resolved_commit.to_owned(),
        })
    }

    /// Resolve a Soyuz-authorized branch through a short-lived, restricted
    /// CONNECT proxy. Only the repository's exact hostname can be dialed.
    pub async fn resolve_ref(
        &self,
        approved: AuthorizedRepository,
    ) -> Result<ResolvedRepository, WorkspaceError> {
        let verified =
            self.authorize_repository(approved.repository_url(), approved.authorized_git_ref())?;
        if verified != approved {
            return Err(WorkspaceError::RepositoryNotApproved);
        }
        let host = approved
            .repository_url
            .host_str()
            .ok_or(WorkspaceError::RepositoryNotApproved)?
            .to_ascii_lowercase();
        let proxy = GitConnectProxy::start(host).await?;
        let args = [
            "ls-remote".to_owned(),
            "--refs".to_owned(),
            "--exit-code".to_owned(),
            approved.repository_url().to_owned(),
            approved.fetch_ref.clone(),
        ];
        let output = self
            .run_git_output(
                &self.workspace_root,
                &args,
                Some(proxy.address),
                MAX_GIT_STDOUT_BYTES,
            )
            .await?;
        drop(proxy);
        let commit = parse_ls_remote_result(&output, &approved.fetch_ref)?;
        Ok(ResolvedRepository {
            approved,
            resolved_commit: commit,
        })
    }

    /// Prepare a verified per-run checkout. The exact persisted commit is
    /// checked again after fetch and before checkout. Existing clean checkouts
    /// with the same identity are reused; mismatched or dirty identities fail
    /// closed.
    pub async fn prepare_checkout(
        &self,
        resolved: &ResolvedRepository,
        identity: WorkspaceIdentity,
    ) -> Result<PreparedWorkspace, WorkspaceError> {
        let verified =
            self.authorize_repository(resolved.repository_url(), resolved.authorized_git_ref())?;
        if verified != resolved.approved {
            return Err(WorkspaceError::RepositoryNotApproved);
        }
        validate_commit(&resolved.resolved_commit)?;
        let final_path = self.workspace_path(&identity);
        if path_exists_no_follow(&final_path)? {
            self.verify_existing_checkout(&final_path, resolved).await?;
            return Ok(self.prepared_workspace(identity, final_path, resolved));
        }

        let staging_path = self
            .workspace_root
            .join(format!(".{}.preparing", identity.as_str()));
        if path_exists_no_follow(&staging_path)? {
            return Err(WorkspaceError::WorkspaceIdentityConflict);
        }
        fs::create_dir(&staging_path)?;
        set_private_permissions(&staging_path)?;

        let result = self.prepare_staging(&staging_path, resolved).await;
        if let Err(error) = result {
            let _ = remove_direct_child(&self.workspace_root, &staging_path);
            return Err(error);
        }

        if path_exists_no_follow(&final_path)? {
            let _ = remove_direct_child(&self.workspace_root, &staging_path);
            return Err(WorkspaceError::WorkspaceIdentityConflict);
        }
        fs::rename(&staging_path, &final_path)
            .map_err(|_| WorkspaceError::WorkspaceIdentityConflict)?;
        Ok(self.prepared_workspace(identity, final_path, resolved))
    }

    /// Compute the only host path permitted for a persisted workspace identity.
    pub fn workspace_path(&self, identity: &WorkspaceIdentity) -> PathBuf {
        self.workspace_root.join(identity.as_str())
    }

    /// Remove only the exact per-run directory below the configured root.
    /// Callers must first persist terminal state and confirm the matching
    /// container is gone.
    pub fn cleanup(&self, identity: &WorkspaceIdentity) -> Result<(), WorkspaceError> {
        let path = self.workspace_path(identity);
        if !path_exists_no_follow(&path)? {
            return Ok(());
        }
        remove_direct_child(&self.workspace_root, &path)
    }

    fn authorize_repository(
        &self,
        repository_url: &str,
        git_ref: &str,
    ) -> Result<AuthorizedRepository, WorkspaceError> {
        validate_repository_url(repository_url)?;
        if !self.allowed_repositories.contains(repository_url) {
            return Err(WorkspaceError::RepositoryNotApproved);
        }
        let fetch_ref = normalize_branch_ref(git_ref)?;
        Ok(AuthorizedRepository {
            repository_url: Url::parse(repository_url)
                .map_err(|_| WorkspaceError::RepositoryNotApproved)?,
            authorized_git_ref: git_ref.to_owned(),
            fetch_ref,
        })
    }

    async fn prepare_staging(
        &self,
        staging_path: &Path,
        resolved: &ResolvedRepository,
    ) -> Result<(), WorkspaceError> {
        let mut init_args = vec!["init", "--template=/dev/null"];
        if resolved.resolved_commit.len() == 64 {
            init_args.push("--object-format=sha256");
        }
        self.run_git_quiet(staging_path, &init_args, None).await?;
        self.run_git_quiet(
            staging_path,
            &[
                "remote",
                "add",
                "origin",
                resolved.approved.repository_url(),
            ],
            None,
        )
        .await?;

        let host = resolved
            .approved
            .repository_url
            .host_str()
            .ok_or(WorkspaceError::RepositoryNotApproved)?
            .to_ascii_lowercase();
        let proxy = GitConnectProxy::start(host).await?;
        self.run_git_quiet(
            staging_path,
            &[
                "fetch",
                "--quiet",
                "--depth=1",
                "--no-tags",
                "origin",
                &resolved.approved.fetch_ref,
            ],
            Some(proxy.address),
        )
        .await?;
        drop(proxy);

        let fetched_commit = self
            .git_text(
                staging_path,
                &["rev-parse", "--verify", "FETCH_HEAD^{commit}"],
                None,
                128,
            )
            .await?;
        if fetched_commit != resolved.resolved_commit {
            return Err(WorkspaceError::ReferenceChanged);
        }

        let tree = self
            .run_git_output(
                staging_path,
                &[
                    "ls-tree".to_owned(),
                    "-r".to_owned(),
                    "-l".to_owned(),
                    "-z".to_owned(),
                    resolved.resolved_commit.clone(),
                ],
                None,
                MAX_GIT_TREE_BYTES,
            )
            .await?;
        validate_repository_tree(&tree)?;

        self.run_git_quiet(
            staging_path,
            &[
                "checkout",
                "--quiet",
                "--detach",
                "--no-recurse-submodules",
                &resolved.resolved_commit,
            ],
            None,
        )
        .await?;

        let head = self
            .git_text(staging_path, &["rev-parse", "--verify", "HEAD"], None, 128)
            .await?;
        if head != resolved.resolved_commit {
            return Err(WorkspaceError::ReferenceChanged);
        }
        let status = self
            .run_git_output(
                staging_path,
                &[
                    "status".to_owned(),
                    "--porcelain=v1".to_owned(),
                    "--untracked-files=all".to_owned(),
                ],
                None,
                MAX_GIT_STDOUT_BYTES,
            )
            .await?;
        if !status.is_empty() {
            return Err(WorkspaceError::GitFailed);
        }
        Ok(())
    }

    async fn verify_existing_checkout(
        &self,
        path: &Path,
        resolved: &ResolvedRepository,
    ) -> Result<(), WorkspaceError> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(WorkspaceError::UnsafeCleanupTarget);
        }
        if path.parent() != Some(self.workspace_root.as_path())
            || fs::canonicalize(path.parent().ok_or(WorkspaceError::UnsafeCleanupTarget)?)?
                != self.workspace_root
        {
            return Err(WorkspaceError::UnsafeCleanupTarget);
        }
        let remote = self
            .git_text(
                path,
                &["config", "--local", "--get", "remote.origin.url"],
                None,
                4096,
            )
            .await?;
        let head = self
            .git_text(path, &["rev-parse", "--verify", "HEAD"], None, 128)
            .await?;
        let status = self
            .run_git_output(
                path,
                &[
                    "status".to_owned(),
                    "--porcelain=v1".to_owned(),
                    "--untracked-files=all".to_owned(),
                ],
                None,
                MAX_GIT_STDOUT_BYTES,
            )
            .await?;
        if remote != resolved.approved.repository_url()
            || head != resolved.resolved_commit
            || !status.is_empty()
        {
            return Err(WorkspaceError::WorkspaceIdentityConflict);
        }
        Ok(())
    }

    fn prepared_workspace(
        &self,
        identity: WorkspaceIdentity,
        path: PathBuf,
        resolved: &ResolvedRepository,
    ) -> PreparedWorkspace {
        PreparedWorkspace {
            identity,
            path,
            repository_url: resolved.approved.repository_url().to_owned(),
            authorized_git_ref: resolved.approved.authorized_git_ref.clone(),
            resolved_commit: resolved.resolved_commit.clone(),
        }
    }

    async fn git_text(
        &self,
        cwd: &Path,
        args: &[&str],
        proxy: Option<SocketAddr>,
        max_bytes: usize,
    ) -> Result<String, WorkspaceError> {
        let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
        let output = self.run_git_output(cwd, &args, proxy, max_bytes).await?;
        let text = std::str::from_utf8(&output).map_err(|_| WorkspaceError::InvalidGitOutput)?;
        Ok(text.trim().to_owned())
    }

    async fn run_git_quiet(
        &self,
        cwd: &Path,
        args: &[&str],
        proxy: Option<SocketAddr>,
    ) -> Result<(), WorkspaceError> {
        let mut command = self.git_command(cwd, proxy);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| WorkspaceError::GitFailed)?;
        let status = timeout(GIT_OPERATION_TIMEOUT, child.wait())
            .await
            .map_err(|_| WorkspaceError::GitTimedOut)?
            .map_err(|_| WorkspaceError::GitFailed)?;
        if status.success() {
            Ok(())
        } else {
            Err(WorkspaceError::GitFailed)
        }
    }

    async fn run_git_output(
        &self,
        cwd: &Path,
        args: &[String],
        proxy: Option<SocketAddr>,
        max_stdout_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceError> {
        let mut command = self.git_command(cwd, proxy);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| WorkspaceError::GitFailed)?;
        let output = timeout(GIT_OPERATION_TIMEOUT, async {
            let mut stdout = child.stdout.take().ok_or(WorkspaceError::GitFailed)?;
            let mut bytes = Vec::with_capacity(max_stdout_bytes.min(8192));
            let mut chunk = [0_u8; 8192];
            loop {
                let count = stdout
                    .read(&mut chunk)
                    .await
                    .map_err(|_| WorkspaceError::GitFailed)?;
                if count == 0 {
                    break;
                }
                if bytes.len().saturating_add(count) > max_stdout_bytes {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return Err(WorkspaceError::GitOutputTooLarge);
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            let status = child.wait().await.map_err(|_| WorkspaceError::GitFailed)?;
            if !status.success() {
                return Err(WorkspaceError::GitFailed);
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| WorkspaceError::GitTimedOut)??;
        Ok(output)
    }

    fn git_command(&self, cwd: &Path, proxy: Option<SocketAddr>) -> Command {
        let mut command = Command::new("git");
        command
            .current_dir(cwd)
            .env_clear()
            .env("PATH", "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin")
            .env("HOME", &self.workspace_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_CEILING_DIRECTORIES", &self.workspace_root)
            .env("GIT_ATTR_NOSYSTEM", "1")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_LFS_SKIP_SMUDGE", "1")
            .env("GIT_ALLOW_PROTOCOL", "https")
            .arg("-c")
            .arg("core.hooksPath=/dev/null")
            .arg("-c")
            .arg("credential.helper=")
            .arg("-c")
            .arg("http.followRedirects=false")
            .arg("-c")
            .arg("fetch.recurseSubmodules=false")
            .arg("-c")
            .arg("submodule.recurse=false")
            .arg("-c")
            .arg("protocol.file.allow=never")
            .arg("-c")
            .arg("protocol.ext.allow=never")
            .arg("-c")
            .arg("protocol.ssh.allow=never")
            .arg("-c")
            .arg("protocol.https.allow=always");
        if let Some(proxy) = proxy {
            command.arg("-c").arg(format!("http.proxy=http://{proxy}"));
        }
        command
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), WorkspaceError> {
    let metadata = fs::metadata(path).map_err(|_| WorkspaceError::InvalidWorkspaceRoot)?;
    if !metadata.is_dir() {
        return Err(WorkspaceError::InvalidWorkspaceRoot);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(WorkspaceError::UnsafeWorkspaceRootPermissions);
        }
    }
    Ok(())
}

fn set_private_permissions(path: &Path) -> Result<(), WorkspaceError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err(WorkspaceError::InvalidWorkspaceRoot);
    }
    Ok(())
}

fn path_exists_no_follow(path: &Path) -> Result<bool, WorkspaceError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(WorkspaceError::Io(error)),
    }
}

fn remove_direct_child(root: &Path, path: &Path) -> Result<(), WorkspaceError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || path.parent() != Some(root)
        || fs::canonicalize(path.parent().ok_or(WorkspaceError::UnsafeCleanupTarget)?)? != root
    {
        return Err(WorkspaceError::UnsafeCleanupTarget);
    }
    fs::remove_dir_all(path)?;
    Ok(())
}

fn validate_repository_url(value: &str) -> Result<Url, WorkspaceError> {
    if value.is_empty() || value.len() > 2048 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(WorkspaceError::RepositoryNotApproved);
    }
    let url = Url::parse(value).map_err(|_| WorkspaceError::RepositoryNotApproved)?;
    let Some(host) = url.host_str() else {
        return Err(WorkspaceError::RepositoryNotApproved);
    };
    let hostname = host.to_ascii_lowercase();
    let ip_literal = host.parse::<IpAddr>().is_ok()
        || host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .is_some_and(|host| host.parse::<Ipv6Addr>().is_ok());
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(443)
        || ip_literal
        || hostname == "localhost"
        || hostname.ends_with(".localhost")
        || hostname.ends_with(".local")
        || url.as_str() != value
    {
        return Err(WorkspaceError::RepositoryNotApproved);
    }
    Ok(url)
}

fn normalize_branch_ref(value: &str) -> Result<String, WorkspaceError> {
    if value.is_empty() || value.len() > 512 || value.starts_with('-') {
        return Err(WorkspaceError::InvalidGitReference);
    }
    let full_ref = if value.starts_with("refs/heads/") {
        value.to_owned()
    } else if value.starts_with("refs/") {
        return Err(WorkspaceError::InvalidGitReference);
    } else {
        format!("refs/heads/{value}")
    };
    let name = full_ref
        .strip_prefix("refs/heads/")
        .ok_or(WorkspaceError::InvalidGitReference)?;
    if name.is_empty()
        || name.ends_with('/')
        || name.ends_with('.')
        || name.contains("//")
        || name.contains("..")
        || name.contains("@{")
        || name.bytes().any(|byte| {
            byte <= b' '
                || byte == 0x7f
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
        || name.split('/').any(|part| {
            part.is_empty() || part.starts_with('.') || part.to_ascii_lowercase().ends_with(".lock")
        })
        || name == "@"
    {
        return Err(WorkspaceError::InvalidGitReference);
    }
    Ok(full_ref)
}

fn validate_commit(value: &str) -> Result<(), WorkspaceError> {
    if !matches!(value.len(), 40 | 64)
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
        || value.bytes().all(|byte| byte == b'0')
    {
        return Err(WorkspaceError::InvalidCommit);
    }
    Ok(())
}

fn parse_ls_remote_result(bytes: &[u8], expected_ref: &str) -> Result<String, WorkspaceError> {
    if bytes.len() > MAX_GIT_STDOUT_BYTES {
        return Err(WorkspaceError::GitOutputTooLarge);
    }
    let text = std::str::from_utf8(bytes).map_err(|_| WorkspaceError::InvalidGitOutput)?;
    let mut matches = text.lines().filter_map(|line| {
        let (commit, reference) = line.trim_end_matches('\r').split_once('\t')?;
        (reference == expected_ref).then_some(commit)
    });
    let Some(commit) = matches.next() else {
        return Err(WorkspaceError::GitFailed);
    };
    if matches.next().is_some() {
        return Err(WorkspaceError::GitFailed);
    }
    validate_commit(commit)?;
    Ok(commit.to_owned())
}

fn validate_repository_tree(bytes: &[u8]) -> Result<(), WorkspaceError> {
    if bytes.len() > MAX_GIT_TREE_BYTES {
        return Err(WorkspaceError::RepositoryTooLarge);
    }
    let mut file_count = 0_usize;
    let mut total_bytes = 0_u64;
    for entry in bytes
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        file_count += 1;
        if file_count > MAX_WORKSPACE_FILES {
            return Err(WorkspaceError::RepositoryTooLarge);
        }
        let Some(tab) = entry.iter().position(|byte| *byte == b'\t') else {
            return Err(WorkspaceError::UnsafeRepositoryTree);
        };
        let (header, path_with_tab) = entry.split_at(tab);
        let path = &path_with_tab[1..];
        if path.is_empty() || path.starts_with(b"/") || path.contains(&b'\\') {
            return Err(WorkspaceError::UnsafeRepositoryTree);
        }
        if path
            .split(|byte| *byte == b'/')
            .any(|component| component.is_empty() || component == b"." || component == b"..")
        {
            return Err(WorkspaceError::UnsafeRepositoryTree);
        }
        std::str::from_utf8(path).map_err(|_| WorkspaceError::UnsafeRepositoryTree)?;
        let header =
            std::str::from_utf8(header).map_err(|_| WorkspaceError::UnsafeRepositoryTree)?;
        let mut fields = header.split_ascii_whitespace();
        let mode = fields.next().ok_or(WorkspaceError::UnsafeRepositoryTree)?;
        let kind = fields.next().ok_or(WorkspaceError::UnsafeRepositoryTree)?;
        let object_id = fields.next().ok_or(WorkspaceError::UnsafeRepositoryTree)?;
        let size = fields.next().ok_or(WorkspaceError::UnsafeRepositoryTree)?;
        if fields.next().is_some()
            || !matches!(mode, "100644" | "100755")
            || kind != "blob"
            || !matches!(object_id.len(), 40 | 64)
            || object_id.bytes().any(|byte| !byte.is_ascii_hexdigit())
        {
            return Err(WorkspaceError::UnsafeRepositoryTree);
        }
        let size = size
            .parse::<u64>()
            .map_err(|_| WorkspaceError::UnsafeRepositoryTree)?;
        if size > MAX_WORKSPACE_FILE_BYTES {
            return Err(WorkspaceError::RepositoryTooLarge);
        }
        total_bytes = total_bytes
            .checked_add(size)
            .ok_or(WorkspaceError::RepositoryTooLarge)?;
        if total_bytes > MAX_WORKSPACE_TOTAL_BYTES {
            return Err(WorkspaceError::RepositoryTooLarge);
        }
    }
    Ok(())
}

/// Return true only for globally routable IP addresses. Private, loopback,
/// link-local, shared, documentation, transition, and metadata ranges fail.
pub fn is_public_destination(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !address.is_private()
        && !address.is_loopback()
        && !address.is_link_local()
        && !address.is_unspecified()
        && !address.is_multicast()
        && !address.is_broadcast()
        && !(a == 0)
        && !(a == 100 && (64..=127).contains(&b))
        && !(a == 192 && b == 0 && c == 0)
        && !(a == 192 && b == 0 && c == 2)
        && !(a == 192 && b == 88 && c == 99)
        && !(a == 198 && (18..=19).contains(&b))
        && !(a == 198 && b == 51 && c == 100)
        && !(a == 203 && b == 0 && c == 113)
        && a < 240
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let octets = address.octets();
    let global_unicast = octets[0] & 0xe0 == 0x20;
    let documentation = octets[0..4] == [0x20, 0x01, 0x0d, 0xb8];
    let documentation_3fff = octets[0] == 0x3f && octets[1] == 0xff && octets[2] & 0xf0 == 0;
    let special_2001 = octets[0] == 0x20 && octets[1] == 0x01 && octets[2] & 0xfe == 0;
    let six_to_four = octets[0] == 0x20 && octets[1] == 0x02;
    global_unicast
        && !address.is_loopback()
        && !address.is_unspecified()
        && !address.is_multicast()
        && !address.is_unicast_link_local()
        && !address.is_unique_local()
        && !documentation
        && !documentation_3fff
        && !special_2001
        && !six_to_four
}

struct GitConnectProxy {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl GitConnectProxy {
    async fn start(allowed_host: String) -> Result<Self, WorkspaceError> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        match incoming {
                            Ok((stream, _)) if handlers.len() < MAX_PROXY_CONNECTIONS => {
                                let host = allowed_host.clone();
                                handlers.spawn(async move { handle_proxy_client(stream, &host).await; });
                            }
                            Ok((mut stream, _)) => {
                                let _ = respond_proxy_error(&mut stream, 503, "Service Unavailable").await;
                            }
                            Err(_) => break,
                        }
                    }
                    joined = handlers.join_next(), if !handlers.is_empty() => {
                        let _ = joined;
                    }
                }
            }
        });
        Ok(Self { address, task })
    }
}

impl Drop for GitConnectProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_proxy_client(mut stream: TcpStream, allowed_host: &str) {
    let request = timeout(
        PROXY_REQUEST_TIMEOUT,
        read_proxy_connect_request(&mut stream),
    )
    .await;
    let Ok(Ok(authority)) = request else {
        let _ = respond_proxy_error(&mut stream, 400, "Bad Request").await;
        return;
    };
    if !allowed_connect_authority(&authority, allowed_host) {
        let _ = respond_proxy_error(&mut stream, 403, "Forbidden").await;
        return;
    }
    let addresses = match timeout(CONNECT_TIMEOUT, lookup_host((allowed_host, 443))).await {
        Ok(Ok(addresses)) => public_git_destinations(addresses),
        _ => Vec::new(),
    };
    if addresses.is_empty() {
        let _ = respond_proxy_error(&mut stream, 403, "Forbidden").await;
        return;
    }
    let mut upstream = None;
    for address in addresses {
        if let Ok(Ok(connection)) = timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await {
            upstream = Some(connection);
            break;
        }
    }
    let Some(upstream) = upstream else {
        let _ = respond_proxy_error(&mut stream, 502, "Bad Gateway").await;
        return;
    };
    if stream
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await
        .is_err()
    {
        return;
    }
    let (mut client_read, mut client_write) = stream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let _ = tokio::try_join!(
        copy_with_limit(&mut client_read, &mut upstream_write),
        copy_with_limit(&mut upstream_read, &mut client_write),
    );
}

async fn read_proxy_connect_request(stream: &mut TcpStream) -> io::Result<String> {
    let mut headers = Vec::with_capacity(1024);
    let mut byte = [0_u8; 1];
    loop {
        if headers.len() == MAX_PROXY_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "headers too large",
            ));
        }
        if stream.read(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete CONNECT headers",
            ));
        }
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") || headers.ends_with(b"\n\n") {
            break;
        }
    }
    parse_proxy_connect_request(&headers)
}

fn parse_proxy_connect_request(headers: &[u8]) -> io::Result<String> {
    if headers.len() > MAX_PROXY_HEADER_BYTES
        || !(headers.ends_with(b"\r\n\r\n") || headers.ends_with(b"\n\n"))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CONNECT headers",
        ));
    }
    let text = std::str::from_utf8(headers)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid CONNECT headers"))?;
    let request_line = text
        .lines()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing CONNECT line"))?;
    let mut fields = request_line.split(' ');
    let method = fields.next();
    let authority = fields.next();
    let version = fields.next();
    if method != Some("CONNECT")
        || !matches!(version, Some("HTTP/1.0" | "HTTP/1.1"))
        || fields.next().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CONNECT",
        ));
    }
    authority
        .map(str::to_owned)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing authority"))
}

fn allowed_connect_authority(authority: &str, allowed_host: &str) -> bool {
    let expected = format!("{allowed_host}:443");
    authority.eq_ignore_ascii_case(&expected)
}

fn public_git_destinations(addresses: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    addresses
        .into_iter()
        .filter(|address| is_public_destination(address.ip()))
        .take(16)
        .collect()
}

async fn respond_proxy_error(stream: &mut TcpStream, status: u16, reason: &str) -> io::Result<()> {
    stream
        .write_all(
            format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                .as_bytes(),
        )
        .await
}

async fn copy_with_limit<R, W>(reader: &mut R, writer: &mut W) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut total = 0_usize;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            writer.shutdown().await?;
            return Ok(());
        }
        total = total.saturating_add(count);
        if total > MAX_GIT_TRANSFER_BYTES_PER_DIRECTION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "restricted Git transfer limit exceeded",
            ));
        }
        writer.write_all(&buffer[..count]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GitConnectProxy, PILOT_REPOSITORY_URL, RepositoryPolicy, WorkspaceError, WorkspaceIdentity,
        allowed_connect_authority, is_public_destination, parse_ls_remote_result,
        parse_proxy_connect_request, public_git_destinations, validate_repository_tree,
    };
    use fossil_core::queue::SoyuzRunRequest;
    use serde_json::json;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn request(repository_url: &str, git_ref: &str, publish_mode: &str) -> SoyuzRunRequest {
        serde_json::from_value(json!({
            "repoUrl": repository_url,
            "ref": git_ref,
            "taskPrompt": "Update the designated test function.",
            "taskMode": "patch",
            "publishMode": publish_mode
        }))
        .unwrap()
    }

    fn policy(root: &std::path::Path) -> RepositoryPolicy {
        let workspace_root = root.join("private-workspaces");
        std::fs::create_dir(&workspace_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&workspace_root, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        RepositoryPolicy::pilot(workspace_root).unwrap()
    }

    #[test]
    fn authorizes_only_the_pilot_repository_and_non_publishing_patch_requests() {
        let directory = tempdir().unwrap();
        let policy = policy(directory.path());
        let approved = policy
            .authorize_request(&request(PILOT_REPOSITORY_URL, "main", "none"))
            .unwrap();

        assert_eq!(approved.repository_url(), PILOT_REPOSITORY_URL);
        assert_eq!(approved.authorized_git_ref(), "main");
        assert_eq!(approved.fetch_ref(), "refs/heads/main");

        assert!(matches!(
            policy.authorize_request(&request(
                "https://github.com/another-owner/another-repository",
                "main",
                "none"
            )),
            Err(WorkspaceError::RepositoryNotApproved)
        ));
        assert!(matches!(
            policy.authorize_request(&request(PILOT_REPOSITORY_URL, "main", "branch")),
            Err(WorkspaceError::PublishModeNotAllowed)
        ));
    }

    #[test]
    fn rejects_inspect_and_missing_prompt_requests() {
        let directory = tempdir().unwrap();
        let policy = policy(directory.path());
        let inspect: SoyuzRunRequest = serde_json::from_value(json!({
            "repoUrl": PILOT_REPOSITORY_URL,
            "ref": "main",
            "taskPrompt": "Inspect this repository carefully.",
            "taskMode": "inspect",
            "publishMode": "none"
        }))
        .unwrap();
        assert!(matches!(
            policy.authorize_request(&inspect),
            Err(WorkspaceError::TaskModeNotAllowed)
        ));

        let startup_check: SoyuzRunRequest = serde_json::from_value(json!({
            "repoUrl": PILOT_REPOSITORY_URL,
            "ref": "main",
            "startupCheck": true,
            "taskMode": "patch",
            "publishMode": "none"
        }))
        .unwrap();
        assert!(matches!(
            policy.authorize_request(&startup_check),
            Err(WorkspaceError::MissingTaskPrompt)
        ));
    }

    #[test]
    fn rejects_credentialed_non_https_and_noncanonical_repository_urls() {
        let directory = tempdir().unwrap();
        let policy = policy(directory.path());
        for repository_url in [
            "http://github.com/CyanAutomation/somatic",
            "https://user:secret@github.com/CyanAutomation/somatic",
            "https://github.com/CyanAutomation/somatic?redirect=example.com",
            "https://github.com/CyanAutomation/somatic#fragment",
            "https://github.com/CyanAutomation/somatic/",
        ] {
            assert!(
                policy
                    .authorize_request(&request(repository_url, "main", "none"))
                    .is_err(),
                "unexpectedly accepted {repository_url}"
            );
        }
    }

    #[test]
    fn rejects_ip_literal_repositories_including_ipv6() {
        let root = tempdir().unwrap();
        let workspace_root = root.path().join("workspace");
        std::fs::create_dir(&workspace_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&workspace_root, std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }

        for repository_url in [
            "https://127.0.0.1/CyanAutomation/somatic",
            "https://[::1]/CyanAutomation/somatic",
            "https://[fe80::1]/CyanAutomation/somatic",
        ] {
            assert!(
                RepositoryPolicy::new(&workspace_root, [repository_url]).is_err(),
                "accepted IP-literal repository {repository_url}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_workspace_root_readable_by_other_users() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let workspace_root = directory.path().join("shared-workspaces");
        std::fs::create_dir(&workspace_root).unwrap();
        std::fs::set_permissions(&workspace_root, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(matches!(
            RepositoryPolicy::pilot(workspace_root),
            Err(WorkspaceError::UnsafeWorkspaceRootPermissions)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn refuses_a_workspace_root_that_is_inside_a_git_repository() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let workspace_root = directory.path().join("workspaces");
        std::fs::create_dir(&workspace_root).unwrap();
        std::fs::set_permissions(&workspace_root, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::create_dir(workspace_root.join(".git")).unwrap();

        assert!(matches!(
            RepositoryPolicy::pilot(workspace_root),
            Err(WorkspaceError::InvalidWorkspaceRoot)
        ));
    }

    #[test]
    fn rejects_refspec_injection_and_non_branch_refs() {
        let directory = tempdir().unwrap();
        let policy = policy(directory.path());
        for git_ref in [
            "-upload-pack=evil",
            "main:refs/heads/evil",
            "refs/tags/v1",
            "main..other",
            "main@{yesterday}",
            "feature//branch",
            "feature\\branch",
            "main.lock",
        ] {
            assert!(
                policy
                    .authorize_request(&request(PILOT_REPOSITORY_URL, git_ref, "none"))
                    .is_err(),
                "unexpectedly accepted ref {git_ref}"
            );
        }
    }

    #[test]
    fn accepts_only_a_single_exact_advertised_branch_commit() {
        let commit = "a".repeat(40);
        assert_eq!(
            parse_ls_remote_result(
                format!("{commit}\trefs/heads/main\n").as_bytes(),
                "refs/heads/main"
            )
            .unwrap(),
            commit
        );
        assert!(
            parse_ls_remote_result(
                format!("{commit}\trefs/heads/main-extra\n").as_bytes(),
                "refs/heads/main"
            )
            .is_err()
        );
        assert!(
            parse_ls_remote_result(
                format!("{commit}\trefs/heads/main\n{commit}\trefs/heads/main\n").as_bytes(),
                "refs/heads/main"
            )
            .is_err()
        );
    }

    #[test]
    fn recovery_requires_the_persisted_identity_to_match_the_canonical_request() {
        let directory = tempdir().unwrap();
        let policy = policy(directory.path());
        let request = request(PILOT_REPOSITORY_URL, "main", "none");
        let commit = "a".repeat(40);
        let restored = policy
            .restore_for_request(&request, PILOT_REPOSITORY_URL, "main", &commit)
            .unwrap();
        assert_eq!(restored.resolved_commit(), commit);
        assert!(matches!(
            policy.restore_for_request(
                &request,
                "https://github.com/another-owner/another-repository",
                "main",
                &commit
            ),
            Err(WorkspaceError::RepositoryNotApproved)
        ));
        assert!(matches!(
            policy.restore_for_request(&request, PILOT_REPOSITORY_URL, "release", &commit),
            Err(WorkspaceError::RepositoryNotApproved)
        ));
    }

    #[test]
    fn rejects_private_loopback_link_local_and_metadata_destinations() {
        for address in [
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "fc00::1".parse().unwrap(),
            "fe80::1".parse().unwrap(),
            "2001:db8::1".parse().unwrap(),
            "3fff::1".parse().unwrap(),
        ] {
            assert!(!is_public_destination(address), "accepted {address}");
        }
        assert!(is_public_destination(IpAddr::V4(Ipv4Addr::new(
            140, 82, 112, 4
        ))));
        assert!(is_public_destination(
            "2606:50c0:8000::153".parse().unwrap()
        ));
    }

    #[test]
    fn restricts_git_connect_proxy_to_one_https_host() {
        assert!(allowed_connect_authority("github.com:443", "github.com"));
        assert!(allowed_connect_authority("GITHUB.COM:443", "github.com"));
        for denied in [
            "github.com:80",
            "api.github.com:443",
            "127.0.0.1:443",
            "github.com.:443",
            "github.com:443:extra",
        ] {
            assert!(!allowed_connect_authority(denied, "github.com"));
        }
    }

    #[test]
    fn parses_bounded_connect_headers_and_rejects_malformed_lines() {
        assert_eq!(
            parse_proxy_connect_request(
                b"CONNECT github.com:443 HTTP/1.1\r\nHost: github.com:443\r\n\r\n"
            )
            .unwrap(),
            "github.com:443"
        );
        assert!(parse_proxy_connect_request(b"CONNECT  github.com:443 HTTP/1.1\r\n\r\n").is_err());
        assert!(parse_proxy_connect_request(b"GET https://github.com/ HTTP/1.1\r\n\r\n").is_err());
        assert!(parse_proxy_connect_request(&vec![b'a'; 8193]).is_err());
    }

    #[test]
    fn filters_private_git_destinations_and_caps_dns_answers() {
        let addresses = (1..=20)
            .map(|octet| SocketAddr::from(([140, 82, 112, octet], 443)))
            .chain([
                SocketAddr::from(([127, 0, 0, 1], 443)),
                SocketAddr::from(([169, 254, 169, 254], 443)),
            ]);
        let filtered = public_git_destinations(addresses);
        assert_eq!(filtered.len(), 16);
        assert!(
            filtered
                .iter()
                .all(|address| is_public_destination(address.ip()))
        );
    }

    #[tokio::test]
    #[ignore = "requires loopback socket binding; run in a network-enabled environment"]
    async fn live_connect_proxy_denies_unapproved_destinations() {
        let proxy = GitConnectProxy::start("github.com".to_owned())
            .await
            .unwrap();
        let mut client = TcpStream::connect(proxy.address).await.unwrap();
        client
            .write_all(b"CONNECT 169.254.169.254:443 HTTP/1.1\r\nHost: 169.254.169.254:443\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
    }

    #[test]
    fn rejects_symlink_submodule_and_traversal_tree_entries() {
        for unsafe_entry in [
            b"120000 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1\tlink\0".as_slice(),
            b"160000 commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa -\tsubmodule\0".as_slice(),
            b"100644 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1\t../escape\0".as_slice(),
            b"100644 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 1\tabsolute\\path\0".as_slice(),
        ] {
            assert!(matches!(
                validate_repository_tree(unsafe_entry),
                Err(WorkspaceError::UnsafeRepositoryTree)
            ));
        }
        assert!(
            validate_repository_tree(
                b"100644 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 12\tsrc/lib.rs\0"
            )
            .is_ok()
        );
    }

    #[test]
    fn workspace_id_is_an_opaque_safe_single_path_component() {
        let id = WorkspaceIdentity::new();
        assert!(id.as_str().starts_with("wsp_"));
        assert_eq!(WorkspaceIdentity::parse(id.as_str()).unwrap(), id);
        for invalid in ["../outside", "wsp_../outside", "wsp_not-a-uuid"] {
            assert!(WorkspaceIdentity::parse(invalid).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_rejects_workspace_symlink_escape() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let identity = WorkspaceIdentity::new();
        let policy = policy(root.path());
        let outside_file = outside.path().join("keep.txt");
        std::fs::write(&outside_file, "preserve").unwrap();
        symlink(outside.path(), policy.workspace_path(&identity)).unwrap();

        assert!(matches!(
            policy.cleanup(&identity),
            Err(WorkspaceError::UnsafeCleanupTarget)
        ));
        assert_eq!(std::fs::read_to_string(outside_file).unwrap(), "preserve");
    }
}
