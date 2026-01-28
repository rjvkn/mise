use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

use eyre::{Result, bail};
use versions::{Chunk, Version};
use xx::file;

use crate::backend::platform_target::PlatformTarget;
use crate::cli::args::BackendArg;
use crate::lockfile::LockfileTool;
use crate::runtime_symlinks::is_runtime_symlink;
use crate::toolset::tool_version::ResolveOptions;
use crate::toolset::{ToolSource, ToolVersion, ToolVersionOptions};
use crate::{backend, lockfile};
use crate::{backend::ABackend, config::Config};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ToolRequest {
    pub backend: Arc<BackendArg>,
    pub source: ToolSource,
    pub options: ToolVersionOptions,
    pub kind: ToolRequestKind,
    pub reason: ToolRequestReason,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ToolRequestKind {
    Version { version: String },
    Prefix { prefix: String },
    Ref { ref_: String, ref_type: String },
    Sub { sub: String, orig_version: String },
    Path { path: PathBuf },
    System,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum ToolRequestReason {
    Install,
    Update,
    Upgrade,
    ListCurrentVersions,
    ResolveRef,
    ResolvePath,
}

impl ToolRequest {
    pub fn new(backend: Arc<BackendArg>, s: &str, source: ToolSource) -> Result<Self> {
        let normalized = match s.split_once('-') {
            Some((ref_type @ ("ref" | "tag" | "branch" | "rev"), r)) => {
                format!("{ref_type}:{r}")
            }
            _ => s.to_string(),
        };

        let options = backend.opts();

        let kind = match normalized.split_once(':') {
            Some((ref_type @ ("ref" | "tag" | "branch" | "rev"), r)) => ToolRequestKind::Ref {
                ref_: r.to_string(),
                ref_type: ref_type.to_string(),
            },
            Some(("prefix", p)) => ToolRequestKind::Prefix {
                prefix: p.to_string(),
            },
            Some(("path", p)) => ToolRequestKind::Path {
                path: PathBuf::from(p),
            },
            Some((p, v)) if p.starts_with("sub-") => ToolRequestKind::Sub {
                sub: p.split_once('-').unwrap().1.to_string(),
                orig_version: v.to_string(),
            },
            None if normalized == "system" => ToolRequestKind::System,
            None => ToolRequestKind::Version {
                version: normalized,
            },
            _ => bail!("invalid tool version request: {normalized}"),
        };

        Ok(Self {
            backend,
            source,
            options,
            kind,
            reason: ToolRequestReason::Install,
        })
    }

    pub fn new_opts(
        backend: Arc<BackendArg>,
        s: &str,
        options: ToolVersionOptions,
        source: ToolSource,
    ) -> Result<Self> {
        let mut r = Self::new(backend, s, source)?;
        r.options = options;
        Ok(r)
    }

    pub fn backend(&self) -> Result<ABackend> {
        self.backend.backend()
    }

    pub fn ba(&self) -> &Arc<BackendArg> {
        &self.backend
    }

    pub fn source(&self) -> &ToolSource {
        &self.source
    }

    pub fn set_source(&mut self, source: ToolSource) -> &mut Self {
        self.source = source;
        self
    }

    pub fn options(&self) -> ToolVersionOptions {
        self.options.clone()
    }

    pub fn set_options(&mut self, options: ToolVersionOptions) -> &mut Self {
        self.options = options;
        self
    }

    pub fn os(&self) -> &Option<Vec<String>> {
        &self.options.os
    }

    pub fn version(&self) -> String {
        match &self.kind {
            ToolRequestKind::Version { version } => version.clone(),
            ToolRequestKind::Prefix { prefix } => format!("prefix:{prefix}"),
            ToolRequestKind::Ref { ref_, ref_type } => format!("{ref_type}:{ref_}"),
            ToolRequestKind::Path { path } => format!("path:{}", path.display()),
            ToolRequestKind::Sub { sub, orig_version } => {
                format!("sub-{sub}:{orig_version}")
            }
            ToolRequestKind::System => "system".into(),
        }
    }

    pub fn key(&self) -> String {
        format!("{}@{}", self.backend.full(), self.version())
    }

    pub fn install_path(&self, config: &Config) -> Option<PathBuf> {
        let backend = &self.backend;

        match &self.kind {
            ToolRequestKind::Version { version } => Some(backend.installs_path.join(version)),

            ToolRequestKind::Ref { ref_, ref_type } => {
                Some(backend.installs_path.join(format!("{ref_type}-{ref_}")))
            }

            ToolRequestKind::Sub { sub, orig_version } => self
                .local_resolve(config, orig_version)
                .ok()
                .flatten()
                .map(|v| backend.installs_path.join(version_sub(&v, sub))),

            ToolRequestKind::Prefix { prefix } => {
                file::ls(&backend.installs_path).ok().and_then(|installs| {
                    installs.into_iter().find(|p| {
                        !is_runtime_symlink(p)
                            && p.file_name().unwrap().to_string_lossy().starts_with(prefix)
                    })
                })
            }

            ToolRequestKind::Path { path } => Some(path.clone()),

            ToolRequestKind::System => None,
        }
    }

    pub fn lockfile_resolve(&self, config: &Config) -> Result<Option<LockfileTool>> {
        self.lockfile_resolve_with_prefix(config, &self.version())
    }

    /// Like lockfile_resolve but uses a custom prefix instead of self.version().
    /// This is used after alias resolution (e.g., "lts" → "24") so the lockfile
    /// prefix match can find entries like "24.13.0".starts_with("24").
    pub fn lockfile_resolve_with_prefix(
        &self,
        config: &Config,
        prefix: &str,
    ) -> Result<Option<LockfileTool>> {
        let request_options = if let Ok(backend) = self.backend() {
            let target = PlatformTarget::from_current();
            backend.resolve_lockfile_options(self, &target)
        } else {
            BTreeMap::new()
        };
        let path = match self.source() {
            ToolSource::MiseToml(path) => Some(path),
            _ => None,
        };
        lockfile::get_locked_version(
            config,
            path.map(|p| p.as_path()),
            &self.ba().short,
            prefix,
            &request_options,
        )
    }

    pub fn local_resolve(&self, config: &Config, v: &str) -> Result<Option<String>> {
        if let Some(lt) = self.lockfile_resolve(config)? {
            return Ok(Some(lt.version));
        }
        if let Some(backend) = backend::get(self.ba()) {
            let matches = backend.list_installed_versions_matching(v);
            if matches.iter().any(|m| m == v) {
                return Ok(Some(v.to_string()));
            }
            if let Some(v) = matches.last() {
                return Ok(Some(v.to_string()));
            }
        }
        Ok(None)
    }

    pub async fn resolve(
        &self,
        config: &Arc<Config>,
        opts: &ResolveOptions,
    ) -> Result<ToolVersion> {
        ToolVersion::resolve(config, self.clone(), opts).await
    }

    pub async fn is_installed(&self, config: &Arc<Config>) -> bool {
        if let Some(backend) = backend::get(self.ba()) {
            match self.resolve(config, &Default::default()).await {
                Ok(tv) => backend.is_version_installed(config, &tv, false),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    pub fn is_os_supported(&self) -> bool {
        if let Some(os) = self.os()
            && !os.contains(&crate::cli::version::OS)
        {
            return false;
        }
        self.ba().is_os_supported()
    }
}

impl Display for ToolRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}@{}", self.backend, self.version())
    }
}

impl ToolRequestReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ToolRequestReason::Install => "install",
            ToolRequestReason::Update => "update",
            ToolRequestReason::Upgrade => "upgrade",
            ToolRequestReason::ListCurrentVersions => "list_current_versions",
            ToolRequestReason::ResolveRef => "resolve_ref",
            ToolRequestReason::ResolvePath => "resolve_path",
        }
    }
}

impl fmt::Display for ToolRequestReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// subtracts sub from orig and removes suffix
pub fn version_sub(orig: &str, sub: &str) -> String {
    let mut orig = Version::new(orig).unwrap();
    let sub = Version::new(sub).unwrap();

    while orig.chunks.0.len() > sub.chunks.0.len() {
        orig.chunks.0.pop();
    }

    for i in 0..orig.chunks.0.len() {
        let m = sub.nth(i).unwrap();
        let orig_val = orig.chunks.0[i].single_digit().unwrap();

        if orig_val < m {
            for j in (0..i).rev() {
                let prev_val = orig.chunks.0[j].single_digit().unwrap();
                if prev_val > 0 {
                    orig.chunks.0[j] = Chunk::Numeric(prev_val - 1);
                    orig.chunks.0.truncate(j + 1);
                    return orig.to_string();
                }
            }
            return "0".to_string();
        }

        orig.chunks.0[i] = Chunk::Numeric(orig_val - m);
    }

    orig.to_string()
}

#[cfg(test)]
mod tests {
    use super::version_sub;
    use pretty_assertions::assert_str_eq;
    use test_log::test;

    #[test]
    fn test_version_sub() {
        assert_str_eq!(version_sub("18.2.3", "2"), "16");
        assert_str_eq!(version_sub("18.2.3", "0.1"), "18.1");
        assert_str_eq!(version_sub("18.2.3", "0.0.1"), "18.2.2");
    }

    #[test]
    fn test_version_sub_underflow() {
        // Test cases that would cause underflow return prefix for higher digit
        assert_str_eq!(version_sub("2.0.0", "0.0.1"), "1");
        assert_str_eq!(version_sub("2.79.0", "0.0.1"), "2.78");
        assert_str_eq!(version_sub("1.0.0", "0.1.0"), "0");
        assert_str_eq!(version_sub("0.1.0", "1"), "0");
        assert_str_eq!(version_sub("1.2.3", "0.2.4"), "0");
        assert_str_eq!(version_sub("1.3.3", "0.2.4"), "1.0");
    }
}
