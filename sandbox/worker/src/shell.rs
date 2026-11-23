//! The shell that runs agent scripts: bash from the Wasmer registry, under
//! WASIX, with the bundled tools (coreutils, grep, sed, findutils).
//!
//! The packages come from one directory: `packages.txt` lists each package
//! as `name version sha256`, next to its `<basename>.webc` file
//! (`scripts/sandbox-packages.sh` fetches them). The worker reads them
//! before its jail applies. A script sees only the file system that the
//! caller mounts at [`WORKSPACE`], and the packages' own files.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use url::Url;
use virtual_fs::{FileSystem, VirtualFile};
use wasmer_config::package::{NamedPackageId, PackageId};
use wasmer_wasix::bin_factory::BinaryPackage;
use wasmer_wasix::runners::wasi::{RuntimeOrEngine, WasiRunner};
use wasmer_wasix::runtime::package_loader::{PackageLoader, load_package_tree};
use wasmer_wasix::runtime::resolver::{
    DistributionInfo, InMemorySource, PackageInfo, PackageSummary, Resolution, WebcHash,
};
use wasmer_wasix::runtime::task_manager::tokio::TokioTaskManager;
use wasmer_wasix::{PluggableRuntime, WasiRuntimeError};
use webc::Container;

/// The directory where a script starts, and where the caller's file system
/// is mounted.
pub const WORKSPACE: &str = "/workspace";

const BASH: &str = "wasmer/bash";

/// The loaded packages and the runtime that runs scripts with them.
pub struct Shell {
    tokio: tokio::runtime::Runtime,
    runtime: Arc<PluggableRuntime>,
    bash: BinaryPackage,
    tools: Vec<BinaryPackage>,
}

impl Shell {
    /// Loads the packages that `dir/packages.txt` lists, and checks each
    /// package file against its SHA-256 in the list.
    ///
    /// # Errors
    ///
    /// Returns an error if the list or a package file cannot be read, a line
    /// is not `name version sha256`, a file does not match its SHA-256, or
    /// the list has no `wasmer/bash`.
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        let list = std::fs::read_to_string(dir.join("packages.txt"))?;
        let mut source = InMemorySource::new();
        let mut bundled = Bundled::default();
        let mut containers = Vec::new();
        for line in list.lines() {
            let [name, version, hash] = line.split_whitespace().collect::<Vec<_>>()[..] else {
                bail!("packages.txt: {line:?} is not \"name version sha256\"");
            };
            let file = dir.join(format!("{}.webc", name.rsplit('/').next().unwrap_or(name)));
            let bytes = std::fs::read(&file).with_context(|| format!("read {}", file.display()))?;
            let webc_sha256 = WebcHash::sha256(&bytes);
            if webc_sha256 != WebcHash::parse_hex(hash)? {
                bail!(
                    "{} does not match its SHA-256 in packages.txt",
                    file.display()
                );
            }
            let container = wasmer_package::utils::from_bytes(bytes)?;
            let id = PackageId::Named(NamedPackageId {
                full_name: name.to_owned(),
                version: version.parse()?,
            });
            let summary = PackageSummary {
                pkg: PackageInfo::from_manifest(id, container.manifest(), container.version())?,
                dist: DistributionInfo {
                    webc: Url::parse(&format!("bundled:{name}"))?,
                    webc_sha256,
                },
            };
            bundled.0.insert(webc_sha256, container.clone());
            source.add(summary);
            containers.push((name.to_owned(), container));
        }
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let mut runtime =
            PluggableRuntime::new(Arc::new(TokioTaskManager::new(tokio.handle().clone())));
        runtime.set_source(source);
        runtime.set_package_loader(bundled);
        let runtime = Arc::new(runtime);
        let mut bash = None;
        let mut tools = Vec::new();
        for (name, container) in containers {
            let package = tokio.block_on(BinaryPackage::from_webc(&container, &*runtime))?;
            if name == BASH {
                bash = Some(package);
            } else {
                tools.push(package);
            }
        }
        let bash = bash.ok_or_else(|| anyhow!("packages.txt does not list {BASH}"))?;
        Ok(Self {
            tokio,
            runtime,
            bash,
            tools,
        })
    }

    /// Runs `script` with `bash -c` in [`WORKSPACE`], where `workspace` is
    /// mounted, and returns the script's exit code.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime fails for a reason other than the
    /// script's exit.
    pub fn run(
        &self,
        script: &str,
        workspace: Arc<dyn FileSystem + Send + Sync>,
        stdout: Box<dyn VirtualFile + Send + Sync>,
        stderr: Box<dyn VirtualFile + Send + Sync>,
    ) -> anyhow::Result<i32> {
        let _context = self.tokio.enter();
        let result = WasiRunner::new()
            .with_injected_packages(self.tools.iter().cloned())
            .with_mount(WORKSPACE.to_owned(), workspace)
            .with_current_dir(WORKSPACE)
            .with_stdout(stdout)
            .with_stderr(stderr)
            .with_args(["-c", script])
            .run_command(
                "bash",
                &self.bash,
                RuntimeOrEngine::Runtime(self.runtime.clone()),
            );
        match result {
            Ok(()) => Ok(0),
            Err(error) => error
                .downcast_ref::<WasiRuntimeError>()
                .and_then(WasiRuntimeError::as_exit_code)
                .map(|code| code.raw())
                .ok_or(error),
        }
    }
}

/// The package loader of the bundled packages. It never downloads.
#[derive(Debug, Default)]
struct Bundled(HashMap<WebcHash, Container>);

#[async_trait::async_trait]
impl PackageLoader for Bundled {
    async fn load(&self, summary: &PackageSummary) -> anyhow::Result<Container> {
        self.0
            .get(&summary.dist.webc_sha256)
            .cloned()
            .ok_or(anyhow!("{} is not a bundled package", summary.pkg.id))
    }

    async fn load_package_tree(
        &self,
        root: &Container,
        resolution: &Resolution,
        root_is_local_dir: bool,
    ) -> anyhow::Result<BinaryPackage> {
        load_package_tree(root, self, resolution, root_is_local_dir).await
    }
}
