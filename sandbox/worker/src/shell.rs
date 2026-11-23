//! The shell that runs agent scripts: bash from the Wasmer registry, under
//! WASIX, with the bundled tools (coreutils, grep, sed, findutils).
//!
//! The packages come from one directory: `packages.txt` lists each package
//! as `name version sha256`, next to its `<basename>.webc` file
//! (`scripts/sandbox-packages.sh` fetches them). `tacklr-sandbox-compile`
//! compiles their modules ahead of time into `compiled/<arch>/<hash>.bin`,
//! so the worker runs them with Wasmer's headless engine and has no
//! compiler. The worker reads all of it before its jail applies. A script
//! sees only the file system that the caller mounts at [`WORKSPACE`], and
//! the packages' own files.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use tokio::io::AsyncReadExt;
use url::Url;
use virtual_fs::{FileSystem, Pipe};
use wasmer::sys::EngineBuilder;
use wasmer::{Engine, Module};
use wasmer_config::package::{NamedPackageId, PackageId};
use wasmer_types::ModuleHash;
use wasmer_wasix::bin_factory::BinaryPackage;
use wasmer_wasix::runners::wasi::{RuntimeOrEngine, WasiRunner};
use wasmer_wasix::runtime::module_cache::{ModuleCache, SharedCache};
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

/// The packages and their compiled modules, read from disk. Loading starts
/// no thread, so the worker loads them before its jail applies and then
/// starts the [`Shell`] inside the jail.
pub struct Packages {
    bundle: Bundle,
    engine: Engine,
    modules: Vec<(ModuleHash, Module)>,
}

impl Packages {
    /// Loads the packages that `dir/packages.txt` lists and their compiled
    /// modules, and checks each package file against its SHA-256 in the
    /// list.
    ///
    /// # Errors
    ///
    /// Returns an error if the list, a package file or a compiled module
    /// cannot be read, a line is not `name version sha256`, a file does not
    /// match its SHA-256, or the list has no `wasmer/bash`.
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        let bundle = Bundle::read(dir)?;
        let engine: Engine = EngineBuilder::headless().engine().into();
        let artifacts = compiled_dir(dir);
        let entries = std::fs::read_dir(&artifacts)
            .with_context(|| format!("read {} (run make sandbox-packages)", artifacts.display()))?;
        let mut modules = Vec::new();
        for entry in entries {
            let path = entry?.path();
            let stem = path.file_stem().unwrap_or_default().to_string_lossy();
            let hash = ModuleHash::from_bytes(WebcHash::parse_hex(&stem)?.as_bytes());
            // SAFETY: tacklr-sandbox-compile wrote these artifacts for this
            // Wasmer version, and the package directory is as trusted as the
            // worker binary.
            let module = unsafe { Module::deserialize_from_file(&engine, &path) }
                .with_context(|| format!("load {}", path.display()))?;
            modules.push((hash, module));
        }
        Ok(Self {
            bundle,
            engine,
            modules,
        })
    }

    /// Compiles the modules of the packages in `dir` for this CPU
    /// architecture, with only its baseline features, and writes them to
    /// `compiled/<arch>/<hash>.bin`. Returns the number of modules.
    ///
    /// # Errors
    ///
    /// Returns an error if the packages cannot be loaded or a module cannot
    /// be compiled or written.
    #[cfg(feature = "compile")]
    pub fn compile(dir: &Path) -> anyhow::Result<usize> {
        use wasmer::sys::{CpuFeature, Cranelift, Target, Triple};

        let target = Target::new(Triple::host(), CpuFeature::set());
        let engine: Engine = EngineBuilder::new(Cranelift::default())
            .set_target(Some(target))
            .engine()
            .into();
        let shell = Shell::start(Self {
            bundle: Bundle::read(dir)?,
            engine: engine.clone(),
            modules: Vec::new(),
        })?;
        let out = compiled_dir(dir);
        std::fs::create_dir_all(&out)?;
        let mut done = std::collections::HashSet::new();
        for command in std::iter::once(&shell.bash)
            .chain(&shell.tools)
            .flat_map(|package| &package.commands)
        {
            if done.insert(*command.hash()) {
                Module::new(&engine, command.atom_ref().as_ref())?
                    .serialize_to_file(out.join(format!("{}.bin", command.hash())))?;
            }
        }
        Ok(done.len())
    }
}

/// The runtime that runs scripts with the packages.
pub struct Shell {
    tokio: tokio::runtime::Runtime,
    runtime: Arc<PluggableRuntime>,
    bash: BinaryPackage,
    tools: Vec<BinaryPackage>,
}

impl Shell {
    /// Starts the runtime for `packages`: the threads of the async runtime
    /// that runs the packages' processes.
    ///
    /// # Errors
    ///
    /// Returns an error if the async runtime cannot start or a package's
    /// dependencies cannot be resolved from the bundle.
    pub fn start(packages: Packages) -> anyhow::Result<Self> {
        let tokio = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let compiled = SharedCache::new();
        for (hash, module) in &packages.modules {
            tokio.block_on(compiled.save(*hash, &packages.engine, module))?;
        }
        let mut runtime =
            PluggableRuntime::new(Arc::new(TokioTaskManager::new(tokio.handle().clone())));
        runtime.set_source(packages.bundle.source);
        runtime.set_package_loader(packages.bundle.loader);
        runtime.set_engine(packages.engine);
        runtime.set_module_cache(compiled);
        let runtime = Arc::new(runtime);
        let bash = tokio.block_on(BinaryPackage::from_webc(&packages.bundle.bash, &*runtime))?;
        let mut tools = Vec::new();
        for container in &packages.bundle.tools {
            tools.push(tokio.block_on(BinaryPackage::from_webc(container, &*runtime))?);
        }
        Ok(Self {
            tokio,
            runtime,
            bash,
            tools,
        })
    }

    /// Runs `script` with `bash -c` in [`WORKSPACE`], where `workspace` is
    /// mounted, and returns the script's exit code. `output` gets the
    /// script's stdout and stderr as they come, from the runtime's threads.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime fails for a reason other than the
    /// script's exit, or if `output` fails.
    pub fn run(
        &self,
        script: &str,
        workspace: Arc<dyn FileSystem + Send + Sync>,
        output: Output,
    ) -> anyhow::Result<i32> {
        let _context = self.tokio.enter();
        let (stdout, stdout_rx) = Pipe::channel();
        let (stderr, stderr_rx) = Pipe::channel();
        let pumps = [
            self.tokio
                .spawn(pump(stdout_rx, Stream::Stdout, output.clone())),
            self.tokio.spawn(pump(stderr_rx, Stream::Stderr, output)),
        ];
        let result = WasiRunner::new()
            .with_injected_packages(self.tools.iter().cloned())
            .with_mount(WORKSPACE.to_owned(), workspace)
            .with_current_dir(WORKSPACE)
            .with_stdout(Box::new(stdout))
            .with_stderr(Box::new(stderr))
            .with_args(["-c", script])
            .run_command(
                "bash",
                &self.bash,
                RuntimeOrEngine::Runtime(self.runtime.clone()),
            );
        for pump in pumps {
            self.tokio.block_on(pump)??;
        }
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

/// The output stream of a script.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    /// The script's stdout.
    Stdout,
    /// The script's stderr.
    Stderr,
}

/// Where [`Shell::run`] sends a script's output.
pub type Output = Arc<dyn Fn(Stream, &[u8]) -> std::io::Result<()> + Send + Sync>;

/// Sends what `pipe` gets to `output`, until the script closes it. It reads
/// with the async API: the blocking read of [`Pipe`] holds the lock that a
/// write needs.
async fn pump(mut pipe: Pipe, stream: Stream, output: Output) -> std::io::Result<()> {
    let mut buffer = vec![0u8; 64 << 10];
    loop {
        let read = pipe.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        output(stream, &buffer[..read])?;
    }
}

/// The packages of `packages.txt`, checked against their SHA-256.
struct Bundle {
    source: InMemorySource,
    loader: Bundled,
    bash: Container,
    tools: Vec<Container>,
}

impl Bundle {
    fn read(dir: &Path) -> anyhow::Result<Self> {
        let list = std::fs::read_to_string(dir.join("packages.txt"))?;
        let mut source = InMemorySource::new();
        let mut loader = Bundled::default();
        let mut bash = None;
        let mut tools = Vec::new();
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
            source.add(PackageSummary {
                pkg: PackageInfo::from_manifest(id, container.manifest(), container.version())?,
                dist: DistributionInfo {
                    webc: Url::parse(&format!("bundled:{name}"))?,
                    webc_sha256,
                },
            });
            loader.0.insert(webc_sha256, container.clone());
            if name == BASH {
                bash = Some(container);
            } else {
                tools.push(container);
            }
        }
        Ok(Self {
            source,
            loader,
            bash: bash.ok_or_else(|| anyhow!("packages.txt does not list {BASH}"))?,
            tools,
        })
    }
}

fn compiled_dir(dir: &Path) -> PathBuf {
    dir.join("compiled").join(std::env::consts::ARCH)
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
