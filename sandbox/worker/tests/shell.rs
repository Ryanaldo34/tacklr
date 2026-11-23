//! The WASIX shell with the bundled packages. Run `make sandbox-packages`
//! first.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tacklr_sandbox_worker::shell::{Output, Packages, Shell, Stream};
use virtual_fs::{FileSystem, mem_fs};

fn packages() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("packages")
}

struct Ran {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(shell: &Shell, script: &str, workspace: Arc<dyn FileSystem + Send + Sync>) -> Ran {
    let streams = Arc::new(Mutex::new((String::new(), String::new())));
    let sink = streams.clone();
    let output: Output = Arc::new(move |stream, bytes| {
        let mut streams = sink.lock().unwrap();
        let text = String::from_utf8_lossy(bytes);
        match stream {
            Stream::Stdout => streams.0.push_str(&text),
            Stream::Stderr => streams.1.push_str(&text),
        }
        Ok(())
    });
    let code = shell.run(script, workspace, output).expect("run");
    let (stdout, stderr) = streams.lock().unwrap().clone();
    Ran {
        code,
        stdout,
        stderr,
    }
}

#[test]
fn shell_runs_a_pipeline_over_the_workspace_and_a_later_run_sees_its_files() {
    let shell = Shell::start(Packages::load(&packages()).expect("load")).expect("start");
    let workspace = Arc::new(mem_fs::FileSystem::default());

    let output = run(
        &shell,
        "printf 'b\\na\\nc\\n' > names.txt && sort names.txt | grep -v c | sed 's/a/A/' > sorted.txt && cat sorted.txt && find . -name '*.txt' | sort",
        workspace.clone(),
    );

    assert_eq!(output.code, 0, "stderr: {}", output.stderr);
    assert_eq!(output.stdout, "A\nb\n./names.txt\n./sorted.txt\n");
    let again = run(&shell, "cat sorted.txt", workspace);
    assert_eq!(again.stdout, "A\nb\n");
}

#[test]
fn shell_returns_the_exit_code_and_stderr_of_the_script() {
    let shell = Shell::start(Packages::load(&packages()).expect("load")).expect("start");

    let output = run(
        &shell,
        "echo oops >&2; exit 3",
        Arc::new(mem_fs::FileSystem::default()),
    );

    assert_eq!(output.code, 3);
    assert_eq!(output.stderr, "oops\n");
}

#[test]
fn shell_refuses_a_package_list_line_that_is_not_name_version_hash() {
    let dir = std::env::temp_dir().join(format!("tacklr-packages-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(dir.join("packages.txt"), "wasmer/bash 1.0.25\n").expect("write");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(
        error.to_string(),
        "packages.txt: \"wasmer/bash 1.0.25\" is not \"name version sha256\""
    );
}

#[test]
fn shell_refuses_a_package_list_without_bash() {
    let dir = std::env::temp_dir().join(format!("tacklr-packages-nobash-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::copy(packages().join("sed.webc"), dir.join("sed.webc")).expect("copy");
    std::fs::write(
        dir.join("packages.txt"),
        "wasmer/sed 4.9.0 3fc12256be87f6b8b7810d68d642359a6220f63b39a2ea6ef7a2bb6d79ec1393\n",
    )
    .expect("write");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(error.to_string(), "packages.txt does not list wasmer/bash");
}

#[test]
fn shell_refuses_a_listed_package_whose_file_is_missing() {
    let dir = std::env::temp_dir().join(format!("tacklr-packages-missing-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::write(
        dir.join("packages.txt"),
        "wasmer/bash 1.0.25 059606d132e2e6bc1afe3b432ee64dcb1b1b059815c8bb213cf3b24798ef21e1\n",
    )
    .expect("write");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(
        error.to_string(),
        format!("read {}", dir.join("bash.webc").display())
    );
}

#[test]
fn shell_refuses_a_package_file_that_does_not_match_its_hash() {
    let dir = std::env::temp_dir().join(format!("tacklr-packages-hash-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    std::fs::copy(packages().join("sed.webc"), dir.join("bash.webc")).expect("copy");
    std::fs::write(
        dir.join("packages.txt"),
        "wasmer/bash 1.0.25 059606d132e2e6bc1afe3b432ee64dcb1b1b059815c8bb213cf3b24798ef21e1\n",
    )
    .expect("write");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(
        error.to_string(),
        format!(
            "{} does not match its SHA-256 in packages.txt",
            dir.join("bash.webc").display()
        )
    );
}

/// A package directory with links to the real packages, and no compiled
/// modules.
fn uncompiled_packages(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tacklr-packages-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    for file in [
        "packages.txt",
        "bash.webc",
        "coreutils.webc",
        "grep.webc",
        "sed.webc",
        "findutils.webc",
    ] {
        let _ = std::fs::remove_file(dir.join(file));
        std::os::unix::fs::symlink(packages().join(file), dir.join(file)).expect("link");
    }
    dir
}

fn compiled_dir(dir: &Path) -> PathBuf {
    dir.join("compiled").join(std::env::consts::ARCH)
}

#[test]
fn shell_refuses_packages_that_are_not_compiled() {
    let dir = uncompiled_packages("uncompiled");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(
        error.to_string(),
        format!(
            "read {} (run make sandbox-packages)",
            compiled_dir(&dir).display()
        )
    );
}

#[test]
fn shell_refuses_a_compiled_module_it_cannot_load() {
    let dir = uncompiled_packages("stale");
    std::fs::create_dir_all(compiled_dir(&dir)).expect("compiled dir");
    let stale = compiled_dir(&dir).join(format!("{}.bin", "AB".repeat(32)));
    std::fs::write(&stale, b"not a wasmer artifact").expect("write");

    let error = Packages::load(&dir).err().expect("load fails");

    assert_eq!(error.to_string(), format!("load {}", stale.display()));
}
