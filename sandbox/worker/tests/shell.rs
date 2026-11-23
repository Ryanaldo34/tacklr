//! The WASIX shell with the bundled packages. Run `make sandbox-packages`
//! first.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tacklr_sandbox_worker::shell::Shell;
use virtual_fs::{FileSystem, Pipe, mem_fs};

fn packages() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("packages")
}

struct Output {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(shell: &Shell, script: &str, workspace: Arc<dyn FileSystem + Send + Sync>) -> Output {
    let (stdout, mut stdout_rx) = Pipe::channel();
    let (stderr, mut stderr_rx) = Pipe::channel();
    let code = shell
        .run(script, workspace, Box::new(stdout), Box::new(stderr))
        .expect("run");
    let mut output = Output {
        code,
        stdout: String::new(),
        stderr: String::new(),
    };
    stdout_rx
        .read_to_string(&mut output.stdout)
        .expect("stdout");
    stderr_rx
        .read_to_string(&mut output.stderr)
        .expect("stderr");
    output
}

#[test]
fn shell_runs_a_pipeline_over_the_workspace_and_a_later_run_sees_its_files() {
    let shell = Shell::load(&packages()).expect("load");
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
    let shell = Shell::load(&packages()).expect("load");

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

    let error = Shell::load(&dir).err().expect("load fails");

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

    let error = Shell::load(&dir).err().expect("load fails");

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

    let error = Shell::load(&dir).err().expect("load fails");

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

    let error = Shell::load(&dir).err().expect("load fails");

    assert_eq!(
        error.to_string(),
        format!(
            "{} does not match its SHA-256 in packages.txt",
            dir.join("bash.webc").display()
        )
    );
}
