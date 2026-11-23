//! Compiles the sandbox packages ahead of time:
//! `tacklr-sandbox-compile <packages dir>`.

use std::path::PathBuf;

use anyhow::Context;
use tacklr_sandbox_worker::shell::Shell;

fn main() -> anyhow::Result<()> {
    let dir: PathBuf = std::env::args_os()
        .nth(1)
        .context("usage: tacklr-sandbox-compile <packages dir>")?
        .into();
    let modules = Shell::compile(&dir)?;
    println!(
        "compiled {modules} modules into {}",
        dir.join("compiled").display()
    );
    Ok(())
}
