//! Embed a build version into the binary at compile time, derived entirely from
//! git (the same source in local, CI, and Docker builds — the Docker build copies
//! `.git` in precisely so this works there too).
//!
//! The version is: the tag on HEAD if one points exactly at it, otherwise the
//! 12-char short commit SHA. There is no single `git describe` flag that yields
//! "exact tag, else *bare* sha" — `--exact-match` is tag-or-error and `--always`
//! does not rescue it — so we run the exact-match describe and fall back to
//! `rev-parse` for the SHA. `"unknown"` is the last resort so the binary always
//! compiles (e.g. building from a tarball with no `.git`).
//!
//! Exposed to the crate as `env!("EVALUATOR_BUILD_VERSION")`.

use std::process::Command;

fn main() {
    // Re-derive when the checkout moves. Best-effort: CI/Docker builds are always
    // fresh, so this only affects incremental local rebuilds. A brand-new loose
    // tag may not trigger a rebuild until HEAD next moves.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/packed-refs");

    let version = git_tag_or_sha().unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=EVALUATOR_BUILD_VERSION={version}");
}

/// The exact tag on HEAD, else the 12-char short SHA. `None` only when git or the
/// repository is unavailable.
fn git_tag_or_sha() -> Option<String> {
    // Exact tag on HEAD -> use it verbatim (e.g. "v1.2.3").
    if let Some(tag) = run_git(&["describe", "--tags", "--exact-match", "HEAD"]) {
        return Some(tag);
    }
    // No tag on HEAD -> the short commit SHA.
    run_git(&["rev-parse", "--short=12", "HEAD"])
}

/// Run `git <args>`, returning trimmed stdout on success. `safe.directory=*`
/// avoids git's "dubious ownership" refusal when `.git` was copied into the
/// Docker builder with a different owner than the build user.
fn run_git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(["-c", "safe.directory=*"])
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
