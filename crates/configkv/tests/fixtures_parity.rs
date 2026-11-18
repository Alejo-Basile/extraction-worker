#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Parity oracle for the shared Consul KV client contract (S11-P1-C10/C13).
//!
//! The fixtures are vendored from the platform repository
//! (`infra/compose/consul/testdata/`) so this repository stays self-contained
//! and CI can run `cargo test` without checking out a sibling repo. When the
//! upstream directory is present (local monorepo layout), `vendored_matches_upstream`
//! keeps the copies honest. Any contract change must be re-synced from upstream.

use configkv::{validate_str, validate_values, RejectReason};
use serde_json::Value;
use std::path::{Path, PathBuf};

fn vendored_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn upstream_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../infra/compose/consul/testdata")
}

fn worker_entries(manifest: &Value) -> Vec<Value> {
    manifest["fixtures"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|e| {
            e["file"]
                .as_str()
                .unwrap_or("")
                .starts_with("extraction-worker.")
        })
        .collect()
}

fn verdict_of(raw: &str) -> (&'static str, Option<String>, Option<String>) {
    match serde_json::from_str::<Value>(raw) {
        Ok(mut v) => {
            if let Some(o) = v.as_object_mut() {
                o.remove("_expected");
            }
            let map = v.as_object().cloned().unwrap_or_default();
            match validate_values(&map) {
                Ok(_) => ("accepted", None, None),
                Err(r) => ("rejected", r.key, Some(r.reason.as_str().to_string())),
            }
        }
        Err(_) => match validate_str(raw) {
            Ok(_) => ("accepted", None, None),
            Err(r) => ("rejected", r.key, Some(r.reason.as_str().to_string())),
        },
    }
}

#[test]
fn parity() {
    let base = vendored_dir();
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(base.join("manifest.json")).unwrap())
            .unwrap();

    let entries = worker_entries(&manifest);
    assert_eq!(
        entries.len(),
        6,
        "oracle must expose the six worker fixtures"
    );

    for e in entries {
        let file = e["file"].as_str().unwrap();
        let raw = std::fs::read_to_string(base.join(file)).unwrap();
        let (verdict, key, reason) = verdict_of(&raw);
        println!(
            "{} -> {}/{}",
            file,
            verdict,
            key.clone().unwrap_or_default()
        );

        assert_eq!(
            verdict,
            e["verdict"].as_str().unwrap(),
            "verdict for {file}"
        );
        let expected_key = e.get("key").and_then(|x| x.as_str());
        assert_eq!(key.as_deref(), expected_key, "key for {file}");
        let expected_reason = e.get("reason").and_then(|x| x.as_str());
        assert_eq!(reason.as_deref(), expected_reason, "reason for {file}");
    }
}

#[test]
fn malformed_fixture_reports_no_key() {
    let raw =
        std::fs::read_to_string(vendored_dir().join("extraction-worker.malformed.json")).unwrap();
    let rejection = validate_str(&raw).unwrap_err();
    assert_eq!(rejection.key, None);
    assert_eq!(rejection.reason, RejectReason::MalformedJson);
}

/// Cross-check against the upstream oracle when the platform repository is
/// available next to this one. Absent upstream (CI checkout) is not a failure:
/// the vendored copy is the test input.
#[test]
fn vendored_matches_upstream() {
    let upstream = upstream_dir();
    if !upstream.join("manifest.json").exists() {
        return;
    }
    for entry in std::fs::read_dir(vendored_dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let name = path.file_name().unwrap();
        let upstream_path = upstream.join(name);
        assert!(
            upstream_path.exists(),
            "fixture {name:?} no longer exists upstream; re-sync the vendored copy"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            std::fs::read(&upstream_path).unwrap(),
            "vendored fixture {name:?} drifted from the upstream contract"
        );
    }
}
