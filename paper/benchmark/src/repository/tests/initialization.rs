//! Tests for repository initialization

use crate::repository::initialize;
use tempfile::tempdir;

// Initialisation tests
#[test]
fn initializes_directories() {
    let tmpdir = tempdir().unwrap();
    let repo_path = tmpdir.path().join("repository");
    initialize(&repo_path).unwrap();
    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"] {
        assert!(repo_path.join(directory).is_dir());
    }
}

#[test]
fn skips_existing_directories() {
    let tmpdir = tempdir().unwrap();
    let repo_path = tmpdir.path().join("repository");
    std::fs::DirBuilder::new().create(&repo_path).unwrap();
    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"] {
        std::fs::DirBuilder::new()
            .create(repo_path.join(directory))
            .unwrap();
    }

    assert!(matches!(initialize(repo_path), Ok(())));
}
