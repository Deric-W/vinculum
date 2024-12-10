//! Tests for repository initialization

use tempfile::tempdir;
use vinculum::backends::files;

// Initialisation tests
#[test]
fn initializes_directories() {
    let tmpdir = tempdir().unwrap();
    let repo_path = tmpdir.path().join("repository");
    files::initialize(&repo_path).unwrap();
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

    assert!(matches!(files::initialize(repo_path), Ok(())));
}
