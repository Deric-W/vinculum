//! Tests for commiting new files

use super::{create_repository, PollOnce};
use crate::ID;
use futures::io::AsyncWriteExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::pin::pin;
use tempfile::tempdir;
use vinculum::ChunkBackend;

#[tokio::test]
async fn moves_file_on_commit() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        Some(Ok(_))
    ));
    assert!(!chunks_path.is_file());

    upload.close().await.unwrap();

    assert!(backend
        .directory()
        .join("incoming")
        .read_dir()
        .unwrap()
        .next()
        .is_none());
    assert!(chunks_path.is_file());
}

#[tokio::test]
async fn commits_correct_content() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
    let test_string = "This is a test!!!";
    let mut buf = String::with_capacity(test_string.len() * 1000000);
    for _ in 0..1000000 {
        buf.push_str(test_string);
        upload.write_all(test_string.as_bytes()).await.unwrap();
    }
    upload.close().await.unwrap();

    assert_eq!(std::fs::read_to_string(chunks_path).unwrap(), buf);
}

#[tokio::test]
async fn removes_file_on_drop() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
        upload.write_all(&[0; 32]).await.unwrap();
    }

    assert!(backend
        .directory()
        .join("incoming")
        .read_dir()
        .unwrap()
        .next()
        .is_none());
    assert!(!chunks_path.is_file());
}

#[tokio::test]
async fn removes_file_on_drop_after_pending_close() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());

        assert!(PollOnce {
            inner: upload.close()
        }
        .await
        .is_none());
    }

    assert!(backend
        .directory()
        .join("incoming")
        .read_dir()
        .unwrap()
        .next()
        .is_none());
    assert!(!chunks_path.is_file());
}

#[tokio::test]
async fn removes_file_on_rename_failure() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
        let upload_path = backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::DirBuilder::new().create(&chunks_path).unwrap();

        assert!(matches!(upload.close().await, Err(e) if e.kind() == ErrorKind::IsADirectory));
        assert!(backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next()
            .is_none());

        // test that the file is not removed twice on drop
        std::fs::File::create_new(upload_path).unwrap();
    }

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        Some(Ok(_))
    ));
}
