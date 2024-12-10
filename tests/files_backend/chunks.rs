//! Tests for the chunks

use super::{create_repository, ID};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::pin::pin;
use tempfile::tempdir;
use vinculum::backends::files;
use vinculum::ChunkBackend;

#[tokio::test]
async fn list_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for chunk in 0..10 {
        let id = ID { inner: [chunk; 32] };
        let chunk_path = backend
            .directory()
            .join("chunks")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(chunk_path, "test".as_bytes()).unwrap();
    }
    let invalid_chunk_path = backend.directory().join("chunks").join("test");
    std::fs::write(invalid_chunk_path, "invalid".as_bytes()).unwrap();
    let chunks: Vec<ID> = pin!(<files::FileBackend as ChunkBackend<ID>>::chunks(&backend)
        .await
        .unwrap())
    .map(|id| id.unwrap())
    .collect()
    .await;

    assert_eq!(chunks.len(), 10);
    for i in 0..10 {
        assert_eq!(chunks.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
async fn add_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));

    for data in ["test1", "test2"] {
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::add_chunk(&backend, &id)
                .await
                .unwrap()
        );
        chunk.write_all(data.as_bytes()).await.unwrap();
        chunk.close().await.unwrap();

        assert_eq!(std::fs::read_to_string(&chunk_path).unwrap(), data);
    }
}

#[tokio::test]
async fn has_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));

    assert!(matches!(
        <files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await,
        Ok(false)
    ));

    std::fs::write(&chunk_path, &[]).unwrap();

    assert!(matches!(
        <files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await,
        Ok(true)
    ));

    std::fs::remove_file(chunk_path).unwrap();
    std::fs::write(&fossil_path, &[]).unwrap();

    assert!(matches!(
        <files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await,
        Ok(false)
    ));
}

#[tokio::test]
async fn read_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    for data in ["test1", "test2"] {
        std::fs::write(&chunk_path, data.as_bytes()).unwrap();
        let mut buf = Vec::with_capacity(data.len());
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &id)
                .await
                .unwrap()
        );

        assert_eq!(chunk.read_to_end(&mut buf).await.unwrap(), data.len());
        assert_eq!(buf.as_slice(), data.as_bytes());
    }
    std::fs::remove_file(chunk_path).unwrap();
    for data in ["test3", "test4"] {
        std::fs::write(&fossil_path, data.as_bytes()).unwrap();
        let mut buf = Vec::with_capacity(data.len());
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &id)
                .await
                .unwrap()
        );

        assert_eq!(chunk.read_to_end(&mut buf).await.unwrap(), data.len());
        assert_eq!(buf.as_slice(), data.as_bytes());
    }

    assert!(
        matches!(<files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &ID { inner: [1; 32]}).await, Err(e) if e.kind() == ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn list_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for chunk in 0..10 {
        let id = ID { inner: [chunk; 32] };
        let chunk_path = backend
            .directory()
            .join("fossils")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(chunk_path, "test".as_bytes()).unwrap();
    }
    let invalid_chunk_path = backend.directory().join("fossils").join("test");
    std::fs::write(invalid_chunk_path, "invalid".as_bytes()).unwrap();
    let chunks: Vec<ID> = pin!(<files::FileBackend as ChunkBackend<ID>>::fossils(&backend)
        .await
        .unwrap())
    .map(|id| id.unwrap())
    .collect()
    .await;

    assert_eq!(chunks.len(), 10);
    for i in 0..10 {
        assert_eq!(chunks.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
async fn make_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&chunk_path, "test".as_bytes()).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::make_fossil(&backend, &id)
        .await
        .unwrap();

    assert!(!chunk_path.exists());
    assert_eq!(
        std::fs::read(&fossil_path).unwrap().as_slice(),
        "test".as_bytes()
    );

    <files::FileBackend as ChunkBackend<ID>>::make_fossil(&backend, &id)
        .await
        .unwrap();

    assert!(fossil_path.exists());
}

#[tokio::test]
async fn recover_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&fossil_path, "test".as_bytes()).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::recover_fossil(&backend, &id)
        .await
        .unwrap();

    assert!(!fossil_path.exists());
    assert_eq!(
        std::fs::read(&chunk_path).unwrap().as_slice(),
        "test".as_bytes()
    );

    <files::FileBackend as ChunkBackend<ID>>::recover_fossil(&backend, &id)
        .await
        .unwrap();

    assert!(!fossil_path.exists());
}

#[tokio::test]
async fn delete_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&fossil_path, &[]).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::delete_fossil(&backend, &id)
        .await
        .unwrap();

    assert!(!chunk_path.exists());
    assert!(!fossil_path.exists());

    <files::FileBackend as ChunkBackend<ID>>::delete_fossil(&backend, &id)
        .await
        .unwrap();
}
