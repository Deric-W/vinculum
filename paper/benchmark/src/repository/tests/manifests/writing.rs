//! Tests for writing manifest files

use super::{create_repository, BigID, EmptyID, SmallID};
use crate::repository::{utils::timestamp_from_bytes, FileRepository, ManifestEncodingError};
use crate::{ChunkID, ID};
use futures::sink::SinkExt;
use std::ffi::OsString;
use std::pin::pin;
use tempfile::tempdir;

fn read_manifest<I, C>(backend: &FileRepository<ID, I, C>, id: &ID) -> Box<[u8]> {
    let path = backend
        .directory()
        .join("manifests")
        .join(<&ID as Into<OsString>>::into(id));
    std::fs::read(path).unwrap().into_boxed_slice()
}

#[tokio::test]
async fn write_valid_manifest() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, ID, ChunkID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let chunk2 = ChunkID::new([3; 32]);
    let mut builder = backend.create_manifest(&id, &creator).await.unwrap();
    builder.feed(&chunk1).await.unwrap();
    builder.feed(&chunk2).await.unwrap();
    let before = std::time::SystemTime::now();
    builder.close().await.unwrap();
    let after = std::time::SystemTime::now();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 152);
    assert_eq!(manifest[0], 8);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[1..9]).unwrap(),
        creator
    );
    assert_eq!(manifest[9], 64);
    assert_eq!(
        <ChunkID as TryFrom<&[u8]>>::try_from(&manifest[10..74]).unwrap(),
        chunk1
    );
    assert_eq!(manifest[74], 64);
    assert_eq!(
        <ChunkID as TryFrom<&[u8]>>::try_from(&manifest[75..139]).unwrap(),
        chunk2
    );
    assert_eq!(manifest[139], 0);
    let timestamp = timestamp_from_bytes(manifest[140..152].try_into().unwrap()).unwrap();
    assert!(timestamp >= before);
    assert!(timestamp <= after);
}

#[tokio::test]
async fn add_empty_chunk() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, ID, EmptyID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let mut builder = backend.create_manifest(&id, &creator).await.unwrap();

    assert!(matches!(
        builder.start_send_unpin(&EmptyID),
        Err(ManifestEncodingError::InvalidChunk)
    ));
}

#[tokio::test]
async fn write_smallest_manifest_content() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, EmptyID, SmallID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let chunk1 = SmallID { inner: 2 };
    let mut builder = backend.create_manifest(&id, &EmptyID).await.unwrap();
    builder.feed(&chunk1).await.unwrap();
    builder.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 16);
    assert_eq!(&manifest[..4], &[0, 1, 2, 0]);
}

#[tokio::test]
async fn write_empty_manifest() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, EmptyID, ID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let mut builder = pin!(backend.create_manifest(&id, &EmptyID).await.unwrap());
    builder.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 14);
    assert_eq!(&manifest[..2], &[0, 0]);
}

#[tokio::test]
async fn remove_on_drop() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, EmptyID, ID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let builder = backend.create_manifest(&id, &EmptyID).await.unwrap();
    let upload_path = backend
        .directory()
        .join("incoming")
        .read_dir()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();

    assert!(upload_path.is_file());
    std::mem::drop(builder);
    assert!(!upload_path.exists());
    assert!(backend
        .directory()
        .join("manifests")
        .read_dir()
        .unwrap()
        .next()
        .is_none());
}

#[tokio::test]
async fn write_limits() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, BigID, BigID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let creator = BigID {
        inner: [1; u8::MAX as usize / 2],
    };
    let chunk = BigID {
        inner: [2; u8::MAX as usize / 2],
    };
    let mut builder = backend.create_manifest(&id, &creator).await.unwrap();
    builder.feed(&chunk).await.unwrap();
    builder.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 525);
    assert_eq!(manifest[0], u8::MAX);
    assert_eq!(
        &manifest[1..256],
        <&BigID as Into<OsString>>::into(&creator).as_encoded_bytes()
    );
    assert_eq!(manifest[256], u8::MAX);
    assert_eq!(
        &manifest[257..512],
        <&BigID as Into<OsString>>::into(&chunk).as_encoded_bytes()
    );
    assert_eq!(manifest[512], 0);
}

#[tokio::test]
async fn write_chunk_after_close() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, EmptyID, ChunkID> = create_repository(tmpdir.path());
    let id = ID::new("manifest_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let mut builder = pin!(backend.create_manifest(&id, &EmptyID).await.unwrap());
    builder.close().await.unwrap();

    assert!(matches!(
        builder.feed(&chunk1).await,
        Err(ManifestEncodingError::InvalidOperation)
    ));
}
