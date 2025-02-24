//! Tests for reading manifest files

use super::{create_repository, BigID, EmptyID};
use crate::repository::{utils::timestamp_to_bytes, FileRepository, ManifestDecodingError};
use crate::{ChunkID, ID};
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::{pin, Pin};
use tempfile::tempdir;
use vinculum::{Manifest, Repository};

fn create_repository_with_manifest<M, I, C>(
    tmpdir: &Path,
    id: &ID,
    content: &[u8],
) -> FileRepository<M, I, C> {
    let backend = create_repository(tmpdir);
    let manifest_path = backend
        .directory()
        .join("manifests")
        .join(<&ID as Into<OsString>>::into(id));
    std::fs::write(manifest_path, content).unwrap();
    backend
}

fn generate_valid_example_manifest(
    creator: &ID,
    chunk1: &ChunkID,
    chunk2: &ChunkID,
    timestamp: std::time::SystemTime,
) -> Box<[u8]> {
    let mut manifest = Vec::with_capacity(208);
    manifest.push(creator.inner.len().try_into().unwrap());
    manifest.extend_from_slice(<&ID as Into<OsString>>::into(creator).as_encoded_bytes());
    manifest.push(64);
    manifest.extend_from_slice(<&ChunkID as Into<OsString>>::into(chunk1).as_encoded_bytes());
    manifest.push(64);
    manifest.extend_from_slice(<&ChunkID as Into<OsString>>::into(chunk2).as_encoded_bytes());
    manifest.push(0);
    manifest.extend_from_slice(&timestamp_to_bytes(timestamp).unwrap());
    manifest.into_boxed_slice()
}

#[tokio::test]
async fn read_valid_manifest() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let chunk2 = ChunkID::new([3; 32]);
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, timestamp);
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = backend.manifest(&id).await.unwrap();
    let chunks: Vec<ChunkID> = Pin::new(&mut manifest).map(|r| r.unwrap()).collect().await;
    let (manifest_creator, manifest_timestamp) = manifest.into_metadata().await.unwrap();

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk1, chunk2]);
    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn read_empty_manifest() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let mut content = [0; 14];
    content[2..].copy_from_slice(&timestamp_to_bytes(std::time::UNIX_EPOCH).unwrap());
    let backend: FileRepository<ID, EmptyID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = backend.manifest(&id).await.unwrap();
    let chunks: Vec<ChunkID> = Pin::new(&mut manifest).map(|r| r.unwrap()).collect().await;
    let (manifest_creator, manifest_timestamp) = manifest.into_metadata().await.unwrap();

    assert_eq!(manifest_creator, EmptyID {});
    assert_eq!(chunks.as_slice(), &[]);
    assert_eq!(manifest_timestamp, std::time::UNIX_EPOCH);
}

#[tokio::test]
async fn handle_empty_file() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &[]);
    let res = backend.manifest(&id).await;

    assert!(
        matches!(res, Err(ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof)
    );
}

#[tokio::test]
async fn handle_truncated_creator() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &[2, 1]);
    let res = backend.manifest(&id).await;

    assert!(
        matches!(res, Err(ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof)
    );
}

#[tokio::test]
async fn handle_invalid_creator() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let backend: FileRepository<ID, ChunkID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &[1, 1]);
    let res = backend.manifest(&id).await;

    assert!(matches!(res, Err(ManifestDecodingError::InvalidCreator)));
}

#[tokio::test]
async fn handle_truncated_chunk() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 64;
    content[66] = 48;
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = pin!(backend.manifest(&id).await.unwrap());
    let res = manifest.next().await;

    assert!(
        matches!(res, Some(Err(ManifestDecodingError::IoError(e))) if e.kind() == ErrorKind::UnexpectedEof)
    );
}

#[tokio::test]
async fn handle_invalid_chunk() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 1;
    content[66] = 48;
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = pin!(backend.manifest(&id).await.unwrap());
    let res = manifest.next().await;

    assert!(matches!(
        res,
        Some(Err(ManifestDecodingError::InvalidChunk))
    ));
}

#[tokio::test]
async fn skip_chunks() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let chunk2 = ChunkID::new([3; 32]);
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, timestamp);
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = backend.manifest(&id).await.unwrap();
    let manifest_timestamp = manifest.into_metadata().await.unwrap().1;

    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn skip_pending_chunks() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let chunk2 = ChunkID::new([3; 32]);
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, timestamp);
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = backend.manifest(&id).await.unwrap();
    let _ = Pin::new(&mut manifest).next().await.unwrap();
    let manifest_timestamp = manifest.into_metadata().await.unwrap().1;

    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn handle_truncated_timestamp() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = backend.manifest(&id).await.unwrap();
    let res = manifest.into_metadata().await;

    assert!(
        matches!(res, Err(ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof)
    );
}

#[tokio::test]
async fn handle_invalid_timestamp() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let mut content = [0; 78];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 0;
    content[66..].copy_from_slice(&[u8::MAX; 12]);
    let backend: FileRepository<ID, ID, ChunkID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = backend.manifest(&id).await.unwrap();
    let res = manifest.into_metadata().await;

    assert!(matches!(
        res,
        Err(ManifestDecodingError::InvalidTimestamp(u64::MAX, u32::MAX))
    ));
}

#[tokio::test]
async fn read_limits() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let creator = BigID {
        inner: [1; u8::MAX as usize / 2],
    };
    let chunk = BigID {
        inner: [2; u8::MAX as usize / 2],
    };
    let mut content: Vec<u8> = Vec::with_capacity(527);
    content.push(u8::MAX);
    content.extend_from_slice(<&BigID as Into<OsString>>::into(&creator).as_encoded_bytes());
    content.push(u8::MAX);
    content.extend_from_slice(<&BigID as Into<OsString>>::into(&chunk).as_encoded_bytes());
    content.push(0);
    content.extend_from_slice(&0u16.to_be_bytes());
    content.extend_from_slice(&[0; 12]);
    let backend: FileRepository<ID, BigID, BigID> =
        create_repository_with_manifest(tmpdir.path(), &id, &content);
    let mut manifest = backend.manifest(&id).await.unwrap();
    let chunks: Vec<BigID> = Pin::new(&mut manifest).map(|r| r.unwrap()).collect().await;
    let manifest_creator = manifest.into_metadata().await.unwrap().0;

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk]);
}
