//! Tests for reading manifest files

use super::{BigID, EmptyID};
use crate::files_backend::{create_repository, ID};
use futures::io::{AsyncRead, AsyncReadExt};
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::{pin, Pin};
use tempfile::tempdir;
use vinculum::backends::files;
use vinculum::utils::timestamp_to_bytes;
use vinculum::{Manifest, ManifestChunks, ManifestTimestamp, Repository};

fn create_repository_with_manifest(tmpdir: &Path, id: &ID, content: &[u8]) -> files::FileBackend {
    let backend = create_repository(tmpdir);
    let manifest_path = backend.directory().join("manifests").join(<&ID as Into<OsString>>::into(id));
    std::fs::write(manifest_path, content).unwrap();
    backend
}

fn generate_valid_example_manifest(creator: &ID, chunk1: &ID, chunk2: &ID, data: [u8; 6], timestamp: std::time::SystemTime) -> Box<[u8]> {
    let mut manifest = Vec::with_capacity(220);
    manifest.push(64);
    manifest.extend_from_slice(<&ID as Into<OsString>>::into(creator).as_encoded_bytes());
    manifest.push(64);
    manifest.extend_from_slice(<&ID as Into<OsString>>::into(chunk1).as_encoded_bytes());
    manifest.push(64);
    manifest.extend_from_slice(<&ID as Into<OsString>>::into(chunk2).as_encoded_bytes());
    manifest.push(0);
    manifest.extend_from_slice(&3u16.to_be_bytes());
    manifest.extend_from_slice(&data[..3]);
    manifest.extend_from_slice(&3u16.to_be_bytes());
    manifest.extend_from_slice(&data[3..]);
    manifest.extend_from_slice(&0u16.to_be_bytes());
    manifest.extend_from_slice(&timestamp_to_bytes(timestamp).unwrap());
    manifest.into_boxed_slice()
}

#[tokio::test]
async fn read_valid_manifest() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let (manifest_creator, mut chunks_stream) = manifest.into_chunks();
    let chunks: Vec<ID> = Pin::new(&mut chunks_stream).map(|r| r.unwrap()).collect().await;
    let mut manifest_data = chunks_stream.into_data().await.unwrap();
    let mut data: Vec<u8> = Vec::new();
    let length = Pin::new(&mut manifest_data).read_to_end(&mut data).await.unwrap();
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk1, chunk2]);
    assert_eq!(length, 6);
    assert_eq!(data.as_slice(), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn read_empty_manifest() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 16];
    content[4..].copy_from_slice(&timestamp_to_bytes(std::time::UNIX_EPOCH).unwrap());
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, EmptyID, ID>>::manifest(&backend, &id).await.unwrap();
    let (manifest_creator, mut chunks_stream) = manifest.into_chunks();
    let chunks: Vec<ID> = Pin::new(&mut chunks_stream).map(|r| r.unwrap()).collect().await;
    let mut manifest_data = chunks_stream.into_data().await.unwrap();
    let mut data: Vec<u8> = Vec::new();
    let length = Pin::new(&mut manifest_data).read_to_end(&mut data).await.unwrap();
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(manifest_creator, EmptyID {});
    assert_eq!(chunks.as_slice(), &[]);
    assert_eq!(length, 0);
    assert_eq!(data.as_slice(), &[]);
    assert_eq!(manifest_timestamp, std::time::UNIX_EPOCH);
}

#[tokio::test]
async fn handle_empty_file() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &[]);
    let res = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await;

    assert!(matches!(res, Err(files::ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_truncated_creator() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &[2, 1]);
    let res = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await;

    assert!(matches!(res, Err(files::ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_invalid_creator() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &[1, 1]);
    let res = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await;

    assert!(matches!(res, Err(files::ManifestDecodingError::InvalidCreator)));
}

#[tokio::test]
async fn handle_truncated_chunk() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 64;
    content[66] = 48;
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let res = Pin::new(&mut manifest.into_chunks().1).next().await;

    assert!(matches!(res, Some(Err(files::ManifestDecodingError::IoError(e))) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_invalid_chunk() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 1;
    content[66] = 48;
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let res = Pin::new(&mut manifest.into_chunks().1).next().await;

    assert!(matches!(res, Some(Err(files::ManifestDecodingError::InvalidChunk))));
}

#[tokio::test]
async fn skip_chunks() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut manifest_data = manifest.into_chunks().1.into_data().await.unwrap();
    let mut data: Vec<u8> = Vec::new();
    let length = Pin::new(&mut manifest_data).read_to_end(&mut data).await.unwrap();
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(length, 6);
    assert_eq!(data.as_slice(), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn skip_pending_chunks() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut chunks_stream = manifest.into_chunks().1;
    let _ = Pin::new(&mut chunks_stream).next().await;
    let mut manifest_data = chunks_stream.into_data().await.unwrap();
    let mut data: Vec<u8> = Vec::new();
    let length = Pin::new(&mut manifest_data).read_to_end(&mut data).await.unwrap();
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(length, 6);
    assert_eq!(data.as_slice(), &[1, 2, 3, 4, 5, 6]);
    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn handle_truncated_data_length() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 67];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut data = pin!(manifest.into_chunks().1.into_data().await.unwrap());
    let res = poll_fn(|cx| data.as_mut().poll_read(cx, &mut [0])).await;

    assert!(matches!(res, Err(e) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_truncated_data() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 69];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 0;
    content[66..68].copy_from_slice(&3u16.to_be_bytes());
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut data = pin!(manifest.into_chunks().1.into_data().await.unwrap());
    let res1 = poll_fn(|cx| data.as_mut().poll_read(cx, &mut [0; 32])).await;
    let res2 = poll_fn(|cx| data.as_mut().poll_read(cx, &mut [0])).await;

    assert!(matches!(res1, Ok(1)));
    assert!(matches!(res2, Err(e) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_short_data_read() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut manifest_data = pin!(manifest.into_chunks().1.into_data().await.unwrap());
    
    for i in 1..7 {
        let mut buf = [0; 1];
        let res = poll_fn(|cx| manifest_data.as_mut().poll_read(cx, &mut buf)).await;

        assert!(matches!(res, Ok(1)));
        assert_eq!(buf[0], i);
    }
}

#[tokio::test]
async fn skip_data() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let manifest_timestamp = manifest.into_chunks().1.into_data().await.unwrap().into_timestamp().await.unwrap();

    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn skip_pending_data() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let mut manifest_data =  manifest.into_chunks().1.into_data().await.unwrap();
    let _ = Pin::new(&mut manifest_data).read(&mut [0]).await;
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(manifest_timestamp, timestamp);
}

#[tokio::test]
async fn handle_truncated_timestamp() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 69];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 0;
    content[66..68].copy_from_slice(&0u16.to_be_bytes());
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let res = manifest.into_chunks().1.into_data().await.unwrap().into_timestamp().await;

    assert!(matches!(res, Err(files::ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::UnexpectedEof));
}

#[tokio::test]
async fn handle_invalid_timestamp() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let mut content = [0; 80];
    content[0] = 64;
    content[1..65].copy_from_slice("00".repeat(32).as_bytes());
    content[65] = 0;
    content[66..68].copy_from_slice(&0u16.to_be_bytes());
    content[68..].copy_from_slice(&[u8::MAX; 12]);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let res = manifest.into_chunks().1.into_data().await.unwrap().into_timestamp().await;

    assert!(matches!(res, Err(files::ManifestDecodingError::InvalidTimestamp(u64::MAX, u32::MAX))));
}

#[tokio::test]
async fn referenced_chunks_equal_to_chunks() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let timestamp = std::time::SystemTime::now();
    let content = generate_valid_example_manifest(&creator, &chunk1, &chunk2, [1, 2, 3, 4, 5, 6], timestamp);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest1 = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let manifest2 = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id).await.unwrap();
    let chunks1: Vec<ID> = manifest1.into_chunks().1.map(|r| r.unwrap()).collect().await;
    let chunks2: Vec<ID> = manifest2.into_chunks().1.map(|r| r.unwrap()).collect().await;

    assert_eq!(chunks1, chunks2);
}

#[tokio::test]
async fn read_limits() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = BigID { inner: [1; u8::MAX as usize / 2] };
    let chunk = BigID { inner: [2; u8::MAX as usize / 2] };
    let mut content: Vec<u8> = Vec::with_capacity(527);
    content.push(u8::MAX);
    content.extend_from_slice(<&BigID as Into<OsString>>::into(&creator).as_encoded_bytes());
    content.push(u8::MAX);
    content.extend_from_slice(<&BigID as Into<OsString>>::into(&chunk).as_encoded_bytes());
    content.push(0);
    content.extend_from_slice(&0u16.to_be_bytes());
    content.extend_from_slice(&[0; 12]);
    let backend = create_repository_with_manifest(tmpdir.path(), &id, &content);
    let manifest = <files::FileBackend as Repository<ID, BigID, BigID>>::manifest(&backend, &id).await.unwrap();
    let (manifest_creator, mut chunks_stream) = manifest.into_chunks();
    let chunks: Vec<BigID> = Pin::new(&mut chunks_stream).map(|r| r.unwrap()).collect().await;

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk]);
}
