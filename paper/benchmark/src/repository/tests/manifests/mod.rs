//! Tests for reading and writing manifests

use super::{create_repository, EmptyID};
use crate::repository::{FileRepository, ManifestDecodingError, ManifestEncodingError};
use crate::{ChunkID, ID};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::pin::{pin, Pin};
use tempfile::tempdir;
use vinculum::{Manifest, Repository};

mod reading;
mod writing;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BigID {
    inner: [u8; u8::MAX as usize / 2],
}

impl TryFrom<&[u8]> for BigID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let filler: u8 = 'x'.try_into().unwrap();
        if matches!(value.get((u8::MAX - 1) as usize), Some(b) if *b == filler) {
            let mut buf = [0; u8::MAX as usize / 2];
            match hex::decode_to_slice(&value[..(u8::MAX - 1) as usize], &mut buf) {
                Ok(()) => Ok(BigID { inner: buf }),
                Err(_) => Err(()),
            }
        } else {
            Err(())
        }
    }
}

impl From<&BigID> for OsString {
    fn from(val: &BigID) -> Self {
        let mut hexbytes = hex::encode(val.inner);
        hexbytes.push('x');
        hexbytes.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SmallID {
    inner: u8,
}

impl TryFrom<&[u8]> for SmallID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            Ok(SmallID { inner: value[0] })
        } else {
            Err(())
        }
    }
}

impl From<&SmallID> for OsString {
    fn from(val: &SmallID) -> Self {
        let mut string = String::with_capacity(1);
        string.push(val.inner.into());
        string.into()
    }
}

#[tokio::test]
async fn list_manifests() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ChunkID, ID, ChunkID> = create_repository(tmpdir.path());
    for i in 0..10 {
        let id = ChunkID::new([i; 32]);
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ChunkID as Into<OsString>>::into(&id));
        std::fs::write(manifest_path, "").unwrap();
    }
    let invalid_manifest_path = backend.directory().join("manifests").join("test");
    std::fs::write(invalid_manifest_path, "").unwrap();
    let manifests: Vec<ChunkID> = pin!(backend.manifests().await.unwrap())
        .map(|id| id.unwrap())
        .collect()
        .await;

    assert_eq!(manifests.len(), 10);
    for i in 0..10 {
        assert_eq!(manifests.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
async fn remove_manifest() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<ID, ID, ChunkID> = create_repository(tmpdir.path());
    for i in 0..3 {
        let id = ID::new(format!("manifest_{}", i));
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(manifest_path, "").unwrap();
    }
    let id = ID::new("manifest_1".to_string());
    backend.remove_manifest(&id).await.unwrap();

    for i in 0..3 {
        let id = ID::new(format!("manifest_{}", i));
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ID as Into<OsString>>::into(&id));
        assert_eq!(manifest_path.exists(), i != 1);
    }
    assert!(
        matches!(backend.remove_manifest(&id).await, Err(e) if e.kind() == ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn is_round_trip() {
    let tmpdir = tempdir().unwrap();
    let id = ID::new("manifest_0".to_string());
    let creator = ID::new("client_0".to_string());
    let chunk1 = ChunkID::new([2; 32]);
    let chunk2 = ChunkID::new([3; 32]);
    let backend: FileRepository<ID, ID, ChunkID> = create_repository(tmpdir.path());
    let mut builder = backend.create_manifest(&id, &creator).await.unwrap();
    builder.feed(&chunk1).await.unwrap();
    builder.feed(&chunk2).await.unwrap();
    let before = std::time::SystemTime::now();
    builder.close().await.unwrap();
    let after = std::time::SystemTime::now();
    let manifest_path = backend
        .directory()
        .join("manifests")
        .join(<&ID as Into<OsString>>::into(&id));

    assert!(manifest_path.is_file());

    let mut manifest = backend.manifest(&id).await.unwrap();
    let chunks: Vec<ChunkID> = Pin::new(&mut manifest).map(|r| r.unwrap()).collect().await;
    let (manifest_creator, manifest_timestamp) = manifest.into_metadata().await.unwrap();

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk1, chunk2]);
    assert!(manifest_timestamp >= before);
    assert!(manifest_timestamp <= after);
}

#[tokio::test]
async fn reject_empty_manifest_id() {
    let tmpdir = tempdir().unwrap();
    let backend: FileRepository<EmptyID, ID, ID> = create_repository(tmpdir.path());

    let res = backend.manifest(&EmptyID).await;
    assert!(matches!(res, Err(ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::Other));

    let res = backend
        .create_manifest(&EmptyID, &ID::new("client_0".to_string()))
        .await;
    assert!(matches!(res, Err(ManifestEncodingError::IoError(e)) if e.kind() == ErrorKind::Other));

    let res = backend.remove_manifest(&EmptyID).await;
    assert!(matches!(res, Err(e) if e.kind() == ErrorKind::Other));
}
