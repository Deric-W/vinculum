//! Tests for reading and writing manifests

use crate::files_backend::{create_repository, EmptyID};
use crate::ID;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::pin::{pin, Pin};
use tempfile::tempdir;
use vinculum::backends::files;
use vinculum::{Manifest, ManifestBuilder, ManifestChunks, ManifestTimestamp, Repository};

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

impl Into<OsString> for &BigID {
    fn into(self) -> OsString {
        let mut hexbytes = hex::encode(self.inner);
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

impl Into<OsString> for &SmallID {
    fn into(self) -> OsString {
        let mut string = String::with_capacity(1);
        string.push(self.inner.into());
        string.into()
    }
}

#[tokio::test]
async fn list_manifests() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for i in 0..10 {
        let id = ID { inner: [i; 32] };
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(manifest_path, "").unwrap();
    }
    let invalid_manifest_path = backend.directory().join("manifests").join("test");
    std::fs::write(invalid_manifest_path, "").unwrap();
    let manifests: Vec<ID> = pin!(<files::FileBackend as Repository<ID, ID, ID>>::manifests(
        &backend
    )
    .await
    .unwrap())
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
    let backend = create_repository(tmpdir.path());
    for i in 0..3 {
        let id = ID { inner: [i; 32] };
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(manifest_path, "").unwrap();
    }
    let id = ID { inner: [1; 32] };
    <files::FileBackend as Repository<ID, ID, ID>>::remove_manifest(&backend, &id)
        .await
        .unwrap();

    for i in 0..3 {
        let id = ID { inner: [i; 32] };
        let manifest_path = backend
            .directory()
            .join("manifests")
            .join(<&ID as Into<OsString>>::into(&id));
        assert_eq!(manifest_path.exists(), i != 1);
    }
    assert!(
        matches!( <files::FileBackend as Repository<ID, ID, ID>>::remove_manifest(&backend, &id).await, Err(e) if e.kind() == ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn is_round_trip() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
    let backend = create_repository(tmpdir.path());
    let mut builder =
        <files::FileBackend as Repository<ID, ID, ID>>::create_manifest(&backend, &id, &creator)
            .await
            .unwrap();
    builder.feed(&chunk1).await.unwrap();
    builder.feed(&chunk2).await.unwrap();
    let mut data = <files::ManifestBuilder as ManifestBuilder<ID>>::add_data(builder)
        .await
        .unwrap();
    data.write_all(&[1, 2, 3, 4, 5, 6]).await.unwrap();
    let before = std::time::SystemTime::now();
    data.close().await.unwrap();
    let after = std::time::SystemTime::now();
    let manifest_path = backend
        .directory()
        .join("manifests")
        .join(<&ID as Into<OsString>>::into(&id));

    assert!(manifest_path.is_file());

    let manifest = <files::FileBackend as Repository<ID, ID, ID>>::manifest(&backend, &id)
        .await
        .unwrap();
    let (manifest_creator, mut chunks_stream) = manifest.into_chunks();
    let chunks: Vec<ID> = Pin::new(&mut chunks_stream)
        .map(|r| r.unwrap())
        .collect()
        .await;
    let mut manifest_data = chunks_stream.into_data().await.unwrap();
    let mut data: Vec<u8> = Vec::new();
    let length = Pin::new(&mut manifest_data)
        .read_to_end(&mut data)
        .await
        .unwrap();
    let manifest_timestamp = manifest_data.into_timestamp().await.unwrap();

    assert_eq!(manifest_creator, creator);
    assert_eq!(chunks.as_slice(), &[chunk1, chunk2]);
    assert_eq!(length, 6);
    assert_eq!(data.as_slice(), &[1, 2, 3, 4, 5, 6]);
    assert!(manifest_timestamp >= before);
    assert!(manifest_timestamp <= after);
}

#[tokio::test]
async fn reject_empty_manifest_id() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());

    let res =
        <files::FileBackend as Repository<EmptyID, ID, ID>>::manifest(&backend, &EmptyID).await;
    assert!(
        matches!(res, Err(files::ManifestDecodingError::IoError(e)) if e.kind() == ErrorKind::Other)
    );

    let res = <files::FileBackend as Repository<EmptyID, ID, ID>>::create_manifest(
        &backend,
        &EmptyID,
        &ID { inner: [1; 32] },
    )
    .await;
    assert!(
        matches!(res, Err(files::ManifestEncodingError::IoError(e)) if e.kind() == ErrorKind::Other)
    );

    let res =
        <files::FileBackend as Repository<EmptyID, ID, ID>>::remove_manifest(&backend, &EmptyID)
            .await;
    assert!(matches!(res, Err(e) if e.kind() == ErrorKind::Other));
}
