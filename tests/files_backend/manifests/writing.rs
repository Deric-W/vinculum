//! Tests for writing manifest files

use super::{BigID, SmallID};
use crate::files_backend::{create_repository, EmptyID};
use crate::ID;
use futures::io::AsyncWriteExt;
use futures::sink::{Sink, SinkExt};
use std::ffi::OsString;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::pin::pin;
use tempfile::tempdir;
use vinculum::backends::files;
use vinculum::utils::timestamp_from_bytes;
use vinculum::{ManifestBuilder, Repository};

fn read_manifest(backend: &files::FileBackend, id: &ID) -> Box<[u8]> {
    let path = backend
        .directory()
        .join("manifests")
        .join(<&ID as Into<OsString>>::into(id));
    std::fs::read(path).unwrap().into_boxed_slice()
}

#[tokio::test]
async fn write_valid_manifest() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let chunk2 = ID { inner: [3; 32] };
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
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 218);
    assert_eq!(manifest[0], 64);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[1..65]).unwrap(),
        creator
    );
    assert_eq!(manifest[65], 64);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[66..130]).unwrap(),
        chunk1
    );
    assert_eq!(manifest[130], 64);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[131..195]).unwrap(),
        chunk2
    );
    assert_eq!(manifest[195], 0);
    assert_eq!(&manifest[196..198], &6u16.to_be_bytes());
    assert_eq!(&manifest[198..204], &[1, 2, 3, 4, 5, 6]);
    assert_eq!(&manifest[204..206], &0u16.to_be_bytes());
    let timestamp = timestamp_from_bytes(manifest[206..218].try_into().unwrap()).unwrap();
    assert!(timestamp >= before);
    assert!(timestamp <= after);
}

#[tokio::test]
async fn add_empty_chunk() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let mut builder = <files::FileBackend as Repository<ID, ID, EmptyID>>::create_manifest(
        &backend, &id, &creator,
    )
    .await
    .unwrap();

    assert!(matches!(
        builder.start_send_unpin(&EmptyID),
        Err(files::ManifestEncodingError::InvalidChunk)
    ));
}

#[tokio::test]
async fn write_smallest_manifest_content() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk1 = SmallID { inner: 2 };
    let mut builder = <files::FileBackend as Repository<ID, EmptyID, SmallID>>::create_manifest(
        &backend, &id, &EmptyID,
    )
    .await
    .unwrap();
    builder.feed(&chunk1).await.unwrap();
    let mut data = <files::ManifestBuilder as ManifestBuilder<SmallID>>::add_data(builder)
        .await
        .unwrap();
    data.write_all(&[]).await.unwrap();
    data.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 18);
    assert_eq!(&manifest[..6], &[0, 1, 2, 0, 0, 0]);
}

#[tokio::test]
async fn write_empty_manifest() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let mut builder = pin!(
        <files::FileBackend as Repository<ID, EmptyID, ID>>::create_manifest(
            &backend, &id, &EmptyID
        )
        .await
        .unwrap()
    );
    poll_fn(|cx| <files::ManifestBuilder as Sink<&ID>>::poll_close(builder.as_mut(), cx))
        .await
        .unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 16);
    assert_eq!(&manifest[..4], &[0, 0, 0, 0]);
}

#[tokio::test]
async fn write_no_data() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let mut builder = pin!(
        <files::FileBackend as Repository<ID, ID, ID>>::create_manifest(&backend, &id, &creator)
            .await
            .unwrap()
    );
    builder.feed(&chunk1).await.unwrap();
    let before = std::time::SystemTime::now();
    poll_fn(|cx| <files::ManifestBuilder as Sink<&ID>>::poll_close(builder.as_mut(), cx))
        .await
        .unwrap();
    let after = std::time::SystemTime::now();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 145);
    assert_eq!(manifest[0], 64);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[1..65]).unwrap(),
        creator
    );
    assert_eq!(manifest[65], 64);
    assert_eq!(
        <ID as TryFrom<&[u8]>>::try_from(&manifest[66..130]).unwrap(),
        chunk1
    );
    assert_eq!(&manifest[130..133], &[0, 0, 0]);
    let timestamp = timestamp_from_bytes(manifest[133..145].try_into().unwrap()).unwrap();
    assert!(timestamp >= before);
    assert!(timestamp <= after);
}

#[tokio::test]
async fn remove_on_drop() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let builder = <files::FileBackend as Repository<ID, EmptyID, ID>>::create_manifest(
        &backend, &id, &EmptyID,
    )
    .await
    .unwrap();
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
    assert!(matches!(
        backend
            .directory()
            .join("manifests")
            .read_dir()
            .unwrap()
            .next(),
        None
    ));
}

#[tokio::test]
async fn write_empty_chunk_id() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let mut builder = <files::FileBackend as Repository<ID, EmptyID, EmptyID>>::create_manifest(
        &backend, &id, &EmptyID,
    )
    .await
    .unwrap();

    assert!(matches!(
        builder.feed(&EmptyID).await,
        Err(files::ManifestEncodingError::InvalidChunk)
    ));
}

#[tokio::test]
async fn write_limits() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let creator = BigID {
        inner: [1; u8::MAX as usize / 2],
    };
    let chunk = BigID {
        inner: [2; u8::MAX as usize / 2],
    };
    let mut builder = <files::FileBackend as Repository<ID, BigID, BigID>>::create_manifest(
        &backend, &id, &creator,
    )
    .await
    .unwrap();
    builder.feed(&chunk).await.unwrap();
    let mut data = <files::ManifestBuilder as ManifestBuilder<SmallID>>::add_data(builder)
        .await
        .unwrap();
    data.write_all(vec![42; u16::MAX as usize * 3].as_slice())
        .await
        .unwrap();
    data.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 197138);
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
    assert_eq!(&manifest[513..515], &u16::MAX.to_be_bytes());
    assert!(manifest[515..66050].iter().all(|b| *b == 42));
    assert_eq!(&manifest[66050..66052], &u16::MAX.to_be_bytes());
    assert!(manifest[66052..131587].iter().all(|b| *b == 42));
    assert_eq!(&manifest[131587..131589], &u16::MAX.to_be_bytes());
    assert!(manifest[131589..197124].iter().all(|b| *b == 42));
}

#[tokio::test]
async fn write_chunk_after_close() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk1 = ID { inner: [2; 32] };
    let mut builder = pin!(
        <files::FileBackend as Repository<ID, EmptyID, ID>>::create_manifest(
            &backend, &id, &EmptyID
        )
        .await
        .unwrap()
    );
    poll_fn(|cx| <files::ManifestBuilder as Sink<&ID>>::poll_close(builder.as_mut(), cx))
        .await
        .unwrap();

    assert!(matches!(
        builder.feed(&chunk1).await,
        Err(files::ManifestEncodingError::InvalidOperation)
    ));
}

#[tokio::test]
async fn write_data_after_close() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let builder = <files::FileBackend as Repository<ID, EmptyID, ID>>::create_manifest(
        &backend, &id, &EmptyID,
    )
    .await
    .unwrap();
    let mut data = <files::ManifestBuilder as ManifestBuilder<SmallID>>::add_data(builder)
        .await
        .unwrap();
    data.close().await.unwrap();

    assert!(matches!(data.write(&[42]).await, Err(e) if e.kind() == ErrorKind::Other));
}

#[tokio::test]
async fn small_writes() {
    let tmpdir = tempdir().unwrap();
    let id = ID { inner: [0; 32] };
    let creator = ID { inner: [1; 32] };
    let backend = create_repository(tmpdir.path());
    let builder =
        <files::FileBackend as Repository<ID, ID, ID>>::create_manifest(&backend, &id, &creator)
            .await
            .unwrap();
    let mut data = <files::ManifestBuilder as ManifestBuilder<ID>>::add_data(builder)
        .await
        .unwrap();
    for _ in 0..u16::MAX {
        data.write_all(&[42]).await.unwrap();
    }
    data.close().await.unwrap();
    let manifest = read_manifest(&backend, &id);

    assert_eq!(manifest.len(), 65617);
    assert_eq!(manifest[0], 64);
    assert_eq!(
        &manifest[1..65],
        <&ID as Into<OsString>>::into(&creator).as_encoded_bytes()
    );
    assert_eq!(manifest[65], 0);
    assert_eq!(&manifest[66..68], &u16::MAX.to_be_bytes());
    assert!(manifest[68..65603].iter().all(|b| *b == 42));
    assert_eq!(&manifest[65603..65605], &0u16.to_be_bytes());
}
