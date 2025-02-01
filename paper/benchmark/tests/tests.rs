//! Tests

use assert_cmd::prelude::*;
use futures::io::AsyncReadExt;
use futures::io::AsyncWriteExt;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::pin::{pin, Pin};
use std::process::Command;
use std::time::SystemTime;
use tempfile::tempdir;
use vinculum::backends::files::{initialize, FileBackend};
use vinculum::{ChunkBackend, ClientBackend, Manifest, ManifestTimestamp, Repository};
use vinculum_benchmark::{load_collection, ChunkID, ID};

fn create_repository(tmpdir: &Path) -> FileBackend {
    initialize(tmpdir).unwrap();
    FileBackend::new(tmpdir)
}

fn benchmark_command() -> Command {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap()
}

async fn create_dummy_manifest<R>(repository: &R, id: &ID, creator: &ID, chunks: &[ChunkID])
where
    R: Repository<ID, ID, ChunkID>,
{
    let mut builder = pin!(repository.create_manifest(id, creator).await.unwrap());
    for chunk in chunks {
        builder.feed(chunk).await.unwrap();
    }
    builder.close().await.unwrap();
}

async fn create_chunks<R>(repository: &R, chunks: &[ChunkID])
where
    R: Repository<ID, ID, ChunkID>,
{
    for chunk in chunks.iter() {
        pin!(repository.add_chunk(chunk).await.unwrap())
            .close()
            .await
            .unwrap();
    }
}

#[test]
fn init_repository() {
    let tmpdir = tempdir().unwrap();
    benchmark_command()
        .arg("init")
        .arg(tmpdir.path())
        .assert()
        .success();
    let directories: HashSet<OsString> = std::fs::read_dir(tmpdir.path())
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                Some(entry.file_name())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        directories,
        ["chunks", "clients", "fossils", "incoming", "manifests"]
            .into_iter()
            .map(|d| d.to_owned().into())
            .collect()
    );
}

#[tokio::test]
async fn create_manifest() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(&tmpdir.path().join("repository"));
    let filepath = tmpdir.path().join("chunk_file.bin");
    {
        let mut file = std::fs::File::create_new(&filepath).unwrap();
        for i in 0..10 {
            file.write_all(&[i; 4096]).unwrap();
        }
    }
    let before = SystemTime::now();
    benchmark_command()
        .arg("create")
        .arg(repository.directory())
        .args(["test_manifest", "test_client"])
        .arg(&filepath)
        .args(["--chunk-size", "4096"])
        .assert()
        .success();
    let after = SystemTime::now();

    let manifests: HashSet<ID> =
        <FileBackend as Repository<ID, ID, ChunkID>>::manifests(&repository)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    assert_eq!(
        manifests,
        [ID::new("test_manifest".to_owned())].into_iter().collect()
    );

    let (creator, mut chunks) = <FileBackend as Repository<ID, ID, ChunkID>>::manifest(
        &repository,
        &ID::new("test_manifest".to_owned()),
    )
    .await
    .unwrap()
    .into_chunks();
    assert_eq!(creator, ID::new("test_client".to_owned()));

    let manifest_chunks: Vec<ChunkID> = Pin::new(&mut chunks).try_collect().await.unwrap();
    assert_eq!(
        manifest_chunks,
        (0..10)
            .map(|i| ChunkID::new(Sha256::digest([i; 4096]).into()))
            .collect::<Vec<_>>()
    );

    let timestamp = chunks.into_timestamp().await.unwrap();
    assert!(timestamp > before);
    assert!(timestamp < after);

    let mut buf = Vec::with_capacity(4096);
    for (i, chunk) in manifest_chunks.iter().enumerate() {
        repository
            .chunk(chunk)
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf.as_slice(), &[i as u8; 4096]);
        buf.clear();
    }
}

#[tokio::test]
async fn create_manifest_smaller_last_chunk() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(&tmpdir.path().join("repository"));
    let filepath = tmpdir.path().join("chunk_file.bin");
    {
        let mut file = std::fs::File::create_new(&filepath).unwrap();
        for i in 0..10 {
            file.write_all(&[i; 4096]).unwrap();
        }
        file.write_all(&[11; 42]).unwrap();
    }
    benchmark_command()
        .arg("create")
        .arg(repository.directory())
        .args(["test_manifest", "test_client"])
        .arg(&filepath)
        .args(["--chunk-size", "4096"])
        .assert()
        .success();

    let chunks = pin!(
        <FileBackend as Repository<ID, ID, ChunkID>>::manifest(
            &repository,
            &ID::new("test_manifest".to_owned()),
        )
        .await
        .unwrap()
        .into_chunks()
        .1
    );
    let manifest_chunks: Vec<ChunkID> = chunks.try_collect().await.unwrap();
    assert_eq!(
        manifest_chunks,
        (0..10)
            .map(|i| ChunkID::new(Sha256::digest([i; 4096]).into()))
            .chain([ChunkID::new(Sha256::digest([11; 42]).into())].into_iter())
            .collect::<Vec<_>>()
    );

    let mut buf = Vec::with_capacity(4096);
    for (i, chunk) in manifest_chunks.iter().enumerate() {
        repository
            .chunk(chunk)
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        if i < 10 {
            assert_eq!(buf.as_slice(), &[i as u8; 4096]);
        } else {
            assert_eq!(buf.as_slice(), &[11; 42]);
        }
        buf.clear();
    }
}

#[tokio::test]
async fn collect_manifests() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(&tmpdir.path().join("repository"));
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = ["test1", "test2", "test3"]
        .into_iter()
        .map(|i| ID::new(i.to_owned()))
        .collect();
    create_chunks(&repository, chunks.as_slice()).await;
    create_dummy_manifest(&repository, &manifests[0], &manifests[0], &chunks[..4]).await;
    create_dummy_manifest(&repository, &manifests[1], &manifests[0], &chunks[3..7]).await;
    create_dummy_manifest(&repository, &manifests[2], &manifests[0], &chunks[7..]).await;
    let before = SystemTime::now();
    benchmark_command()
        .arg("collect")
        .arg(repository.directory())
        .arg(tmpdir.path().join("collection.cbor"))
        .args(["test2", "test3"])
        .assert()
        .success();
    let after = SystemTime::now();

    let collection = load_collection(&tmpdir.path().join("collection.cbor"));
    assert!(collection.timestamp() > before);
    assert!(collection.timestamp() < after);

    let repository_chunks: HashSet<ChunkID> = repository
        .chunks()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(repository_chunks, chunks[..4].iter().cloned().collect());

    let repository_manifests: HashSet<ID> =
        <FileBackend as Repository<ID, ID, ChunkID>>::manifests(&repository)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    assert_eq!(
        repository_manifests,
        manifests[..1].iter().cloned().collect()
    );
    assert_eq!(
        repository_manifests,
        collection.iter_seen_manifests().cloned().collect()
    );

    let fossils: HashSet<ChunkID> = repository
        .fossils()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(fossils, chunks[4..].iter().cloned().collect());
    assert_eq!(fossils, collection.iter_fossils().cloned().collect());
}

#[tokio::test]
async fn delete_manifests() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(&tmpdir.path().join("repository"));
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = ["test1", "test2", "test3"]
        .into_iter()
        .map(|i| ID::new(i.to_owned()))
        .collect();
    create_chunks(&repository, chunks.as_slice()).await;
    create_dummy_manifest(&repository, &manifests[0], &manifests[0], &chunks[..4]).await;
    create_dummy_manifest(&repository, &manifests[1], &manifests[0], &chunks[3..7]).await;
    create_dummy_manifest(&repository, &manifests[2], &manifests[0], &chunks[7..]).await;
    benchmark_command()
        .arg("collect")
        .arg(repository.directory())
        .arg(tmpdir.path().join("collection.cbor"))
        .args(["test2", "test3"])
        .assert()
        .success();
    create_dummy_manifest(&repository, &manifests[1], &manifests[0], &chunks[3..7]).await;
    benchmark_command()
        .arg("delete")
        .arg(repository.directory())
        .arg(tmpdir.path().join("collection.cbor"))
        .assert()
        .success();

    assert!(!tmpdir.path().join("collection.cbor").exists());
    let repository_chunks: HashSet<ChunkID> = repository
        .chunks()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(repository_chunks, chunks[..7].iter().cloned().collect());

    let repository_manifests: HashSet<ID> =
        <FileBackend as Repository<ID, ID, ChunkID>>::manifests(&repository)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    assert_eq!(
        repository_manifests,
        manifests[..2].iter().cloned().collect()
    );

    let fossils: HashSet<ChunkID> = repository
        .fossils()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(fossils, [].iter().cloned().collect());
}

#[tokio::test]
async fn delete_and_collect_manifests() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(&tmpdir.path().join("repository"));
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = ["test1", "test2", "test3"]
        .into_iter()
        .map(|i| ID::new(i.to_owned()))
        .collect();
    create_chunks(&repository, chunks.as_slice()).await;
    create_dummy_manifest(&repository, &manifests[0], &manifests[0], &chunks[..4]).await;
    create_dummy_manifest(&repository, &manifests[1], &manifests[0], &chunks[3..7]).await;
    create_dummy_manifest(&repository, &manifests[2], &manifests[0], &chunks[7..]).await;
    benchmark_command()
        .arg("collect")
        .arg(repository.directory())
        .arg(tmpdir.path().join("collection.cbor"))
        .args(["test2", "test3"])
        .assert()
        .success();
    create_dummy_manifest(&repository, &manifests[1], &manifests[0], &chunks[3..7]).await;
    benchmark_command()
        .arg("delete")
        .arg(repository.directory())
        .arg(tmpdir.path().join("collection.cbor"))
        .args(["--collect", "test1", "--collect", "test2"])
        .assert()
        .success();

    let collection = load_collection(&tmpdir.path().join("collection.cbor"));
    let repository_chunks: HashSet<ChunkID> = repository
        .chunks()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(repository_chunks, [].iter().cloned().collect());

    let repository_manifests: HashSet<ID> =
        <FileBackend as Repository<ID, ID, ChunkID>>::manifests(&repository)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
    assert_eq!(repository_manifests, [].iter().cloned().collect());
    assert_eq!(
        repository_manifests,
        collection.iter_seen_manifests().cloned().collect()
    );

    let fossils: HashSet<ChunkID> = repository
        .fossils()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(fossils, chunks[..7].iter().cloned().collect());
    assert_eq!(fossils, collection.iter_fossils().cloned().collect());
}

#[tokio::test]
async fn add_client() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    benchmark_command()
        .arg("add-client")
        .arg(tmpdir.path())
        .arg("test1")
        .assert()
        .success();
    benchmark_command()
        .arg("add-client")
        .arg(tmpdir.path())
        .arg("test2")
        .assert()
        .success();
    let clients: HashSet<ID> = repository
        .clients()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        clients,
        ["test1", "test2"]
            .into_iter()
            .map(|s| ID::new(s.to_owned()))
            .collect()
    );
}

#[tokio::test]
async fn remove_client() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    for client in ["test1", "test2"] {
        let id = ID::new(client.to_owned());
        let mut body = pin!(repository.add_client(&id).await.unwrap());
        body.close().await.unwrap();
    }
    benchmark_command()
        .arg("remove-client")
        .arg(tmpdir.path())
        .arg("test1")
        .assert()
        .success();
    let clients: HashSet<ID> = repository
        .clients()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        clients,
        ["test2"]
            .into_iter()
            .map(|s| ID::new(s.to_owned()))
            .collect()
    );
}
