//! Tests for fossil deletion.

mod utilities;

use futures::io::AsyncWriteExt;
use std::iter::Iterator;
use std::pin::pin;
use std::time::SystemTime;
use tempfile::tempdir;
use utilities::{
    assert_chunks, assert_eq_unordered, assert_fossils, create_chunks, create_manifest,
    create_repository,
};
use vinculum::{FossilCollection, FossilCollectionBuilder, FossilDeletionError};
use vinculum_benchmark::{ChunkID, ID};

#[tokio::test]
async fn delete_collection() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID::new("manifest_1".to_string());
    create_manifest(&repository, &manifest2, &manifest2, &chunks[2..]).await;
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    repository.remove_manifest(&manifest1).await.unwrap();
    collection.delete(&repository, 1).await.unwrap();

    assert_chunks(&repository, chunks[2..].iter().cloned()).await;
    assert_fossils(&repository, chunks[..0].iter().cloned()).await;
}

#[tokio::test]
async fn delete_with_seen_manifests() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID::new("manifest_1".to_string());
    create_manifest(&repository, &manifest2, &manifest2, &chunks[5..]).await;
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest2.clone());
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    repository.remove_manifest(&manifest1).await.unwrap();
    collection.delete(&repository, 1).await.unwrap();

    assert_chunks(&repository, chunks[5..].iter().cloned()).await;
    assert_fossils(&repository, chunks[..0].iter().cloned()).await;
}

#[tokio::test]
async fn delete_collection_too_early() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let client1 = ID::new("client_0".to_string());
    pin!(repository.add_client(&client1).await.unwrap())
        .close()
        .await
        .unwrap();
    let client2 = ID::new("client_1".to_string());
    pin!(repository.add_client(&client2).await.unwrap())
        .close()
        .await
        .unwrap();
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest1, &client1, chunks.as_slice()).await;
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();

    assert!(matches!(
        collection.delete(&repository, 1).await,
        Err(FossilDeletionError::TooEarly)
    ));

    let manifest2 = ID::new("manifest_1".to_string());
    create_manifest(&repository, &manifest2, &client1, chunks.as_slice()).await;

    assert!(matches!(
        collection.delete(&repository, 1).await,
        Err(FossilDeletionError::TooEarly)
    ));

    let manifest3 = ID::new("manifest_2".to_string());
    create_manifest(&repository, &manifest3, &client2, chunks.as_slice()).await;

    assert!(matches!(collection.delete(&repository, 1).await, Ok(())));
}

#[tokio::test]
async fn pipelined_deletion() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID::new("manifest_1".to_string());
    create_manifest(&repository, &manifest2, &manifest2, &chunks[2..]).await;
    let manifest3 = ID::new("manifest_2".to_string());
    create_manifest(&repository, &manifest3, &manifest3, &chunks[5..]).await;
    let manifest4 = ID::new("manifest_3".to_string());
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest3.clone());
    builder.add_seen_manifest(manifest4);
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    repository.remove_manifest(&manifest1).await.unwrap();
    let mut builder = collection.pipelined_delete::<ID>();
    for chunk in &chunks[2..] {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_expiring_manifest(manifest2.clone(), manifest2.clone(), SystemTime::now());
    for chunk in &chunks[5..] {
        builder.add_referenced_chunk(chunk.clone());
    }
    builder.add_seen_manifest(manifest3.clone(), manifest3.clone(), SystemTime::now());

    let builder = builder.delete(&repository, 1).await.unwrap();

    assert_chunks(&repository, chunks[2..].iter().cloned()).await;
    assert_fossils(&repository, chunks[..0].iter().cloned()).await;

    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    repository.remove_manifest(&manifest2).await.unwrap();

    assert_chunks(&repository, chunks[5..].iter().cloned()).await;
    assert_fossils(&repository, chunks[2..5].iter().cloned()).await;
    assert_eq!(
        collection
            .iter_seen_manifests()
            .collect::<Vec<&ID>>()
            .as_slice(),
        &[&manifest3]
    );
}

#[test]
fn pipelined_deletion_chunks() {
    let chunks: Vec<ChunkID> = (0..2).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = [42, 43]
        .into_iter()
        .map(|i| ID::new(format!("manifest_{}", i)))
        .collect();
    let collection = FossilCollection::from_parts(
        chunks[..1].iter().cloned(),
        manifests[..1].iter().cloned(),
        SystemTime::now(),
    );
    let mut builder = collection.pipelined_delete::<ID>();

    assert_eq!(builder.fossil_candidates(), 0);
    assert_eq!(builder.referenced_chunks(), 0);
    assert!(builder.has_seen_manifest(&manifests[0]));

    builder.add_fossil_candidate(chunks[0].clone());
    builder.add_fossil_candidate(chunks[1].clone());
    builder.add_referenced_chunk(chunks[1].clone());
    builder.add_fossil_candidate(chunks[1].clone());

    assert_eq!(builder.fossil_candidates(), 1);
    assert_eq!(builder.referenced_chunks(), 1);
    assert!(builder.has_fossil_candidate(&chunks[0]));
    assert!(!builder.has_fossil_candidate(&chunks[1]));
    assert_eq!(
        builder
            .iter_fossil_candidates()
            .cloned()
            .collect::<Vec<ChunkID>>(),
        &chunks[..1]
    );
    assert_eq!(
        builder
            .iter_referenced_chunks()
            .cloned()
            .collect::<Vec<ChunkID>>(),
        &chunks[1..]
    );
    assert!(builder.has_referenced_chunk(&chunks[1]));
    assert!(!builder.has_referenced_chunk(&chunks[0]));
}

#[tokio::test]
async fn pipelined_move_expired_but_seen_manifest() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..2).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = (3..5).map(|i| ID::new(format!("manifest_{}", i))).collect();
    let collection = FossilCollection::from_parts(
        chunks[..1].iter().cloned(),
        manifests[..1].iter().cloned(),
        SystemTime::now(),
    );
    let mut builder = collection.pipelined_delete::<ID>();

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..1]);

    builder.add_expiring_manifest(
        manifests[1].clone(),
        manifests[1].clone(),
        SystemTime::now(),
    );

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..2]);

    builder.add_seen_manifest(
        manifests[1].clone(),
        manifests[1].clone(),
        SystemTime::now(),
    );

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..2]);

    let builder = builder.delete(&repository, 1).await.unwrap();

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[1..2]);
}

#[tokio::test]
async fn pipelined_ignore_seen_but_expired_manifest() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..2).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ID> = (3..5).map(|i| ID::new(format!("manifest_{}", i))).collect();
    let collection = FossilCollection::from_parts(
        chunks[..1].iter().cloned(),
        manifests[..1].iter().cloned(),
        SystemTime::now(),
    );
    let mut builder = collection.pipelined_delete::<ID>();

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..1]);

    builder.add_seen_manifest(
        manifests[1].clone(),
        manifests[1].clone(),
        SystemTime::now(),
    );

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..2]);

    builder.add_expiring_manifest(
        manifests[1].clone(),
        manifests[1].clone(),
        SystemTime::now(),
    );

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[..2]);

    let builder = builder.delete(&repository, 1).await.unwrap();

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests[1..2]);
}

#[test]
fn pipelined_seen_manifests_no_duplicates() {
    let manifests: Vec<ID> = (0..5).map(|i| ID::new(format!("manifest_{}", i))).collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts([], manifests[..2].iter().cloned(), SystemTime::now());
    let mut builder = collection.pipelined_delete::<ID>();
    for manifest in manifests[1..4].iter() {
        builder.add_expiring_manifest(manifest.clone(), manifest.clone(), SystemTime::now());
    }
    for manifest in manifests[3..5].iter() {
        builder.add_seen_manifest(manifest.clone(), manifest.clone(), SystemTime::now());
    }

    assert_eq_unordered(builder.iter_seen_manifests(), manifests.iter());
}

#[test]
fn pipelined_seen_manifests() {
    let manifests: Vec<ID> = (0..20)
        .map(|i| ID::new(format!("manifest_{}", i)))
        .collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts([], manifests[..10].iter().cloned(), SystemTime::now());
    let mut builder = collection.pipelined_delete::<ID>();
    let client = ID::new("client".to_string());
    for manifest in manifests[5..15].iter() {
        builder.add_seen_manifest(manifest.clone(), client.clone(), SystemTime::now());
    }
    for manifest in manifests[10..].iter() {
        builder.add_expiring_manifest(manifest.clone(), client.clone(), SystemTime::now());
    }

    assert_eq_unordered(builder.iter_seen_manifests(), &manifests);
}

#[test]
fn pipelined_seen_manifests_fused() {
    let manifests: Vec<ID> = (0..20)
        .map(|i| ID::new(format!("manifest_{}", i)))
        .collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts([], manifests[..10].iter().cloned(), SystemTime::now());
    let mut builder = collection.pipelined_delete::<ID>();
    let client = ID::new("client".to_string());
    for manifest in manifests[5..15].iter() {
        builder.add_seen_manifest(manifest.clone(), client.clone(), SystemTime::now());
    }
    for manifest in manifests[10..].iter() {
        builder.add_expiring_manifest(manifest.clone(), client.clone(), SystemTime::now());
    }
    let mut iter = builder.iter_seen_manifests();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}
