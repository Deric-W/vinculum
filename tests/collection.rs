//! Tests for fossil collection.

mod utilities;

use std::iter::Iterator;
use std::time::SystemTime;
use tempfile::tempdir;
use utilities::{
    assert_chunks, assert_eq_unordered, assert_fossils, create_chunks, create_manifest,
    create_repository,
};
use vinculum::{FossilCollection, FossilCollectionBuilder, Repository};
use vinculum_benchmark::{ChunkID, ID};

#[tokio::test]
async fn create_fossils() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest, &manifest, &chunks[..5]).await;
    let mut builder = FossilCollectionBuilder::new();
    for chunk in &chunks[5..] {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest.clone());
    let before = SystemTime::now();
    let fossil_collection = builder.collect_fossils(&repository, 3).await.unwrap();
    let after = SystemTime::now();

    assert_eq!(fossil_collection.fossils(), 5);
    assert_eq_unordered(fossil_collection.iter_fossils(), chunks[5..].iter());
    assert_eq!(fossil_collection.seen_manifests(), 1);
    assert_eq_unordered(
        fossil_collection.iter_seen_manifests(),
        [&manifest].into_iter(),
    );
    assert!(fossil_collection.has_seen_manifest(&manifest));
    assert_chunks(&repository, chunks[..5].iter().cloned()).await;
    assert_fossils(&repository, chunks[5..].iter().cloned()).await;
    assert!(before < fossil_collection.timestamp());
    assert!(fossil_collection.timestamp() < after);
}

#[tokio::test]
async fn remove_fossil_candidates() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest, &manifest, &chunks[..5]).await;
    let mut builder = FossilCollectionBuilder::new();
    builder.add_referenced_chunk(chunks[7].clone());
    for chunk in &chunks[5..] {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest.clone());
    builder.add_referenced_chunk(chunks[8].clone());

    assert_eq!(builder.referenced_chunks(), 2);
    assert_eq_unordered(builder.iter_referenced_chunks(), chunks[7..9].iter());
    assert_eq!(builder.fossil_candidates(), 3);
    assert_eq_unordered(
        builder.iter_fossil_candidates(),
        [5, 6, 9].into_iter().map(|i| &chunks[i]),
    );

    let fossil_collection = builder.collect_fossils(&repository, 1).await.unwrap();

    assert_eq_unordered(
        fossil_collection.iter_fossils(),
        [5, 6, 9].into_iter().map(|i| &chunks[i]),
    );
    assert!(fossil_collection
        .iter_seen_manifests()
        .eq([manifest].iter()));
    assert_chunks(
        &repository,
        [0, 1, 2, 3, 4, 7, 8].into_iter().map(|i| chunks[i].clone()),
    )
    .await;
    assert_fossils(
        &repository,
        [5, 6, 9].into_iter().map(|i| chunks[i].clone()),
    )
    .await;
}

#[tokio::test]
async fn collect_all_fossils() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..11).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    repository.fossilize_chunk(&chunks[10]).await.unwrap();
    repository.fossilize_chunk(&chunks[8]).await.unwrap();
    create_chunks(&repository, &chunks[8..9]).await;
    let manifest = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest, &manifest, &chunks[..5]).await;
    let mut builder = FossilCollectionBuilder::new();
    for chunk in &chunks[5..10] {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest.clone());
    let before = SystemTime::now();
    let fossil_collection = builder.collect_all_fossils(&repository, 1).await.unwrap();
    let after = SystemTime::now();

    assert_eq!(fossil_collection.fossils(), 6);
    assert_eq_unordered(fossil_collection.iter_fossils(), chunks[5..].iter());
    assert_eq!(fossil_collection.seen_manifests(), 0);
    assert_chunks(&repository, chunks[..5].iter().cloned()).await;
    assert_fossils(&repository, chunks[5..].iter().cloned()).await;
    assert!(before < fossil_collection.timestamp());
    assert!(fossil_collection.timestamp() < after);
}

#[tokio::test]
async fn collect_all_fossils_still_referenced() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    for chunk in &chunks[..5] {
        repository.fossilize_chunk(chunk).await.unwrap();
    }
    let referencing_manifest = ID::new("referencing_manifest".to_string());
    create_manifest(
        &repository,
        &referencing_manifest,
        &referencing_manifest,
        &chunks[..3],
    )
    .await;
    let manifest = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest, &manifest, &chunks[5..]).await;
    let mut builder = FossilCollectionBuilder::new();
    for chunk in &chunks[5..] {
        builder.add_referenced_chunk(chunk.clone());
    }
    builder.add_seen_manifest(manifest.clone());
    for chunk in &chunks[..3] {
        builder.add_referenced_chunk(chunk.clone());
    }
    builder.add_seen_manifest(referencing_manifest.clone());
    let fossil_collection = builder.collect_all_fossils(&repository, 1).await.unwrap();

    assert_eq_unordered(fossil_collection.iter_fossils(), chunks[..5].iter());
    assert_eq!(fossil_collection.seen_manifests(), 0);

    fossil_collection.delete(&repository, 1).await.unwrap();

    assert_chunks(&repository, chunks[5..].iter().chain(&chunks[..3]).cloned()).await;
    assert_fossils(&repository, []).await;
}

#[tokio::test]
async fn remove_unreferenced_chunks() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest = ID::new("manifest_0".to_string());
    create_manifest(&repository, &manifest, &manifest, &chunks[5..]).await;
    let mut builder = FossilCollectionBuilder::new();
    for chunk in &chunks[5..] {
        builder.add_referenced_chunk(chunk.clone());
    }
    builder.add_seen_manifest(manifest.clone());
    builder.consider_all_chunks(&repository).await.unwrap();
    let fossil_collection = builder.collect_fossils(&repository, 1).await.unwrap();

    assert_eq_unordered(fossil_collection.iter_fossils(), chunks[..5].iter());
    assert_eq!(fossil_collection.seen_manifests(), 1);

    fossil_collection.delete(&repository, 1).await.unwrap();

    assert_chunks(&repository, chunks[5..].iter().cloned()).await;
    assert_fossils(&repository, []).await;
}

#[test]
fn merge_collections() {
    let fossils: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let manifests: Vec<ChunkID> = (10..20).map(|i| ChunkID::new([i; 32])).collect();
    let timestamp1 = SystemTime::now();
    let timestamp2 = timestamp1 + std::time::Duration::new(1, 0);
    let mut collection1 = FossilCollection::from_parts(
        fossils[..8].iter().cloned(),
        manifests[..8].iter().cloned(),
        timestamp1,
    );
    let collection2 = FossilCollection::from_parts(
        fossils[2..].iter().cloned(),
        manifests[2..].iter().cloned(),
        timestamp2,
    );
    collection1.merge(collection2);

    assert_eq!(collection1.fossils(), fossils.len());
    assert_eq_unordered(collection1.iter_fossils(), fossils.iter());
    assert_eq_unordered(collection1.iter_seen_manifests(), manifests[2..8].iter());
    assert_eq!(collection1.timestamp(), timestamp2);
}

#[test]
fn fossils_size_hint() {
    let fossils: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts(fossils, [], SystemTime::now());

    assert_eq!(collection.iter_fossils().size_hint(), (10, Some(10)));
}

#[test]
fn fossils_fused() {
    let fossils: Vec<ChunkID> = (0..10).map(|i| ChunkID::new([i; 32])).collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts(fossils, [], SystemTime::now());
    let mut iter = collection.iter_fossils();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}

#[test]
fn seen_manifests_size_hint() {
    let manifests: Vec<ID> = (10..20)
        .map(|i| ID::new(format!("manifest_{}", i)))
        .collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts([], manifests, SystemTime::now());

    assert_eq!(collection.iter_seen_manifests().size_hint(), (10, Some(10)));
}

#[test]
fn seen_manifests_fused() {
    let manifests: Vec<ID> = (10..20)
        .map(|i| ID::new(format!("manifest_{}", i)))
        .collect();
    let collection: FossilCollection<ID, ChunkID> =
        FossilCollection::from_parts([], manifests, SystemTime::now());
    let mut iter = collection.iter_seen_manifests();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}

#[test]
fn fossil_candidates_size_hint() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in 0..10 {
        builder.add_fossil_candidate(ChunkID::new([chunk; 32]));
    }

    assert_eq!(builder.iter_fossil_candidates().size_hint(), (10, Some(10)));
}

#[test]
fn fossil_candidates_fused() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in 0..10 {
        builder.add_fossil_candidate(ChunkID::new([chunk; 32]));
    }
    let mut iter = builder.iter_fossil_candidates();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}

#[test]
fn builder_seen_manifests_size_hint() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for manifest in 0..10 {
        builder.add_seen_manifest(ID::new(format!("manifest_{}", manifest)));
    }

    assert_eq!(builder.iter_seen_manifests().size_hint(), (10, Some(10)));
}

#[test]
fn builder_seen_manifests_fused() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for manifest in 0..10 {
        builder.add_seen_manifest(ID::new(format!("manifest_{}", manifest)));
    }
    let mut iter = builder.iter_seen_manifests();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}

#[test]
fn referenced_chunks_size_hint() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in 0..10 {
        builder.add_referenced_chunk(ChunkID::new([chunk; 32]));
    }

    assert_eq!(builder.iter_referenced_chunks().size_hint(), (10, Some(10)));
}

#[test]
fn referenced_chunks_fused() {
    let mut builder = FossilCollectionBuilder::<ID, ChunkID>::new();
    for chunk in 0..10 {
        builder.add_referenced_chunk(ChunkID::new([chunk; 32]));
    }
    let mut iter = builder.iter_referenced_chunks();
    for _ in iter.by_ref() {}

    assert!(iter.next().is_none());
}
