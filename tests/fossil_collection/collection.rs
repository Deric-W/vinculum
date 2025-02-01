//! Tests for fossil collection.

use super::{create_chunks, create_manifest, create_repository};
use crate::{assert_chunks, assert_eq_unordered, assert_fossils, ID};
use std::iter::Iterator;
use std::time::SystemTime;
use tempfile::tempdir;
use vinculum::{ChunkBackend, FossilCollection, FossilCollectionBuilder};

#[tokio::test]
async fn create_fossils() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest = ID { inner: [42; 32] };
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
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest = ID { inner: [42; 32] };
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
    let chunks: Vec<ID> = (0..11).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    repository.make_fossil(&chunks[10]).await.unwrap();
    repository.make_fossil(&chunks[8]).await.unwrap();
    create_chunks(&repository, &chunks[8..9]).await;
    let manifest = ID { inner: [42; 32] };
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

#[test]
fn merge_collections() {
    let fossils: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    let manifests: Vec<ID> = (10..20).map(|i| ID { inner: [i; 32] }).collect();
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
