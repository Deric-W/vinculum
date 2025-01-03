//! Tests for fossil deletion.

use super::{create_chunks, create_manifest, create_repository};
use crate::ID;
use futures::io::AsyncWriteExt;
use futures::stream::{StreamExt, TryStreamExt};
use std::collections::HashSet;
use std::iter::Iterator;
use std::pin::pin;
use std::time::SystemTime;
use tempfile::tempdir;
use vinculum::{
    backends::files::FileBackend, ChunkBackend, ClientBackend, FossilCollection,
    FossilCollectionBuilder, FossilDeletionError, Repository,
};

#[tokio::test]
async fn delete_collection() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID { inner: [42; 32] };
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID { inner: [43; 32] };
    create_manifest(&repository, &manifest2, &manifest2, &chunks[2..]).await;
    let mut builder = FossilCollectionBuilder::<ID, ID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    <FileBackend as Repository<ID, ID, ID>>::remove_manifest(&repository, &manifest1)
        .await
        .unwrap();
    collection.delete::<_, ID>(&repository, 1).await.unwrap();

    assert_eq!(
        repository
            .chunks()
            .await
            .unwrap()
            .try_collect::<HashSet<ID>>()
            .await
            .unwrap(),
        chunks[2..].iter().cloned().collect()
    );
    assert!(<FileBackend as ChunkBackend<ID>>::fossils(&repository)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
}

#[tokio::test]
async fn delete_with_seen_manifests() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID { inner: [42; 32] };
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID { inner: [43; 32] };
    create_manifest(&repository, &manifest2, &manifest2, &chunks[5..]).await;
    let mut builder = FossilCollectionBuilder::<ID, ID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest2.clone());
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    <FileBackend as Repository<ID, ID, ID>>::remove_manifest(&repository, &manifest1)
        .await
        .unwrap();
    collection.delete::<_, ID>(&repository, 1).await.unwrap();

    assert_eq!(
        repository
            .chunks()
            .await
            .unwrap()
            .try_collect::<HashSet<ID>>()
            .await
            .unwrap(),
        chunks[5..].iter().cloned().collect()
    );
    assert!(<FileBackend as ChunkBackend<ID>>::fossils(&repository)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
}

#[tokio::test]
async fn delete_collection_too_early() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let client1 = ID { inner: [24; 32] };
    pin!(repository.add_client(&client1).await.unwrap())
        .close()
        .await
        .unwrap();
    let client2 = ID { inner: [25; 32] };
    pin!(repository.add_client(&client2).await.unwrap())
        .close()
        .await
        .unwrap();
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID { inner: [42; 32] };
    create_manifest(&repository, &manifest1, &client1, chunks.as_slice()).await;
    let mut builder = FossilCollectionBuilder::<ID, ID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();

    assert!(matches!(
        collection.delete::<_, ID>(&repository, 1).await,
        Err(FossilDeletionError::TooEarly)
    ));

    let manifest2 = ID { inner: [43; 32] };
    create_manifest(&repository, &manifest2, &client1, chunks.as_slice()).await;

    assert!(matches!(
        collection.delete::<_, ID>(&repository, 1).await,
        Err(FossilDeletionError::TooEarly)
    ));

    let manifest3 = ID { inner: [44; 32] };
    create_manifest(&repository, &manifest3, &client2, chunks.as_slice()).await;

    assert!(matches!(
        collection.delete::<_, ID>(&repository, 1).await,
        Ok(())
    ));
}

#[tokio::test]
async fn pipelined_deletion() {
    let tmpdir = tempdir().unwrap();
    let repository = create_repository(tmpdir.path());
    let chunks: Vec<ID> = (0..10).map(|i| ID { inner: [i; 32] }).collect();
    create_chunks(&repository, chunks.as_slice()).await;
    let manifest1 = ID { inner: [42; 32] };
    create_manifest(&repository, &manifest1, &manifest1, &chunks[..5]).await;
    let manifest2 = ID { inner: [43; 32] };
    create_manifest(&repository, &manifest2, &manifest2, &chunks[2..]).await;
    let manifest3 = ID { inner: [44; 32] };
    create_manifest(&repository, &manifest3, &manifest3, &chunks[5..]).await;
    let manifest4 = ID { inner: [45; 32] };
    let mut builder = FossilCollectionBuilder::<ID, ID>::new();
    for chunk in chunks[..5].iter() {
        builder.add_fossil_candidate(chunk.clone());
    }
    builder.add_seen_manifest(manifest3.clone());
    builder.add_seen_manifest(manifest4);
    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    <FileBackend as Repository<ID, ID, ID>>::remove_manifest(&repository, &manifest1)
        .await
        .unwrap();
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

    assert_eq!(
        repository
            .chunks()
            .await
            .unwrap()
            .try_collect::<HashSet<ID>>()
            .await
            .unwrap(),
        chunks[2..].iter().cloned().collect()
    );
    assert!(<FileBackend as ChunkBackend<ID>>::fossils(&repository)
        .await
        .unwrap()
        .next()
        .await
        .is_none());

    let collection = builder.collect_fossils(&repository, 1).await.unwrap();
    <FileBackend as Repository<ID, ID, ID>>::remove_manifest(&repository, &manifest2)
        .await
        .unwrap();

    assert_eq!(
        repository
            .chunks()
            .await
            .unwrap()
            .try_collect::<HashSet<ID>>()
            .await
            .unwrap(),
        chunks[5..].iter().cloned().collect()
    );
    assert_eq!(
        repository
            .fossils()
            .await
            .unwrap()
            .try_collect::<HashSet<ID>>()
            .await
            .unwrap(),
        chunks[2..5].iter().cloned().collect()
    );
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
    let chunks: Vec<ID> = (0..2).map(|i| ID { inner: [i; 32] }).collect();
    let manifests: Vec<ID> = [42, 43]
        .into_iter()
        .map(|i| ID { inner: [i; 32] })
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
            .collect::<Vec<ID>>(),
        &chunks[..1]
    );
    assert_eq!(
        builder
            .iter_referenced_chunks()
            .cloned()
            .collect::<Vec<ID>>(),
        &chunks[1..]
    );
    assert!(builder.has_referenced_chunk(&chunks[1]));
    assert!(!builder.has_referenced_chunk(&chunks[0]));
}
