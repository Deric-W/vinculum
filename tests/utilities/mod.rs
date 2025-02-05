//! Test utilities.
//!
//! The tests currently require the file repository but may in the future use
//! an in-memory one.

use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use futures::AsyncWriteExt;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::Path;
use std::pin::pin;
use vinculum::Repository;
use vinculum_benchmark::repository::{initialize, FileRepository};
use vinculum_benchmark::{ChunkID, ID};

pub fn assert_eq_unordered<A, B>(a: A, b: B)
where
    A: IntoIterator,
    B: IntoIterator<Item = A::Item>,
    A::Item: Eq + Hash + std::fmt::Debug,
{
    let items_a: Vec<_> = a.into_iter().collect();
    let items_b: Vec<_> = b.into_iter().collect();

    assert_eq!(
        items_a.len(),
        items_b.len(),
        "Iterators have different lengths: {} != {}",
        items_a.len(),
        items_b.len()
    );

    let set_a: HashSet<A::Item> = items_a.into_iter().collect();
    let set_b: HashSet<B::Item> = items_b.into_iter().collect();
    assert_eq!(set_a, set_b);
}

pub async fn assert_chunks<B>(repository: &FileRepository<ID, ID, ChunkID>, chunks: B)
where
    B: IntoIterator<Item = ChunkID>,
{
    let items: Vec<ChunkID> = repository
        .chunks()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq_unordered(items, chunks);
}

pub async fn assert_fossils<B>(repository: &FileRepository<ID, ID, ChunkID>, fossils: B)
where
    B: IntoIterator<Item = ChunkID>,
{
    let items: Vec<ChunkID> = repository
        .fossils()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq_unordered(items, fossils);
}

pub fn create_repository(tmpdir: &Path) -> FileRepository<ID, ID, ChunkID> {
    let repo_path = tmpdir.join("repository");
    initialize(&repo_path).unwrap();
    FileRepository::new(repo_path)
}

pub async fn create_manifest(
    repository: &FileRepository<ID, ID, ChunkID>,
    id: &ID,
    creator: &ID,
    chunks: &[ChunkID],
) {
    let mut builder = pin!(repository.create_manifest(id, creator).await.unwrap());
    for chunk in chunks {
        builder.feed(chunk).await.unwrap();
    }
    builder.close().await.unwrap();
}

pub async fn create_chunks(repository: &FileRepository<ID, ID, ChunkID>, chunks: &[ChunkID]) {
    for chunk in chunks.iter() {
        pin!(repository.add_chunk(chunk).await.unwrap())
            .close()
            .await
            .unwrap();
    }
}
