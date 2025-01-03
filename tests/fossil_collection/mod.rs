//! Tests for fossil collection and deletion.
//!
//! These tests currently require the files backend but may in the future use
//! an in-memory one.

mod collection;
mod deletion;

use crate::ID;
use futures::sink::SinkExt;
use futures::AsyncWriteExt;
use std::path::Path;
use std::pin::pin;
use vinculum::backends::files;
use vinculum::Repository;

fn create_repository(tmpdir: &Path) -> files::FileBackend {
    let repo_path = tmpdir.join("repository");
    files::initialize(&repo_path).unwrap();
    files::FileBackend::new(repo_path)
}

async fn create_manifest<R>(repository: &R, id: &ID, creator: &ID, chunks: &[ID])
where
    R: Repository<ID, ID, ID>,
{
    let mut builder = pin!(repository.create_manifest(id, creator).await.unwrap());
    for chunk in chunks {
        builder.feed(chunk).await.unwrap();
    }
    builder.close().await.unwrap();
}

async fn create_chunks<R>(repository: &R, chunks: &[ID])
where
    R: Repository<ID, ID, ID>,
{
    for chunk in chunks.into_iter() {
        pin!(repository.add_chunk(chunk).await.unwrap())
            .close()
            .await
            .unwrap();
    }
}
