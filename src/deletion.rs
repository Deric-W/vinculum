//! Implementations of the fossil deletion step.

use crate::{FossilCollection, Manifest, Repository};
use futures::stream::{StreamExt, TryStreamExt};
use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::Hash;
use std::pin::{pin, Pin};
use std::time::SystemTime;
use thiserror::Error;

/// Error of a failed fossil deletion operation.
#[derive(Error, Debug)]
pub enum FossilDeletionError<R, M> {
    /// A repository operation failed.
    #[error("repository operation failed with {0}")]
    RepositoryError(#[source] R),
    /// A manifest reading operation failed
    #[error("manifest reading operation failed with {0}")]
    ManifestError(#[source] M),
    /// Some clients have not created a new manifest since the associated fossil collection finished.
    #[error("Some clients have not created a new manifest since the associated fossil collection finished")]
    TooEarly,
}

/// Delete a fossil collection.
///
/// This deleter is used internally by [`FossilCollection::delete`] and does not
/// track unreferenced chunks.
#[derive(Debug)]
pub struct SimpleFossilDeleter<'a, M, I, C> {
    fossil_collection: &'a FossilCollection<M, C>,
    referenced_chunks: HashSet<C>,
    valid_clients: HashSet<I>,
    seen_manifests: HashSet<M>,
}

impl<'a, M, I, C> SimpleFossilDeleter<'a, M, I, C> {
    pub(crate) fn new(
        fossil_collection: &'a FossilCollection<M, C>,
    ) -> SimpleFossilDeleter<'a, M, I, C> {
        SimpleFossilDeleter {
            fossil_collection,
            referenced_chunks: HashSet::new(),
            valid_clients: HashSet::new(),
            seen_manifests: HashSet::new(),
        }
    }
}

impl<M, I, C> SimpleFossilDeleter<'_, M, I, C>
where
    M: Hash + Eq,
    I: Hash + Eq,
    C: Hash + Eq,
{
    pub(crate) async fn delete<R>(
        &mut self,
        repository: &R,
        concurrency: Option<usize>,
    ) -> Result<(), FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ClientID = I, ChunkID = C>,
    {
        <Self as FossilDeleter<M, I, C>>::delete(self, repository, concurrency).await
    }
}

impl<M, I, C> FossilDeleter<M, I, C> for SimpleFossilDeleter<'_, M, I, C>
where
    M: Hash + Eq,
    I: Hash + Eq,
    C: Hash + Eq,
{
    fn add_referenced_chunk(&mut self, id: C) {
        self.referenced_chunks.insert(id);
    }

    fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime) {
        if timestamp > self.fossil_collection.timestamp() {
            self.valid_clients.insert(creator);
        }
        if !self.fossil_collection.has_seen_manifest(&id) {
            self.seen_manifests.insert(id);
        }
    }

    fn has_seen_manifest(&self, id: &M) -> bool {
        self.seen_manifests.contains(id) || self.fossil_collection.has_seen_manifest(id)
    }

    fn has_referenced_chunk(&self, id: &C) -> bool {
        self.referenced_chunks.contains(id)
    }

    fn has_valid_client(&self, id: &I) -> bool {
        self.valid_clients.contains(id)
    }

    fn fossil_collection(&self) -> &FossilCollection<M, C> {
        self.fossil_collection
    }
}

/// Internal trait which reduces code duplication.
pub(crate) trait FossilDeleter<M, I, C> {
    fn add_referenced_chunk(&mut self, id: C);

    fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime);

    fn has_seen_manifest(&self, id: &M) -> bool;

    fn has_referenced_chunk(&self, id: &C) -> bool;

    fn has_valid_client(&self, id: &I) -> bool;

    fn fossil_collection(&self) -> &FossilCollection<M, C>;

    async fn add_missing_manifests<R>(
        &mut self,
        repository: &R,
        concurrency: Option<usize>,
    ) -> Result<(), FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ClientID = I, ChunkID = C>,
    {
        let cell = RefCell::new(&mut *self);
        let manifest_stream = repository
            .manifests()
            .await
            .map_err(FossilDeletionError::RepositoryError)?
            .map_err(FossilDeletionError::RepositoryError);

        manifest_stream
            .try_for_each_concurrent(concurrency, |manifest| {
                check_manifest(repository, manifest, &cell)
            })
            .await?;
        Ok(())
    }

    async fn delete<R>(
        &mut self,
        repository: &R,
        concurrency: Option<usize>,
    ) -> Result<(), FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ClientID = I, ChunkID = C>,
    {
        // check Policy 3
        let mut client_stream = pin!(repository
            .clients()
            .await
            .map_err(FossilDeletionError::RepositoryError)?);
        while let Some(res) = client_stream.next().await {
            match res {
                Ok(client) if self.has_valid_client(&client) => (),
                Ok(_) => return Err(FossilDeletionError::TooEarly),
                Err(e) => return Err(FossilDeletionError::RepositoryError(e)),
            }
        }

        // iterate through manifests a second time to make sure manifests created during iteration are picked up
        self.add_missing_manifests(repository, concurrency).await?;

        // deal with fossils
        let fossil_stream = futures::stream::iter(self.fossil_collection().iter_fossils().map(Ok));
        fossil_stream
            .try_for_each_concurrent(concurrency, |fossil| async {
                let res = if self.has_referenced_chunk(fossil) {
                    repository.recover_fossil(fossil).await
                } else {
                    repository.delete_fossil(fossil).await
                };
                res.map_err(FossilDeletionError::RepositoryError)
            })
            .await?;

        Ok(())
    }
}

async fn check_manifest<R, D>(
    repository: &R,
    id: R::ManifestID,
    deleter: &RefCell<&mut D>,
) -> Result<(), FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>>
where
    R: Repository + ?Sized,
    D: FossilDeleter<
            R::ManifestID,
            <R::Manifest as Manifest>::ClientID,
            <R::Manifest as Manifest>::ChunkID,
        > + ?Sized,
{
    if deleter.borrow().has_seen_manifest(&id) {
        return Ok(());
    }
    let mut manifest = repository
        .manifest(&id)
        .await
        .map_err(FossilDeletionError::ManifestError)?;
    let mut pinned_chunks = Pin::new(&mut manifest);
    while let Some(res) = pinned_chunks.as_mut().next().await {
        let chunk = res.map_err(FossilDeletionError::ManifestError)?;
        deleter.borrow_mut().add_referenced_chunk(chunk);
    }
    let (creator, timestamp) = manifest
        .into_metadata()
        .await
        .map_err(FossilDeletionError::ManifestError)?;
    deleter
        .borrow_mut()
        .add_seen_manifest(id, creator, timestamp);
    Ok(())
}
