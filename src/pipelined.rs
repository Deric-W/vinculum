//! Implementations of pipelined fossil collection and deletion.

use crate::deletion::FossilDeleter;
use crate::{
    BuilderSeenManifests, CollectionSeenManifests, FossilCandidates, FossilCollection,
    FossilCollectionBuilder, FossilDeletionError, Manifest, ReferencedChunks, Repository,
};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;
use std::iter::{Chain, FusedIterator, Iterator};
use std::time::SystemTime;
use thiserror::Error;

/// Error of a failed call to [`PipelinedFossilCollectionBuilder::delete`].
#[derive(Error, Debug)]
#[error("error during pipelined fossil deletion: {error}")]
pub struct PipelinedFossilDeletionError<'a, M, I, C, E> {
    builder: PipelinedFossilCollectionBuilder<'a, M, I, C>,
    #[source]
    error: E,
}

impl<'a, M, I, C, E> PipelinedFossilDeletionError<'a, M, I, C, E> {
    fn new(
        builder: PipelinedFossilCollectionBuilder<'a, M, I, C>,
        error: E,
    ) -> PipelinedFossilDeletionError<'a, M, I, C, E> {
        PipelinedFossilDeletionError { builder, error }
    }

    /// The error which caused the fossil deletion operation to fail.
    pub fn error(&self) -> &E {
        &self.error
    }

    /// The builder which performed the fossil deletion operation.
    pub fn builder(&self) -> &PipelinedFossilCollectionBuilder<'a, M, I, C> {
        &self.builder
    }

    /// Consumes this error, returning the underlying error and
    /// builder to allow for the operation to be retried.
    pub fn into_inner(self) -> (PipelinedFossilCollectionBuilder<'a, M, I, C>, E) {
        (self.builder, self.error)
    }
}

/// Workaround since [`std::iter::Filter`] requires naming the closure.
#[derive(Debug)]
struct NotInSet<'a, M> {
    inner: Chain<BuilderSeenManifests<'a, M>, std::collections::hash_set::Iter<'a, M>>,
    set: &'a HashSet<M>,
}

impl<'a, M> NotInSet<'a, M> {
    fn new(
        seen_manifests: BuilderSeenManifests<'a, M>,
        expiring_manifests: std::collections::hash_set::Iter<'a, M>,
        set: &'a HashSet<M>,
    ) -> NotInSet<'a, M> {
        NotInSet {
            inner: seen_manifests.chain(expiring_manifests),
            set,
        }
    }
}

impl<'a, M> Iterator for NotInSet<'a, M>
where
    M: Eq + Hash,
{
    type Item = &'a M;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(|id| !self.set.contains(id))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.inner.size_hint();
        (0, upper) // can't know a lower bound, due to the filtering
    }
}

impl<M> FusedIterator for NotInSet<'_, M> where M: Eq + Hash {}

/// Iterator produced by [`PipelinedFossilCollectionBuilder::iter_seen_manifests`].
#[derive(Debug)]
pub struct PipelinedSeenManifests<'a, M> {
    inner: Chain<NotInSet<'a, M>, CollectionSeenManifests<'a, M>>,
}

impl<'a, M> PipelinedSeenManifests<'a, M>
where
    M: Eq + Hash,
{
    fn new(
        seen_manifests: NotInSet<'a, M>,
        collection_manifests: CollectionSeenManifests<'a, M>,
    ) -> PipelinedSeenManifests<'a, M> {
        PipelinedSeenManifests {
            inner: seen_manifests.chain(collection_manifests),
        }
    }
}

impl<'a, M> Iterator for PipelinedSeenManifests<'a, M>
where
    M: Eq + Hash,
{
    type Item = &'a M;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<M> FusedIterator for PipelinedSeenManifests<'_, M> where M: Eq + Hash {}

/// Combine fossil deletion with fossil collection.
///
/// When periodically collecting and deleting fossils it is possible to
/// streamline the process by creating a new fossil collection immediately
/// after deleting the previous one.
/// The problem with [`FossilCollection::delete`] is that the downloaded
/// manifest information is lost and has to be downloaded again when creating
/// a new fossil collection with [`FossilCollectionBuilder`].
/// To prevent this [`PipelinedFossilCollectionBuilder`] saves the information
/// required by [`FossilCollectionBuilder`] and can convert it into a
/// new instance after fossil deletion.
#[derive(Debug)]
pub struct PipelinedFossilCollectionBuilder<'a, M, I, C> {
    fossil_collection: &'a FossilCollection<M, C>,
    builder: FossilCollectionBuilder<M, C>,
    valid_clients: HashSet<I>,
    expiring_manifests: HashSet<M>,
}

impl<'a, M, I, C> PipelinedFossilCollectionBuilder<'a, M, I, C> {
    pub(crate) fn new(
        fossil_collection: &'a FossilCollection<M, C>,
    ) -> PipelinedFossilCollectionBuilder<'a, M, I, C> {
        PipelinedFossilCollectionBuilder {
            fossil_collection,
            builder: FossilCollectionBuilder::new(),
            valid_clients: HashSet::new(),
            expiring_manifests: HashSet::new(),
        }
    }

    /// The fossil collection which will be deleted by this builder.
    pub fn fossil_collection(&self) -> &'a FossilCollection<M, C> {
        self.fossil_collection
    }

    /// The number of chunks currently marked as fossil candidates.
    pub fn fossil_candidates(&self) -> usize {
        self.builder.fossil_candidates()
    }

    /// Iterate through the chunks currently marked as fossil candidates.
    pub fn iter_fossil_candidates(&self) -> FossilCandidates<'_, C> {
        self.builder.iter_fossil_candidates()
    }

    /// The number of currently referenced chunks.
    pub fn referenced_chunks(&self) -> usize {
        self.builder.referenced_chunks()
    }

    /// Iterate through the currently referenced chunks.
    pub fn iter_referenced_chunks(&self) -> ReferencedChunks<'_, C> {
        self.builder.iter_referenced_chunks()
    }
}

impl<M, I, C> PipelinedFossilCollectionBuilder<'_, M, I, C>
where
    M: Eq + Hash,
{
    /// Iterate through all manifests which where seen by either this builder
    /// or its associated fossil collection.
    pub fn iter_seen_manifests(&self) -> PipelinedSeenManifests<'_, M> {
        let seen_manifests = NotInSet::new(
            self.builder.iter_seen_manifests(),
            self.expiring_manifests.iter(),
            &self.fossil_collection.seen_manifests,
        );
        PipelinedSeenManifests::new(seen_manifests, self.fossil_collection.iter_seen_manifests())
    }

    /// Check whether a manifest has been seen by this builder or its fossil collection.
    ///
    /// This method is similar to [`FossilCollection::has_seen_manifest`] and
    /// can be used to limit the number of manifests which need to be downloaded
    /// on fossil deletion.
    /// Since this builder is also creating a fossil collection it makes
    /// sense to add them regardless to allow for a more efficient fossil collection.
    pub fn has_seen_manifest<Q>(&self, id: &Q) -> bool
    where
        M: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.builder.has_seen_manifest(id)
            || self.expiring_manifests.contains(id)
            || self.fossil_collection().has_seen_manifest(id)
    }
}

impl<M, I, C> PipelinedFossilCollectionBuilder<'_, M, I, C>
where
    M: Eq + Hash,
    I: Eq + Hash,
{
    /// This method is similar to [`FossilCollectionBuilder::add_seen_manifest`]
    /// but records additional information required for fossil deletion.
    ///
    /// It is required that any chunks referenced by this manifest have been
    /// passed to [`PipelinedFossilCollectionBuilder::add_referenced_chunk`].
    ///
    /// The fossil deletion can only proceed after for every client a manifest
    /// has been added which was created after the fossil collection of this
    /// builder finished.
    pub fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime) {
        if timestamp > self.fossil_collection.timestamp() {
            self.valid_clients.insert(creator);
        }
        // remove to prevent duplicates
        self.expiring_manifests.remove(&id);
        // add even if seen by the fossil collection to pass them to the
        // created FossilCollectionBuilder
        self.builder.add_seen_manifest(id);
    }

    /// This method is similar to [`PipelinedFossilCollectionBuilder::add_seen_manifest`]
    /// but does not pass the manifest to the created [`FossilCollectionBuilder`].
    ///
    /// This is useful when a manifest should be seen by the fossil deletion operation
    /// (for example to signal that a client has created a manifest since the fossil
    /// collection operation) but its chunks should be fossil candidates for the
    /// following fossil collection operation.
    ///
    /// Unlike [`PipelinedFossilCollectionBuilder::add_seen_manifest`] this method
    /// requires that any chunks referenced by this manifest have been passed to either
    /// [`PipelinedFossilCollectionBuilder::add_referenced_chunk`] or
    /// [`PipelinedFossilCollectionBuilder::add_fossil_candidate`].
    pub fn add_expiring_manifest(&mut self, id: M, creator: I, timestamp: SystemTime) {
        if timestamp > self.fossil_collection.timestamp() {
            self.valid_clients.insert(creator);
        }
        // do not insert if already seen
        if !self.fossil_collection.has_seen_manifest(&id) && !self.builder.has_seen_manifest(&id) {
            self.expiring_manifests.insert(id);
        }
    }
}

impl<M, I, C> PipelinedFossilCollectionBuilder<'_, M, I, C>
where
    C: Eq + Hash,
{
    /// Add a possibly unreferenced chunk.
    ///
    /// The chunk will be turned into a fossil on fossil collection unless
    /// it is (or was) added as referenced, in which case it will stay that way.
    pub fn add_fossil_candidate(&mut self, id: C) {
        self.builder.add_fossil_candidate(id);
    }

    /// Check if a chunk is a fossil candidate.
    pub fn has_fossil_candidate<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.builder.has_fossil_candidate(id)
    }

    /// Add a chunk referenced by a manifest.
    ///
    /// This method removes matching fossil candidates.
    pub fn add_referenced_chunk(&mut self, id: C) {
        self.builder.add_referenced_chunk(id);
    }

    /// Check if a chunk is referenced.
    pub fn has_referenced_chunk<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.builder.has_referenced_chunk(id)
    }
}

impl<'a, M, I, C> PipelinedFossilCollectionBuilder<'a, M, I, C>
where
    M: Eq + Hash,
    I: Eq + Hash,
    C: Eq + Hash,
{
    /// Perform the fossil deletion operation.
    ///
    /// This function is similar to [`FossilCollection::delete`] but returns
    /// a [`FossilCollectionBuilder`] on success which can be used to perform
    /// a fossil collection.
    pub async fn delete<R>(
        mut self,
        repository: &R,
        concurrency: impl Into<Option<usize>>,
    ) -> Result<
        FossilCollectionBuilder<M, C>,
        PipelinedFossilDeletionError<
            'a,
            M,
            I,
            C,
            FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>,
        >,
    >
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ClientID = I, ChunkID = C>,
    {
        // do not include manifests of fossil collection or expiring manifests
        // in builder since they might reference the fossil candidates
        match <Self as FossilDeleter<M, I, C>>::delete(&mut self, repository, concurrency.into())
            .await
        {
            Ok(()) => Ok(self.builder),
            Err(e) => Err(PipelinedFossilDeletionError::new(self, e)),
        }
    }
}

impl<M, I, C> FossilDeleter<M, I, C> for PipelinedFossilCollectionBuilder<'_, M, I, C>
where
    M: Hash + Eq,
    I: Hash + Eq,
    C: Hash + Eq,
{
    fn add_referenced_chunk(&mut self, id: C) {
        self.add_referenced_chunk(id);
    }

    fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime) {
        self.add_seen_manifest(id, creator, timestamp);
    }

    fn has_seen_manifest(&self, id: &M) -> bool {
        self.has_seen_manifest(id)
    }

    fn has_referenced_chunk(&self, id: &C) -> bool {
        // preserve fossil canidates until fossil collection
        self.builder.has_referenced_chunk(id) || self.builder.has_fossil_candidate(id)
    }

    fn has_valid_client(&self, id: &I) -> bool {
        self.valid_clients.contains(id)
    }

    fn fossil_collection(&self) -> &FossilCollection<M, C> {
        self.fossil_collection()
    }
}
