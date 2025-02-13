//! Implementations of the fossil collection step.

use crate::{FossilCollection, Manifest, Repository};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;
use std::iter::{ExactSizeIterator, FusedIterator, Iterator};
use std::pin::pin;
use std::time::SystemTime;
use thiserror::Error;

/// Error of a failed fossil collection operation.
#[derive(Error, Debug)]
#[error("error during fossil collection: {error}")]
pub struct FossilCollectionError<M, C, E> {
    builder: FossilCollectionBuilder<M, C>,
    #[source]
    error: E,
}

impl<M, C, E> FossilCollectionError<M, C, E> {
    fn new(builder: FossilCollectionBuilder<M, C>, error: E) -> FossilCollectionError<M, C, E> {
        FossilCollectionError { builder, error }
    }

    /// The error which caused the fossil collection operation to fail.
    pub fn error(&self) -> &E {
        &self.error
    }

    /// The builder which performed the fossil collection operation.
    pub fn builder(&self) -> &FossilCollectionBuilder<M, C> {
        &self.builder
    }

    /// Consumes this error, returning the underlying error and
    /// builder to allow for the operation to be retried.
    pub fn into_inner(self) -> (FossilCollectionBuilder<M, C>, E) {
        (self.builder, self.error)
    }
}

/// Iterator produced by [`FossilCollectionBuilder::iter_fossil_candidates`].
#[derive(Debug)]
pub struct FossilCandidates<'a, C> {
    inner: std::collections::hash_set::Iter<'a, C>,
}

impl<C> FossilCandidates<'_, C> {
    fn new(inner: std::collections::hash_set::Iter<C>) -> FossilCandidates<C> {
        FossilCandidates { inner }
    }
}

impl<'a, C> Iterator for FossilCandidates<'a, C> {
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<C> FusedIterator for FossilCandidates<'_, C> {}

impl<C> ExactSizeIterator for FossilCandidates<'_, C> {}

/// Iterator produced by [`FossilCollectionBuilder::iter_seen_manifests`].
#[derive(Debug)]
pub struct BuilderSeenManifests<'a, M> {
    inner: std::collections::hash_set::Iter<'a, M>,
}

impl<M> BuilderSeenManifests<'_, M> {
    fn new(inner: std::collections::hash_set::Iter<M>) -> BuilderSeenManifests<'_, M> {
        BuilderSeenManifests { inner }
    }
}

impl<'a, M> Iterator for BuilderSeenManifests<'a, M> {
    type Item = &'a M;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<M> FusedIterator for BuilderSeenManifests<'_, M> {}

impl<M> ExactSizeIterator for BuilderSeenManifests<'_, M> {}

/// Iterator produced by [`FossilCollectionBuilder::iter_referenced_chunks`].
#[derive(Debug)]
pub struct ReferencedChunks<'a, C> {
    inner: std::collections::hash_set::Iter<'a, C>,
}

impl<C> ReferencedChunks<'_, C> {
    fn new(inner: std::collections::hash_set::Iter<C>) -> ReferencedChunks<C> {
        ReferencedChunks { inner }
    }
}

impl<'a, C> Iterator for ReferencedChunks<'a, C> {
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<C> FusedIterator for ReferencedChunks<'_, C> {}

impl<C> ExactSizeIterator for ReferencedChunks<'_, C> {}

/// Builder for creating fossil collections.
///
/// This builder receives chunks which should be deleted and manifests
/// which where checked to not reference them.
/// Ideally this should include all manifests existing in the repository
/// at a specific point in time, but might exclude manifests which where
/// in the process of being created.
/// Excluding some manifests has no ill effects besides them being downloaded
/// and checked when the created fossil collection is deleted.
///
/// On why this does not cause chunks to be deleted prematurely see [`FossilCollection`].
#[derive(Debug)]
pub struct FossilCollectionBuilder<M, C> {
    seen_manifests: HashSet<M>,
    fossil_candidates: HashSet<C>,
    referenced_chunks: HashSet<C>,
}

impl<M, C> FossilCollectionBuilder<M, C> {
    /// Create a new instance, containing no fossil candidates or seen manifests.
    pub fn new() -> FossilCollectionBuilder<M, C> {
        FossilCollectionBuilder {
            seen_manifests: HashSet::new(),
            fossil_candidates: HashSet::new(),
            referenced_chunks: HashSet::new(),
        }
    }

    /// The number of fossil candidates.
    pub fn fossil_candidates(&self) -> usize {
        self.fossil_candidates.len()
    }

    /// The chunks marked as fossil candidates.
    pub fn iter_fossil_candidates(&self) -> FossilCandidates<C> {
        FossilCandidates::new(self.fossil_candidates.iter())
    }

    /// The number of referenced chunks.
    pub fn referenced_chunks(&self) -> usize {
        self.referenced_chunks.len()
    }

    /// Iterate through the currently referenced chunks.
    pub fn iter_referenced_chunks(&self) -> ReferencedChunks<C> {
        ReferencedChunks::new(self.referenced_chunks.iter())
    }

    /// The number of seen manifests.
    pub fn seen_manifests(&self) -> usize {
        self.seen_manifests.len()
    }

    /// The manifests seen by this builder.
    pub fn iter_seen_manifests(&self) -> BuilderSeenManifests<M> {
        BuilderSeenManifests::new(self.seen_manifests.iter())
    }

    /// Perform the fossil collection operation, creating a fossil collection.
    ///
    /// This method turns all fossil candidates into fossils and stores them
    /// together with the seen manifests and a timestamp in a new [`FossilCollection`].
    ///
    /// Should this operation fail it is possible to retry it by extracting the builder
    /// from the returned [`FossilCollectionError`].
    ///
    /// The `concurrency` argument controls the amount of actions executed
    /// concurrently, either as an upper limit (which can be passed without wrapping
    /// it in an [`Option`] first) or no limit when [`None`] or zero is passed.
    pub async fn collect_fossils<R>(
        self,
        repository: &R,
        concurrency: impl Into<Option<usize>>,
    ) -> Result<FossilCollection<M, C>, FossilCollectionError<M, C, R::Error>>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ChunkID = C>,
    {
        if let Err(e) = self
            .apply_fossil_operations(repository, concurrency.into())
            .await
        {
            return Err(FossilCollectionError::new(self, e));
        }

        let timestamp = SystemTime::now();
        let fossils: Vec<C> = self.fossil_candidates.into_iter().collect();
        Ok(FossilCollection::new(
            fossils,
            self.seen_manifests,
            timestamp,
        ))
    }

    async fn apply_fossil_operations<R>(
        &self,
        repository: &R,
        concurrency: Option<usize>,
    ) -> Result<(), R::Error>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ChunkID = C>,
    {
        let fossil_stream = futures::stream::iter(self.iter_fossil_candidates().map(Ok));
        fossil_stream
            .try_for_each_concurrent(concurrency, |fossil| repository.fossilize_chunk(fossil))
            .await?;

        Ok(())
    }
}

impl<M, C> FossilCollectionBuilder<M, C>
where
    C: Hash + Eq,
{
    /// Add a possibly unreferenced chunk.
    ///
    /// The chunk will be turned into a fossil on fossil collection unless
    /// it is (or was) added as referenced, in which case it will stay that way.
    pub fn add_fossil_candidate(&mut self, id: C) {
        if !self.referenced_chunks.contains(&id) {
            self.fossil_candidates.insert(id);
        }
    }

    /// Check if a chunk is a fossil candidate.
    pub fn has_fossil_candidate<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fossil_candidates.contains(id)
    }

    /// Add a chunk referenced by a manifest.
    ///
    /// This method removes matching fossil candidates.
    pub fn add_referenced_chunk(&mut self, id: C) {
        self.fossil_candidates.remove(&id);
        self.referenced_chunks.insert(id);
    }

    /// Check if a chunk is referenced.
    pub fn has_referenced_chunk<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.referenced_chunks.contains(id)
    }

    /// Consider all chunks existing in the repository as fossil candidates.
    ///
    /// This is a variant of [`FossilCollectionBuilder::add_fossil_candidate`]
    /// which can be used to remove chunks which are not referenced by any manifest,
    /// for example chunks which are left behind after creating a manifest was aborted.
    ///
    /// While chunks belonging to manifests being created or not passed to
    /// [`FossilCollectionBuilder::add_referenced_chunk`] for other reasons are
    /// turned into fossils they are recovered when the created [`FossilCollection`]
    /// is deleted.
    /// Since this can cause additional and unnecessary calls to [`Repository::fossilize_chunk`]
    /// and [`Repository::recover_fossil`] this method should be used sparingly.
    pub async fn consider_all_chunks<R>(&mut self, repository: &R) -> Result<(), R::Error>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ChunkID = C>,
    {
        let mut chunk_stream = pin!(repository.chunks().await?);
        while let Some(chunk) = chunk_stream.try_next().await? {
            self.add_fossil_candidate(chunk);
        }
        Ok(())
    }

    /// Variant of [`FossilCollectionBuilder::collect_fossils`] which cleans up orphaned fossils.
    ///
    /// Should a fossil collection be abandoned (or a failed fossil collection operation not retried successfully)
    /// where may be fossils left behind in the repository.
    ///
    /// To prevent these orphaned fossils from accumulating this method adds
    /// them to the existing list of fossils to either recover or delete them
    /// on fossil deletion.
    /// Doing this safely requires ignoring manifests added by [`FossilCollectionBuilder::add_seen_manifest`]
    /// which in turn causes all manifests being downloaded and checked on deletion.
    pub async fn collect_all_fossils<R>(
        self,
        repository: &R,
        concurrency: impl Into<Option<usize>>,
    ) -> Result<FossilCollection<M, C>, FossilCollectionError<M, C, R::Error>>
    where
        R: Repository<ManifestID = M> + ?Sized,
        R::Manifest: Manifest<ChunkID = C>,
    {
        let mut fossil_stream = match repository.fossils().await {
            Ok(stream) => pin!(stream),
            Err(e) => return Err(FossilCollectionError::new(self, e)),
        };

        let mut fossils: Vec<C> = Vec::with_capacity(fossil_stream.size_hint().0);
        while let Some(res) = fossil_stream.next().await {
            match res {
                // prevent duplicate fossils
                Ok(fossil) if !self.has_fossil_candidate(&fossil) => {
                    fossils.push(fossil);
                }
                Ok(_) => (),
                Err(e) => return Err(FossilCollectionError::new(self, e)),
            }
        }

        if let Err(e) = self
            .apply_fossil_operations(repository, concurrency.into())
            .await
        {
            return Err(FossilCollectionError::new(self, e));
        }

        let timestamp = SystemTime::now();
        fossils.extend(self.fossil_candidates);
        Ok(FossilCollection::new(
            fossils,
            HashSet::with_capacity(0),
            timestamp,
        ))
    }
}

impl<M, C> FossilCollectionBuilder<M, C>
where
    M: Hash + Eq,
{
    /// Add a manifest which does not reference any fossil candidates.
    ///
    /// It will be used to reduce the number of manifests which will be checked
    /// when the created fossil collection is deleted.
    ///
    /// Since [`FossilCollectionBuilder::add_referenced_chunk`] removes matching
    /// fossil candidates manifests which referenced chunks where added as such
    /// to this builder may be passed to this method.
    pub fn add_seen_manifest(&mut self, id: M) {
        self.seen_manifests.insert(id);
    }

    /// Check whether a manifest was added to this builder.
    pub fn has_seen_manifest<Q>(&self, id: &Q) -> bool
    where
        M: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.seen_manifests.contains(id)
    }
}

impl<M, C> Default for FossilCollectionBuilder<M, C> {
    fn default() -> Self {
        FossilCollectionBuilder::new()
    }
}
