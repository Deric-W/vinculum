//! Implementation of Lock-Free Deduplication in Rust.
//!
//! This crates provides an implementation of the algorithm used by tools
//! such as [Duplicacy](https://duplicacy.com) as described in their
//! [paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).
//!
//! ## Overview
//!
//! The most important object is the repository, which stores chunks, clients
//! and manifests and is available to a number of clients.
//! It is represented by the [`Repository`] trait, which has implementations
//! defined in the [`backends`] module.
//!
//! Clients represent individual users which may perform operations on the
//! repository at the same time as other clients, like creating chunks or
//! manifests.
//! A single client can only perform some operations like manifest creation
//! sequentially, while multiple clients can perform them in parallel.
//! Furthermore, clients have to be registered with the repository before
//! they perform any operations and should be removed when they won't create
//! new manifests for a long time.
//!
//! Manifests represent data uploaded by a client which has been divided
//! into chunks and stored in the repository.
//! Manifests only store references to their chunks and may share them with
//! other manifests, which requires periodic chunk removals after some manifests
//! have been deleted.
//!
//! Chunks are pieces of data uploaded to the repository and may be referenced
//! by manifests.
//! They allow for deduplicating manifest contents by associating them with an
//! id based on their content, causing chunks with the same content to only be
//! stored once in the repository.
//!
//! Chunk creation can be done in parallel (even by a single client), but chunk
//! removal is more complicated because other clients can be in the process of
//! creating manifests referencing them, which would cause invalid references
//! should these chunks be deleted.
//! To prevent this chunks should be first turned into a special type of chunk
//! called "fossils" by using [`FossilCollectionBuilder`], which produces a
//! [`FossilCollection`].
//! This collection can be deleted when every client has created a manifest
//! after the fossil collection was created, which will either permanently
//! delete fossils or turn them back into chunks.
//! It is possible to combine the deletion of a fossil collection with the
//! creation of the next one using [`PipelinedFossilCollectionBuilder`].
//!
//! ## Features
//!
//! - `files`: enables a repository implementation utilizing the local file system.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub mod backends;
pub mod utils;

use futures::future::FutureExt;
use futures::io::{AsyncRead, AsyncWrite};
use futures::sink::Sink;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::future::Future;
use std::hash::{Hash, RandomState};
use std::iter::{Extend, IntoIterator, Iterator};
use std::pin::{pin, Pin};
use std::time::SystemTime;

/// The client backend, with the type of client Id as a generic parameter (for example a UUID).
///
/// Clients represent actors which can access a repository independently.
pub trait ClientBackend<I> {
    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// Enumerate all clients currently registered with the repository.
    ///
    /// Changes by adding or removing clients during enumeration may or may not
    /// be picked up.
    fn clients(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<I, Self::Error>>, Self::Error>>;

    /// Request data associated with a client.
    ///
    /// Each client can have associated data stored in the repository.
    fn client(&self, id: &I) -> impl Future<Output = Result<impl AsyncRead, Self::Error>>;

    /// Register a client with the repository.
    ///
    /// The client will be registered when the async write is closed.
    ///
    /// A client has to be registered with the repository before
    /// he can perform any operations on it.
    /// Should the client already be registered with the repository
    /// its associated data will be overwritten.
    fn add_client(&self, id: &I) -> impl Future<Output = Result<impl AsyncWrite, Self::Error>>;

    /// Remove a client from the repository.
    ///
    /// Clients have to finish all pending operations before being removed from the repository.
    fn remove_client(&self, id: &I) -> impl Future<Output = Result<(), Self::Error>>;
}

/// The chunk backend with the type of chunk Id as a generic parameter.
///
/// Chunks represent pieces of data uploaded by clients and may be referenced by multiple manifest files.
/// This can for example be a hash of its contents.
/// It is important that chunks with the same content receive the same Id since it allows
/// duplicate data to be shared between multiple manifest files.
pub trait ChunkBackend<C> {
    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// Enumerate all chunks existing within the repository.
    ///
    /// Changes by adding or removing chunks during enumeration may or may not
    /// be picked up.
    fn chunks(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<C, Self::Error>>, Self::Error>>;

    /// Request the contents of a chunk.
    ///
    /// Its fossil can be used in case the original chunk does not exist.
    fn chunk(&self, id: &C) -> impl Future<Output = Result<impl AsyncRead, Self::Error>>;

    /// Check whether a chunk exists in the repository.
    ///
    /// The result of this query may be used to skip uploading chunks which
    /// already exist in the repository and may not consider fossils.
    fn has_chunk(&self, id: &C) -> impl Future<Output = Result<bool, Self::Error>>;

    /// Store a chunk in the repository.
    ///
    /// The chunk will be added when the async write is closed.
    ///
    /// Should a chunk already exist within a repository its contents
    /// will be overwritten.
    fn add_chunk(&self, id: &C) -> impl Future<Output = Result<impl AsyncWrite, Self::Error>>;

    /// Enumerate all fossils existing within the repository.
    ///
    /// Changes by fossilizing chunks or recovering or deletion fossils during
    /// enumeration may or may not be picked up.
    fn fossils(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<C, Self::Error>>, Self::Error>>;

    /// Turn a chunk into a fossil.
    ///
    /// Since a fossil may be referenced by new manifest files after creation
    /// it is allowed to be used in place of its original chunk, but not during
    /// manifest creation.
    /// During manifest creation chunks having the same ID as the original chunk of
    /// the fossil must be created again.
    ///
    /// When the chunk does not exists this method should not return an error
    /// but instead treat the fossil as having been created and return its ID.
    fn make_fossil(&self, id: &C) -> impl Future<Output = Result<(), Self::Error>>;

    /// Turn a fossil back into a chunk.
    ///
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    fn recover_fossil(&self, id: &C) -> impl Future<Output = Result<(), Self::Error>>;

    /// Delete a fossil permanently.
    ///
    /// A fossil being deleted may cause data loss should there still be
    /// manifests referencing it, which is why this operation should be
    /// performed by a fossil deletion step.
    ///
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    fn delete_fossil(&self, id: &C) -> impl Future<Output = Result<(), Self::Error>>;
}

/// Data uploaded by a client, represented as a sequence of chunks and additional data.
///
/// The generic parameters represent the type of client and chunk ids.
///
/// The process of reading a manifest begins by calling either [`Manifest::into_chunks`]
/// or [`Manifest::into_referenced_chunks`], depending whether the original data
/// or dependency information should be retrieved.
///
/// Should [`Manifest::into_chunks`] be called the additional data stored with
/// the manifest can be accessed by calling [`ManifestChunks::into_data`], which
/// will skip any remaining chunks.
///
/// The returned values of [`ManifestChunks::into_data`] and [`Manifest::into_referenced_chunks`]
/// both support the extraction of a timestamp recorded after all manifest chunks where uploaded
/// and all additional data was written, which can be done using the [`ManifestTimestamp::into_timestamp`]
/// function which will skip any remaining additional data or referenced chunks.
///
/// The reason for this sequence of operations is to allow implementations the
/// possibility to stream the manifest which prevents it from needing to be buffered in memory.
pub trait Manifest<I, C> {
    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// The Type representing the chunks and additional data uploaded by the user.
    type Chunks: ManifestChunks<C, Self::Error>;

    /// The Type representing the actual chunks referenced by the manifest.
    ///
    /// This can be different from [`Manifest::Chunks`] if for example the actual
    /// content of the manifest is itself stored in chunks.
    type ReferencedChunks: Stream<Item = Result<C, Self::Error>> + ManifestTimestamp<Self::Error>;

    /// Client which created this manifest.
    fn creator(&self) -> &I;

    /// Read this manifest as the chunks and additional data uploaded by the user.
    ///
    /// This also transfers ownership of the creator id.
    fn into_chunks(self) -> (I, Self::Chunks);

    /// Read this manifest as the actual chunks referenced by it.
    ///
    /// This also transfers ownership of the creator id.
    fn into_referenced_chunks(self) -> (I, Self::ReferencedChunks);
}

/// Sequence of chunks uploaded by a client.
///
/// The generic parameters represent the chunk ids and error type.
pub trait ManifestChunks<C, E>: Stream<Item = Result<C, E>> {
    /// The Type representing the additional manifest data.
    type Data: AsyncRead + ManifestTimestamp<E>;

    /// Continue with reading the additional manifest data uploaded by the client.
    fn into_data(self) -> impl Future<Output = Result<Self::Data, E>>;
}

/// Final state of the manifest decoding process, producing the timestamp.
///
/// The generic parameter represents the error type.
pub trait ManifestTimestamp<E> {
    /// Convert this manifest data into its timestamp.
    ///
    /// The timestamp is recorded after all chunks where uploaded and and additional
    /// data was written, but may be before the manifest upload finished.
    fn into_timestamp(self) -> impl Future<Output = Result<SystemTime, E>>;
}

/// Trait representing a manifest creation process with the type of chunk Id as a generic parameter.
///
/// It receives chunks which where added to the repository by the user beforehand,
/// either by uploading them or making sure they already exists.
///
/// A backend can not depend on the list of chunks being complete.
/// Middlewares can encode additional chunks in the additional data or
/// store the list as chunks themselves.
///
/// The manifest will be created when the builder or the [`AsyncWrite`] instance
/// returned by [`ManifestBuilder<I>.add_data`] is closed, trying to add additional
/// data after closing will result in errors.
pub trait ManifestBuilder<C>:
    for<'a> Sink<&'a C, Error = <Self as ManifestBuilder<C>>::Error>
{
    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// The Type representing additional manifest data.
    type Data: AsyncWrite;

    /// Add additional data to the manifest.
    ///
    /// Manifests can store custom data in addition to the sequence of its
    /// chunks, which allows clients to store (for example) additional metadata.
    ///
    /// When the returned async write is closed a timestamp will be recorded
    /// and the manifest created, replacing any existing manifest.
    fn add_data(
        self,
    ) -> impl Future<Output = Result<Self::Data, <Self as ManifestBuilder<C>>::Error>>;
}

/// A repository storing clients, chunks and manifests.
///
/// The generic parameters represent the type of manifest, client, chunk and fossil Ids.
pub trait Repository<M, I, C>: ClientBackend<I> + ChunkBackend<C> {
    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// Type of manifest stored in this repository.
    ///
    /// See [`Repository::manifest`].
    type Manifest: Manifest<I, C>;

    /// Type representing a manifest being created.
    ///
    /// See [`Repository::create_manifest`].
    type Builder: ManifestBuilder<C>;

    /// Enumerate all manifests existing within the repository.
    ///
    /// Changes by creating or removing manifests during enumeration may or may
    /// not be picked up.
    #[allow(clippy::type_complexity)]
    fn manifests(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<M, <Self as Repository<M, I, C>>::Error>>,
            <Self as Repository<M, I, C>>::Error,
        >,
    >;

    /// Request a manifest.
    fn manifest(
        &self,
        id: &M,
    ) -> impl Future<Output = Result<Self::Manifest, <Self::Manifest as Manifest<I, C>>::Error>>;

    /// Create a manifest.
    ///
    /// When a manifest with an id has been created recreating it with the
    /// same id but different content is not allowed.
    ///
    /// Furthermore, a client creating a manifest while the same client is downloading a
    /// chunk can cause the download to fail by making it appear as if the
    /// chunk does not exist.
    fn create_manifest(
        &self,
        id: &M,
        client: &I,
    ) -> impl Future<Output = Result<Self::Builder, <Self::Builder as ManifestBuilder<C>>::Error>>;

    /// Remove a manifest.
    ///
    /// Removing a manifest may leave unreferenced chunks behind, which is why
    /// this operation should be performed by a fossil collection step.
    fn remove_manifest(
        &self,
        id: &M,
    ) -> impl Future<Output = Result<(), <Self as Repository<M, I, C>>::Error>>;
}

/// Error of a failed fossil deletion operation.
#[derive(Debug)]
pub enum FossilDeletionError<R, M, I, C> {
    /// A repository operation failed.
    RepositoryError(R),
    /// A manifest reading operation failed
    ManifestError(M),
    /// A client operation failed.
    ClientError(I),
    /// A fossil operation failed
    FossilError(C),
    /// Some clients have not created a new manifest since the associated fossil collection finished.
    TooEarly,
}

impl<R, M, I, C> std::fmt::Display for FossilDeletionError<R, M, I, C>
where
    R: std::fmt::Display,
    M: std::fmt::Display,
    I: std::fmt::Display,
    C: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FossilDeletionError::RepositoryError(e) => write!(f, "repository operation failed with {}", e),
            FossilDeletionError::ManifestError(e) => write!(f, "manifest operation failed with {}", e),
            FossilDeletionError::ClientError(e) => write!(f, "client operation failed with {}", e),
            FossilDeletionError::FossilError(e) => write!(f, "fossil operation failed with {}", e),
            FossilDeletionError::TooEarly => write!(f, "Some clients have not created a new manifest since the associated fossil collection finished")
        }
    }
}

impl<R, M, I, C> std::error::Error for FossilDeletionError<R, M, I, C>
where
    R: std::error::Error,
    M: std::error::Error,
    I: std::error::Error,
    C: std::error::Error,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FossilDeletionError::RepositoryError(e) => e.source(),
            FossilDeletionError::ManifestError(e) => e.source(),
            FossilDeletionError::ClientError(e) => e.source(),
            FossilDeletionError::FossilError(e) => e.source(),
            FossilDeletionError::TooEarly => None,
        }
    }
}

/// A set of fossils await either recovery or deletion.
///
/// When deleting unreferenced chunks there is the possibility that deleting them
/// immediately can cause data loss since another client can be in the process of
/// creating a manifest referencing them.
///
/// To prevent this chunks are first turned into fossils by a [`FossilCollectionBuilder`]
/// and the resulting fossil collection can only be deleted after it can be proven
/// that no new manifest referencing these fossils can be created, which allows
/// erroneously created fossils to be safely recovered.
///
/// It is important that only one client performs the fossil collection and deletion
/// operations and that only one fossil collection exists per repository.
#[derive(Debug)]
pub struct FossilCollection<M, C> {
    // are assumed to be unique
    fossils: Vec<C>,
    seen_manifests: HashSet<M>,
    timestamp: std::time::SystemTime,
}

impl<M, C> FossilCollection<M, C> {
    fn new(
        fossils: Vec<C>,
        seen_manifests: HashSet<M>,
        timestamp: std::time::SystemTime,
    ) -> FossilCollection<M, C> {
        FossilCollection {
            fossils,
            seen_manifests,
            timestamp,
        }
    }

    /// Create an instance from raw data.
    ///
    /// This operation is required when deserializing a serialized fossil collection
    /// but can result in data loss or other problems should the following
    /// constraints be violated:
    ///
    ///  - the fossils may not contain duplicates
    ///  - the seen manifests do not reference the fossils
    ///  - the timestamp was recorded after the fossils where created
    pub fn from_parts<F, S>(
        fossils: F,
        seen_manifests: S,
        timestamp: std::time::SystemTime,
    ) -> FossilCollection<M, C>
    where
        F: IntoIterator<Item = C>,
        S: IntoIterator<Item = M>,
        M: Hash + Eq,
        C: Hash + Eq,
    {
        FossilCollection::new(
            fossils.into_iter().collect(),
            seen_manifests.into_iter().collect(),
            timestamp,
        )
    }

    /// The number fossils in this collection.
    pub fn fossils(&self) -> usize {
        self.fossils.len()
    }

    /// The fossils in this collection, without duplicates.
    pub fn iter_fossils(&self) -> std::slice::Iter<C> {
        self.fossils.iter()
    }

    /// The number of manifests seen when creating this collection.
    pub fn seen_manifests(&self) -> usize {
        self.seen_manifests.len()
    }

    /// The manifests seen when creating this collection.
    ///
    /// They are used to limit the number of manifests which need
    /// to be checked on deletion, see [`FossilCollection::has_seen_manifest`].
    pub fn iter_seen_manifests(&self) -> std::collections::hash_set::Iter<M> {
        self.seen_manifests.iter()
    }

    /// Check whether a specific manifest was seen when creating this collection.
    ///
    /// This can be used to limit the number of manifests which need
    /// to be checked on deletion since only new manifests need to be
    /// checked for fossil references.
    pub fn has_seen_manifest<Q>(&self, id: &Q) -> bool
    where
        M: Hash + Eq + Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.seen_manifests.contains(id)
    }

    /// A timestamp recorded after the fossil collection finished.
    ///
    /// It is used to determine if there are still clients which might
    /// be in the process of creating a manifest referencing fossils from
    /// this collection, see [`FossilCollection::delete`].
    pub fn timestamp(&self) -> std::time::SystemTime {
        self.timestamp
    }

    /// Merge two fossil collections.
    ///
    /// Normally there can be only one pending fossil collection.
    /// If one wishes to start another fossil collection without
    /// either deleting or abandoning the previous one it can be
    /// merged with the new one.
    pub fn merge(&mut self, mut other: FossilCollection<M, C>)
    where
        M: Hash + Eq,
        C: Hash + Eq,
    {
        self.timestamp = std::cmp::max(other.timestamp, self.timestamp);
        self.seen_manifests
            .retain(|manifest| other.has_seen_manifest(manifest));
        let existing_fossils: HashSet<&C> = self.fossils.iter().collect();
        other.fossils.retain(|f| !existing_fossils.contains(f));
        self.fossils.append(&mut other.fossils);
    }

    /// Delete this fossil collection.
    ///
    /// This function will check whether the fossil collection can be deleted
    /// by every client having created at least one new manifest after the timestamp
    /// of this fossil collection and downloads any unseen manifests if this is the
    /// case, returning a [`FossilDeletionError::TooEarly`] otherwise.
    ///
    /// Based on whether a fossil is referenced by these manifests or not
    /// it will either be deleted or recovered back into a chunk.
    pub async fn delete<R, I>(
        &self,
        repository: &R,
        parallelism: usize,
    ) -> Result<
        (),
        FossilDeletionError<
            <R as Repository<M, I, C>>::Error,
            <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
            <R as ClientBackend<I>>::Error,
            <R as ChunkBackend<C>>::Error,
        >,
    >
    where
        R: Repository<M, I, C>,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
        M: Hash + Eq,
        I: Hash + Eq,
        C: Hash + Eq,
    {
        let mut deleter = SimpleFossilDeleter::new(self);
        deleter
            .add_missing_manifests(repository, parallelism)
            .await?;
        deleter.delete(repository, parallelism).await
    }

    /// Delete this fossil collection while also preparing a new one.
    ///
    /// For more details see [`PipelinedFossilCollectionBuilder`].
    pub fn pipelined_delete<I>(&self) -> PipelinedFossilCollectionBuilder<'_, M, I, C> {
        PipelinedFossilCollectionBuilder::new(self)
    }
}

/// Error of a failed fossil collection operation.
#[derive(Debug)]
pub struct FossilCollectionError<M, C, E> {
    builder: FossilCollectionBuilder<M, C>,
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

impl<M, C, E> std::fmt::Display for FossilCollectionError<M, C, E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error during fossil collection: {}", self.error())
    }
}

impl<M, C, E> std::error::Error for FossilCollectionError<M, C, E>
where
    M: std::fmt::Debug,
    C: std::fmt::Debug,
    E: std::error::Error,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error().source()
    }
}

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
}

impl<M, C> FossilCollectionBuilder<M, C> {
    /// Create a new instance, containing no fossil candidates or seen manifests.
    pub fn new() -> FossilCollectionBuilder<M, C> {
        FossilCollectionBuilder {
            seen_manifests: HashSet::new(),
            fossil_candidates: HashSet::new(),
        }
    }

    /// The number of fossil candidates.
    pub fn fossil_candidates(&self) -> usize {
        self.fossil_candidates.len()
    }

    /// The chunks marked as fossil candidates.
    pub fn iter_fossil_candidates(&self) -> std::collections::hash_set::Iter<C> {
        self.fossil_candidates.iter()
    }

    /// The number of seen manifests.
    pub fn seen_manifests(&self) -> usize {
        self.seen_manifests.len()
    }

    /// The manifests seen by this builder.
    pub fn iter_seen_manifests(&self) -> std::collections::hash_set::Iter<M> {
        self.seen_manifests.iter()
    }

    /// Perform the fossil collection operation, creating a fossil collection.
    ///
    /// This method turns all fossil candidates into fossils and stores them
    /// together with the seen manifests and a timestamp in a new [`FossilCollection`].
    ///
    /// Should this operation fail it is possible to retry it by extracting the builder
    /// from the returned [`FossilCollectionError`].
    pub async fn collect_fossils<R>(
        self,
        repository: &R,
        parallelism: usize,
    ) -> Result<FossilCollection<M, C>, FossilCollectionError<M, C, R::Error>>
    where
        R: ChunkBackend<C>,
    {
        if let Err(e) = self.apply_fossil_operations(repository, parallelism).await {
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
        parallelism: usize,
    ) -> Result<(), R::Error>
    where
        R: ChunkBackend<C>,
    {
        let mut fossil_stream = futures::stream::iter(self.iter_fossil_candidates())
            .map(|id| Ok(repository.make_fossil(id)))
            .try_buffer_unordered(parallelism);

        while let Some(res) = fossil_stream.next().await {
            res?;
        }

        Ok(())
    }
}

impl<M, C> FossilCollectionBuilder<M, C>
where
    C: Hash + Eq,
{
    /// Add a chunk to be deleted.
    ///
    /// The chunk may not be referenced by any manifest passed to [`FossilCollectionBuilder::add_seen_manifest`].
    pub fn add_fossil_candidate(&mut self, id: C) {
        self.fossil_candidates.insert(id);
    }

    /// Remove a chunk from this builder.
    ///
    /// Should a manifest be discovered which references chunks which where
    /// thought to be unreferenced its referenced chunks must be removed from
    /// the builder before passing it to [`FossilCollectionBuilder::add_seen_manifest`].
    ///
    /// This can be used to avoid storing unnecessary data in memory since manifests
    /// can be inspected one by one and referenced chunks tracked in a [`HashSet`].
    /// Should a manifest reference chunks they are first removed from the builder
    /// before being added to the hash set, which is checked when adding new fossil candidates.
    pub fn remove_fossil_candidate(&mut self, id: &C) {
        self.fossil_candidates.remove(id);
    }

    /// Check if a chunk is a fossil candidate.
    pub fn has_fossil_candidate<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.fossil_candidates.contains(id)
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
        parallelism: usize,
    ) -> Result<FossilCollection<M, C>, FossilCollectionError<M, C, R::Error>>
    where
        R: ChunkBackend<C>,
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

        if let Err(e) = self.apply_fossil_operations(repository, parallelism).await {
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

/// Delete a fossil collection.
///
/// This deleter is used internally by [`FossilCollection::delete`] and does not
/// track unreferenced chunks.
#[derive(Debug)]
struct SimpleFossilDeleter<'a, M, I, C> {
    fossil_collection: &'a FossilCollection<M, C>,
    referenced_chunks: HashSet<C>,
    valid_clients: HashSet<I>,
    seen_manifests: HashSet<M>,
}

impl<'a, M, I, C> SimpleFossilDeleter<'a, M, I, C> {
    fn new(fossil_collection: &'a FossilCollection<M, C>) -> SimpleFossilDeleter<'a, M, I, C> {
        SimpleFossilDeleter {
            fossil_collection,
            referenced_chunks: HashSet::new(),
            valid_clients: HashSet::new(),
            seen_manifests: HashSet::new(),
        }
    }
}

impl<'a, M, I, C> SimpleFossilDeleter<'a, M, I, C>
where
    M: Hash + Eq,
    I: Hash + Eq,
    C: Hash + Eq,
{
    pub async fn delete<R>(
        &mut self,
        repository: &R,
        parallelism: usize,
    ) -> Result<
        (),
        FossilDeletionError<
            <R as Repository<M, I, C>>::Error,
            <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
            <R as ClientBackend<I>>::Error,
            <R as ChunkBackend<C>>::Error,
        >,
    >
    where
        R: Repository<M, I, C>,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
    {
        <Self as FossilDeleter<M, I, C>>::delete(self, repository, parallelism).await
    }
}

/// Error of a failed call to [`PipelinedFossilCollectionBuilder::delete`].
#[derive(Debug)]
pub struct PipelinedFossilDeletionError<'a, M, I, C, E> {
    builder: PipelinedFossilCollectionBuilder<'a, M, I, C>,
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

impl<M, I, C, E> std::fmt::Display for PipelinedFossilDeletionError<'_, M, I, C, E>
where
    E: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error during pipelined fossil deletion: {}",
            self.error()
        )
    }
}

impl<M, I, C, E> std::error::Error for PipelinedFossilDeletionError<'_, M, I, C, E>
where
    M: std::fmt::Debug,
    I: std::fmt::Debug,
    C: std::fmt::Debug,
    E: std::error::Error,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error().source()
    }
}

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
    referenced_chunks: HashSet<C>,
    fossil_candidates: HashSet<C>,
    valid_clients: HashSet<I>,
    seen_manifests: HashSet<M>,
    expiring_manifests: HashSet<M>,
}

impl<'a, M, I, C> PipelinedFossilCollectionBuilder<'a, M, I, C> {
    fn new(
        fossil_collection: &'a FossilCollection<M, C>,
    ) -> PipelinedFossilCollectionBuilder<'a, M, I, C> {
        PipelinedFossilCollectionBuilder {
            fossil_collection,
            referenced_chunks: HashSet::new(),
            fossil_candidates: HashSet::new(),
            valid_clients: HashSet::new(),
            seen_manifests: HashSet::new(),
            expiring_manifests: HashSet::new(),
        }
    }

    /// The fossil collection which will be deleted by this builder.
    pub fn fossil_collection(&self) -> &'a FossilCollection<M, C> {
        self.fossil_collection
    }

    /// The number of chunks currently marked as fossil candidates.
    pub fn fossil_candidates(&self) -> usize {
        self.fossil_candidates.len()
    }

    /// Iterate through the chunks currently marked as fossil candidates.
    pub fn iter_fossil_candidates(&self) -> std::collections::hash_set::Iter<C> {
        self.fossil_candidates.iter()
    }

    /// The number of currently referenced chunks.
    pub fn referenced_chunks(&self) -> usize {
        self.referenced_chunks.len()
    }

    /// Iterate through the currently referenced chunks.
    pub fn iter_referenced_chunks(&self) -> std::collections::hash_set::Iter<C> {
        self.referenced_chunks.iter()
    }
}

impl<M, I, C> PipelinedFossilCollectionBuilder<'_, M, I, C>
where
    M: Eq + Hash,
{
    /// Iterate through all manifests which where seen by either this builder
    /// or its associated fossil collection.
    pub fn iter_seen_manifests(&self) -> std::collections::hash_set::Union<'_, M, RandomState> {
        self.seen_manifests
            .union(&self.fossil_collection.seen_manifests)
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
        self.seen_manifests.contains(id)
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
    /// Since this removes any matching fossil candidates the manifests
    /// referencing them can still be added to this builder.
    ///
    /// The fossil deletion can only proceed after for every client a manifest
    /// has been added which was created after the fossil collection of this
    /// builder finished.
    pub fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime) {
        if timestamp > self.fossil_collection.timestamp() {
            self.valid_clients.insert(creator);
        }
        // insert even if seen by the fossil collection to pass them to the
        // created FossilCollectionBuilder
        self.seen_manifests.insert(id);
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
        if !self.fossil_collection.has_seen_manifest(&id) {
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
    /// This method is similar to [`FossilCollectionBuilder::add_fossil_candidate`]
    /// but checks whether the fossil candidate was already added as a referenced chunk,
    /// ignoring it if this is the case.
    ///
    /// This method does not place the same restriction on
    /// [`PipelinedFossilCollectionBuilder::add_seen_manifest`], for more information
    /// visit its description.
    pub fn add_fossil_candidate(&mut self, id: C) {
        if !self.referenced_chunks.contains(&id) {
            self.fossil_candidates.insert(id);
        }
    }

    /// Check if a chunk is a fossil candidate.
    pub fn has_fossil_candidate<Q>(&self, id: &Q) -> bool
    where
        C: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
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
        parallelism: usize,
    ) -> Result<
        FossilCollectionBuilder<M, C>,
        PipelinedFossilDeletionError<
            'a,
            M,
            I,
            C,
            FossilDeletionError<
                <R as Repository<M, I, C>>::Error,
                <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
                <R as ClientBackend<I>>::Error,
                <R as ChunkBackend<C>>::Error,
            >,
        >,
    >
    where
        R: Repository<M, I, C>,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
    {
        match <Self as FossilDeleter<M, I, C>>::delete(&mut self, repository, parallelism).await {
            Ok(()) => {
                let builder = FossilCollectionBuilder {
                    // do not include manifests of fossil collection or expiring manifests
                    // since they might reference the fossil candidates
                    seen_manifests: self.seen_manifests,
                    fossil_candidates: self.fossil_candidates,
                };
                Ok(builder)
            }
            Err(e) => Err(PipelinedFossilDeletionError::new(self, e)),
        }
    }
}

/// Internal trait which reduces code duplication.
trait FossilDeleter<M, I, C> {
    fn add_referenced_chunk(&mut self, id: C);

    fn add_seen_manifest(&mut self, id: M, creator: I, timestamp: SystemTime);

    fn has_seen_manifest(&self, id: &M) -> bool;

    fn has_referenced_chunk(&self, id: &C) -> bool;

    fn has_valid_client(&self, id: &I) -> bool;

    fn fossil_collection(&self) -> &FossilCollection<M, C>;

    async fn add_missing_manifests<R>(
        &mut self,
        repository: &R,
        parallelism: usize,
    ) -> Result<
        (),
        FossilDeletionError<
            <R as Repository<M, I, C>>::Error,
            <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
            <R as ClientBackend<I>>::Error,
            <R as ChunkBackend<C>>::Error,
        >,
    >
    where
        R: Repository<M, I, C>,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
    {
        let cell = RefCell::new(&mut *self);
        let mut manifest_stream = pin!(repository
            .manifests()
            .await
            .map_err(FossilDeletionError::RepositoryError)?
            .map_ok(|manifest| check_manifest(repository, manifest, &cell))
            .map_err(FossilDeletionError::RepositoryError)
            .try_buffer_unordered(parallelism));
        while let Some(res) = manifest_stream.next().await {
            res?;
        }
        Ok(())
    }

    async fn delete<R>(
        &mut self,
        repository: &R,
        parallelism: usize,
    ) -> Result<
        (),
        FossilDeletionError<
            <R as Repository<M, I, C>>::Error,
            <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
            <R as ClientBackend<I>>::Error,
            <R as ChunkBackend<C>>::Error,
        >,
    >
    where
        R: Repository<M, I, C>,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
    {
        // check Policy 3
        let mut client_stream = pin!(repository
            .clients()
            .await
            .map_err(FossilDeletionError::ClientError)?);
        while let Some(res) = client_stream.next().await {
            match res {
                Ok(client) if self.has_valid_client(&client) => (),
                Ok(_) => return Err(FossilDeletionError::TooEarly),
                Err(e) => return Err(FossilDeletionError::ClientError(e)),
            }
        }

        // iterate through manifests a second time to make sure manifests created during iteration are picked up
        self.add_missing_manifests(repository, parallelism).await?;

        // deal with fossils
        let mut fossil_stream = futures::stream::iter(self.fossil_collection().iter_fossils())
            .map(|fossil| {
                if self.has_referenced_chunk(fossil) {
                    Ok(repository.recover_fossil(fossil).left_future())
                } else {
                    Ok(repository.delete_fossil(fossil).right_future())
                }
            })
            .try_buffer_unordered(parallelism);
        while let Some(res) = fossil_stream.next().await {
            res.map_err(FossilDeletionError::FossilError)?;
        }

        Ok(())
    }
}

async fn check_manifest<R, M, I, C, D>(
    repository: &R,
    id: M,
    deleter: &RefCell<&mut D>,
) -> Result<
    (),
    FossilDeletionError<
        <R as Repository<M, I, C>>::Error,
        <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error,
        <R as ClientBackend<I>>::Error,
        <R as ChunkBackend<C>>::Error,
    >,
>
where
    R: Repository<M, I, C>,
    D: FossilDeleter<M, I, C> + ?Sized,
    <<R as Repository<M, I, C>>::Manifest as Manifest<I, C>>::ReferencedChunks: Unpin,
{
    if deleter.borrow().has_seen_manifest(&id) {
        return Ok(());
    }
    let (creator, mut referenced_chunks) = repository
        .manifest(&id)
        .await
        .map_err(FossilDeletionError::ManifestError)?
        .into_referenced_chunks();
    let mut pinned_chunks = Pin::new(&mut referenced_chunks);
    while let Some(res) = pinned_chunks.as_mut().next().await {
        let chunk = res.map_err(FossilDeletionError::ManifestError)?;
        deleter.borrow_mut().add_referenced_chunk(chunk);
    }
    let timestamp = referenced_chunks
        .into_timestamp()
        .await
        .map_err(FossilDeletionError::ManifestError)?;
    deleter
        .borrow_mut()
        .add_seen_manifest(id, creator, timestamp);
    Ok(())
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
        self.referenced_chunks.contains(id) || self.fossil_candidates.contains(id)
    }

    fn has_valid_client(&self, id: &I) -> bool {
        self.valid_clients.contains(id)
    }

    fn fossil_collection(&self) -> &FossilCollection<M, C> {
        self.fossil_collection()
    }
}
