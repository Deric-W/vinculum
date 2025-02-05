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
//! It is represented by the [`Repository`] trait, which is designed to be
//! implemented by you.
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
//! ## Concurrency
//!
//! While this crate does not depend on a particular async runtime it performs
//! actions concurrently when possible, which can be controlled by the `concurrency`
//! argument of some functions.
//!
//! This is achieved by using [`futures::stream::FuturesUnordered`], which in
//! turn requires that any blocking operations (for example filesystem operations
//! or hashing large amounts of data) are queued on an external thread pool should
//! parallel execution be desired.
//!
//! ## Features
//!
//! - `serde`: implements [`serde::Serialize`] and [`serde::Deserialize`] for [`FossilCollection`].

#![cfg_attr(docsrs, feature(doc_auto_cfg))]

mod collection;
mod deletion;
mod pipelined;

pub use collection::{FossilCollectionBuilder, FossilCollectionError};
pub use deletion::FossilDeletionError;
use deletion::{FossilDeleter, SimpleFossilDeleter};
use futures::stream::Stream;
pub use pipelined::{PipelinedFossilCollectionBuilder, PipelinedFossilDeletionError};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::time::SystemTime;

/// A repository storing clients, chunks and manifests.
pub trait Repository {
    /// Id used to identify manifests.
    ///
    /// When a manifest with an id has been created recreating it with the
    /// same id but different content is not allowed.
    type ManifestID;

    type Manifest: Manifest;

    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// Enumerate all manifests existing within the repository.
    ///
    /// Changes by creating or removing manifests during enumeration may or may
    /// not be picked up.
    fn manifests(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Self::ManifestID, Self::Error>>, Self::Error>,
    >;

    /// Enumerate all clients currently registered with the repository.
    ///
    /// Changes by adding or removing clients during enumeration may or may not
    /// be picked up.
    #[allow(clippy::type_complexity)]
    fn clients(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<<Self::Manifest as Manifest>::ClientID, Self::Error>>,
            Self::Error,
        >,
    >;

    /// Request a manifest.
    fn manifest(
        &self,
        id: &Self::ManifestID,
    ) -> impl Future<Output = Result<Self::Manifest, <Self::Manifest as Manifest>::Error>>;

    /// Enumerate all chunks existing within the repository.
    ///
    /// Changes by adding or removing chunks during enumeration may or may not
    /// be picked up.
    #[allow(clippy::type_complexity)]
    fn chunks(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<<Self::Manifest as Manifest>::ChunkID, Self::Error>>,
            Self::Error,
        >,
    >;

    /// Enumerate all fossils existing within the repository.
    ///
    /// Changes by fossilizing chunks or recovering or deleting fossils during
    /// enumeration may or may not be picked up.
    #[allow(clippy::type_complexity)]
    fn fossils(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<<Self::Manifest as Manifest>::ChunkID, Self::Error>>,
            Self::Error,
        >,
    >;

    /// Turn a chunk into a fossil.
    ///
    /// Since a fossil may be referenced by new manifest files after creation
    /// it is allowed to be used if the original chunk is missing, but not during
    /// manifest creation.
    /// Should both a referenced chunk and its fossil appear to be missing there
    /// is a possibility that the chunk was turned into a fossil and recovered
    /// just before the respective object could be accessed.
    /// In this case retrying the access operations a second time will succeed,
    /// assuming the client did not create a new manifest in the meantime.
    ///
    /// During manifest creation chunks having the same ID as the original chunk of
    /// the fossil must be created again, even when the fossil still exists.
    ///
    /// When the chunk does not exists this method should not return an error
    /// but instead treat the fossil as having been created.
    fn fossilize_chunk(
        &self,
        chunk: &<Self::Manifest as Manifest>::ChunkID,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Turn a fossil back into a chunk.
    ///
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    fn recover_fossil(
        &self,
        fossil: &<Self::Manifest as Manifest>::ChunkID,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Delete a fossil permanently.
    ///
    /// A fossil being deleted may cause data loss should there still be
    /// manifests referencing it, which is why this operation should be
    /// performed by a fossil deletion step.
    ///
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    fn delete_fossil(
        &self,
        fossil: &<Self::Manifest as Manifest>::ChunkID,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

/// Data uploaded by a client, represented as a set of chunks and metadata.
///
/// The set of chunks produced by a manifest represents all chunks referenced
/// by it and may include chunks which were not present when creating the manifest,
/// for example when the actual list of chunks is itself stored in chunks.
///
/// The Metadata includes the client which created this manifest and a timestamp.
/// The timestamp is recorded after all chunks where uploaded and and additional
/// data was written, but may be before the manifest upload finished.
pub trait Manifest: Stream<Item = Result<Self::ChunkID, Self::Error>> + Unpin {
    /// Id used to identify chunks.
    ///
    /// It is imporant that chunks receiving the same id have the same content.
    type ChunkID;

    /// Id used to identify clients.
    ///
    /// Clients represent actors which can access a repository independently.
    type ClientID;

    /// The Type of errors produced by this implementation.
    type Error: std::error::Error;

    /// Extract the client which created this manifest with the associated timestamp.
    fn into_metadata(
        self,
    ) -> impl Future<Output = Result<(Self::ClientID, SystemTime), Self::Error>>;
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FossilCollection<M, C> {
    // are assumed to be unique
    fossils: Vec<C>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(deserialize = "M: Eq + Hash + serde::Deserialize<'de>"))
    )]
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
    ///
    /// Consider enabling the `serde` feature when working with serde to implement
    /// the [`serde::Serialize`] and [`serde::Deserialize`] traits.
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
    ///
    /// The `concurrency` argument controls the amount of actions executed
    /// concurrently, either as an upper limit (which can be passed without wrapping
    /// it in an [`Option`] first) or no limit when [`None`] or zero is passed.
    pub async fn delete<R>(
        &self,
        repository: &R,
        concurrency: impl Into<Option<usize>>,
    ) -> Result<(), FossilDeletionError<R::Error, <R::Manifest as Manifest>::Error>>
    where
        R: Repository<ManifestID = M>,
        R::Manifest: Manifest<ChunkID = C>,
        R::ManifestID: Hash + Eq,
        <R::Manifest as Manifest>::ClientID: Hash + Eq,
        <R::Manifest as Manifest>::ChunkID: Hash + Eq,
    {
        let concurrency: Option<usize> = concurrency.into();
        let mut deleter = SimpleFossilDeleter::new(self);
        deleter
            .add_missing_manifests(repository, concurrency)
            .await?;
        deleter.delete(repository, concurrency).await
    }

    /// Delete this fossil collection while also preparing a new one.
    ///
    /// For more details see [`PipelinedFossilCollectionBuilder`].
    pub fn pipelined_delete<I>(&self) -> PipelinedFossilCollectionBuilder<'_, M, I, C> {
        PipelinedFossilCollectionBuilder::new(self)
    }
}
