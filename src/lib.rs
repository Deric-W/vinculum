//! Implementation of Lock-Free Deduplication in Rust.
//!
//! This crates provides an implementation of the algorithm used by tools
//! such as [Duplicacy](https://duplicacy.com) as described in their
//! [paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).

pub mod backends;
mod utils;

use futures::io::{AsyncRead, AsyncWrite};
use futures::stream::Stream;
use futures::sink::Sink;

/// The client backend, with the type of client Id as a generic parameter (for example a UUID).
/// 
/// Clients represent actors which can access a repository independently.
pub trait ClientBackend<I> {
    /// The Type of errors produced by this implementation.
    type Error;

    /// Enumerate all clients currently registered with the repository.
    /// 
    /// Changes by adding or removing clients during enumeration may or may not
    /// be picked up.
    async fn clients(&self) -> Result<impl Stream<Item = Result<I, Self::Error>>, Self::Error>;

    /// Request data associated with a client.
    /// 
    /// Each client can have associated data stored in the repository.
    async fn client(&self, id: &I) -> Result<impl AsyncRead, Self::Error>;

    /// Register a client with the repository.
    /// 
    /// The client will be registerd when the async write is closed.
    /// 
    /// A client has to be registered with the repository before
    /// he can perform any operations on it.
    /// Should the client already be registered with the repository
    /// its associated data will be overwritten.
    async fn add_client(&self, id: &I) -> Result<impl AsyncWrite, Self::Error>;

    /// Remove a client from the repository.
    /// 
    /// Clients have to finish all pending operations before being removed from the repository.
    async fn remove_client(&self, id: &I) -> Result<(), Self::Error>;
}

/// The chunk backend with the type of chunk Id as a generic parameter.
/// 
/// Chunks represent pieces of data uploaded by clients and may be referenced by multiple manifest files.
/// This can for example be a hash of its contents.
/// It is important that chunks with the same content receive the same Id since it allows
/// duplicate data to be shared between multiple manifest files.
pub trait ChunkBackend<C> {
    /// The Type of errors produced by this implementation.
    type Error;

    /// Enumerate all chunks existing within the repository.
    /// 
    /// Changes by adding or removing chunks during enumeration may or may not
    /// be picked up.
    async fn chunks(&self) -> Result<impl Stream<Item = Result<C, Self::Error>>, Self::Error>;

    /// Request the contents of a chunk.
    /// 
    /// Its fossil can be used in case the original chunk does not exist.
    async fn chunk(&self, id: &C) -> Result<impl AsyncRead, Self::Error>;

    /// Check whether a chunk exists in the repository.
    /// 
    /// The result of this query may be used to skip uploading chunks which
    /// already exist in the repository and may not consider fossils.
    async fn has_chunk(&self, id: &C) -> Result<bool, Self::Error>;

    /// Store a chunk in the repository.
    /// 
    /// The chunk will be added when the async write is closed.
    /// 
    /// Should a chunk already exist within a repository its contents
    /// will be overwritten.
    async fn add_chunk(&self, id: &C) -> Result<impl AsyncWrite, Self::Error>;

    /// Enumerate all fossils existing within the repository.
    /// 
    /// Changes by fossilising chunks or recovering or deletion fossils during
    /// enumeration may or may not be picked up.
    async fn fossils(&self) -> Result<impl Stream<Item = Result<C, Self::Error>>, Self::Error>;

    /// Turn a chunk into a fossil.
    /// 
    /// Since a [`Fossil`] may be referenced by new manifest files after creation
    /// it is allowed to be used in place of its original chunk, but not during
    /// manifest creation.
    /// During manifest creation chunks having the same ID as the original chunk of
    /// the fossil must be created again.
    /// 
    /// When the chunk does not exists this method should not return an error
    /// but instead treat the fossil as having been created and return its ID. 
    async fn make_fossil(&self, id: &C) -> Result<(), Self::Error>;

    /// Turn a fossil back into a chunk.
    /// 
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    async fn recover_fossil(&self, id: &C) -> Result<(), Self::Error>;

    /// Delete a fossil permanently.
    /// 
    /// A fossil being deleted may cause data loss should there still be
    /// manifests referencing it, which is why this operation should be
    /// performed by a fossil deletion step.
    /// 
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    async fn delete_fossil(&self, id: &C) -> Result<(), Self::Error>;
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
/// will skip any remaning chunks.
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
    type Error;

    /// The Type representing the chunks and additional data uploaded by the user.
    type Chunks: ManifestChunks<C, Error = Self::Error>;

    /// The Type representing the actual chunks referenced by the manifest.
    /// 
    /// This can be different from [`Manifest::Chunks`] if for example the actual
    /// content of the manifest is itself stored in chunks.
    type ReferencedChunks: Stream<Item = Result<C, Self::Error>> + ManifestTimestamp;

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
/// The generic parameter represents the chunk ids.
pub trait ManifestChunks<C>: Stream<Item = Result<C, <Self as ManifestChunks<C>>::Error>> {
    /// The Type of errors produced by this implementation.
    type Error;

    /// The Type representing the additional manifest data.
    type Data: AsyncRead + ManifestTimestamp<Error = Self::Error>;

    /// Continue with reading the additional manifest data uploaded by the client.
    async fn into_data(self) -> Result<Self::Data, Self::Error>;
}

/// Final state of the manifest decoding process, producing the timestamp.
pub trait ManifestTimestamp {
    /// The Type of errors produced by this implementation.
    type Error;

    /// Convert this manifest data into its timestamp.
    /// 
    /// The timestamp is recorded after all chunks where uploaded and and additional
    /// data was written, but may be before the manifest upload finished.
    async fn into_timestamp(self) -> Result<std::time::SystemTime, Self::Error>;
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
pub trait ManifestBuilder<I>: for<'a> Sink<&'a I, Error = <Self as ManifestBuilder<I>>::Error> {
    /// The Type of errors produced by this implementation.
    type Error;

    /// Add additional data to the manifest.
    /// 
    /// Manifests can store custom data in addition to the sequence of its
    /// chunks, which allows clients to store (for example) additional metadata.
    /// 
    /// When the returned async write is closed a timestamp will be recorded
    /// and the manifest created.
    async fn add_data(self) -> Result<impl AsyncWrite, <Self as ManifestBuilder<I>>::Error>;
}

/// A repository storing clients, chunks and manifests.
/// 
/// The generic parameters represent the type of manifest, client, chunk and fossil Ids.
pub trait Repository<M, I, C>: ClientBackend<I> + ChunkBackend<C> {
    /// The Type of errors produced by this implementation.
    type Error;

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
    async fn manifests(&self) -> Result<impl Stream<Item = Result<M, <Self as Repository<M, I, C>>::Error>>, <Self as Repository<M, I, C>>::Error>;

    /// Request a manifest.
    async fn manifest(&self, id: &M) -> Result<Self::Manifest, <<Self as Repository<M, I, C>>::Manifest as Manifest<I, C>>::Error>;

    /// Create a manifest.
    /// 
    /// A client creating a manifest while the same client is downloading a
    /// chunk can cause the download to fail by making it appear as if the
    /// chunk does not exist.
    async fn create_manifest(&self, id: &M, client: &I) -> Result<Self::Builder, <Self as Repository<M, I, C>>::Error>;

    /// Remove a manifest.
    /// 
    /// Removing a manifest may leave unreferenced chunks behind, which is why
    /// this operation should be performed by a fossil collection step.
    async fn remove_manifest(&self, id: &M) -> Result<(), <Self as Repository<M, I, C>>::Error>;
}
