//! Implementation of Lock-Free Deduplication in Rust.
//!
//! This crates provides an implementation of the algorithm used by tools
//! such as [Duplicacy](https://duplicacy.com) as described in their
//! [paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).

pub mod backends;

use futures::io::{AsyncRead, AsyncWrite, Result as IoResult};
use futures::stream::Stream;

/// The client backend.
/// 
/// Clients represent actors which can access a repository independently.
pub trait ClientBackend {
    /// Type representing a client.
    /// 
    /// This can be for example a UUID.
    type ClientID;

    /// Enumerate all clients currently registered with the repository.
    /// 
    /// Changes by adding or removing clients during enumeration may or may not
    /// be picked up.
    async fn clients(&self) -> IoResult<impl Stream<Item = IoResult<Self::ClientID>>>;

    /// Request data associated with a client.
    /// 
    /// Each client can have associated data stored in the repository.
    async fn client(&self, id: &Self::ClientID) -> IoResult<impl AsyncRead>;

    /// Register a client with the repository.
    /// 
    /// The client will be registerd when the async write is closed.
    /// 
    /// A client has to be registered with the repository before
    /// he can perform any operations on it.
    /// Should the client already be registered with the repository
    /// its associated data will be overwritten.
    async fn add_client(&self, id: &Self::ClientID) -> IoResult<impl AsyncWrite>;

    /// Remove a client from the repository.
    /// 
    /// Clients have to finish all pending operations before being removed from the repository.
    async fn remove_client(&self, id: &Self::ClientID) -> IoResult<()>;
}

/// A trait which allows fossils to give information about which chunk they represent.
///
/// A fossil is a chunk which has been renamed in preparation for deletion.
/// Because of the optimistic nature of Lock-Free Deduplication fossils may have to
/// be restored when they are referenced by a new manifest file.
///
/// # Examples
/// ```
/// use vinculum::Fossil;
///
/// #[derive(Clone, Copy)]
/// pub struct ChunkID {
///     hash: [u8; 32]
/// }
///
/// pub struct FossilID {
///     original_chunk: ChunkID
/// }
///
/// impl Fossil for FossilID {
///     type ChunkID = ChunkID;
///
///     fn original_chunk(&self) -> Self::ChunkID {
///         self.original_chunk
///     }
/// }
/// ```
pub trait Fossil {
    /// The type of the original chunk.
    type ChunkID;

    /// Calculate the original chunk from which this fossil was created.
    fn original_chunk(&self) -> Self::ChunkID;
}

/// The chunk backend.
/// 
/// Chunks represent pieces of data uploaded by clients and may be referenced by multiple manifest files.
pub trait ChunkBackend {
    /// Type representing a chunk.
    /// 
    /// This can for example be a hash of its contents.
    /// It is important that chunks with the same content receive the same Id since it allows
    /// duplicate data to be shared between multiple manifest files.
    type ChunkID;

    /// Type representing a fossilised chunk.
    /// 
    /// See [`Fossil`].
    type Fossil: Fossil<ChunkID = Self::ChunkID>;

    /// Enumerate all chunks existing within the repository.
    /// 
    /// Changes by adding or removing chunks during enumeration may or may not
    /// be picked up.
    async fn chunks(&self) -> IoResult<impl Stream<Item = IoResult<Self::ChunkID>>>;

    /// Request the contents of a chunk.
    /// 
    /// Its fossil can be used in case the original chunk does not exist.
    async fn chunk(&self, id: &Self::ChunkID) -> IoResult<impl AsyncRead>;

    /// Check whether a chunk exists in the repository.
    /// 
    /// The result of this query may be used to skip uploading chunks which
    /// already exist in the repository.
    async fn has_chunk(&self, id: &Self::ChunkID) -> IoResult<bool>;

    /// Store a chunk in the repository.
    /// 
    /// The chunk will be added when the async write is closed.
    /// 
    /// Should a chunk already exist within a repository its contents
    /// will be overwritten.
    async fn add_chunk(&self, id: &Self::ChunkID) -> IoResult<impl AsyncWrite>;

    /// Enumerate all chunks existing within the repository.
    /// 
    /// Changes by fossilising chunks or recovering or deletion fossils during
    /// enumeration may or may not be picked up.
    async fn fossils(&self) -> IoResult<impl Stream<Item = IoResult<Self::Fossil>>>;

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
    async fn make_fossil(&self, id: &Self::ChunkID) -> IoResult<Self::Fossil>;

    /// Turn a fossil back into a chunk.
    /// 
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    async fn recover_fossil(&self, id: &Self::Fossil) -> IoResult<()>;

    /// Delete a fossil permanently.
    /// 
    /// A fossil being deleted may cause data loss should there still be
    /// manifests referencing it, which is why this operation should be
    /// performed by a fossil deletion step.
    /// 
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    async fn delete_fossil(&self, id: &Self::Fossil) -> IoResult<()>;
}

/// Data uploaded by a client, represented as a sequence of chunks.
pub trait Manifest {
    /// Type representing a client.
    /// 
    /// This can be for example a UUID.
    type ClientID;

    /// Type representing a chunk.
    /// 
    /// This can for example be a hash of its contents.
    type ChunkID;

    /// Timestamp indicating the time of creation.
    /// 
    /// This is after all chunks where uploaded but may be before the manifest
    /// upload finished.
    fn creation_timestamp(&self) -> std::time::Instant;

    /// Client which created this manifest.
    fn creator(&self) -> Self::ClientID;

    /// Enumerate the chunks this manifest is made of.
    async fn chunks(&self) -> IoResult<impl Stream<Item = IoResult<Self::ChunkID>>>;
}

/// Trait representing a manifest creation process.
pub trait ManifestBuilder {
    /// Type representing a chunk.
    /// 
    /// This can for example be a hash of its contents.
    type ChunkID;

    /// Add a chunk to the manifest file.
    /// 
    /// The chunk has to be added to the repository by the user before calling
    /// this function, either by uploading it or making sure it already exists.
    /// 
    /// A backend can not depend on the list chunks being complete.
    /// Middlewares can encode additional chunks in the additional data or
    /// store the list as chunks themselves.
    async fn add_chunk(&mut self, id: &Self::ChunkID) -> IoResult<()>;

    /// Add additional data to the manifest.
    /// 
    /// Manifests can store custom data in addition to the sequence of its
    /// chunks, which allows clients to store (for example) additional metadata.
    /// 
    /// When the returned async write is closed the manifest will be created.
    async fn add_data(self) -> IoResult<impl AsyncWrite>;
}

/// A repository storing clients, chunks and manifests.
pub trait Repository: ClientBackend + ChunkBackend {
    /// Type representing a manifest.
    /// 
    /// This can for example be a UUID.
    type ManifestID;

    /// Type of manifest stored in this repository.
    /// 
    /// See [`Repository::manifest`].
    type Manifest: Manifest<
        ClientID = <Self as ClientBackend>::ClientID,
        ChunkID = <Self as ChunkBackend>::ChunkID,
    >;

    /// Type representing a manifest being created.
    /// 
    /// See [`Repository::create_manifest`].
    type Builder: ManifestBuilder<ChunkID = <Self as ChunkBackend>::ChunkID>;

    /// Enumerate all manifests existing within the repository.
    /// 
    /// Changes by creating or removing manifests during enumeration may or may
    /// not be picked up.
    async fn manifests(&self) -> IoResult<impl Stream<Item = IoResult<Self::ManifestID>>>;

    /// Request a manifest.
    async fn manifest(&self, id: &Self::ManifestID) -> IoResult<Self::Manifest>;

    /// Check whether a manifest exists.
    async fn has_manifest(&self, id: &Self::ManifestID) -> IoResult<bool>;

    /// Create a manifest.
    /// 
    /// A client creating a manifest while the same client is downloading a
    /// chunk can cause the download to fail by making it appear as if the
    /// chunk does not exist.
    fn create_manifest(&self, id: &Self::ManifestID) -> Self::Builder;

    /// Remove a manifest.
    /// 
    /// Removing a manifest may leave unreferenced chunks behind, which is why
    /// this operation should be performed by a fossil collection step.
    async fn remove_manifest(&self, id: &Self::ManifestID) -> IoResult<()>;
}
