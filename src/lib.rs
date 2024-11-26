//! Implementation of Lock-Free Deduplication in Rust.
//!
//! This crates provides an implementation of the algorithm used by tools
//! such as [Duplicacy](https://duplicacy.com) as described in their
//! [paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).

pub mod backends;

use futures::io::{AsyncRead, AsyncWrite, Error as IoError};
use futures::stream::Stream;
use std::future::Future;

/// The client backend.
/// 
/// Clients represent actors which can access a repository independently.
pub trait ClientBackend {
    /// Type representing a client.
    /// 
    /// This can be for example a UUID.
    type ClientID;

    /// Type representing data stored for a client.
    /// 
    /// See [`ClientBackend::client`].
    type Client: AsyncRead;

    /// Set of clients currently registered with the repository.
    /// 
    /// See [`ClientBackend::clients`].
    type Clients: Stream<Item = Result<Self::ClientID, IoError>>;

    /// Type representing data to be stored for a client.
    /// 
    /// See [`ClientBackend::add_client`].
    type UploadFuture: AsyncWrite;

    /// Future representing a request for client data.
    /// 
    /// See [`ClientBackend::client`].
    type DownloadFuture: Future<Output = Result<Self::Client, IoError>>;

    /// Future representing a client removal operation.
    /// 
    /// See [`ClientBackend::remove_client`]
    type RemoveFuture: std::future::Future<Output = Result<(), IoError>>;

    /// Enumerate all clients currently registered with the repository.
    /// 
    /// Changes by adding or removing clients during enumeration may or may not
    /// be picked up.
    fn clients(&self) -> Self::Clients;

    /// Request data associated with a client.
    /// 
    /// Each client can have associated data stored in the repository.
    fn client(&self, id: &Self::ClientID) -> Self::DownloadFuture;

    /// Register a client with the repository.
    /// 
    /// The client will be registerd when the future is closed.
    /// 
    /// A client has to be registered with the repository before
    /// he can perform any operations on it.
    /// Should the client already be registered with the repository
    /// its associated data will be overwritten.
    fn add_client(&self, id: &Self::ClientID) -> Self::UploadFuture;

    /// Remove a client from the repository.
    /// 
    /// Clients have to finish all pending operations before being removed from the repository.
    fn remove_client(&self, id: &Self::ClientID) -> Self::RemoveFuture;
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

    /// Type representing the contents of a chunk.
    /// 
    /// See [`ChunkBackend::chunk`].
    type Chunk: AsyncRead;

    /// Set of chunks currently stored in the repository.
    /// 
    /// See [`ChunkBackend::chunks`].
    type Chunks: Stream<Item = Result<Self::ChunkID, IoError>>;

    /// Type representing the contents of a chunk to be uploaded.
    /// 
    /// See [`ChunkBackend::add_chunk`].
    type UploadFuture: AsyncWrite;

    /// Future representing a query for chunk existence.
    /// 
    /// See [`ChunkBackend::has_chunk`].
    type HasFuture: Future<Output = Result<bool, IoError>>;

    /// Future representing a request for the contents of a chunk.
    /// 
    /// See [`ChunkBackend::chunk`].
    type DownloadFuture: Future<Output = Result<Self::Chunk, IoError>>;

    /// Set of fossils currently existing within the repository.
    /// 
    /// See [`ChunkBackend::fossils`].
    type Fossils: Stream<Item = Result<Self::Fossil, IoError>>;

    /// Future representing a request to turn a chunk into a fossil.
    /// 
    /// See [`ChunkBackend::make_fossil`].
    type MakeFossilFuture: Future<Output = Result<Self::Fossil, IoError>>;

    /// Future representing a request to turn a fossil back into a chunk.
    /// 
    /// See [`ChunkBackend::recover_fossil`].
    type RecoverFuture: Future<Output = Result<(), IoError>>;

    /// Future representing a request to delete a fossil.
    /// 
    /// See [`ChunkBackend::delete_fossil`].
    type DeleteFuture: Future<Output = Result<(), IoError>>;

    /// Enumerate all chunks existing within the repository.
    /// 
    /// Changes by adding or removing chunks during enumeration may or may not
    /// be picked up.
    fn chunks(&self) -> Self::Chunks;

    /// Request the contents of a chunk.
    /// 
    /// Its fossil can be used in case the original chunk does not exist.
    fn chunk(&self, id: &Self::ChunkID) -> Self::DownloadFuture;

    /// Check whether a chunk exists in the repository.
    /// 
    /// The result of this query may be used to skip uploading chunks which
    /// already exist in the repository.
    fn has_chunk(&self, id: &Self::ChunkID) -> Self::HasFuture;

    /// Store a chunk in the repository.
    /// 
    /// Should a chunk already exist within a repository its contents
    /// will be overwritten.
    fn add_chunk(&self, id: &Self::ChunkID) -> Self::UploadFuture;

    /// Enumerate all chunks existing within the repository.
    /// 
    /// Changes by fossilising chunks or recovering or deletion fossils during
    /// enumeration may or may not be picked up.
    fn fossils(&self) -> Self::Fossils;

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
    fn make_fossil(&self, id: &Self::ChunkID) -> Self::MakeFossilFuture;

    /// Turn a fossil back into a chunk.
    /// 
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    fn recover_fossil(&self, id: &Self::Fossil) -> Self::RecoverFuture;

    /// Delete a fossil permanently.
    /// 
    /// A fossil being deleted may cause data loss should there still be
    /// manifests referencing it, which is why this operation should be
    /// performed by a fossil deletion step.
    /// 
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    fn delete_fossil(&self, id: &Self::Fossil) -> Self::DeleteFuture;
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

    /// Type representing the uploaded data as a sequence of chunks.
    type Chunks: Stream<Item = Result<Self::ChunkID, IoError>>;

    /// Timestamp indicating the time of creation.
    /// 
    /// This is after all chunks where uploaded but may be before the manifest
    /// upload finished.
    fn creation_timestamp(&self) -> std::time::Instant;

    /// Client which created this manifest.
    fn creator(&self) -> Self::ClientID;

    /// Enumerate the chunks this manifest is made of.
    fn chunks(&self) -> Self::Chunks;
}

/// Trait representing a manifest creation process.
pub trait ManifestBuilder {
    /// Type representing a chunk.
    /// 
    /// This can for example be a hash of its contents.
    type ChunkID;

    /// Future representing the process of adding a chunk to the manifest.
    /// 
    /// See [`ManifestBuilder::add_chunk`].
    type ChunkFuture: Future<Output = Result<(), IoError>>;

    /// Type representing additional data to store in the manifest.
    /// 
    /// See [`ManifestBuilder::add_data`].
    type DataFuture: AsyncWrite;

    /// Add a chunk to the manifest file.
    /// 
    /// The chunk has to be added to the repository by the user before calling
    /// this function, either by uploading it or making sure it already exists.
    /// 
    /// A backend can not depend on the list chunks being complete.
    /// Middlewares can encode additional chunks in the additional data or
    /// store the list as chunks themselves.
    fn add_chunk(&mut self, id: &Self::ChunkID) -> Self::ChunkFuture;

    /// Add additional data to the manifest.
    /// 
    /// Manifests can store custom data in addition to the sequence of its
    /// chunks, which allows clients to store (for example) additional metadata.
    /// 
    /// When the returned future is closed the manifest will be created.
    fn add_data(self) -> Self::DataFuture;
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

    /// Type representing the set of manifests currently existing within the
    /// repository.
    /// 
    /// See [`Repository::manifests`].
    type Manifests: Stream<Item = Result<Self::ManifestID, IoError>>;

    /// Type representing a manifest being created.
    /// 
    /// See [`Repository::create_manifest`].
    type Builder: ManifestBuilder<ChunkID = <Self as ChunkBackend>::ChunkID>;

    /// Future representing the query for manifest existence.
    /// 
    /// See [`Repository::has_manifest`].
    type HasFuture: Future<Output = Result<bool, IoError>>;

    /// Future representing the request for a manifest.
    /// 
    /// See [`Repository::manifest`].
    type DownloadFuture: Future<Output = Result<Self::Manifest, IoError>>;

    /// Future representing a manifest removal operation.
    /// 
    /// See [`Repository::remove_manifest`].
    type RemoveFuture: Future<Output = Result<(), IoError>>;

    /// Enumerate all manifests existing within the repository.
    /// 
    /// Changes by creating or removing manifests during enumeration may or may
    /// not be picked up.
    fn manifests(&self) -> Self::Manifests;

    /// Request a manifest.
    fn manifest(&self, id: &Self::ManifestID) -> <Self as Repository>::DownloadFuture;

    /// Check whether a manifest exists.
    fn has_manifest(&self, id: &Self::ManifestID) -> <Self as Repository>::HasFuture;

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
    fn remove_manifest(&self, id: &Self::ManifestID) -> <Self as Repository>::RemoveFuture;
}
