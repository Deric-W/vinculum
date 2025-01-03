//! Backend implemented in the local UNIX file system.
//!
//! This backend uses types from [`tokio::fs`] and therefore depends on it.

mod builder;
mod committing;
mod manifest;

pub use builder::{ManifestBuilder, ManifestBuilderData, ManifestEncodingError};
pub use manifest::{Manifest, ManifestChunks, ManifestData, ManifestDecodingError};

use crate::{ChunkBackend, ClientBackend, Repository};

use committing::{sync_directory, upload_file, upload_manifest};
use futures::io::{AsyncRead, AsyncWrite, Error as IoError, Result as IoResult};
use futures::stream::{Stream, StreamExt};
use std::ffi::OsString;
use std::fs::DirBuilder;
use std::io::ErrorKind;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Backend implemented in the local UNIX file system.
///
/// This backend stores chunks, fossils, clients and manifests using individual
/// files, performing no compression, encryption or further deduplication.
///
/// ## Power loss
///
/// To be more resistant files are created in a special directory and only moved
/// into their final position after being closed and fsynced, with the directory
/// entries of manifests and their chunks being additionally fsynced after the move.
/// While this prevents incomplete files from being visible to other clients operations
/// such as creating, recovering and deleting fossils and clients is not synchronized
/// and can therefore get lost after a power failure.
///
/// Furthermore, the way tokio handles task cancellation allows running operations to
/// complete on its thread pool after their future has been dropped, requiring a shutdown
/// of the used tokio runtime should a future of this backend be canceled and
/// other operations happen only after its completion.
///
/// ## Trait bounds
///
/// Ids used with this backend will be converted into [`OsString`]s to be used in
/// file names and may therefore not contain path separators or be empty.
///
/// The inverse operation may fail, which is interpreted as an corrupted id.
/// This will cause files to be skipped during enumeration and errors to be produced
/// when reading a manifest.
///
/// ## Manifest format
///
/// The manifest begins with a `u8` encoding the length of the byte representation
/// of the creator id, followed by these bytes.
/// Next will be a sequence of `u8` encoding the length of the byte representation
/// of a chunk id followed by these bytes until the length is zero.
/// After that there will be a sequence of u16 encoding the length of a batch of
/// additional data in bytes, followed by it until the length is zero.
/// Last there will be the timestamp, stored as seconds and nanoseconds since the
/// UNIX epoch.
///
/// This limits the length of the creator id to [`u8::MAX`] bytes and the length
/// of chunk ids to [`u8::MAX`] bytes and greater than zero.
///
/// All numbers are stored in big-endian and therefore portable between architectures.
#[derive(Debug, Clone)]
pub struct FileBackend {
    directory: Box<Path>,
}

impl FileBackend {
    /// Create an instance from an initialized directory.
    ///
    /// This function assumes the directory was initialized using
    /// [`initialize`] and that no other backends exists for the client
    /// for the duration of its existence.
    pub fn new<P>(directory: P) -> FileBackend
    where
        P: AsRef<Path>,
    {
        FileBackend {
            directory: directory.as_ref().into(),
        }
    }

    /// The directory containing this repository.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Consume this object, returning the directory used for creation.
    pub fn into_inner(self) -> Box<Path> {
        self.directory
    }

    async fn upload_file(
        &self,
        destination: PathBuf,
    ) -> IoResult<futures::io::BufWriter<impl AsyncWrite>> {
        let directory = self.directory().join("incoming");
        let writer = upload_file(directory, destination).await?;
        Ok(futures::io::BufWriter::new(writer))
    }

    async fn upload_manifest(
        &self,
        destination: PathBuf,
    ) -> IoResult<futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>> {
        let directory = self.directory().join("incoming");
        let writer = upload_manifest(directory, destination).await?;
        Ok(futures::io::BufWriter::new(Box::pin(writer)))
    }

    async fn list_directory<P, I>(&self, path: P) -> IoResult<impl Stream<Item = IoResult<I>>>
    where
        P: AsRef<Path>,
        for<'a> I: TryFrom<&'a [u8], Error = ()>,
    {
        let path = self.directory().join(path);
        let stream = ReadDirStream::new(tokio::fs::read_dir(path).await?);
        let ids = stream.filter_map(|res| {
            std::future::ready(match res {
                Ok(entry) => match I::try_from(entry.file_name().as_encoded_bytes()) {
                    Ok(id) => Some(Ok(id)),
                    Err(()) => None,
                },
                Err(e) => Some(Err(e)),
            })
        });
        Ok(ids)
    }

    fn create_path_from_id<P, I>(&self, subdirectory: P, id: &I) -> IoResult<PathBuf>
    where
        P: AsRef<Path>,
        for<'a> &'a I: Into<OsString>,
    {
        let string = id.into();
        if string.len() == 0 {
            Err(IoError::other("received id with length zero"))
        } else {
            let mut buf = self.directory().to_path_buf();
            buf.push(subdirectory);
            buf.push(string);
            Ok(buf)
        }
    }
}

impl<I> ClientBackend<I> for FileBackend
where
    for<'a> I: TryFrom<&'a [u8], Error = ()>,
    for<'a> &'a I: Into<OsString>,
{
    type Error = IoError;

    async fn clients(&self) -> IoResult<impl Stream<Item = IoResult<I>>> {
        self.list_directory("clients").await
    }

    async fn client(&self, id: &I) -> IoResult<impl AsyncRead> {
        let path = self.create_path_from_id("clients", id)?;
        let file = tokio::fs::File::open(path).await?;
        Ok(tokio::io::BufReader::new(file).compat())
    }

    async fn add_client(&self, id: &I) -> IoResult<impl AsyncWrite> {
        let path = self.create_path_from_id("clients", id)?;
        self.upload_file(path).await
    }

    async fn remove_client(&self, id: &I) -> IoResult<()> {
        let path = self.create_path_from_id("clients", id)?;
        tokio::fs::remove_file(path).await
    }
}

impl<C> ChunkBackend<C> for FileBackend
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
    for<'a> &'a C: Into<OsString>,
{
    type Error = IoError;

    async fn chunks(&self) -> IoResult<impl Stream<Item = IoResult<C>>> {
        self.list_directory("chunks").await
    }

    async fn chunk(&self, id: &C) -> IoResult<impl AsyncRead> {
        let mut buf = self.directory().to_owned();
        let file_name = id.into();
        if file_name.len() == 0 {
            return Err(IoError::other("received id with length zero"));
        }
        for _ in 0..2 {
            buf.push("chunks");
            buf.push(&file_name);
            match tokio::fs::File::open(&buf).await {
                Ok(file) => return Ok(tokio::io::BufReader::new(file).compat()),
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            };
            buf.pop();
            buf.pop();
            buf.push("fossils");
            buf.push(&file_name);
            match tokio::fs::File::open(&buf).await {
                Ok(file) => return Ok(tokio::io::BufReader::new(file).compat()),
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            };
        }
        Err(ErrorKind::NotFound.into())
    }

    async fn has_chunk(&self, id: &C) -> IoResult<bool> {
        let path = self.create_path_from_id("chunks", id)?;
        tokio::fs::try_exists(path).await
    }

    async fn add_chunk(&self, id: &C) -> IoResult<impl AsyncWrite> {
        let path = self.create_path_from_id("chunks", id)?;
        self.upload_file(path).await
    }

    async fn fossils(&self) -> IoResult<impl Stream<Item = IoResult<C>>> {
        self.list_directory("fossils").await
    }

    async fn make_fossil(&self, id: &C) -> IoResult<()> {
        let file_name = id.into();
        if file_name.len() == 0 {
            return Err(IoError::other("received id with length zero"));
        }
        let mut from = self.directory().to_owned();
        from.push("chunks");
        from.push(&file_name);
        let mut to = self.directory().to_owned();
        to.push("fossils");
        to.push(file_name);
        match tokio::fs::rename(from, to).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn recover_fossil(&self, id: &C) -> IoResult<()> {
        let file_name = id.into();
        if file_name.len() == 0 {
            return Err(IoError::other("received id with length zero"));
        }
        let mut from = self.directory().to_owned();
        from.push("fossils");
        from.push(&file_name);
        let mut to = self.directory().to_owned();
        to.push("chunks");
        to.push(file_name);
        match tokio::fs::rename(from, to).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn delete_fossil(&self, id: &C) -> IoResult<()> {
        let path = self.create_path_from_id("fossils", id)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<M, I, C> Repository<M, I, C> for FileBackend
where
    for<'a> I: TryFrom<&'a [u8], Error = ()>,
    for<'a> &'a I: Into<OsString>,
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
    for<'a> &'a C: Into<OsString>,
    for<'a> M: TryFrom<&'a [u8], Error = ()>,
    for<'a> &'a M: Into<OsString>,
{
    type Error = IoError;

    type Builder = ManifestBuilder;

    type Manifest = Manifest<I, C>;

    async fn manifests(
        &self,
    ) -> Result<
        impl Stream<Item = Result<M, <Self as Repository<M, I, C>>::Error>>,
        <Self as Repository<M, I, C>>::Error,
    > {
        self.list_directory("manifests").await
    }

    async fn manifest(&self, id: &M) -> Result<Self::Manifest, ManifestDecodingError> {
        let path = self
            .create_path_from_id("manifests", id)
            .map_err(|e| ManifestDecodingError::IoError(e))?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| ManifestDecodingError::IoError(e))?;
        let reader = futures::io::BufReader::new(file.compat());
        Manifest::from_file(reader).await
    }

    async fn create_manifest(
        &self,
        id: &M,
        client: &I,
    ) -> Result<Self::Builder, ManifestEncodingError> {
        let path = self
            .create_path_from_id("manifests", id)
            .map_err(|e| ManifestEncodingError::IoError(e))?;
        let writer = self
            .upload_manifest(path)
            .await
            .map_err(|e| ManifestEncodingError::IoError(e))?;
        ManifestBuilder::from_upload(writer, client).await
    }

    async fn remove_manifest(&self, id: &M) -> Result<(), <Self as Repository<M, I, C>>::Error> {
        let path = self.create_path_from_id("manifests", id)?;
        tokio::fs::remove_file(path).await
    }
}

/// Initialize a repository if it does not exist.
///
/// This function calls [`initialize_with_mode`] with mode `0o775`.
pub fn initialize<P>(path: P) -> Result<(), IoError>
where
    P: AsRef<Path>,
{
    initialize_with_mode(path, 0o775)
}

/// Initialize a repository if it does not exist.
///
/// This function takes the path to the root directory of the new repository
/// and creates it with the following structures should they not exist:
///
///  - a `chunks` directory containing chunks
///  - a `fossils` directory containing fossils
///  - a `clients` directory containing client data
///  - a `manifests` directory containing manifests
///  - a `incoming` directory containing files in the process of being created
///
/// The `mode` argument determines the mode of newly created directories.
pub fn initialize_with_mode<P>(path: P, mode: u32) -> Result<(), IoError>
where
    P: AsRef<Path>,
{
    fn create_directory(path: &PathBuf, mode: u32) -> Result<(), IoError> {
        DirBuilder::new().mode(mode).create(path).or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })
    }

    let mut buf = path.as_ref().to_owned();
    create_directory(&buf, mode)?;

    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"] {
        buf.push(directory);
        create_directory(&buf, mode)?;
        sync_directory(&buf)?;
        buf.pop();
    }

    sync_directory(&buf)
}
