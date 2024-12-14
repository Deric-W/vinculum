//! Backend implemented in the local unix filesystem.
//! 
//! This backend uses types from [`tokio::fs`] and therefore depends on it.

use crate::{ChunkBackend, ClientBackend, ManifestTimestamp, Repository};
use crate::utils::{timestamp_from_bytes, timestamp_to_bytes};

use std::iter::Iterator;
use std::marker::PhantomData;
use futures::io::{Error as IoError, Result as IoResult, AsyncRead, AsyncWrite, AsyncBufRead};
use futures::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::ReadDirStream;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::fs::{DirBuilder, OpenOptions};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::fs::{OpenOptionsExt, DirBuilderExt};
use std::pin::{pin, Pin};
use futures::stream::{Stream, StreamExt};
use futures::sink::Sink;
use std::task::{ready, Context, Poll};
use std::future::{poll_fn, Future};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use pin_project::pin_project;
use rand::distributions::{Alphanumeric, DistString};

/// Backend implemented in the local unix filesystem.
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
/// complete on its threadpool after their future has been dropped, requiring a shutdown
/// of the used tokio runtime should a future of this backend be canceled and
/// other operations happen only after its completion.
/// 
/// ## Trait bounds
/// 
/// Ids used with this backend will be converted into [`OsString`]s to be used in
/// file names and may therefore not contain path seperators.
/// 
/// The inverse operation may fail, which is interpreted as an corrupted id.
/// This will cause files to be skipped during enumeration and errors to be produced
/// when reading a manifest.
/// 
/// ## Manifest format
/// 
/// The manifest begins with a u8 encoding the length of the byte represention
/// of the creator id, followed by these bytes.
/// Next will be a sequence of u8 encoding the length of the byte representation
/// of a chunk id followed by these bytes until the length is zero.
/// After that there will be a sequence of u16 encoding the length of a batch of
/// additional data in bytes, followed by it until the length is zero.
/// Last there will be the timestamp, stored as seconds and nanoseconds since the
/// unix epoch.
/// 
/// This limits the length of the creator id to [`u8::MAX`] bytes and the length
/// of chunk ids to [`u8::MAX`] bytes and greater than zero.
/// 
/// All numbers are stored in big-endian and therefore portable between architectures.
#[derive(Debug, Clone)]
pub struct FileBackend {
    directory: Box<Path>
}

impl FileBackend {
    /// Create an instance from an initialized directory.
    /// 
    /// This function assumes the directory was initialized using
    /// [`initialize`] and that no other backends exists for the client
    /// for the duration of its existence.
    pub fn new<P>(directory: P) -> FileBackend
    where
        P: AsRef<Path>
    {
        FileBackend {
            directory: directory.as_ref().into()
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

    async fn upload(&self, destination: PathBuf) -> IoResult<impl AsyncWrite> {
        let file = self.create_incoming().await?.compat_write();
        let writer = CommitOnClose {
            state: CommitOnCloseState::Writing(Some((file, commit_file, destination)))
        };
        Ok(futures::io::BufWriter::new(writer))
    }

    async fn upload_manifest(&self, destination: PathBuf) -> IoResult<futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>> {
        let file = self.create_incoming().await?.compat_write();
        let writer = CommitOnClose {
            state: CommitOnCloseState::Writing(Some((file, commit_manifest, destination)))
        };
        Ok(futures::io::BufWriter::new(Box::pin(writer)))
    }

    async fn create_incoming(&self) -> IoResult<RemoveOnDrop> {
        let mut options = OpenOptions::new();
        options.write(true);
        options.create_new(true);
        options.mode(0o444);
        let mut from = self.directory().to_owned();
        from.push("incoming");
        let mut rng = rand::thread_rng();
        let mut buffer = String::with_capacity(16);
        loop {
            Alphanumeric.append_string(&mut rng, &mut buffer, 16);
            from.push(&buffer);
            // perform open and RemoveOnDrop::new as one operation which can not be canceled in between
            let res = asyncify(move || {
                match options.open(&from) {
                    Ok(file) => Ok(RemoveOnDrop::new(tokio::fs::File::from_std(file), from)),
                    Err(e) => Err((e, options, from))
                }
            }).await?;
            match res {
                Ok(file) => return Ok(file),
                Err((e, o, mut f)) if e.kind() == ErrorKind::AlreadyExists => {
                    buffer.clear();
                    f.pop();
                    from = f;
                    options = o;
                },
                Err((e, _, _)) => return Err(e)
            }
        }
    }

    async fn list_directory<P, I>(&self, path: P) -> IoResult<impl Stream<Item = IoResult<I>>>
    where
        P: AsRef<Path>,
        for<'a> I: TryFrom<&'a [u8], Error=()>
    {
        let path = self.directory().join(path);
        let stream = ReadDirStream::new(tokio::fs::read_dir(path).await?);
        let ids = stream.filter_map(|res| std::future::ready(match res {
            Ok(entry) => match I::try_from(entry.file_name().as_encoded_bytes()) {
                Ok(id) => Some(Ok(id)),
                Err(()) => None
            },
            Err(e) => Some(Err(e))
        }));
        Ok(ids)
    }
}

async fn commit_file(file: RemoveOnDrop, to: PathBuf) -> IoResult<()> {
    file.get_inner_ref().sync_all().await?;
    // perform into_inner and rename as one operation which can not be canceled in between
    asyncify(move || {
        let from = file.into_inner().1;
        match std::fs::rename(&from, to) {
            Ok(()) => Ok(()),
            Err(e) => {
                // ignore error when removing temporary file fails
                let _ = std::fs::remove_file(from);
                Err(e)
            }
        }
    }).await?
}

async fn commit_manifest(file: RemoveOnDrop, to: PathBuf) -> IoResult<()> {
    file.get_inner_ref().sync_all().await?;
    let chunks_dir = to.ancestors().nth(2).unwrap().join("chunks");
    // make sure directory entries of uploaded chunks are persisted
    let chunks_dir = asyncify(move || {
        sync_directory(&chunks_dir)?;
        Ok::<PathBuf, IoError>(chunks_dir)
    }).await??;
    // perform into_inner and rename as one operation which can not be canceled in between
    asyncify(move || {
        let from = file.into_inner().1;
        match std::fs::rename(&from, to) {
            Ok(()) => Ok(()),
            Err(e) => {
                // ignore error when removing temporary file fails
                let _ = std::fs::remove_file(from);
                Err(e)
            }
        }
    }).await??;
    // make sure directory entry of manifest is persisted
    asyncify(move || {
        sync_directory(chunks_dir)
    }).await?
}

#[derive(Debug)]
struct RemoveOnDrop {
    inner: Option<(tokio::fs::File, PathBuf)>
}

impl RemoveOnDrop {
    fn new(file: tokio::fs::File, location: PathBuf) -> RemoveOnDrop {
        RemoveOnDrop {
            inner: Some((file, location))
        }
    }

    fn get_inner_ref(&self) -> &tokio::fs::File {
        &self.inner.as_ref().unwrap().0
    }

    fn get_inner_mut(&mut self) -> &mut tokio::fs::File {
        &mut self.inner.as_mut().unwrap().0
    }

    fn into_inner(mut self) -> (tokio::fs::File, PathBuf) {
        self.inner.take().unwrap()
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Some((_, location)) = &self.inner {
            let _ = std::fs::remove_file(location);
        }
    }
}

impl tokio::io::AsyncWrite for RemoveOnDrop {
    fn is_write_vectored(&self) -> bool {
        self.get_inner_ref().is_write_vectored()
    }

    fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
        pin!(self.get_inner_mut()).poll_write(cx, buf)
    }

    fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<Result<usize, std::io::Error>> {
        pin!(self.get_inner_mut()).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
        pin!(self.get_inner_mut()).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
        pin!(self.get_inner_mut()).poll_shutdown(cx)
    }
}

#[pin_project]
#[derive(Debug)]
struct CommitOnClose<C, F, S> {
    #[pin]
    state: CommitOnCloseState<C, F, S>
}

#[pin_project(project = CommitOnCloseStateProj)]
#[derive(Debug)]
enum CommitOnCloseState<C, F, S> {
    Writing(Option<(tokio_util::compat::Compat<RemoveOnDrop>, C, S)>),
    Commiting(#[pin] F)
}

impl<C, F, S> AsyncWrite for CommitOnClose<C, F, S>
where
    C: Fn(RemoveOnDrop, S) -> F,
    F: Future<Output = IoResult<()>>
{
    fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> Poll<IoResult<usize>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => pin!(writer).poll_write(cx, buf),
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(ErrorKind::Other, "writer is being closed"))),
            _ => unreachable!()
        }
    }

    fn poll_write_vectored(
                self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> Poll<IoResult<usize>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => pin!(writer).poll_write_vectored(cx, bufs),
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(ErrorKind::Other, "writer is being closed"))),
            _ => unreachable!()
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<IoResult<()>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => pin!(writer).poll_flush(cx),
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(ErrorKind::Other, "writer is being closed"))),
            _ => unreachable!()
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<IoResult<()>> {
        let mut projection = self.project();
        match projection.state.as_mut().project() {
            CommitOnCloseStateProj::Writing(inner) => {
                let res = match inner {
                    Some((writer, _, _)) => ready!(pin!(writer).poll_close(cx)),
                    None => unreachable!()
                };
                match res {
                    Ok(()) => {
                        let (writer, creator, state) = inner.take().unwrap();
                        let future = creator(writer.into_inner(), state);
                        projection.state.set(CommitOnCloseState::Commiting(future));
                        match projection.state.project() {
                            CommitOnCloseStateProj::Commiting(f) => f.poll(cx),
                            _ => unreachable!()
                        }
                    }
                    Err(e) => Poll::Ready(Err(e))
                }
            },
            CommitOnCloseStateProj::Commiting(future) => future.poll(cx)
        }
    }
}


impl<I> ClientBackend<I> for FileBackend
where
    for<'a> I: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a I: Into<OsString>
{
    type Error = IoError;

    async fn clients(&self) -> IoResult<impl Stream<Item = IoResult<I>>> {
        self.list_directory("clients").await
    }

    async fn client(&self, id: &I) -> IoResult<impl AsyncRead> {
        let mut buf = self.directory().to_owned();
        buf.push("clients");
        buf.push(id.into());
        let file = tokio::fs::File::open(buf).await?;
        Ok(tokio::io::BufReader::new(file).compat())
    }

    async fn add_client(&self, id: &I) -> IoResult<impl AsyncWrite> {
        let mut buf = self.directory().to_owned();
        buf.push("clients");
        buf.push(id.into());
        self.upload(buf).await
    }

    async fn remove_client(&self, id: &I) -> IoResult<()> {
        let mut buf = self.directory().to_owned();
        buf.push("clients");
        buf.push(id.into());
        tokio::fs::remove_file(buf).await
    }
}

impl<C> ChunkBackend<C> for FileBackend
where
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a C: Into<OsString>
{
    type Error = IoError;

    async fn chunks(&self) -> IoResult<impl Stream<Item = IoResult<C>>> {
        self.list_directory("chunks").await
    }

    async fn chunk(&self, id: &C) -> IoResult<impl AsyncRead> {
        let mut buf = self.directory().to_owned();
        let file_name = id.into();
        for _ in 0..2 {
            buf.push("chunks");
            buf.push(&file_name);
            match tokio::fs::File::open(&buf).await {
                Ok(file) => return Ok(tokio::io::BufReader::new(file).compat()),
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e)
            };
            buf.pop();
            buf.pop();
            buf.push("fossils");
            buf.push(&file_name);
            match tokio::fs::File::open(&buf).await {
                Ok(file) => return Ok(tokio::io::BufReader::new(file).compat()),
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e)
            };
        };
        Err(ErrorKind::NotFound.into())
    }

    async fn has_chunk(&self, id: &C) -> IoResult<bool> {
        let mut buf = self.directory().to_owned();
        buf.push("chunks");
        buf.push(id.into());
        tokio::fs::try_exists(buf).await
    }

    async fn add_chunk(&self, id: &C) -> IoResult<impl AsyncWrite> {
        let mut buf = self.directory().to_owned();
        buf.push("chunks");
        buf.push(id.into());
        self.upload(buf).await
    }

    async fn fossils(&self) -> IoResult<impl Stream<Item = IoResult<C>>> {
        self.list_directory("fossils").await
    }

    async fn make_fossil(&self, id: &C) -> IoResult<()> {
        let file_name = id.into();
        let mut from = self.directory().to_owned();
        from.push("chunks");
        from.push(&file_name);
        let mut to = self.directory().to_owned();
        to.push("fossils");
        to.push(file_name);
        match tokio::fs::rename(from, to).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e)
        }
    }

    async fn recover_fossil(&self, id: &C) -> IoResult<()> {
        let file_name = id.into();
        let mut from = self.directory().to_owned();
        from.push("fossils");
        from.push(&file_name);
        let mut to = self.directory().to_owned();
        to.push("chunks");
        to.push(file_name);
        match tokio::fs::rename(from, to).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e)
        }
    }

    async fn delete_fossil(&self, id: &C) -> IoResult<()> {
        let mut buf = self.directory().to_owned();
        buf.push("fossils");
        buf.push(id.into());
        match tokio::fs::remove_file(buf).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e)
        }
    }
}

/// Error produced by manifest encoding operations.
#[derive(Debug)]
pub enum ManifestEncodingError {
    /// An I/O error occured.
    IoError(IoError),
    /// The length of the creator id exceeds [`u8::MAX`] bytes.
    InvalidCreator,
    /// The length of the chunk id exceeds [`u8::MAX`] bytes or is empty.
    InvalidChunk,
    /// Calculating the timestamp failed, containing the difference from [`std::time::UNIX_EPOCH`].
    InvalidTimestamp(std::time::SystemTimeError),
    /// Invalid operation (such as adding more chunks after closing the builder).
    InvalidOperation
}

impl std::fmt::Display for ManifestEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestEncodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestEncodingError::InvalidCreator => write!(f, "invalid creator"),
            ManifestEncodingError::InvalidChunk => write!(f, "invalid chunk"),
            ManifestEncodingError::InvalidTimestamp(e) => write!(f, "invalid timestamp: {}", e),
            Self::InvalidOperation => write!(f, "invalid operation")
        }
    }
}

impl std::error::Error for ManifestEncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestEncodingError::IoError(e) => e.source(),
            ManifestEncodingError::InvalidTimestamp(e) => e.source(),
            _ => None
        }
    }
}

/// State of the manifest chunk encoding process
#[derive(Debug)]
enum ChunksEncodingState {
    /// No operation is pending
    Idle,
    /// Chunk pending with the number of bytes already written (+ length)
    ChunkPending(OsString, usize),
    /// Timestamp pending with the number of bytes already written (+ zero lengths of chunks and data batches)
    TimestampPending([u8; 12], usize),
    /// Timestamp was written
    Finished
}

// TODO: document size limit, use new format, better errors
#[pin_project]
pub struct ManifestBuilder {
    // since CommitOnClose has unnameable type parameters
    #[pin]
    writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>,
    state: ChunksEncodingState
}

impl ManifestBuilder {
    fn new(writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>) -> ManifestBuilder {
        ManifestBuilder {
            writer,
            state: ChunksEncodingState::Idle
        }
    }

    async fn from_upload<I>(mut writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>, client: &I) -> Result<ManifestBuilder, ManifestEncodingError>
    where
        for<'a> &'a I: Into<OsString>
    {
        let creator: OsString = client.into();
        match <usize as TryInto<u8>>::try_into(creator.len()) {
            Ok(length) => {
                let mut pinned = Pin::new(&mut writer);
                pinned.as_mut().write_all(&length.to_be_bytes()).await.map_err(|e| ManifestEncodingError::IoError(e))?;
                pinned.as_mut().write_all(creator.as_encoded_bytes()).await.map_err(|e| ManifestEncodingError::IoError(e))?;
            },
            Err(_) => return Err(ManifestEncodingError::InvalidCreator)
        }
        Ok(ManifestBuilder::new(writer))
    }

    /// Try to transition from [`ChunksEncodingState::ChunkPending`], returning the new state.
    fn poll_flush_chunk(cx: &mut Context<'_>, mut writer: Pin<&mut dyn AsyncWrite>, chunk: &OsStr, written: &mut usize) -> Poll<Result<ChunksEncodingState, ManifestEncodingError>> {
        let length: u8 = chunk.len().try_into().unwrap();
        while *written < 1 {
            let bytes = length.to_be_bytes();
            match ready!(writer.as_mut().poll_write(cx, &bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                },
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
            }
        }
        while *written - 1 < chunk.len() {
            let bytes = &chunk.as_encoded_bytes()[*written - 1..];
            match ready!(writer.as_mut().poll_write(cx, bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                },
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
            }
        }
        Poll::Ready(Ok(ChunksEncodingState::Idle))
    }

    /// Try to transition from [`ChunksEncodingState::TimestampPending`], returning the new state.
    fn poll_flush_timestamp(cx: &mut Context<'_>, mut writer: Pin<&mut dyn AsyncWrite>, timestamp: &[u8], written: &mut usize) -> Poll<Result<ChunksEncodingState, ManifestEncodingError>> {
        while *written < 3 {
            match ready!(writer.as_mut().poll_write(cx, &[0, 0, 0])) {
                Ok(amount) => {
                    *written = *written + amount;
                },
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
            }
        }
        while *written - 3 < 12 {
            match ready!(writer.as_mut().poll_write(cx, timestamp)) {
                Ok(amount) => {
                    *written = *written + amount;
                },
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
            }
        }
        Poll::Ready(Ok(ChunksEncodingState::Finished))
    }
}

impl<C> Sink<&C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>
{
    type Error = ManifestEncodingError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::Idle => return Poll::Ready(Ok(())),
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(cx, this.writer.as_mut(), chunk, written))?;
                },
                _ => return Poll::Ready(Err(ManifestEncodingError::InvalidOperation))
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: &C) -> Result<(), Self::Error> {
        if let ChunksEncodingState::Idle = self.state {
            let string: OsString = item.into();
            let size: Result<u8, _> = string.len().try_into();
            return match size {
                Ok(len) if len > 0 => {
                    self.state = ChunksEncodingState::ChunkPending(string, 0);
                    Ok(())
                },
                Ok(_) | Err(_) => Err(ManifestEncodingError::InvalidChunk)
            }
        }
        Err(ManifestEncodingError::InvalidOperation)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(cx, this.writer.as_mut(), chunk, written))?;
                },
                _ => return Poll::Ready(ready!(this.writer.as_mut().poll_flush(cx)).map_err(|e| ManifestEncodingError::IoError(e)))
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::Idle => {
                    match timestamp_to_bytes(std::time::SystemTime::now()) {
                        Ok(bytes) => {
                            *this.state = ChunksEncodingState::TimestampPending(bytes, 0);
                        },
                        Err(e) => return Poll::Ready(Err(ManifestEncodingError::InvalidTimestamp(e)))
                    }
                },
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(cx, this.writer.as_mut(), chunk, written))?;
                },
                ChunksEncodingState::TimestampPending(ref timestamp, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_timestamp(cx, this.writer.as_mut(), timestamp, written))?;
                },
                ChunksEncodingState::Finished => return Poll::Ready(ready!(this.writer.as_mut().poll_close(cx)).map_err(|e| ManifestEncodingError::IoError(e)))
            }
        }
    }
}

impl<C> crate::ManifestBuilder<C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>
{
    type Error = ManifestEncodingError;

    type Data = ManifestBuilderData;

    async fn add_data(mut self) -> Result<Self::Data, ManifestEncodingError> {
        let mut writer = Pin::new(&mut self.writer);
        loop {
            match self.state {
                ChunksEncodingState::Idle => {
                    writer.write_all(&[0]).await.map_err(|e| ManifestEncodingError::IoError(e))?;
                    writer.flush().await.map_err(|e| ManifestEncodingError::IoError(e))?;
                    return Ok(ManifestBuilderData::new(self.writer.into_inner()))
                },
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    self.state = poll_fn(|cx| ManifestBuilder::poll_flush_chunk(cx, writer.as_mut(), chunk, written)).await?;
                },
                _ => return Err(ManifestEncodingError::InvalidOperation)
            }
        }
    }
}

/// State of the manifest chunk encoding process
#[derive(Debug)]
enum DataEncodingState {
    /// Buffer contains only some amount of unwritten data, with the first two bytes being the length (not yet set)
    Accumulating(usize),
    /// Buffer contains partially witten data, with the number of bytes written and the total amount
    DataPending(usize, usize),
    /// Buffer contains partially written data and timestamp, with the number of bytes written and the total amount
    TimestampPending(usize, usize),
    /// Timestamp was written
    Finished
}

/// Implementation of [`crate::ManifestBuilder::Data`].
#[pin_project]
pub struct ManifestBuilderData {
    writer: Pin<Box<dyn AsyncWrite>>,
    state: DataEncodingState,
    // we need more control over the buffer
    buf: Box<[u8]>
}

impl ManifestBuilderData {
    fn new(writer: Pin<Box<dyn AsyncWrite>>) -> ManifestBuilderData {
        ManifestBuilderData {
            writer,
            state: DataEncodingState::Accumulating(2),
            buf: vec![0; <u16 as Into<usize>>::into(u16::MAX).saturating_add(2)].into_boxed_slice()
        }
    }

    /// Set the length in the buffer
    fn set_length(buf: &mut [u8], length: usize) {
        let data_length: u16 = (length - 2).try_into().unwrap();
        buf[..2].copy_from_slice(&data_length.to_be_bytes());
        }

    /// Try to transition from [`DataEncodingState::DataPending`] or [`DataEncodingState::TimestampPending`] by flushing the buffer.
    fn poll_data_pending(cx: &mut Context<'_>, mut writer: Pin<&mut dyn AsyncWrite>, buf: &[u8], written: &mut usize, length: usize) -> Poll<Result<(), IoError>> {
        while *written < length {
            let bytes = &buf[*written..length];
            match ready!(writer.as_mut().poll_write(cx, bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                },
                Err(e) => return Poll::Ready(Err(e))
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ManifestBuilderData {
    fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<IoResult<usize>> {
        let this = self.project();
        loop {
            match this.state {
                DataEncodingState::Accumulating(ref mut length) if *length < this.buf.len() => {
                    let consumed = std::cmp::min(this.buf.len() - *length, buf.len());
                    let dst = &mut this.buf[*length..*length + consumed];
                    let src = &buf[..consumed];
                    dst.copy_from_slice(src);
                    *length = *length + consumed;
                    return Poll::Ready(Ok(consumed))
                },
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                },
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(cx, this.writer.as_mut(), this.buf, written, *length))?;
                    *this.state = DataEncodingState::Accumulating(2);
                },
                _ => return Poll::Ready(Err(IoError::other("invalid operation, writer is closed")))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let this = self.project();
        loop {
            match this.state {
                DataEncodingState::Accumulating(length) if *length <= 2 => return this.writer.as_mut().poll_flush(cx),
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                },
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(cx, this.writer.as_mut(), this.buf, written, *length))?;
                    *this.state = DataEncodingState::Accumulating(2);
                },
                DataEncodingState::TimestampPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(cx, this.writer.as_mut(), this.buf, written, *length))?;
                    *this.state = DataEncodingState::Finished;
                },
                DataEncodingState::Finished => return this.writer.as_mut().poll_flush(cx)
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let this = self.project();
        loop {
            match this.state {
                DataEncodingState::Accumulating(length) if this.buf.len() - *length >= 14 => {
                    // dont write zero length two times
                    let start = if *length <= 2 {
                        0
                    } else {
                        ManifestBuilderData::set_length(this.buf, *length);
                        *length
                    };
                    let timestamp = match timestamp_to_bytes(std::time::SystemTime::now()) {
                        Ok(t) => t,
                        Err(_) => return Poll::Ready(Err(IoError::other("invalid timestamp")))
                    };
                    this.buf[start.. start + 2].copy_from_slice(&0u16.to_be_bytes());
                    this.buf[start + 2..start + 14].copy_from_slice(&timestamp);
                    *this.state = DataEncodingState::TimestampPending(0, start + 14);
                },
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                },
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(cx, this.writer.as_mut(), this.buf, written, *length))?;
                    *this.state = DataEncodingState::Accumulating(2);
                },
                DataEncodingState::TimestampPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(cx, this.writer.as_mut(), this.buf, written, *length))?;
                    *this.state = DataEncodingState::Finished;
                },
                DataEncodingState::Finished => return Poll::Ready(ready!(this.writer.as_mut().poll_close(cx)))
            }
        }
    }
}

/// Error produced by manifest decoding operations.
#[derive(Debug)]
pub enum ManifestDecodingError {
    /// An I/O error occured.
    IoError(IoError),
    /// The parsing of the creator failed.
    InvalidCreator,
    /// The parsing of a chunk failed.
    InvalidChunk,
    /// The parsing of the timestamp failed, containing the seconds and nanoseconds since [`std::time::UNIX_EPOCH`].
    InvalidTimestamp(u64, u32)
}

impl std::fmt::Display for ManifestDecodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestDecodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestDecodingError::InvalidCreator => write!(f, "invalid creator"),
            ManifestDecodingError::InvalidChunk => write!(f, "invalid chunk"),
            ManifestDecodingError::InvalidTimestamp(secs, nsecs) => write!(f, "invalid timestamp (secs: {0}, nsecs: {1}", secs, nsecs)
        }
    }
}

impl std::error::Error for ManifestDecodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestDecodingError::IoError(e) => e.source(),
            _ => None
        }
    }
}

/// Decoder of the manifest format used by [`FileBackend`].
#[derive(Debug)]
pub struct Manifest<I, C> {
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    creator: I,
    // to reuse the allocation
    buf: Box<[u8]>,
    phantom: PhantomData<Box<C>>
}

impl<I, C> Manifest<I, C> {
    /// Expects buf.len() >= u8::MAX
    fn new(reader: futures::io::BufReader<Compat<tokio::fs::File>>, creator: I, buf: Box<[u8]>) -> Manifest<I, C> {
        Manifest {
            reader,
            creator,
            buf,
            phantom: PhantomData
        }
    }
}

impl<I, C> Manifest<I, C>
where
    for<'a> I: TryFrom<&'a [u8], Error=()>,
{
    async fn from_file(mut reader: futures::io::BufReader<Compat<tokio::fs::File>>) -> Result<Manifest<I, C>, ManifestDecodingError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf).await.map_err(|e| ManifestDecodingError::IoError(e))?;
        let length = u8::from_be_bytes(buf);
        let mut buf = vec![0; u8::MAX.into()].into_boxed_slice();
        let creator_buf = &mut buf[..length.into()];
        reader.read_exact(creator_buf).await.map_err(|e| ManifestDecodingError::IoError(e))?;
        let creator = I::try_from(creator_buf).map_err(|_| ManifestDecodingError::InvalidCreator)?;
        Ok(Manifest::new(reader, creator, buf))
    }
}

impl<I, C> crate::Manifest<I, C> for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>
{
    type Error = ManifestDecodingError;

    type Chunks = ManifestChunks<C>;

    type ReferencedChunks = ManifestChunks<C>;

    fn creator(&self) -> &I {
        &self.creator
    }

    fn into_chunks(self) -> (I, Self::Chunks) {
        (self.creator, ManifestChunks::new(self.reader, self.buf))
    }

    fn into_referenced_chunks(self) -> (I, Self::ReferencedChunks) {
        self.into_chunks()
    }
}

/// State of the decoding process of the chunks.
#[derive(Debug, Clone, Copy)]
enum ChunksDecodingState {
    /// Reading a length field of a chunk id, containing the number of bytes already read.
    ChunkLength(u8),
    /// Reading a chunk id, containing the number of bytes already read and total length.
    Chunk(u8, u8),
    /// Finished decoding process.
    Finished
}

/// Implementation of [`crate::Manifest::Chunks`] and [`crate::Manifest::ReferencedChunks`].
/// 
/// Since this backend stores data withother further modification the chunks provided
/// by the client are identical to the chunks referenced by the manifest.
#[derive(Debug)]
#[pin_project]
pub struct ManifestChunks<C> {
    #[pin]
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    buf: Box<[u8]>,
    state: ChunksDecodingState,
    phantom: PhantomData<Box<C>>
}

impl<C> ManifestChunks<C> {
    /// Expects buf.len() >= u8::MAX
    fn new(reader: futures::io::BufReader<Compat<tokio::fs::File>>, buf: Box<[u8]>) -> ManifestChunks<C> {
        ManifestChunks {
            reader,
            buf,
            state: ChunksDecodingState::ChunkLength(0),
            phantom: PhantomData
        }
    }
}

impl<C> ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>
{
    /// Try to transition from [`ChunksDecodingState::ChunkLength`], returning the new state.
    fn poll_chunk_length(cx: &mut Context<'_>, reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, buf: &mut [u8], read: &mut u8) -> Poll<Result<ChunksDecodingState, ManifestDecodingError>> {
        if *read < 1 {
            match ready!(reader.poll_read(cx, &mut buf[..1])) {
                Ok(length) if length > 0 => (),
                Ok(_) => return Poll::Ready(Err(ManifestDecodingError::IoError(ErrorKind::UnexpectedEof.into()))),
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e)))
            }
        }
        let length = buf[0];
        if length == 0 {
            Poll::Ready(Ok(ChunksDecodingState::Finished))
        } else {
            Poll::Ready(Ok(ChunksDecodingState::Chunk(0, length)))
        }
    }

    /// Try to transition from [`ChunksDecodingState::Chunk`], returning the chunk and new state.
    fn poll_chunk(cx: &mut Context<'_>, mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, buf: &mut [u8], read: &mut u8, length: u8) -> Poll<Result<(C, ChunksDecodingState), ManifestDecodingError>> {
        let bytes = &mut buf[..length.into()];
        while *read < length {
            match ready!(reader.as_mut().poll_read(cx, &mut bytes[(*read).into()..])) {
                Ok(length) if length > 0 => {
                    *read = *read + length as u8;
                },
                Ok(_) => return Poll::Ready(Err(ManifestDecodingError::IoError(ErrorKind::UnexpectedEof.into()))),
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e)))
            }
        }
        let chunk = C::try_from(bytes).map_err(|_| ManifestDecodingError::InvalidChunk)?;
        Poll::Ready(Ok((chunk, ChunksDecodingState::ChunkLength(0))))
    }

    /// Try to transition from [`DecodingState::Chunk`], returning the new state.
    /// 
    /// This function assumes the buffer has a length == length of the chunk.
    fn poll_skip_chunk(cx: &mut Context<'_>, reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, read: u8, length: u8) -> Poll<Result<ChunksDecodingState, ManifestDecodingError>> {
        if read < length {
            ready!(reader.poll_seek_relative(cx, (length - read).into())).map_err(|e| ManifestDecodingError::IoError(e))?;
        }
        Poll::Ready(Ok(ChunksDecodingState::ChunkLength(0)))
    }
}

impl<C> Stream for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>
{
    type Item = Result<C, ManifestDecodingError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    *this.state = ready!(ManifestChunks::<C>::poll_chunk_length(cx, this.reader.as_mut(), this.buf, read))?;
                },
                ChunksDecodingState::Chunk(ref mut read, length) => {
                    let (chunk, state) = ready!(ManifestChunks::<C>::poll_chunk(cx, this.reader.as_mut(), this.buf, read, *length))?;
                    *this.state = state;
                    return Poll::Ready(Some(Ok(chunk)))
                },
                ChunksDecodingState::Finished => return Poll::Ready(None)
            }
        }
    }
}

impl<C> crate::ManifestChunks<C> for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>
{
    type Error = ManifestDecodingError;

    type Data = ManifestData;

    async fn into_data(mut self) -> Result<Self::Data, Self::Error> {
        let mut reader = Pin::new(&mut self.reader);
        loop {
            match self.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    self.state = poll_fn(|cx| ManifestChunks::<C>::poll_chunk_length(cx, reader.as_mut(), &mut self.buf, read)).await?;
                },
                ChunksDecodingState::Chunk(read, length) => {
                    self.state = poll_fn(|cx| ManifestChunks::<C>::poll_skip_chunk(cx, reader.as_mut(), read, length)).await?;
                },
                ChunksDecodingState::Finished => {
                    return Ok(ManifestData::new(self.reader))
                }
            }
        }
    }
}

/// State of the decoding process of the data.
#[derive(Debug, Clone, Copy)]
enum DataDecodingState {
    /// Reading a length field of a data batch, containing the number of bytes already read.
    DataLength(u8),
    /// Reading a data batch, containing the number of bytes remanining.
    Data(u16),
    /// Reading a timestamp, containing the number of bytes already read.
    Timestamp(u8)
}

/// Implementation of [`crate::ManifestChunks::Data`].
#[derive(Debug)]
#[pin_project]
pub struct ManifestData {
    #[pin]
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    state: DataDecodingState,
    buf: [u8; 12]
}

impl ManifestData {
    fn new(reader: futures::io::BufReader<Compat<tokio::fs::File>>) -> ManifestData {
        ManifestData {
            reader,
            state: DataDecodingState::DataLength(0),
            buf: [0; 12]
        }
    }

    /// Try to transition from [`DataDecodingState::DataLength`], returning the new state.
    fn poll_data_length(cx: &mut Context<'_>, mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, buf: &mut [u8; 12], read: &mut u8) -> Poll<Result<DataDecodingState, IoError>> {
        while (*read) < 2 {
            match ready!(reader.as_mut().poll_read(cx, &mut buf[(*read).into()..2])) {
                Ok(length) if length > 0 => {
                    *read = *read + length as u8;
                },
                Ok(_) => return Poll::Ready(Err(ErrorKind::UnexpectedEof.into())),
                Err(e) => return Poll::Ready(Err(e))
            }
        }
        let length = u16::from_be_bytes(buf[..2].try_into().unwrap());
        if length == 0 {
            Poll::Ready(Ok(DataDecodingState::Timestamp(0)))
        } else {
            Poll::Ready(Ok(DataDecodingState::Data(length)))
        }
    }

    /// Try to transition from [`DataDecodingState::Data`], returning the amount of data read and the new state.
    fn poll_data(cx: &mut Context<'_>, mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, remaining: &mut u16, requested: &mut [u8]) -> Poll<Result<(usize, DataDecodingState), IoError>> {
        if requested.len() == 0 {
            return Poll::Ready(Ok((0, DataDecodingState::Data(*remaining))))
        }
        if *remaining > 0 {
            match ready!(reader.as_mut().poll_fill_buf(cx)) {
                Ok(data) if data.len() > 0 => {
                    let read_data = std::cmp::min((*remaining).into(), data.len());
                    let consumed_data = std::cmp::min(read_data, requested.len());
                    *remaining = *remaining - consumed_data as u16;
                    requested[..consumed_data].copy_from_slice(&data[..consumed_data]);
                    reader.consume(consumed_data);
                    if *remaining > 0 {
                        return Poll::Ready(Ok((consumed_data, DataDecodingState::Data(*remaining))))
                    } else {
                        return Poll::Ready(Ok((consumed_data, DataDecodingState::DataLength(0))))
                    }
                },
                Ok(_) => return Poll::Ready(Err(ErrorKind::UnexpectedEof.into())),
                Err(e) => return Poll::Ready(Err(e))
            }
        }
        Poll::Ready(Ok((0, DataDecodingState::DataLength(0))))
    }

    /// Try to transition from [`DataDecodingState::Data`], returning the new state.
    fn poll_skip_data(cx: &mut Context<'_>, reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, remaining: u16) -> Poll<Result<DataDecodingState, IoError>> {
        if remaining > 0 {
            ready!(reader.poll_seek_relative(cx, remaining.into()))?;
        }
        Poll::Ready(Ok(DataDecodingState::DataLength(0)))
    }

    /// Try to transition from [`DataDecodingState::Timestamp`], returning the timestamp.
    /// 
    /// This function assumes the buffer has a length >= 12.
    fn poll_timestamp(cx: &mut Context<'_>, mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>, buf: &mut [u8; 12], read: &mut u8) -> Poll<Result<std::time::SystemTime, ManifestDecodingError>> {
        while (*read) < 12 {
            match ready!(reader.as_mut().poll_read(cx, &mut buf[(*read).into()..12])) {
                Ok(length) if length > 0 => {
                    *read = *read + length as u8;
                },
                Ok(_) => return Poll::Ready(Err(ManifestDecodingError::IoError(ErrorKind::UnexpectedEof.into()))),
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e)))
            }
        }
        match timestamp_from_bytes(buf.as_slice().try_into().unwrap()) {
            Ok(timestamp) => Poll::Ready(Ok(timestamp)),
            Err((secs, nsecs)) => Poll::Ready(Err(ManifestDecodingError::InvalidTimestamp(secs, nsecs)))
        }
    }
}

impl AsyncRead for ManifestData {
    fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<IoResult<usize>> {
        let mut this = self.project();
        loop {
            match this.state {
                DataDecodingState::DataLength(ref mut read) => {
                    *this.state = ready!(ManifestData::poll_data_length(cx, this.reader.as_mut(), this.buf, read))?;
                },
                DataDecodingState::Data(ref mut remaining) => {
                    let (read, state) = ready!(ManifestData::poll_data(cx, this.reader.as_mut(), remaining, buf))?;
                    *this.state = state;
                    return Poll::Ready(Ok(read))
                },
                DataDecodingState::Timestamp(_) => return Poll::Ready(Ok(0))
            }
        }
    }
}

impl ManifestTimestamp for ManifestData {
    type Error = ManifestDecodingError;

    async fn into_timestamp(mut self) -> Result<std::time::SystemTime, Self::Error> {
        let mut reader = pin!(self.reader);
        loop {
            match self.state {
                DataDecodingState::DataLength(ref mut read) => {
                    self.state = poll_fn(|cx| ManifestData::poll_data_length(cx, reader.as_mut(), &mut self.buf, read)).await.map_err(|e| ManifestDecodingError::IoError(e))?;
                },
                DataDecodingState::Data(remaining) => {
                    self.state = poll_fn(|cx| ManifestData::poll_skip_data(cx, reader.as_mut(), remaining)).await.map_err(|e| ManifestDecodingError::IoError(e))?;
                },
                DataDecodingState::Timestamp(ref mut read) => {
                    let timestamp = poll_fn(|cx| ManifestData::poll_timestamp(cx, reader.as_mut(), &mut self.buf, read)).await?;
                    return Ok(timestamp);
                }
            }
        }
    }
}

impl<C> crate::ManifestTimestamp for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>
{
    type Error = ManifestDecodingError;

    async fn into_timestamp(self) -> Result<std::time::SystemTime, Self::Error> {
        let data = <ManifestChunks<C> as crate::ManifestChunks<C>>::into_data(self).await?;
        data.into_timestamp().await
    }
}

impl<M, I, C> Repository<M, I, C> for FileBackend
where
    for<'a> I: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a I: Into<OsString>,
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a C: Into<OsString>,
    for<'a> M: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a M: Into<OsString>
{
    type Error = IoError;

    type Builder = ManifestBuilder;

    type Manifest = Manifest<I, C>;

    async fn manifests(&self) -> Result<impl Stream<Item = Result<M, <Self as Repository<M, I, C>>::Error>>, <Self as Repository<M, I, C>>::Error> {
        self.list_directory("manifests").await
    }

    async fn manifest(&self, id: &M) -> Result<Self::Manifest, ManifestDecodingError> {
        let mut buf = self.directory().to_owned();
        buf.push("manifests");
        buf.push(id.into());
        let file = tokio::fs::File::open(buf).await.map_err(|e|  ManifestDecodingError::IoError(e))?;
        let reader = futures::io::BufReader::new(file.compat());
        Manifest::from_file(reader).await
    }

    // TODO: better handling of oversized client ids, better encoding / decoding errors in general (try to not use as)
    async fn create_manifest(&self, id: &M, client: &I) -> Result<Self::Builder, ManifestEncodingError> {
        let mut buf = self.directory().to_owned();
        buf.push("manifests");
        buf.push(id.into());
        let writer = self.upload_manifest(buf).await.map_err(|e|  ManifestEncodingError::IoError(e))?;
        ManifestBuilder::from_upload(writer, client).await
    }

    async fn remove_manifest(&self, id: &M) -> Result<(), <Self as Repository<M, I, C>>::Error> {
        let mut buf = self.directory().to_owned();
        buf.push("manifests");
        buf.push(id.into());
        tokio::fs::remove_file(buf).await
    }
}

/// Initialize a repository if it does not exist.
/// 
/// This function calls [`FileBackend::initialize_with_mode`] with mode `0o775`.
pub fn initialize<P>(path: P) -> Result<(), IoError>
where
    P: AsRef<Path>
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
    P: AsRef<Path>
{
    fn create_directory(path: &PathBuf, mode: u32) -> Result<(), IoError> {
        DirBuilder::new()
            .mode(mode)
            .create(path)
            .or_else(|e|{
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })
    }

    let mut buf = path.as_ref().to_owned();
    create_directory(&buf, mode)?;

    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"]{
        buf.push(directory);
        create_directory(&buf, mode)?;
        sync_directory(&buf)?;
        buf.pop();
    }

    sync_directory(&buf)
}


fn sync_directory<P>(path: P) -> Result<(), IoError>
where
    P: AsRef<Path>
{
    let fd = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)?;
    fsync(fd.as_fd())
}

fn fsync(fd: BorrowedFd<'_>) -> Result<(), IoError> {
    let res = unsafe { libc::fsync(fd.as_raw_fd()) };
    if res < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

async fn asyncify<F, R>(f: F) -> IoResult<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => Ok(res),
        Err(_) => Err(IoError::new(ErrorKind::Other, "tokio task failed"))
    }
}
