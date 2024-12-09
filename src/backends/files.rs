//! Backend implemented in the local unix filesystem.
//! 
//! This backend uses types from [`tokio::fs`] and therefore depends on it.

use crate::{ChunkBackend, ClientBackend, Repository};
use crate::utils::{timestamp_from_bytes, timestamp_to_bytes};

use std::iter::Iterator;
use std::marker::PhantomData;
use futures::io::{Error as IoError, Result as IoResult, AsyncRead, AsyncWrite};
use futures::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::ReadDirStream;
use std::ffi::OsString;
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
use std::future::Future;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use pin_project::pin_project;
use rand::distributions::{Alphanumeric, DistString};

// TODO: document power loss guarantees, tokio cancellation behaviour and TryInto semantic and manifest format
// Format: u32 length, creator, (u16 length, chunk)*, 0u16, i64 secs, u32 nsecs, custom (move timestamp + length of custom data to end and use seeking?)
#[derive(Debug)]
pub struct FileBackend {
    directory: PathBuf
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
            directory: directory.as_ref().to_owned()
        }
    }

    /// The directory containing this repository.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Consume this object, returning the directory used for creation.
    pub fn into_inner(self) -> PathBuf {
        self.directory
    }

    async fn upload(&self, destination: PathBuf) -> IoResult<impl AsyncWrite> {
        let file = self.create_incoming().await?.compat_write();
        let writer = CommitOnClose {
            state: CommitOnCloseState::Writing(Some((file, commit_file, destination)))
        };
        Ok(futures::io::BufWriter::new(writer))
    }

    async fn upload_manifest(&self, destination: PathBuf) -> IoResult<impl AsyncWrite> {
        let file = self.create_incoming().await?.compat_write();
        let writer = CommitOnClose {
            state: CommitOnCloseState::Writing(Some((file, commit_manifest, destination)))
        };
        Ok(futures::io::BufWriter::new(writer))
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

// TODO: document size limit
pub struct ManifestBuilder {
    // since CommitOnClose has unnameable type parameters
    writer: Pin<Box<dyn AsyncWrite>>,
    state: EncodingState
}

enum EncodingState {
    Idle,
    ChunkPending(OsString, u32),
    TimestampPending([u8; 12], u8),
    Finished
}

#[derive(Debug)]
pub enum ManifestEncodingError {
    IoError(IoError),
    EncodingError
}

impl std::fmt::Display for ManifestEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestEncodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestEncodingError::EncodingError => write!(f, "encoding error")
        }
    }
}

impl std::error::Error for ManifestEncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestEncodingError::IoError(e) => e.source(),
            ManifestEncodingError::EncodingError => None
        }
    }
}

impl ManifestBuilder {
    fn poll_flush_chunk(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ManifestEncodingError>> {
        if let EncodingState::Idle = self.state {
            return Poll::Ready(Ok(()))
        }
        if let EncodingState::ChunkPending(ref chunk, ref mut written) = self.state {
            while *written < 2 {
                let size: u16 = chunk.len().try_into().unwrap();
                let bytes = size.to_be_bytes();
                match ready!(self.writer.as_mut().poll_write(cx, &bytes)) {
                    Ok(amount) => {
                        *written = *written + amount as u32;
                    },
                    Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
                }
            }
            while *written - 2 < chunk.len() as u32 {
                let bytes = &chunk.as_encoded_bytes()[*written as usize..];
                match ready!(self.writer.as_mut().poll_write(cx, bytes)) {
                    Ok(amount) => {
                        *written = *written + amount as u32;
                    },
                    Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
                }
            }
            self.state = EncodingState::Idle;
            return Poll::Ready(Ok(()))
        }
        Poll::Ready(Err(ManifestEncodingError::EncodingError))
        
    }

    fn poll_flush_timestamp(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ManifestEncodingError>> {
        if let EncodingState::Finished = self.state {
            return Poll::Ready(Ok(()))
        }
        if let EncodingState::TimestampPending(ref buf, ref mut written) = self.state {
            while *written < 2 {
                match ready!(self.writer.as_mut().poll_write(cx, &[0; 2])) {
                    Ok(amount) => {
                        *written = *written + amount as u8;
                    },
                    Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
                }
            }
            while *written - 2 < buf.len() as u8 {
                match ready!(self.writer.as_mut().poll_write(cx, buf)) {
                    Ok(amount) => {
                        *written = *written + amount as u8;
                    },
                    Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e)))
                }
            }
            self.state = EncodingState::Finished;
            return Poll::Ready(Ok(()))
        }
        Poll::Ready(Err(ManifestEncodingError::EncodingError))
    }

    fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ManifestEncodingError>> {
        loop {
            match self.state {
                EncodingState::Idle => {
                    let timestamp = std::time::SystemTime::now();
                    match timestamp_to_bytes(timestamp) {
                        Ok(buf) => {
                            self.state = EncodingState::TimestampPending(buf, 0);
                        },
                        Err(_) => return Poll::Ready(Err(ManifestEncodingError::EncodingError))
                    }
                    ready!(self.poll_flush_timestamp(cx))?;
                },
                EncodingState::ChunkPending(..) => ready!(self.poll_flush_chunk(cx))?,
                EncodingState::TimestampPending(..) => ready!(self.poll_flush_timestamp(cx))?,
                EncodingState::Finished => return Poll::Ready(Ok(()))
            }
        }
    }
}

impl<C> Sink<&C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>
{
    type Error = ManifestEncodingError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush_chunk(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: &C) -> Result<(), Self::Error> {
        if let EncodingState::Idle = self.state {
            let string: OsString = item.into();
            let size: Result<u16, _> = string.len().try_into();
            return match size {
                Ok(len) if len > 0 => {
                    self.state = EncodingState::ChunkPending(string, 0);
                    Ok(())
                },
                Ok(_) => Err(ManifestEncodingError::EncodingError),
                Err(_) => Err(ManifestEncodingError::EncodingError)
            }
        }
        Err(ManifestEncodingError::EncodingError)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush_chunk(cx))?;
        match ready!(pin!(self.writer.as_mut()).poll_flush(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) =>  Poll::Ready(Err(ManifestEncodingError::IoError(e)))
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.poll_finished(cx))?;
        match ready!(pin!(self.writer.as_mut()).poll_close(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) =>  Poll::Ready(Err(ManifestEncodingError::IoError(e)))
        }
    }
}

impl<C> crate::ManifestBuilder<C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>
{
    type Error = ManifestEncodingError;

    async fn add_data(mut self) -> Result<impl AsyncWrite, ManifestEncodingError> {
        std::future::poll_fn(|cx| self.poll_finished(cx)).await?;
        Ok(self.writer)
    }
}

#[derive(Debug)]
pub enum ManifestDecodingError {
    IoError(IoError),
    DecodingError
}

impl std::fmt::Display for ManifestDecodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestDecodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestDecodingError::DecodingError => write!(f, "decoding error"),
        }
    }
}

impl std::error::Error for ManifestDecodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestDecodingError::IoError(e) => e.source(),
            ManifestDecodingError::DecodingError => None
        }
    }
}

#[derive(Debug)]
pub struct Manifest<I, C> {
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    buf: Vec<u8>,
    state: DecodingState,
    creator: I,
    phantom: PhantomData<Box<C>>
}

#[derive(Debug, Clone, Copy)]
enum DecodingState {
    Length(u8),
    Chunk(u16, u16),
    Finished
}

// TODO: try to remove Unpin bound und replace length in chunk state by vector length
impl<I, C> Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    I: Unpin
{
    fn poll_length(&mut self, mut read: u8, cx: &mut Context<'_>) -> Poll<Option<Result<u16, ManifestDecodingError>>> {
        if self.buf.len() < 2 {
            self.buf.resize(2, 0);
        }
        let bytes = &mut self.buf[read as usize..2];
        loop {
            match ready!(pin!(&mut self.reader).poll_read(cx, bytes)) {
                Ok(amount) if read as usize + amount >= 2 => {
                    let length = u16::from_be_bytes(bytes.try_into().unwrap());
                    if length == 0 {
                        self.state = DecodingState::Finished;
                        return Poll::Ready(None)
                    } else {
                        if self.buf.len() < length as usize {
                            self.buf.resize(length as usize, 0);
                        }
                        self.state = DecodingState::Chunk(length, 0);
                        return Poll::Ready(Some(Ok(length)))
                    }
                },
                Ok(amount) if amount > 0 => {
                    read += amount as u8;
                    self.state = DecodingState::Length(read);
                },
                Ok(_) => return Poll::Ready(Some(Err(ManifestDecodingError::DecodingError))),
                Err(e) => return Poll::Ready(Some(Err(ManifestDecodingError::IoError(e))))
            };
        }
    }

    fn poll_chunk(&mut self, length: u16, mut read: u16, cx: &mut Context<'_>) -> Poll<Result<C, ManifestDecodingError>> {
        let bytes = &mut self.buf[read as usize..length as usize];
        loop {
            match ready!(pin!(&mut self.reader).poll_read(cx, bytes)) {
                Ok(amount) if read as usize + amount >= length as usize => {
                    self.state = DecodingState::Length(0);
                    match C::try_from(bytes) {
                        Ok(chunk) => return Poll::Ready(Ok(chunk)),
                        Err(_) => return Poll::Ready(Err(ManifestDecodingError::DecodingError))
                    }
                },
                Ok(amount) if amount > 0 => {
                    read += amount as u16;
                    self.state = DecodingState::Chunk(length, read);
                },
                Ok(_) => return Poll::Ready(Err(ManifestDecodingError::DecodingError)),
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e)))
            }
        }
    }

    fn poll_skip_chunk(mut self: Pin<&mut Self>, length: u16, read: u16, cx: &mut Context<'_>) -> Poll<Result<(), ManifestDecodingError>> {
        let remaning = length - read;
        let r = Pin::new(&mut self.as_mut().get_mut().reader);
        match ready!(r.poll_seek_relative(cx, remaning as i64)) {
            Ok(()) => {
                self.state = DecodingState::Length(0);
                Poll::Ready(Ok(()))
            },
            Err(e) => Poll::Ready(Err(ManifestDecodingError::IoError(e)))
        }
    }
}

impl<I, C> Stream for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    I: Unpin
{
    type Item = Result<C, ManifestDecodingError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let DecodingState::Length(read) = this.state {
            match ready!(this.poll_length(read, cx)) {
                Some(Ok(_)) => (),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None)
            }
        }
        if let DecodingState::Chunk(length, read) = this.state {
            match ready!(this.poll_chunk(length, read, cx)) {
                Ok(chunk) => return Poll::Ready(Some(Ok(chunk))),
                Err(e) => return Poll::Ready(Some(Err(e)))
            }
        }
        Poll::Ready(None)
    }
}

impl<I, C> crate::Manifest<I, C> for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    I: Unpin
{
    type Error = ManifestDecodingError;

    fn creator(&self) -> &I {
        &self.creator
    }

    async fn into_metadata(mut self) -> (I, Result<(impl AsyncRead, std::time::SystemTime), Self::Error>) {
        let mut pinned = Pin::new(&mut self);
        loop {
            match pinned.state {
                DecodingState::Length(read) => {
                    match std::future::poll_fn(|cx| pinned.poll_length(read, cx)).await {
                        Some(Ok(_)) => (),
                        Some(Err(e)) => return (self.creator, Err(e)),
                        None => ()
                    }
                },
                DecodingState::Chunk(length, read) => {
                    let mut pinned2 = pinned.as_mut();
                    match std::future::poll_fn(move |cx| pinned2.as_mut().poll_skip_chunk(length, read, cx)).await {
                        Ok(()) => (),
                        Err(e) => return (self.creator, Err(e))
                    }
                },
                DecodingState::Finished => {
                    let mut buf = [0; 12];
                    match self.reader.read_exact(&mut buf).await {
                        Ok(()) => {
                            match timestamp_from_bytes(buf) {
                                Ok(timestamp) => return (self.creator, Ok((self.reader, timestamp))),
                                Err((_, _)) => return (self.creator, Err(ManifestDecodingError::DecodingError))
                            }
                        },
                        Err(e) => return (self.creator, Err(ManifestDecodingError::IoError(e)))
                    }
                }
            }
        }
    }
}

impl<M, I, C> Repository<M, I, C> for FileBackend
where
    for<'a> I: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a I: Into<OsString>,
    for<'a> C: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a C: Into<OsString>,
    for<'a> M: TryFrom<&'a [u8], Error=()>,
    for<'a> &'a M: Into<OsString>,
    I: Unpin
{
    type Error = IoError;

    type Builder = ManifestBuilder;

    type Manifest = Manifest<I, C>;

    async fn manifests(&self) -> Result<impl Stream<Item = Result<M, <Self as Repository<M, I, C>>::Error>>, <Self as Repository<M, I, C>>::Error> {
        self.list_directory("manifests").await
    }

    async fn manifest(&self, id: &M) -> Result<Self::Manifest, <Self as Repository<M, I, C>>::Error> {
        let mut buf = self.directory().to_owned();
        buf.push("manifests");
        buf.push(id.into());
        let file = tokio::fs::File::open(buf).await?;
        let mut reader = futures::io::BufReader::new(file.compat());
        let mut buf = [0; 4];
        reader.read_exact(&mut buf).await?;
        let length = u32::from_be_bytes(buf);
        let mut buf = Vec::new();
        buf.resize(length as usize, 0);
        reader.read_exact(buf.as_mut_slice()).await?;
        let creator = match I::try_from(buf.as_slice()) {
            Ok(i) => Ok(i),
            Err(()) => Err(IoError::new(ErrorKind::Other, "parsing creator failed"))
        }?;
        buf.resize(0, 0);
        Ok(Manifest {
            reader,
            buf,
            state: DecodingState::Length(0),
            creator,
            phantom: PhantomData
        })
    }

    // TODO: better handling of oversized client ids, better encoding / decoding errors in general (try to not use as)
    async fn create_manifest(&self, id: &M, client: &I) -> Result<Self::Builder, <Self as Repository<M, I, C>>::Error> {
        let mut buf = self.directory().to_owned();
        buf.push("manifests");
        buf.push(id.into());
        let mut writer = Box::pin(self.upload_manifest(buf).await?);
        let creator: OsString = client.into();
        writer.write_all(&(creator.len() as u32).to_be_bytes()).await?;
        writer.write_all(creator.as_encoded_bytes()).await?;
        Ok(ManifestBuilder {
            writer,
            state: EncodingState::Idle
        })
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
