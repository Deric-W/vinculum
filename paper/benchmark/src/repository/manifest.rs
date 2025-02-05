//! Utilities for reading manifest files.

use crate::repository::utils::timestamp_from_bytes;

use futures::io::{AsyncRead, Error as IoError};
use futures::stream::Stream;
use futures::AsyncReadExt;
use pin_project::pin_project;
use std::fmt::Debug;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};
use thiserror::Error;
use tokio_util::compat::Compat;

/// Error produced by manifest decoding operations.
#[derive(Error, Debug)]
pub enum ManifestDecodingError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    IoError(#[source] IoError),
    /// The parsing of the creator failed.
    #[error("invalid creator")]
    InvalidCreator,
    /// The parsing of a chunk failed.
    #[error("invalid chunk")]
    InvalidChunk,
    /// The parsing of the timestamp failed, containing the seconds and nanoseconds since [`std::time::UNIX_EPOCH`].
    #[error("invalid timestamp (secs: {0}, nsecs: {1})")]
    InvalidTimestamp(u64, u32),
}

/// State of the decoding process of the chunks.
#[derive(Debug)]
enum ChunksDecodingState {
    /// Reading a length field of a chunk id, containing the number of bytes already read.
    ChunkLength(u8),
    /// Reading a chunk id, containing the number of bytes already read and total length.
    Chunk(u8, u8),
    /// Finished decoding process.
    Finished,
}

/// Decoder of the manifest format used by [`FileRepository`](super::FileRepository).
#[derive(Debug)]
#[pin_project]
pub struct Manifest<I, C> {
    #[pin]
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    creator: I,
    buf: Box<[u8]>,
    state: ChunksDecodingState,
    phantom: PhantomData<Box<C>>,
}

impl<I, C> Manifest<I, C> {
    /// Expects buf.len() >= u8::MAX
    fn new(
        reader: futures::io::BufReader<Compat<tokio::fs::File>>,
        creator: I,
        buf: Box<[u8]>,
    ) -> Manifest<I, C> {
        Manifest {
            reader,
            creator,
            buf,
            state: ChunksDecodingState::ChunkLength(0),
            phantom: PhantomData,
        }
    }

    /// Try to transition from [`ChunksDecodingState::ChunkLength`], returning the new state.
    fn poll_chunk_length(
        cx: &mut Context<'_>,
        reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        buf: &mut [u8],
        read: &mut u8,
    ) -> Poll<Result<ChunksDecodingState, ManifestDecodingError>> {
        if *read < 1 {
            match ready!(reader.poll_read(cx, &mut buf[..1])) {
                Ok(length) if length > 0 => (),
                Ok(_) => {
                    return Poll::Ready(Err(ManifestDecodingError::IoError(
                        ErrorKind::UnexpectedEof.into(),
                    )))
                }
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e))),
            }
        }
        let length = buf[0];
        if length == 0 {
            Poll::Ready(Ok(ChunksDecodingState::Finished))
        } else {
            Poll::Ready(Ok(ChunksDecodingState::Chunk(0, length)))
        }
    }

    /// Try to transition from [`ChunksDecodingState::Chunk`], returning the new state.
    ///
    /// This function assumes the buffer has a length == length of the chunk.
    fn poll_skip_chunk(
        cx: &mut Context<'_>,
        reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        read: u8,
        length: u8,
    ) -> Poll<Result<ChunksDecodingState, ManifestDecodingError>> {
        if read < length {
            ready!(reader.poll_seek_relative(cx, (length - read).into()))
                .map_err(ManifestDecodingError::IoError)?;
        }
        Poll::Ready(Ok(ChunksDecodingState::ChunkLength(0)))
    }
}

impl<I, C> Manifest<I, C>
where
    for<'a> I: TryFrom<&'a [u8], Error = ()>,
{
    pub(super) async fn from_file(
        mut reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    ) -> Result<Manifest<I, C>, ManifestDecodingError> {
        let mut buf = [0u8; 1];
        reader
            .read_exact(&mut buf)
            .await
            .map_err(ManifestDecodingError::IoError)?;
        let length = u8::from_be_bytes(buf);
        let mut buf = vec![0; u8::MAX.into()].into_boxed_slice();
        let creator_buf = &mut buf[..length.into()];
        reader
            .read_exact(creator_buf)
            .await
            .map_err(ManifestDecodingError::IoError)?;
        let creator =
            I::try_from(creator_buf).map_err(|_| ManifestDecodingError::InvalidCreator)?;
        Ok(Manifest::new(reader, creator, buf))
    }
}

impl<I, C> Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    /// Try to transition from [`ChunksDecodingState::Chunk`], returning the chunk and new state.
    fn poll_chunk(
        cx: &mut Context<'_>,
        mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        buf: &mut [u8],
        read: &mut u8,
        length: u8,
    ) -> Poll<Result<(C, ChunksDecodingState), ManifestDecodingError>> {
        let bytes = &mut buf[..length.into()];
        while *read < length {
            match ready!(reader.as_mut().poll_read(cx, &mut bytes[(*read).into()..])) {
                Ok(length) if length > 0 => {
                    *read += length as u8;
                }
                Ok(_) => {
                    return Poll::Ready(Err(ManifestDecodingError::IoError(
                        ErrorKind::UnexpectedEof.into(),
                    )))
                }
                Err(e) => return Poll::Ready(Err(ManifestDecodingError::IoError(e))),
            }
        }
        let chunk = C::try_from(bytes).map_err(|_| ManifestDecodingError::InvalidChunk)?;
        Poll::Ready(Ok((chunk, ChunksDecodingState::ChunkLength(0))))
    }
}

impl<I, C> vinculum::Manifest for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    type ClientID = I;

    type ChunkID = C;

    type Error = ManifestDecodingError;

    async fn into_metadata(
        mut self,
    ) -> Result<(Self::ClientID, std::time::SystemTime), Self::Error> {
        let mut reader = Pin::new(&mut self.reader);
        loop {
            match self.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    self.state = poll_fn(|cx| {
                        Manifest::<I, C>::poll_chunk_length(
                            cx,
                            reader.as_mut(),
                            &mut self.buf,
                            read,
                        )
                    })
                    .await?;
                }
                ChunksDecodingState::Chunk(read, length) => {
                    self.state = poll_fn(|cx| {
                        Manifest::<I, C>::poll_skip_chunk(cx, reader.as_mut(), read, length)
                    })
                    .await?;
                }
                ChunksDecodingState::Finished => break,
            }
        }
        let mut timestamp_buf = [0; 12];
        reader
            .read_exact(&mut timestamp_buf)
            .await
            .map_err(ManifestDecodingError::IoError)?;
        match timestamp_from_bytes(timestamp_buf) {
            Ok(timestamp) => Ok((self.creator, timestamp)),
            Err((secs, nsecs)) => Err(ManifestDecodingError::InvalidTimestamp(secs, nsecs)),
        }
    }
}

impl<I, C> Stream for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    type Item = Result<C, ManifestDecodingError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    *this.state = ready!(Manifest::<I, C>::poll_chunk_length(
                        cx,
                        this.reader.as_mut(),
                        this.buf,
                        read
                    ))?;
                }
                ChunksDecodingState::Chunk(ref mut read, length) => {
                    let (chunk, state) = ready!(Manifest::<I, C>::poll_chunk(
                        cx,
                        this.reader.as_mut(),
                        this.buf,
                        read,
                        *length
                    ))?;
                    *this.state = state;
                    return Poll::Ready(Some(Ok(chunk)));
                }
                ChunksDecodingState::Finished => return Poll::Ready(None),
            }
        }
    }
}
