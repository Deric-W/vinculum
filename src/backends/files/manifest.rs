//! Utilities for reading manifest files.

use crate::utils::timestamp_from_bytes;
use crate::ManifestTimestamp;

use futures::io::{AsyncBufRead, AsyncRead, Error as IoError, Result as IoResult};
use futures::stream::Stream;
use futures::AsyncReadExt;
use pin_project::pin_project;
use std::fmt::Debug;
use std::future::poll_fn;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};
use tokio_util::compat::Compat;

/// Error produced by manifest decoding operations.
#[derive(Debug)]
pub enum ManifestDecodingError {
    /// An I/O error occurred.
    IoError(IoError),
    /// The parsing of the creator failed.
    InvalidCreator,
    /// The parsing of a chunk failed.
    InvalidChunk,
    /// The parsing of the timestamp failed, containing the seconds and nanoseconds since [`std::time::UNIX_EPOCH`].
    InvalidTimestamp(u64, u32),
}

impl std::fmt::Display for ManifestDecodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestDecodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestDecodingError::InvalidCreator => write!(f, "invalid creator"),
            ManifestDecodingError::InvalidChunk => write!(f, "invalid chunk"),
            ManifestDecodingError::InvalidTimestamp(secs, nsecs) => {
                write!(f, "invalid timestamp (secs: {}, nsecs: {}", secs, nsecs)
            }
        }
    }
}

impl std::error::Error for ManifestDecodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestDecodingError::IoError(e) => e.source(),
            _ => None,
        }
    }
}

/// Decoder of the manifest format used by [`FileBackend`](super::FileBackend).
#[derive(Debug)]
pub struct Manifest<I, C> {
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    creator: I,
    // to reuse the allocation
    buf: Box<[u8]>,
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
            phantom: PhantomData,
        }
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

impl<I, C> crate::Manifest<I, C> for Manifest<I, C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
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
#[derive(Debug)]
enum ChunksDecodingState {
    /// Reading a length field of a chunk id, containing the number of bytes already read.
    ChunkLength(u8),
    /// Reading a chunk id, containing the number of bytes already read and total length.
    Chunk(u8, u8),
    /// Finished decoding process.
    Finished,
}

/// Implementation of [`Manifest::Chunks`](crate::Manifest::Chunks) and
/// [`Manifest::ReferencedChunks`](crate::Manifest::ReferencedChunks).
///
/// Since this backend stores data without further modification the chunks provided
/// by the client are identical to the chunks referenced by the manifest.
#[derive(Debug)]
#[pin_project]
pub struct ManifestChunks<C> {
    #[pin]
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    buf: Box<[u8]>,
    state: ChunksDecodingState,
    phantom: PhantomData<Box<C>>,
}

impl<C> ManifestChunks<C> {
    /// Expects buf.len() >= u8::MAX
    fn new(
        reader: futures::io::BufReader<Compat<tokio::fs::File>>,
        buf: Box<[u8]>,
    ) -> ManifestChunks<C> {
        ManifestChunks {
            reader,
            buf,
            state: ChunksDecodingState::ChunkLength(0),
            phantom: PhantomData,
        }
    }
}

impl<C> ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
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

    /// Try to transition from [`DecodingState::Chunk`], returning the new state.
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

impl<C> Stream for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    type Item = Result<C, ManifestDecodingError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    *this.state = ready!(ManifestChunks::<C>::poll_chunk_length(
                        cx,
                        this.reader.as_mut(),
                        this.buf,
                        read
                    ))?;
                }
                ChunksDecodingState::Chunk(ref mut read, length) => {
                    let (chunk, state) = ready!(ManifestChunks::<C>::poll_chunk(
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

impl<C> crate::ManifestChunks<C, ManifestDecodingError> for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    type Data = ManifestData;

    async fn into_data(mut self) -> Result<Self::Data, ManifestDecodingError> {
        let mut reader = Pin::new(&mut self.reader);
        loop {
            match self.state {
                ChunksDecodingState::ChunkLength(ref mut read) => {
                    self.state = poll_fn(|cx| {
                        ManifestChunks::<C>::poll_chunk_length(
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
                        ManifestChunks::<C>::poll_skip_chunk(cx, reader.as_mut(), read, length)
                    })
                    .await?;
                }
                ChunksDecodingState::Finished => return Ok(ManifestData::new(self.reader)),
            }
        }
    }
}

/// State of the decoding process of the data.
#[derive(Debug)]
enum DataDecodingState {
    /// Reading a length field of a data batch, containing the number of bytes already read.
    DataLength(u8),
    /// Reading a data batch, containing the number of bytes remaining.
    Data(u16),
    /// Reading a timestamp, containing the number of bytes already read.
    Timestamp(u8),
}

/// Implementation of [`ManifestChunks::Data`](crate::ManifestChunks::Data).
#[derive(Debug)]
#[pin_project]
pub struct ManifestData {
    #[pin]
    reader: futures::io::BufReader<Compat<tokio::fs::File>>,
    state: DataDecodingState,
    buf: [u8; 12],
}

impl ManifestData {
    fn new(reader: futures::io::BufReader<Compat<tokio::fs::File>>) -> ManifestData {
        ManifestData {
            reader,
            state: DataDecodingState::DataLength(0),
            buf: [0; 12],
        }
    }

    /// Try to transition from [`DataDecodingState::DataLength`], returning the new state.
    fn poll_data_length(
        cx: &mut Context<'_>,
        mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        buf: &mut [u8; 12],
        read: &mut u8,
    ) -> Poll<Result<DataDecodingState, IoError>> {
        while (*read) < 2 {
            match ready!(reader.as_mut().poll_read(cx, &mut buf[(*read).into()..2])) {
                Ok(length) if length > 0 => {
                    *read += length as u8;
                }
                Ok(_) => return Poll::Ready(Err(ErrorKind::UnexpectedEof.into())),
                Err(e) => return Poll::Ready(Err(e)),
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
    fn poll_data(
        cx: &mut Context<'_>,
        mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        remaining: &mut u16,
        requested: &mut [u8],
    ) -> Poll<Result<(usize, DataDecodingState), IoError>> {
        if requested.is_empty() {
            return Poll::Ready(Ok((0, DataDecodingState::Data(*remaining))));
        }
        if *remaining > 0 {
            match ready!(reader.as_mut().poll_fill_buf(cx)) {
                Ok(data) if !data.is_empty() => {
                    let read_data = std::cmp::min((*remaining).into(), data.len());
                    let consumed_data = std::cmp::min(read_data, requested.len());
                    *remaining -= consumed_data as u16;
                    requested[..consumed_data].copy_from_slice(&data[..consumed_data]);
                    reader.consume(consumed_data);
                    if *remaining > 0 {
                        return Poll::Ready(Ok((
                            consumed_data,
                            DataDecodingState::Data(*remaining),
                        )));
                    } else {
                        return Poll::Ready(Ok((consumed_data, DataDecodingState::DataLength(0))));
                    }
                }
                Ok(_) => return Poll::Ready(Err(ErrorKind::UnexpectedEof.into())),
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        Poll::Ready(Ok((0, DataDecodingState::DataLength(0))))
    }

    /// Try to transition from [`DataDecodingState::Data`], returning the new state.
    fn poll_skip_data(
        cx: &mut Context<'_>,
        reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        remaining: u16,
    ) -> Poll<Result<DataDecodingState, IoError>> {
        if remaining > 0 {
            ready!(reader.poll_seek_relative(cx, remaining.into()))?;
        }
        Poll::Ready(Ok(DataDecodingState::DataLength(0)))
    }

    /// Try to transition from [`DataDecodingState::Timestamp`], returning the timestamp.
    ///
    /// This function assumes the buffer has a length >= 12.
    fn poll_timestamp(
        cx: &mut Context<'_>,
        mut reader: Pin<&mut futures::io::BufReader<Compat<tokio::fs::File>>>,
        buf: &mut [u8; 12],
        read: &mut u8,
    ) -> Poll<Result<std::time::SystemTime, ManifestDecodingError>> {
        while (*read) < 12 {
            match ready!(reader.as_mut().poll_read(cx, &mut buf[(*read).into()..12])) {
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
        match timestamp_from_bytes(*buf) {
            Ok(timestamp) => Poll::Ready(Ok(timestamp)),
            Err((secs, nsecs)) => {
                Poll::Ready(Err(ManifestDecodingError::InvalidTimestamp(secs, nsecs)))
            }
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
                    *this.state = ready!(ManifestData::poll_data_length(
                        cx,
                        this.reader.as_mut(),
                        this.buf,
                        read
                    ))?;
                }
                DataDecodingState::Data(ref mut remaining) => {
                    let (read, state) = ready!(ManifestData::poll_data(
                        cx,
                        this.reader.as_mut(),
                        remaining,
                        buf
                    ))?;
                    *this.state = state;
                    return Poll::Ready(Ok(read));
                }
                DataDecodingState::Timestamp(_) => return Poll::Ready(Ok(0)),
            }
        }
    }
}

impl ManifestTimestamp<ManifestDecodingError> for ManifestData {
    async fn into_timestamp(mut self) -> Result<std::time::SystemTime, ManifestDecodingError> {
        let mut reader = pin!(self.reader);
        loop {
            match self.state {
                DataDecodingState::DataLength(ref mut read) => {
                    self.state = poll_fn(|cx| {
                        ManifestData::poll_data_length(cx, reader.as_mut(), &mut self.buf, read)
                    })
                    .await
                    .map_err(ManifestDecodingError::IoError)?;
                }
                DataDecodingState::Data(remaining) => {
                    self.state =
                        poll_fn(|cx| ManifestData::poll_skip_data(cx, reader.as_mut(), remaining))
                            .await
                            .map_err(ManifestDecodingError::IoError)?;
                }
                DataDecodingState::Timestamp(ref mut read) => {
                    let timestamp = poll_fn(|cx| {
                        ManifestData::poll_timestamp(cx, reader.as_mut(), &mut self.buf, read)
                    })
                    .await?;
                    return Ok(timestamp);
                }
            }
        }
    }
}

impl<C> crate::ManifestTimestamp<ManifestDecodingError> for ManifestChunks<C>
where
    for<'a> C: TryFrom<&'a [u8], Error = ()>,
{
    async fn into_timestamp(self) -> Result<std::time::SystemTime, ManifestDecodingError> {
        let data =
            <ManifestChunks<C> as crate::ManifestChunks<C, ManifestDecodingError>>::into_data(self)
                .await?;
        data.into_timestamp().await
    }
}
