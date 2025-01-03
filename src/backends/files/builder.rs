//! Utilities for writing manifest files.

use crate::utils::timestamp_to_bytes;

use futures::io::{AsyncWrite, Error as IoError, Result as IoResult};
use futures::sink::Sink;
use futures::AsyncWriteExt;
use pin_project::pin_project;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::future::poll_fn;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};

/// Error produced by manifest encoding operations.
#[derive(Debug)]
pub enum ManifestEncodingError {
    /// An I/O error occurred.
    IoError(IoError),
    /// The length of the creator id exceeds [`u8::MAX`] bytes.
    InvalidCreator,
    /// The length of the chunk id exceeds [`u8::MAX`] bytes or is empty.
    InvalidChunk,
    /// Calculating the timestamp failed, containing the difference from [`std::time::UNIX_EPOCH`].
    InvalidTimestamp(std::time::SystemTimeError),
    /// Invalid operation (such as adding more chunks after closing the builder).
    InvalidOperation,
}

impl std::fmt::Display for ManifestEncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestEncodingError::IoError(e) => write!(f, "I/O error: {}", e),
            ManifestEncodingError::InvalidCreator => write!(f, "invalid creator"),
            ManifestEncodingError::InvalidChunk => write!(f, "invalid chunk"),
            ManifestEncodingError::InvalidTimestamp(e) => write!(f, "invalid timestamp: {}", e),
            Self::InvalidOperation => write!(f, "invalid operation"),
        }
    }
}

impl std::error::Error for ManifestEncodingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestEncodingError::IoError(e) => e.source(),
            ManifestEncodingError::InvalidTimestamp(e) => e.source(),
            _ => None,
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
    Finished,
}

/// Encoder of the manifest format used by [`FileBackend`](super::FileBackend).
#[pin_project]
pub struct ManifestBuilder {
    // since CommitOnClose has unnameable type parameters
    #[pin]
    writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>,
    state: ChunksEncodingState,
}

impl ManifestBuilder {
    fn new(writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>) -> ManifestBuilder {
        ManifestBuilder {
            writer,
            state: ChunksEncodingState::Idle,
        }
    }

    pub(super) async fn from_upload<I>(
        mut writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>,
        client: &I,
    ) -> Result<ManifestBuilder, ManifestEncodingError>
    where
        for<'a> &'a I: Into<OsString>,
    {
        let creator: OsString = client.into();
        match <usize as TryInto<u8>>::try_into(creator.len()) {
            Ok(length) => {
                let mut pinned = Pin::new(&mut writer);
                pinned
                    .as_mut()
                    .write_all(&length.to_be_bytes())
                    .await
                    .map_err(|e| ManifestEncodingError::IoError(e))?;
                pinned
                    .as_mut()
                    .write_all(creator.as_encoded_bytes())
                    .await
                    .map_err(|e| ManifestEncodingError::IoError(e))?;
            }
            Err(_) => return Err(ManifestEncodingError::InvalidCreator),
        }
        Ok(ManifestBuilder::new(writer))
    }

    /// Try to transition from [`ChunksEncodingState::ChunkPending`], returning the new state.
    fn poll_flush_chunk(
        cx: &mut Context<'_>,
        mut writer: Pin<&mut dyn AsyncWrite>,
        chunk: &OsStr,
        written: &mut usize,
    ) -> Poll<Result<ChunksEncodingState, ManifestEncodingError>> {
        let length: u8 = chunk.len().try_into().unwrap();
        while *written < 1 {
            let bytes = length.to_be_bytes();
            match ready!(writer.as_mut().poll_write(cx, &bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        while *written - 1 < chunk.len() {
            let bytes = &chunk.as_encoded_bytes()[*written - 1..];
            match ready!(writer.as_mut().poll_write(cx, bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        Poll::Ready(Ok(ChunksEncodingState::Idle))
    }

    /// Try to transition from [`ChunksEncodingState::TimestampPending`], returning the new state.
    fn poll_flush_timestamp(
        cx: &mut Context<'_>,
        mut writer: Pin<&mut dyn AsyncWrite>,
        timestamp: &[u8],
        written: &mut usize,
    ) -> Poll<Result<ChunksEncodingState, ManifestEncodingError>> {
        while *written < 3 {
            match ready!(writer.as_mut().poll_write(cx, &[0, 0, 0])) {
                Ok(amount) => {
                    *written = *written + amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        while *written - 3 < 12 {
            match ready!(writer.as_mut().poll_write(cx, timestamp)) {
                Ok(amount) => {
                    *written = *written + amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        Poll::Ready(Ok(ChunksEncodingState::Finished))
    }
}

impl<C> Sink<&C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>,
{
    type Error = ManifestEncodingError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::Idle => return Poll::Ready(Ok(())),
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(
                        cx,
                        this.writer.as_mut(),
                        chunk,
                        written
                    ))?;
                }
                _ => return Poll::Ready(Err(ManifestEncodingError::InvalidOperation)),
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
                }
                Ok(_) | Err(_) => Err(ManifestEncodingError::InvalidChunk),
            };
        }
        Err(ManifestEncodingError::InvalidOperation)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(
                        cx,
                        this.writer.as_mut(),
                        chunk,
                        written
                    ))?;
                }
                _ => {
                    return Poll::Ready(
                        ready!(this.writer.as_mut().poll_flush(cx))
                            .map_err(|e| ManifestEncodingError::IoError(e)),
                    )
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match this.state {
                ChunksEncodingState::Idle => match timestamp_to_bytes(std::time::SystemTime::now())
                {
                    Ok(bytes) => {
                        *this.state = ChunksEncodingState::TimestampPending(bytes, 0);
                    }
                    Err(e) => return Poll::Ready(Err(ManifestEncodingError::InvalidTimestamp(e))),
                },
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_chunk(
                        cx,
                        this.writer.as_mut(),
                        chunk,
                        written
                    ))?;
                }
                ChunksEncodingState::TimestampPending(ref timestamp, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::poll_flush_timestamp(
                        cx,
                        this.writer.as_mut(),
                        timestamp,
                        written
                    ))?;
                }
                ChunksEncodingState::Finished => {
                    return Poll::Ready(
                        ready!(this.writer.as_mut().poll_close(cx))
                            .map_err(|e| ManifestEncodingError::IoError(e)),
                    )
                }
            }
        }
    }
}

impl<C> crate::ManifestBuilder<C> for ManifestBuilder
where
    for<'a> &'a C: Into<OsString>,
{
    type Error = ManifestEncodingError;

    type Data = ManifestBuilderData;

    async fn add_data(mut self) -> Result<Self::Data, ManifestEncodingError> {
        let mut writer = Pin::new(&mut self.writer);
        loop {
            match self.state {
                ChunksEncodingState::Idle => {
                    writer
                        .write_all(&[0])
                        .await
                        .map_err(|e| ManifestEncodingError::IoError(e))?;
                    writer
                        .flush()
                        .await
                        .map_err(|e| ManifestEncodingError::IoError(e))?;
                    return Ok(ManifestBuilderData::new(self.writer.into_inner()));
                }
                ChunksEncodingState::ChunkPending(ref chunk, ref mut written) => {
                    self.state = poll_fn(|cx| {
                        ManifestBuilder::poll_flush_chunk(cx, writer.as_mut(), chunk, written)
                    })
                    .await?;
                }
                _ => return Err(ManifestEncodingError::InvalidOperation),
            }
        }
    }
}

/// State of the manifest chunk encoding process
#[derive(Debug)]
enum DataEncodingState {
    /// Buffer contains only some amount of unwritten data, with the first two bytes being the length (not yet set)
    Accumulating(usize),
    /// Buffer contains partially written data, with the number of bytes written and the total amount
    DataPending(usize, usize),
    /// Buffer contains partially written data and timestamp, with the number of bytes written and the total amount
    TimestampPending(usize, usize),
    /// Timestamp was written
    Finished,
}

/// Implementation of [`ManifestBuilder::Data`](crate::ManifestBuilder::Data).
#[pin_project]
pub struct ManifestBuilderData {
    writer: Pin<Box<dyn AsyncWrite>>,
    state: DataEncodingState,
    // we need more control over the buffer
    buf: Box<[u8]>,
}

impl ManifestBuilderData {
    fn new(writer: Pin<Box<dyn AsyncWrite>>) -> ManifestBuilderData {
        ManifestBuilderData {
            writer,
            state: DataEncodingState::Accumulating(2),
            buf: vec![0; <u16 as Into<usize>>::into(u16::MAX).saturating_add(2)].into_boxed_slice(),
        }
    }

    /// Set the length in the buffer
    fn set_length(buf: &mut [u8], length: usize) {
        let data_length: u16 = (length - 2).try_into().unwrap();
        buf[..2].copy_from_slice(&data_length.to_be_bytes());
    }

    /// Try to transition from [`DataEncodingState::DataPending`] or [`DataEncodingState::TimestampPending`] by flushing the buffer.
    fn poll_data_pending(
        cx: &mut Context<'_>,
        mut writer: Pin<&mut dyn AsyncWrite>,
        buf: &[u8],
        written: &mut usize,
        length: usize,
    ) -> Poll<Result<(), IoError>> {
        while *written < length {
            let bytes = &buf[*written..length];
            match ready!(writer.as_mut().poll_write(cx, bytes)) {
                Ok(amount) => {
                    *written = *written + amount;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ManifestBuilderData {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        let this = self.project();
        loop {
            match this.state {
                DataEncodingState::Accumulating(ref mut length) if *length < this.buf.len() => {
                    let consumed = std::cmp::min(this.buf.len() - *length, buf.len());
                    let dst = &mut this.buf[*length..*length + consumed];
                    let src = &buf[..consumed];
                    dst.copy_from_slice(src);
                    *length = *length + consumed;
                    return Poll::Ready(Ok(consumed));
                }
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                }
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(
                        cx,
                        this.writer.as_mut(),
                        this.buf,
                        written,
                        *length
                    ))?;
                    *this.state = DataEncodingState::Accumulating(2);
                }
                _ => {
                    return Poll::Ready(Err(IoError::other("invalid operation, writer is closed")))
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        let this = self.project();
        loop {
            match this.state {
                DataEncodingState::Accumulating(length) if *length <= 2 => {
                    return this.writer.as_mut().poll_flush(cx)
                }
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                }
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(
                        cx,
                        this.writer.as_mut(),
                        this.buf,
                        written,
                        *length
                    ))?;
                    *this.state = DataEncodingState::Accumulating(2);
                }
                DataEncodingState::TimestampPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(
                        cx,
                        this.writer.as_mut(),
                        this.buf,
                        written,
                        *length
                    ))?;
                    *this.state = DataEncodingState::Finished;
                }
                DataEncodingState::Finished => return this.writer.as_mut().poll_flush(cx),
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
                        Err(_) => return Poll::Ready(Err(IoError::other("invalid timestamp"))),
                    };
                    this.buf[start..start + 2].copy_from_slice(&0u16.to_be_bytes());
                    this.buf[start + 2..start + 14].copy_from_slice(&timestamp);
                    *this.state = DataEncodingState::TimestampPending(0, start + 14);
                }
                DataEncodingState::Accumulating(length) => {
                    ManifestBuilderData::set_length(this.buf, *length);
                    *this.state = DataEncodingState::DataPending(0, *length);
                }
                DataEncodingState::DataPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(
                        cx,
                        this.writer.as_mut(),
                        this.buf,
                        written,
                        *length
                    ))?;
                    *this.state = DataEncodingState::Accumulating(2);
                }
                DataEncodingState::TimestampPending(ref mut written, length) => {
                    ready!(ManifestBuilderData::poll_data_pending(
                        cx,
                        this.writer.as_mut(),
                        this.buf,
                        written,
                        *length
                    ))?;
                    *this.state = DataEncodingState::Finished;
                }
                DataEncodingState::Finished => {
                    return Poll::Ready(ready!(this.writer.as_mut().poll_close(cx)))
                }
            }
        }
    }
}
