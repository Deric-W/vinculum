//! Utilities for writing manifest files.

use crate::repository::utils::timestamp_to_bytes;

use futures::io::{AsyncWrite, Error as IoError};
use futures::sink::Sink;
use futures::AsyncWriteExt;
use pin_project::pin_project;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};
use thiserror::Error;

/// Error produced by manifest encoding operations.
#[derive(Error, Debug)]
pub enum ManifestEncodingError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    IoError(#[source] IoError),
    /// The length of the creator id exceeds [`u8::MAX`] bytes.
    #[error("invalid creator")]
    InvalidCreator,
    /// The length of the chunk id exceeds [`u8::MAX`] bytes or is empty.
    #[error("invalid chunk")]
    InvalidChunk,
    /// Calculating the timestamp failed, containing the difference from [`std::time::UNIX_EPOCH`].
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(#[source] std::time::SystemTimeError),
    /// Invalid operation (such as adding more chunks after closing the builder).
    #[error("invalid operation")]
    InvalidOperation,
}

/// State of the manifest chunk encoding process
#[derive(Debug)]
enum ChunksEncodingState {
    /// No operation is pending
    Idle,
    /// Chunk pending with the number of bytes already written (+ length)
    ChunkPending(OsString, usize),
    /// Timestamp pending with the number of bytes already written (+ zero length of chunks)
    TimestampPending([u8; 12], usize),
    /// Timestamp was written
    Finished,
}

/// Encoder of the manifest format used by [`FileRepository`](super::FileRepository).
///
/// It receives chunks which will be added to the repository by the user before
/// the manifest will be created, either by uploading them or making sure they
/// already exist.
///
/// The manifest will be created when the builder is closed, trying to add additional
/// data after closing will result in errors.
#[pin_project]
pub struct ManifestBuilder<C> {
    // since CommitOnClose has unnameable type parameters
    #[pin]
    writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>,
    state: ChunksEncodingState,
    phantom: PhantomData<C>,
}

impl<C> ManifestBuilder<C> {
    fn new(writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>) -> ManifestBuilder<C> {
        ManifestBuilder {
            writer,
            state: ChunksEncodingState::Idle,
            phantom: PhantomData,
        }
    }

    pub(super) async fn from_upload<I>(
        mut writer: futures::io::BufWriter<Pin<Box<dyn AsyncWrite>>>,
        client: &I,
    ) -> Result<ManifestBuilder<C>, ManifestEncodingError>
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
                    .map_err(ManifestEncodingError::IoError)?;
                pinned
                    .as_mut()
                    .write_all(creator.as_encoded_bytes())
                    .await
                    .map_err(ManifestEncodingError::IoError)?;
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
                    *written += amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        while *written - 1 < chunk.len() {
            let bytes = &chunk.as_encoded_bytes()[*written - 1..];
            match ready!(writer.as_mut().poll_write(cx, bytes)) {
                Ok(amount) => {
                    *written += amount;
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
        while *written < 1 {
            match ready!(writer.as_mut().poll_write(cx, &[0])) {
                Ok(amount) => {
                    *written += amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        while *written - 1 < 12 {
            match ready!(writer.as_mut().poll_write(cx, timestamp)) {
                Ok(amount) => {
                    *written += amount;
                }
                Err(e) => return Poll::Ready(Err(ManifestEncodingError::IoError(e))),
            }
        }
        Poll::Ready(Ok(ChunksEncodingState::Finished))
    }
}

impl<C> Sink<&C> for ManifestBuilder<C>
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
                    *this.state = ready!(ManifestBuilder::<C>::poll_flush_chunk(
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
                    *this.state = ready!(ManifestBuilder::<C>::poll_flush_chunk(
                        cx,
                        this.writer.as_mut(),
                        chunk,
                        written
                    ))?;
                }
                _ => {
                    return Poll::Ready(
                        ready!(this.writer.as_mut().poll_flush(cx))
                            .map_err(ManifestEncodingError::IoError),
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
                    *this.state = ready!(ManifestBuilder::<C>::poll_flush_chunk(
                        cx,
                        this.writer.as_mut(),
                        chunk,
                        written
                    ))?;
                }
                ChunksEncodingState::TimestampPending(ref timestamp, ref mut written) => {
                    *this.state = ready!(ManifestBuilder::<C>::poll_flush_timestamp(
                        cx,
                        this.writer.as_mut(),
                        timestamp,
                        written
                    ))?;
                }
                ChunksEncodingState::Finished => {
                    return Poll::Ready(
                        ready!(this.writer.as_mut().poll_close(cx))
                            .map_err(ManifestEncodingError::IoError),
                    )
                }
            }
        }
    }
}
