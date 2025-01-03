//! Utilities for safely committing files to the repository.

use futures::io::{AsyncWrite, Error as IoError, Result as IoResult};
use pin_project::pin_project;
use rand::distributions::{Alphanumeric, DistString};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::future::Future;
use std::io::ErrorKind;
use std::iter::Iterator;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::pin::{pin, Pin};
use std::task::{ready, Poll};
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub fn sync_directory<P>(path: P) -> Result<(), IoError>
where
    P: AsRef<Path>,
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
    })
    .await?
}

async fn commit_manifest(file: RemoveOnDrop, to: PathBuf) -> IoResult<()> {
    file.get_inner_ref().sync_all().await?;
    let chunks_dir = to.ancestors().nth(2).unwrap().join("chunks");
    // make sure directory entries of uploaded chunks are persisted
    let chunks_dir = asyncify(move || {
        sync_directory(&chunks_dir)?;
        Ok::<PathBuf, IoError>(chunks_dir)
    })
    .await??;
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
    })
    .await??;
    // make sure directory entry of manifest is persisted
    asyncify(move || sync_directory(chunks_dir)).await?
}

async fn asyncify<F, R>(f: F) -> IoResult<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => Ok(res),
        Err(_) => Err(IoError::new(ErrorKind::Other, "tokio task failed")),
    }
}

async fn create_incoming(mut directory: PathBuf) -> IoResult<RemoveOnDrop> {
    let mut options = OpenOptions::new();
    options.write(true);
    options.create_new(true);
    options.mode(0o444);
    let mut rng = rand::thread_rng();
    let mut buffer = String::with_capacity(16);
    loop {
        Alphanumeric.append_string(&mut rng, &mut buffer, 16);
        directory.push(&buffer);
        // perform open and RemoveOnDrop::new as one operation which can not be canceled in between
        let res = asyncify(move || match options.open(&directory) {
            Ok(file) => Ok(RemoveOnDrop::new(
                tokio::fs::File::from_std(file),
                directory,
            )),
            Err(e) => Err((e, options, directory)),
        })
        .await?;
        match res {
            Ok(file) => return Ok(file),
            Err((e, o, mut d)) if e.kind() == ErrorKind::AlreadyExists => {
                buffer.clear();
                d.pop();
                directory = d;
                options = o;
            }
            Err((e, _, _)) => return Err(e),
        }
    }
}

pub async fn upload_file(
    incoming_directory: PathBuf,
    destination: PathBuf,
) -> IoResult<impl AsyncWrite> {
    let file = create_incoming(incoming_directory).await?.compat_write();
    let writer = CommitOnClose::new(file, commit_file, destination);
    Ok(writer)
}

pub async fn upload_manifest(
    incoming_directory: PathBuf,
    destination: PathBuf,
) -> IoResult<impl AsyncWrite> {
    let file = create_incoming(incoming_directory).await?.compat_write();
    let writer = CommitOnClose::new(file, commit_manifest, destination);
    Ok(writer)
}

#[derive(Debug)]
struct RemoveOnDrop {
    inner: Option<(tokio::fs::File, PathBuf)>,
}

impl RemoveOnDrop {
    fn new(file: tokio::fs::File, location: PathBuf) -> RemoveOnDrop {
        RemoveOnDrop {
            inner: Some((file, location)),
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

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        pin!(self.get_inner_mut()).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        pin!(self.get_inner_mut()).poll_shutdown(cx)
    }
}

#[pin_project(project = CommitOnCloseStateProj)]
#[derive(Debug)]
enum CommitOnCloseState<C, F, S> {
    Writing(Option<(tokio_util::compat::Compat<RemoveOnDrop>, C, S)>),
    Commiting(#[pin] F),
}

#[pin_project]
#[derive(Debug)]
pub struct CommitOnClose<C, F, S> {
    #[pin]
    state: CommitOnCloseState<C, F, S>,
}

impl<C, F, S> CommitOnClose<C, F, S> {
    fn new(
        writer: tokio_util::compat::Compat<RemoveOnDrop>,
        creator: C,
        state: S,
    ) -> CommitOnClose<C, F, S> {
        CommitOnClose {
            state: CommitOnCloseState::Writing(Some((writer, creator, state))),
        }
    }
}

impl<C, F, S> AsyncWrite for CommitOnClose<C, F, S>
where
    C: Fn(RemoveOnDrop, S) -> F,
    F: Future<Output = IoResult<()>>,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => {
                pin!(writer).poll_write(cx, buf)
            }
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(
                ErrorKind::Other,
                "writer is being closed",
            ))),
            _ => unreachable!(),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<IoResult<usize>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => {
                pin!(writer).poll_write_vectored(cx, bufs)
            }
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(
                ErrorKind::Other,
                "writer is being closed",
            ))),
            _ => unreachable!(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<IoResult<()>> {
        match self.project().state.project() {
            CommitOnCloseStateProj::Writing(Some((writer, _, _))) => pin!(writer).poll_flush(cx),
            CommitOnCloseStateProj::Commiting(_) => Poll::Ready(Err(IoError::new(
                ErrorKind::Other,
                "writer is being closed",
            ))),
            _ => unreachable!(),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<IoResult<()>> {
        let mut projection = self.project();
        match projection.state.as_mut().project() {
            CommitOnCloseStateProj::Writing(inner) => {
                let res = match inner {
                    Some((writer, _, _)) => ready!(pin!(writer).poll_close(cx)),
                    None => unreachable!(),
                };
                match res {
                    Ok(()) => {
                        let (writer, creator, state) = inner.take().unwrap();
                        let future = creator(writer.into_inner(), state);
                        projection.state.set(CommitOnCloseState::Commiting(future));
                        match projection.state.project() {
                            CommitOnCloseStateProj::Commiting(f) => f.poll(cx),
                            _ => unreachable!(),
                        }
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            CommitOnCloseStateProj::Commiting(future) => future.poll(cx),
        }
    }
}
