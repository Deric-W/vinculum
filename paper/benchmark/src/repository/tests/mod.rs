//! Tests for the file repository

use pin_project::pin_project;
use std::ffi::OsString;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll};

mod chunks;
mod clients;
mod initialization;
mod manifests;
mod uploading;

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmptyID;

impl TryFrom<&[u8]> for EmptyID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Ok(EmptyID {})
        } else {
            Err(())
        }
    }
}

impl From<&EmptyID> for OsString {
    fn from(_: &EmptyID) -> Self {
        "".into()
    }
}

#[pin_project]
struct PollOnce<F> {
    #[pin]
    inner: F,
}

impl<F> std::future::Future for PollOnce<F>
where
    F: std::future::Future,
{
    type Output = Option<F::Output>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.poll(cx) {
            Poll::Pending => Poll::Ready(None),
            Poll::Ready(o) => Poll::Ready(Some(o)),
        }
    }
}

fn create_repository<M, I, C>(tmpdir: &Path) -> super::FileRepository<M, I, C> {
    let repo_path = tmpdir.join("repository");
    super::initialize(&repo_path).unwrap();
    super::FileRepository::new(repo_path)
}
