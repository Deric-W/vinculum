//! Test utilities for the files backend

use pin_project::pin_project;
use std::ffi::OsString;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll};
use vinculum::backends::files;

mod initialization;
mod uploading;
mod clients;
mod chunks;
mod manifests;

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmptyID;

impl TryFrom<&[u8]> for EmptyID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() == 0 {
            Ok(EmptyID {})
        } else {
            Err(())
        }
    }
}

impl Into<OsString> for &EmptyID {
    fn into(self) -> OsString {
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

fn create_repository(tmpdir: &Path) -> files::FileBackend {
    let repo_path = tmpdir.join("repository");
    files::initialize(&repo_path).unwrap();
    files::FileBackend::new(repo_path)
}
