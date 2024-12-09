//! Test for the files backend

use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::stream::StreamExt;
#[cfg(feature = "files")]
use pin_project::pin_project;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll};
use tempfile::tempdir;
#[cfg(feature = "files")]
use vinculum::backends::files;
use vinculum::{ChunkBackend, ClientBackend};

struct ID {
    inner: [u8; 32],
}

impl TryFrom<&[u8]> for ID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut buf = [0; 32];
        match hex::decode_to_slice(value, &mut buf) {
            Ok(()) => Ok(ID { inner: buf }),
            Err(_) => Err(()),
        }
    }
}

impl Into<OsString> for &ID {
    fn into(self) -> OsString {
        hex::encode(self.inner).into()
    }
}

#[cfg(feature = "files")]
#[pin_project]
struct PollOnce<F> {
    #[pin]
    inner: F,
}

#[cfg(feature = "files")]
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

#[cfg(feature = "files")]
fn create_repository(tmpdir: &Path) -> files::FileBackend {
    let repo_path = tmpdir.join("repository");
    files::initialize(&repo_path).unwrap();
    files::FileBackend::new(repo_path)
}

// Initialisation tests
#[test]
#[cfg(feature = "files")]
fn initializes_directories() {
    let tmpdir = tempdir().unwrap();
    let repo_path = tmpdir.path().join("repository");
    files::initialize(&repo_path).unwrap();
    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"] {
        assert!(repo_path.join(directory).is_dir());
    }
}

#[test]
#[cfg(feature = "files")]
fn skips_existing_directories() {
    let tmpdir = tempdir().unwrap();
    let repo_path = tmpdir.path().join("repository");
    std::fs::DirBuilder::new().create(&repo_path).unwrap();
    for directory in ["chunks", "clients", "fossils", "manifests", "incoming"] {
        std::fs::DirBuilder::new()
            .create(repo_path.join(directory))
            .unwrap();
    }

    assert!(matches!(files::initialize(repo_path), Ok(())));
}

// general uploading tests
#[tokio::test]
#[cfg(feature = "files")]
async fn moves_file_on_commit() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        Some(Ok(_))
    ));
    assert!(!chunks_path.is_file());

    upload.close().await.unwrap();

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        None
    ));
    assert!(chunks_path.is_file());
}

#[tokio::test]
#[cfg(feature = "files")]
async fn commits_correct_content() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
    let test_string = "This is a test!!!";
    let mut buf = String::with_capacity(test_string.len() * 1000000);
    for _ in 0..1000000 {
        buf.push_str(test_string);
        upload.write_all(test_string.as_bytes()).await.unwrap();
    }
    upload.close().await.unwrap();

    assert_eq!(std::fs::read_to_string(chunks_path).unwrap(), buf);
}

#[tokio::test]
#[cfg(feature = "files")]
async fn removes_file_on_drop() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
        upload.write_all(&[0; 32]).await.unwrap();
    }

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        None
    ));
    assert!(!chunks_path.is_file());
}

#[tokio::test]
#[cfg(feature = "files")]
async fn removes_file_on_drop_after_pending_close() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());

        assert!(matches!(
            PollOnce {
                inner: upload.close()
            }
            .await,
            None
        ));
    }

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        None
    ));
    assert!(!chunks_path.is_file());
}

#[tokio::test]
#[cfg(feature = "files")]
async fn removes_file_on_rename_failure() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let chunk = ID { inner: [0; 32] };
    let chunks_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&chunk));
    {
        let mut upload = pin!(backend.add_chunk(&chunk).await.unwrap());
        let upload_path = backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::DirBuilder::new().create(&chunks_path).unwrap();

        assert!(matches!(upload.close().await, Err(e) if e.kind() == ErrorKind::IsADirectory));
        assert!(matches!(
            backend
                .directory()
                .join("incoming")
                .read_dir()
                .unwrap()
                .next(),
            None
        ));

        // test that the file is not removed twice on drop
        std::fs::File::create_new(upload_path).unwrap();
    }

    assert!(matches!(
        backend
            .directory()
            .join("incoming")
            .read_dir()
            .unwrap()
            .next(),
        Some(Ok(_))
    ));
}

// client backend tests
#[tokio::test]
#[cfg(feature = "files")]
async fn lists_clients() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for client in 0..10 {
        let id = ID {
            inner: [client; 32],
        };
        let client_path = backend
            .directory()
            .join("clients")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(client_path, "test".as_bytes()).unwrap();
    }
    let invalid_client_path = backend.directory().join("clients").join("test");
    std::fs::write(invalid_client_path, "invalid".as_bytes()).unwrap();
    let clients: Vec<ID> = pin!(<files::FileBackend as ClientBackend<ID>>::clients(&backend)
        .await
        .unwrap())
    .map(|id| id.unwrap())
    .collect()
    .await;

    assert_eq!(clients.len(), 10);
    for i in 0..10 {
        assert_eq!(clients.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
#[cfg(feature = "files")]
async fn add_clients() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let client_path = backend
        .directory()
        .join("clients")
        .join(<&ID as Into<OsString>>::into(&id));

    for data in ["test1", "test2"] {
        let mut client = pin!(
            <files::FileBackend as ClientBackend<ID>>::add_client(&backend, &id)
                .await
                .unwrap()
        );
        client.write_all(data.as_bytes()).await.unwrap();
        client.close().await.unwrap();

        assert_eq!(std::fs::read_to_string(&client_path).unwrap(), data);
    }
}

#[tokio::test]
#[cfg(feature = "files")]
async fn read_clients() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let client_path = backend
        .directory()
        .join("clients")
        .join(<&ID as Into<OsString>>::into(&id));
    for data in ["test1", "test2"] {
        std::fs::write(&client_path, data.as_bytes()).unwrap();
        let mut buf = Vec::with_capacity(data.len());
        let mut client = pin!(
            <files::FileBackend as ClientBackend<ID>>::client(&backend, &id)
                .await
                .unwrap()
        );

        assert_eq!(client.read_to_end(&mut buf).await.unwrap(), data.len());
        assert_eq!(buf.as_slice(), data.as_bytes());
    }

    assert!(matches!(<files::FileBackend as ClientBackend<ID>>::client(&backend, &ID { inner: [1; 32]}).await, Err(e) if e.kind() == ErrorKind::NotFound));
}

#[tokio::test]
#[cfg(feature = "files")]
async fn remove_clients() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for client in 0..3 {
        let id = ID {
            inner: [client; 32],
        };
        let client_path = backend
            .directory()
            .join("clients")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(client_path, "test".as_bytes()).unwrap();
    }
    let id = ID { inner: [1; 32] };
    <files::FileBackend as ClientBackend<ID>>::remove_client(&backend, &id)
        .await
        .unwrap();

    for client in 0..3 {
        let id = ID {
            inner: [client; 32],
        };
        let client_path = backend
            .directory()
            .join("clients")
            .join(<&ID as Into<OsString>>::into(&id));
        assert_eq!(client_path.exists(), client != 1);
    }
    assert!(
        matches!( <files::FileBackend as ClientBackend<ID>>::remove_client(&backend, &id).await, Err(e) if e.kind() == ErrorKind::NotFound)
    );
}

// chunk backend tests
#[tokio::test]
#[cfg(feature = "files")]
async fn list_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for chunk in 0..10 {
        let id = ID {
            inner: [chunk; 32],
        };
        let chunk_path = backend
            .directory()
            .join("chunks")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(chunk_path, "test".as_bytes()).unwrap();
    }
    let invalid_chunk_path = backend.directory().join("chunks").join("test");
    std::fs::write(invalid_chunk_path, "invalid".as_bytes()).unwrap();
    let chunks: Vec<ID> = pin!(<files::FileBackend as ChunkBackend<ID>>::chunks(&backend)
        .await
        .unwrap())
    .map(|id| id.unwrap())
    .collect()
    .await;

    assert_eq!(chunks.len(), 10);
    for i in 0..10 {
        assert_eq!(chunks.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
#[cfg(feature = "files")]
async fn add_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));

    for data in ["test1", "test2"] {
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::add_chunk(&backend, &id)
                .await
                .unwrap()
        );
        chunk.write_all(data.as_bytes()).await.unwrap();
        chunk.close().await.unwrap();

        assert_eq!(std::fs::read_to_string(&chunk_path).unwrap(), data);
    }
}

#[tokio::test]
#[cfg(feature = "files")]
async fn has_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));

    assert!(matches!(<files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await, Ok(false)));

    std::fs::write(&chunk_path, &[]).unwrap();

    assert!(matches!(<files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await, Ok(true)));

    std::fs::remove_file(chunk_path).unwrap();
    std::fs::write(&fossil_path, &[]).unwrap();

    assert!(matches!(<files::FileBackend as ChunkBackend<ID>>::has_chunk(&backend, &id).await, Ok(false)));
}

#[tokio::test]
#[cfg(feature = "files")]
async fn read_chunks() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    for data in ["test1", "test2"] {
        std::fs::write(&chunk_path, data.as_bytes()).unwrap();
        let mut buf = Vec::with_capacity(data.len());
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &id)
                .await
                .unwrap()
        );

        assert_eq!(chunk.read_to_end(&mut buf).await.unwrap(), data.len());
        assert_eq!(buf.as_slice(), data.as_bytes());
    }
    std::fs::remove_file(chunk_path).unwrap();
    for data in ["test3", "test4"] {
        std::fs::write(&fossil_path, data.as_bytes()).unwrap();
        let mut buf = Vec::with_capacity(data.len());
        let mut chunk = pin!(
            <files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &id)
                .await
                .unwrap()
        );

        assert_eq!(chunk.read_to_end(&mut buf).await.unwrap(), data.len());
        assert_eq!(buf.as_slice(), data.as_bytes());
    }

    assert!(matches!(<files::FileBackend as ChunkBackend<ID>>::chunk(&backend, &ID { inner: [1; 32]}).await, Err(e) if e.kind() == ErrorKind::NotFound));
}

#[tokio::test]
#[cfg(feature = "files")]
async fn list_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    for chunk in 0..10 {
        let id = ID {
            inner: [chunk; 32],
        };
        let chunk_path = backend
            .directory()
            .join("fossils")
            .join(<&ID as Into<OsString>>::into(&id));
        std::fs::write(chunk_path, "test".as_bytes()).unwrap();
    }
    let invalid_chunk_path = backend.directory().join("fossils").join("test");
    std::fs::write(invalid_chunk_path, "invalid".as_bytes()).unwrap();
    let chunks: Vec<ID> = pin!(<files::FileBackend as ChunkBackend<ID>>::fossils(&backend)
        .await
        .unwrap())
    .map(|id| id.unwrap())
    .collect()
    .await;

    assert_eq!(chunks.len(), 10);
    for i in 0..10 {
        assert_eq!(chunks.iter().filter(|id| id.inner == [i; 32]).count(), 1);
    }
}

#[tokio::test]
#[cfg(feature = "files")]
async fn make_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&chunk_path, "test".as_bytes()).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::make_fossil(&backend, &id).await.unwrap();

    assert!(!chunk_path.exists());
    assert_eq!(std::fs::read(&fossil_path).unwrap().as_slice(), "test".as_bytes());

   <files::FileBackend as ChunkBackend<ID>>::make_fossil(&backend, &id).await.unwrap();

    assert!(fossil_path.exists());
}

#[tokio::test]
#[cfg(feature = "files")]
async fn recover_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&fossil_path, "test".as_bytes()).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::recover_fossil(&backend, &id).await.unwrap();

    assert!(!fossil_path.exists());
    assert_eq!(std::fs::read(&chunk_path).unwrap().as_slice(), "test".as_bytes());

    <files::FileBackend as ChunkBackend<ID>>::recover_fossil(&backend, &id).await.unwrap();

    assert!(!fossil_path.exists());
}

#[tokio::test]
#[cfg(feature = "files")]
async fn delete_fossils() {
    let tmpdir = tempdir().unwrap();
    let backend = create_repository(tmpdir.path());
    let id = ID { inner: [0; 32] };
    let chunk_path = backend
        .directory()
        .join("chunks")
        .join(<&ID as Into<OsString>>::into(&id));
    let fossil_path = backend
        .directory()
        .join("fossils")
        .join(<&ID as Into<OsString>>::into(&id));
    std::fs::write(&fossil_path, &[]).unwrap();
    <files::FileBackend as ChunkBackend<ID>>::delete_fossil(&backend, &id).await.unwrap();

    assert!(!chunk_path.exists());
    assert!(!fossil_path.exists());

    <files::FileBackend as ChunkBackend<ID>>::delete_fossil(&backend, &id).await.unwrap();
}
