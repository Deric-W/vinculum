//! Tests for the clients

use super::{create_repository, ID};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::stream::StreamExt;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::pin::pin;
use tempfile::tempdir;
use vinculum::backends::files;
use vinculum::ClientBackend;

#[tokio::test]
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

    assert!(
        matches!(<files::FileBackend as ClientBackend<ID>>::client(&backend, &ID { inner: [1; 32]}).await, Err(e) if e.kind() == ErrorKind::NotFound)
    );
}

#[tokio::test]
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
