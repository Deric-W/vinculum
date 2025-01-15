//! A simple frontend for benchmark purposes.

use crate::ids::{ChunkID, ID};
use clap::{Args, Parser, Subcommand};
use futures::io::AsyncWriteExt;
use futures::sink::SinkExt;
use futures::stream::{iter, StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use std::cell::{Cell, RefCell};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::{pin, Pin};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{channel, Receiver};
use vinculum::backends::files::{initialize, FileBackend};
use vinculum::{
    ClientBackend, FossilCollection, FossilCollectionBuilder, Manifest, ManifestTimestamp,
    Repository,
};

mod ids;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The action to perform
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an repository
    Init(InitArgs),
    /// Create an new manifest from a single file
    Create(CreateArgs),
    /// Create a fossil collection
    Collect(CollectArgs),
    /// Delete a fossil collection
    Delete(DeleteArgs),
    /// Add a client
    AddClient(ClientArgs),
    /// Remove a client
    RemoveClient(ClientArgs),
}

#[derive(Args)]
struct InitArgs {
    /// The path of the new repository
    repository: PathBuf,
}

#[derive(Args)]
struct CreateArgs {
    /// The path of the repository
    repository: PathBuf,
    /// The name of the new manifest
    manifest: String,
    /// The client creating the manifest
    client: String,
    /// File containing the chunks of the manifest
    path: PathBuf,
    /// Size of the generated chunks
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    #[clap(default_value_t = 4096)]
    chunk_size: u64,
    /// Number of operations to perform in parallel
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    #[clap(default_value_t = 1)]
    parallelism: u16,
}

#[derive(Args)]
struct CollectArgs {
    /// The path of the repository
    repository: PathBuf,
    /// Path to the fossil collection being created
    collection: PathBuf,
    /// Manifests to delete
    manifests: Vec<String>,
    /// Number of operations to perform in parallel
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    #[clap(default_value_t = 1)]
    parallelism: u16,
}

#[derive(Args)]
struct DeleteArgs {
    /// The path of the repository
    repository: PathBuf,
    /// Path to the fossil collection being deleted
    collection: PathBuf,
    /// Number of operations to perform in parallel
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    #[clap(default_value_t = 1)]
    parallelism: u16,
    /// Manifests to delete during pipelined collection
    #[arg(long)]
    collect: Vec<String>,
}

#[derive(Args)]
struct ClientArgs {
    /// The path of the repository
    repository: PathBuf,
    /// The name of the client
    client: String,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Init(args) => initialize(&args.repository).unwrap(),
        Command::Create(args) => {
            let chunk_size: usize = args.chunk_size.try_into().unwrap();
            let mut file = std::fs::File::open(args.path).unwrap();
            let repository = FileBackend::new(args.repository);
            let (sender, receiver) = channel::<Option<(ChunkID, Vec<u8>)>>(32);
            let upload_thread = std::thread::spawn(move || {
                let runtime = create_runtime();
                runtime.block_on(create_manifest(
                    &repository,
                    args.parallelism.into(),
                    &ID::new(args.manifest),
                    &ID::new(args.client),
                    receiver,
                ));
            });

            let take_amount: u64 = chunk_size.try_into().unwrap();
            let mut hasher = Sha256::new();
            loop {
                let mut buffer = Vec::with_capacity(chunk_size);
                let read = file
                    .by_ref()
                    .take(take_amount)
                    .read_to_end(&mut buffer)
                    .unwrap();
                if read == 0 {
                    break;
                }
                hasher.update(buffer.as_slice());
                let digest: [u8; 32] = hasher.finalize_reset().into();
                sender
                    .blocking_send(Some((ChunkID::new(digest), buffer)))
                    .unwrap();
                if read < chunk_size {
                    break;
                }
            }

            // tell the uploader that we did not panic
            sender.blocking_send(None).unwrap();
            std::mem::drop(sender);
            upload_thread.join().unwrap();
        }
        Command::Collect(args) => {
            let repository = FileBackend::new(args.repository);
            let runtime = create_runtime();
            let collection = runtime.block_on(collect_fossils(
                &repository,
                args.parallelism.into(),
                args.manifests.into_iter().map(ID::new),
            ));
            store_collection(&collection, &args.collection);
        }
        Command::Delete(args) => {
            let collection = load_collection(&args.collection);
            let repository = FileBackend::new(args.repository);
            let runtime = create_runtime();
            if args.collect.is_empty() {
                runtime
                    .block_on(collection.delete::<_, ID>(&repository, args.parallelism.into()))
                    .unwrap();
                std::fs::remove_file(args.collection).unwrap();
            } else {
                let collection = runtime.block_on(pipelined_delete(
                    &repository,
                    args.parallelism.into(),
                    collection,
                    args.collect.into_iter().map(ID::new),
                ));
                store_collection(&collection, &args.collection);
            }
        }
        Command::AddClient(args) => {
            let repository = FileBackend::new(args.repository);
            let runtime = create_runtime();
            runtime.block_on(async {
                let id = ID::new(args.client);
                let mut writer = pin!(repository.add_client(&id).await.unwrap());
                writer.close().await.unwrap();
            })
        }
        Command::RemoveClient(args) => {
            let repository = FileBackend::new(args.repository);
            let runtime = create_runtime();
            runtime
                .block_on(repository.remove_client(&ID::new(args.client)))
                .unwrap();
        }
    };
}

fn create_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread().build().unwrap()
}

#[allow(clippy::await_holding_refcell_ref)]
async fn create_manifest<R>(
    repository: &R,
    parallelism: usize,
    name: &ID,
    client: &ID,
    receiver: Receiver<Option<(ChunkID, Vec<u8>)>>,
) where
    R: Repository<ID, ID, ChunkID>,
{
    let mut builder = pin!(repository.create_manifest(name, client).await.unwrap());
    let completed = Cell::new(false);
    let builder_cell = RefCell::new(builder.as_mut());
    tokio_stream::wrappers::ReceiverStream::new(receiver)
        .filter_map(|msg| async {
            match msg {
                Some((id, chunk)) => {
                    // triggers clippy::await_holding_refcell_ref but is ok
                    // since at most one instance is running at a time
                    let mut builder = builder_cell.borrow_mut();
                    builder.feed(&id).await.unwrap();
                    Some((id, chunk))
                }
                None => {
                    completed.set(true);
                    None
                }
            }
        })
        .for_each_concurrent(parallelism, |(id, chunk)| async move {
            if !repository.has_chunk(&id).await.unwrap() {
                let mut writer = pin!(repository.add_chunk(&id).await.unwrap());
                writer.write_all(chunk.as_slice()).await.unwrap();
                writer.close().await.unwrap();
            }
        })
        .await;
    // make sure that the chunker did not panic
    if completed.get() {
        builder.close().await.unwrap();
    }
}

fn store_collection(collection: &FossilCollection<ID, ChunkID>, path: &Path) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    ciborium::into_writer(collection, std::io::BufWriter::new(file)).unwrap();
}

fn load_collection(path: &Path) -> FossilCollection<ID, ChunkID> {
    let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
    ciborium::from_reader_with_buffer(
        std::io::BufReader::new(file),
        &mut vec![0; 65536].into_boxed_slice(),
    )
    .unwrap()
}

async fn download_manifest_chunks<R, F>(
    repository: &R,
    id: &ID,
    mut on_chunk: F,
) -> (ID, <R::Manifest as Manifest<ID, ChunkID>>::ReferencedChunks)
where
    R: Repository<ID, ID, ChunkID>,
    <R::Manifest as Manifest<ID, ChunkID>>::ReferencedChunks: Unpin,
    F: FnMut(ChunkID),
{
    let (creator, mut referenced_chunks) = repository
        .manifest(id)
        .await
        .unwrap()
        .into_referenced_chunks();
    let mut pinned_chunks = Pin::new(&mut referenced_chunks);
    while let Some(chunk) = pinned_chunks.try_next().await.unwrap() {
        on_chunk(chunk);
    }
    (creator, referenced_chunks)
}

async fn remove_manifests<'a, R, I>(repository: &R, parallelism: usize, manifests: I)
where
    R: Repository<ID, ID, ChunkID>,
    I: IntoIterator<Item = &'a ID>,
{
    let stream = pin!(iter(manifests.into_iter().map(Ok)));
    stream
        .try_for_each_concurrent(parallelism, |id| repository.remove_manifest(id))
        .await
        .unwrap();
}

async fn collect_fossils<R, M>(
    repository: &R,
    parallelism: usize,
    manifests: M,
) -> FossilCollection<ID, ChunkID>
where
    R: Repository<ID, ID, ChunkID>,
    M: IntoIterator<Item = ID>,
    <R as Repository<ID, ID, ChunkID>>::Error: 'static,
    <R::Manifest as Manifest<ID, ChunkID>>::ReferencedChunks: Unpin,
    <R::Manifest as Manifest<ID, ChunkID>>::Error: 'static,
{
    let pruned_manifests: std::collections::HashSet<ID> = manifests.into_iter().collect();
    let mut builder = FossilCollectionBuilder::new();
    let cell = RefCell::new(&mut builder);
    let current_manifests = pin!(repository.manifests().await.unwrap());
    current_manifests
        .try_for_each_concurrent(parallelism, |id| async {
            if !pruned_manifests.contains(&id) {
                let _ = download_manifest_chunks(repository, &id, |chunk| {
                    cell.borrow_mut().add_referenced_chunk(chunk)
                })
                .await;
                cell.borrow_mut().add_seen_manifest(id);
            }
            Ok(())
        })
        .await
        .unwrap();
    let pruned_stream = pin!(iter(pruned_manifests.iter()));
    pruned_stream
        .for_each_concurrent(parallelism, |id| async {
            let _ = download_manifest_chunks(repository, id, |chunk| {
                cell.borrow_mut().add_fossil_candidate(chunk);
            })
            .await;
        })
        .await;
    let collection = builder
        .collect_fossils(repository, parallelism)
        .await
        .unwrap();
    remove_manifests(repository, parallelism, pruned_manifests.iter()).await;
    collection
}

async fn pipelined_delete<R, M>(
    repository: &R,
    parallelism: usize,
    collection: FossilCollection<ID, ChunkID>,
    manifests: M,
) -> FossilCollection<ID, ChunkID>
where
    R: Repository<ID, ID, ChunkID>,
    M: IntoIterator<Item = ID>,
    <R::Manifest as Manifest<ID, ChunkID>>::ReferencedChunks: Unpin,
{
    let pruned_manifests: std::collections::HashSet<ID> = manifests.into_iter().collect();
    let mut builder = collection.pipelined_delete();
    let cell = RefCell::new(&mut builder);
    let current_manifests = pin!(repository.manifests().await.unwrap());
    current_manifests
        .try_for_each_concurrent(parallelism, |id| async {
            if !pruned_manifests.contains(&id) {
                let (creator, referenced_chunks) =
                    download_manifest_chunks(repository, &id, |chunk| {
                        cell.borrow_mut().add_referenced_chunk(chunk)
                    })
                    .await;
                let timestamp = referenced_chunks.into_timestamp().await.unwrap();
                cell.borrow_mut().add_seen_manifest(id, creator, timestamp);
            }
            Ok(())
        })
        .await
        .unwrap();
    let pruned_stream = pin!(iter(pruned_manifests.iter()));
    pruned_stream
        .for_each_concurrent(parallelism, |id| async {
            let (creator, referenced_chunks) = download_manifest_chunks(repository, id, |chunk| {
                cell.borrow_mut().add_fossil_candidate(chunk)
            })
            .await;
            let timestamp = referenced_chunks.into_timestamp().await.unwrap();
            cell.borrow_mut()
                .add_expiring_manifest(id.clone(), creator, timestamp);
        })
        .await;
    let builder = builder.delete(repository, parallelism).await.unwrap();
    let collection = builder
        .collect_fossils(repository, parallelism)
        .await
        .unwrap();
    remove_manifests(repository, parallelism, pruned_manifests.iter()).await;
    collection
}
