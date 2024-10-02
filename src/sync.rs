use super::{
    Archive, Fossil, FossilCollection, FossilCollectionError, FossilCollector, FossilDeletionError,
    ValidClients,
};
use core::hash::Hash;
use core::iter::Iterator;
use std::collections::HashSet;

/// Extension for archives which provide synchronous access methods.
pub trait SyncArchive: Archive {
    /// Type of the ID each chunk receives based on its content.
    type ChunkID;

    /// Iterator yielding the chunks this archive is made of.
    type Chunks: Iterator<Item = Result<Self::ChunkID, Self::Error>>;

    /// Error which can happen during chunk enumeration.
    type Error;

    /// Enumerate the chunks this archvie is made of.
    fn chunks(&self) -> Self::Chunks;
}

/// Repository containing archives with their associated chunks.
/// 
/// A repository can be used concurrently by multiple clients, but each client
/// may only perform one operation (archive creation, fossil collection and fossil deletion) at at time.
pub trait SyncRepository {
    /// Type of archive stored in this repository.
    type Archive: SyncArchive;

    /// Type of a unique ID for each archive.
    type ArchiveID;

    /// Type of a ID for each fossil based on the chunk it was created from.
    type FossilID: Fossil<ChunkID = <<Self as SyncRepository>::Archive as SyncArchive>::ChunkID>;

    /// Iterator of all clients which may access the repository.
    type Clients: Iterator<
        Item = Result<<<Self as SyncRepository>::Archive as Archive>::ClientID, Self::Error>,
    >;

    /// Iterator of all archives existing in the repository.
    type Archives: Iterator<Item = Result<Self::ArchiveID, Self::Error>>;

    /// Error which can happen during repository access.
    type Error;

    /// Enumerate all clients which may access the repository.
    fn clients(&self) -> Self::Clients;

    /// Enumerate all archives existing in the repository.
    fn archives(&self) -> Self::Archives;

    /// Fetch the metadate associated with each archive.
    fn archive(&self, id: &Self::ArchiveID) -> Result<Self::Archive, Self::Error>;

    /// Turn a chunk into a fossil.
    /// 
    /// Since a [`Fossil`] may be referenced by new archives after creation it is
    /// allowed to be used in place of its original chunk, but not during archive
    /// creation.
    /// During archive creation chunks having the same ID as the original chunk of
    /// the fossil must be created again.
    /// 
    /// When the chunk does not exists this method should not return an error
    /// but instead treat the fossil as having been created and return its ID. 
    fn make_fossil(
        &mut self,
        chunk: &<<Self as SyncRepository>::Archive as SyncArchive>::ChunkID,
    ) -> Result<Self::FossilID, Self::Error>;

    /// Turn a fossil back into a chunk.
    /// 
    /// When the fossil does not exist this method should not return an error
    /// but instead treat the chunk as having been restored.
    fn recover_fossil(&mut self, fossil: &Self::FossilID) -> Result<(), Self::Error>;

    /// Delete a fossil permanently.
    /// 
    /// When the fossil does not exists this method should not return an error
    /// but instead treat the fossil as having been deleted successfully.
    fn delete_fossil(&mut self, fossil: &Self::FossilID) -> Result<(), Self::Error>;
}

impl<C> FossilCollector<C>
where
    C: Eq + Hash,
{
    /// Add an archive which should be retained during fossil collection.
    /// 
    /// When this method fails some chunks may have already been marked as referenced
    /// which prevents them from becoming a fossil candidate.
    pub fn retain_archive<A>(&mut self, archive: &A) -> Result<(), A::Error>
    where
        A: SyncArchive<ChunkID = C>,
    {
        for chunk in archive.chunks() {
            self.add_reference(chunk?);
        }
        Ok(())
    }

    /// Add an archive which should be pruned during fossil collection.
    /// 
    /// When this method fails some chunks may have already been marked as unreferenced
    /// which allows them to become fossil candidates when not marked as referenced.
    pub fn prune_archive<A>(&mut self, archive: &A) -> Result<(), A::Error>
    where
        A: SyncArchive<ChunkID = C>,
    {
        for chunk in archive.chunks() {
            self.add_chunk(chunk?);
        }
        Ok(())
    }
}

/// Perform fossil collection.
/// 
/// Create a fossil collection from the chunks which should become unreferenced
/// when pruning some archives while keeping all others.
/// While the chunks of the pruned archives will still be accessible as fossils
/// they will be removed during fossil deletion which means that the
/// archive should be removed before considering the fossil collection to be successful.
pub fn collect_fossils<'a, 'b, R, K, P>(
    kept_archives: K,
    pruned_archives: P,
    repository: &mut R,
) -> Result<
    FossilCollection<R::FossilID, &'a R::ArchiveID>,
    FossilCollectionError<R::Error, <R::Archive as SyncArchive>::Error>,
>
where
    R: SyncRepository,
    R::Archive: 'b,
    <R::Archive as SyncArchive>::ChunkID: Eq + Hash,
    K: Iterator<Item = (&'a R::ArchiveID, &'b R::Archive)>,
    P: Iterator<Item = &'b R::Archive>,
{
    let mut collector = FossilCollector::new();
    let mut seen_archives = Vec::new();
    for (archive_id, archive) in kept_archives {
        seen_archives.push(archive_id);
        collector.retain_archive(archive).map_err(FossilCollectionError::Archive)?;
    }
    for archive in pruned_archives {
        collector.prune_archive(archive).map_err(FossilCollectionError::Archive)?;
    }
    let fossils: Box<[R::FossilID]> = collector
        .fossil_candidates()
        .map(|candiate| repository.make_fossil(candiate))
        .collect::<Result<_, _>>()
        .map_err(FossilCollectionError::Repository)?;
    Ok(FossilCollection::new(
        fossils,
        seen_archives.into_boxed_slice(),
    ))
}

/// Perform fossil deletion.
/// 
/// Restore any fossils from a collection which have become referenced again
/// and permanently delete the rest.
pub fn delete_fossils<R>(
    collection: &FossilCollection<R::FossilID, &R::ArchiveID>,
    repository: &mut R,
) -> Result<(), FossilDeletionError<R::Error, <R::Archive as SyncArchive>::Error>>
where
    R: SyncRepository,
    <R::Archive as Archive>::ClientID: Hash + Eq,
    <R::Archive as SyncArchive>::ChunkID: Hash + Eq,
    R::ArchiveID: Hash + Eq,
{
    let seen_archives: HashSet<&R::ArchiveID> = collection.seen_archives().copied().collect();
    let mut new_referenced_chunks = HashSet::new();
    let mut valid_clients = ValidClients::new(collection.collection_timestamp());
    for request in repository.archives() {
        let archive_id = request.map_err(FossilDeletionError::Repository)?;
        if !seen_archives.contains(&archive_id) {
            let archive = repository
                .archive(&archive_id)
                .map_err(FossilDeletionError::Repository)?;
            for chunk in archive.chunks() {
                let chunk_id = chunk.map_err(FossilDeletionError::Archive)?;
                new_referenced_chunks.insert(chunk_id);
            }
            valid_clients.add_owned_archive(archive);
        }
    }
    for client in repository.clients() {
        let client_id = client.map_err(FossilDeletionError::Repository)?;
        if !valid_clients.contains(&client_id) {
            return Err(FossilDeletionError::Uncollectible);
        }
    }
    for fossil in collection.fossils() {
        if new_referenced_chunks.contains(&fossil.original_chunk()) {
            repository
                .recover_fossil(fossil)
                .map_err(FossilDeletionError::Repository)?;
        } else {
            repository
                .delete_fossil(fossil)
                .map_err(FossilDeletionError::Repository)?;
        }
    }
    Ok(())
}
