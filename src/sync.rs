use super::{
    Archive, Fossil, FossilCollection, FossilCollectionError, FossilCollector, FossilDeletionError,
    ValidClients,
};
use core::hash::Hash;
use core::iter::Iterator;
use std::collections::HashSet;

pub trait SyncArchive: Archive {
    type ChunkID;

    type Chunks: Iterator<Item = Result<Self::ChunkID, Self::Error>>;

    type Error;

    fn chunks(&self) -> Self::Chunks;
}

pub trait SyncRepository {
    type Archive: SyncArchive;

    type ArchiveID;

    type FossilID: Fossil<ChunkID = <<Self as SyncRepository>::Archive as SyncArchive>::ChunkID>;

    type Clients: Iterator<
        Item = Result<<<Self as SyncRepository>::Archive as Archive>::ClientID, Self::Error>,
    >;

    type Archives: Iterator<Item = Result<Self::ArchiveID, Self::Error>>;

    type Error;

    fn clients(&self) -> Self::Clients;

    fn archives(&self) -> Self::Archives;

    fn archive(&self, id: &Self::ArchiveID) -> Result<Self::Archive, Self::Error>;

    fn make_fossil(
        &mut self,
        chunk: &<<Self as SyncRepository>::Archive as SyncArchive>::ChunkID,
    ) -> Result<Self::FossilID, Self::Error>;

    fn recover_fossil(&mut self, fossil: &Self::FossilID) -> Result<(), Self::Error>;

    fn delete_fossil(&mut self, fossil: &Self::FossilID) -> Result<(), Self::Error>;
}

impl<C> FossilCollector<C>
where
    C: Eq + Hash,
{
    pub fn retain_archive<A>(&mut self, archive: &A) -> Result<(), A::Error>
    where
        A: SyncArchive<ChunkID = C>,
    {
        archive
            .chunks()
            .try_for_each(|request| request.map(|chunk| self.add_reference(chunk)))
    }

    pub fn prune_archive<A>(&mut self, archive: &A) -> Result<(), A::Error>
    where
        A: SyncArchive<ChunkID = C>,
    {
        archive
            .chunks()
            .try_for_each(|request| request.map(|chunk| self.add_chunk(chunk)))
    }
}

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
        for chunk in archive.chunks() {
            collector.add_reference(chunk.map_err(FossilCollectionError::Archive)?);
        }
    }
    for archive in pruned_archives {
        for chunk in archive.chunks() {
            collector.add_chunk(chunk.map_err(FossilCollectionError::Archive)?);
        }
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
    let mut valid_clients = ValidClients::new(collection);
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
