use core::error::Error;
use core::fmt::{Display, Formatter};
use core::hash::Hash;
use std::collections::HashSet;
use std::time::Instant;

pub mod sync;

pub trait Fossil {
    type ChunkID;

    fn original_chunk(&self) -> Self::ChunkID;
}

#[derive(Clone, Debug)]
pub struct FossilCollection<F, A> {
    collection_timestamp: Instant,

    fossils: Box<[F]>,

    seen_archives: Box<[A]>,
}

impl<F, A> FossilCollection<F, A> {
    pub fn new(fossils: Box<[F]>, seen_archives: Box<[A]>) -> FossilCollection<F, A> {
        FossilCollection::with_timestamp(Instant::now(), fossils, seen_archives)
    }

    pub fn with_timestamp(
        collection_timestamp: Instant,
        fossils: Box<[F]>,
        seen_archives: Box<[A]>,
    ) -> FossilCollection<F, A> {
        FossilCollection {
            collection_timestamp,
            fossils,
            seen_archives,
        }
    }

    pub fn collection_timestamp(&self) -> Instant {
        self.collection_timestamp
    }

    pub fn fossils(&self) -> impl Iterator<Item = &F> {
        self.fossils.iter()
    }

    pub fn seen_archives(&self) -> impl Iterator<Item = &A> {
        self.seen_archives.iter()
    }

    pub fn into_inner(self) -> (Box<[F]>, Box<[A]>) {
        (self.fossils, self.seen_archives)
    }
}

#[derive(Clone, Debug)]
pub struct ValidClients<'a, F, A, C> {
    collection: &'a FossilCollection<F, A>,

    clients: HashSet<C>,
}

impl<'a, F, A, C> ValidClients<'a, F, A, C> {
    pub fn new(fossils: &FossilCollection<F, A>) -> ValidClients<F, A, C> {
        ValidClients {
            collection: fossils,
            clients: HashSet::new(),
        }
    }

    pub fn with_capacity(
        capacity: usize,
        fossils: &FossilCollection<F, A>,
    ) -> ValidClients<F, A, C> {
        ValidClients {
            collection: fossils,
            clients: HashSet::with_capacity(capacity),
        }
    }

    pub fn collection(&self) -> &FossilCollection<F, A> {
        self.collection
    }

    pub fn iter(&self) -> impl Iterator<Item = &C> {
        self.clients.iter()
    }
}

impl<'a, F, A, C> ValidClients<'a, F, A, C>
where
    C: Eq + Hash,
{
    pub fn contains(&self, value: &C) -> bool {
        self.clients.contains(value)
    }
}

impl<'a, F, A, C> ValidClients<'a, F, A, C>
where
    C: Eq + Hash,
{
    pub fn add_owned_archive<I>(&mut self, archive: I)
    where
        I: Archive<ClientID = C>,
    {
        if archive.finished_timestamp() > self.collection.collection_timestamp() {
            self.clients.insert(archive.into_creator());
        }
    }
}

impl<'a, F, A, C> ValidClients<'a, F, A, &'a C>
where
    C: Eq + Hash,
{
    pub fn add_borrowed_archive<I>(&mut self, archive: &'a I)
    where
        I: Archive<ClientID = C>,
    {
        if archive.finished_timestamp() > self.collection.collection_timestamp() {
            self.clients.insert(archive.creator());
        }
    }
}

impl<'a, F, A, C> IntoIterator for &'a ValidClients<'a, F, A, C> {
    type Item = &'a C;

    type IntoIter = <&'a HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.clients.iter()
    }
}

impl<'a, F, A, C> IntoIterator for ValidClients<'a, F, A, C> {
    type Item = C;

    type IntoIter = <HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.clients.into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct FossilCollector<C> {
    referenced_chunks: HashSet<C>,

    unreferenced_chunks: HashSet<C>,
}

impl<'a, C> FossilCollector<C> {
    pub fn new() -> FossilCollector<C> {
        FossilCollector {
            referenced_chunks: HashSet::new(),
            unreferenced_chunks: HashSet::new(),
        }
    }

    pub fn with_capacity(referenced: usize, unreferenced: usize) -> FossilCollector<C> {
        FossilCollector {
            referenced_chunks: HashSet::with_capacity(referenced),
            unreferenced_chunks: HashSet::with_capacity(unreferenced),
        }
    }

    pub fn fossil_candidates(&'a self) -> impl Iterator<Item = &'a C> {
        self.into_iter()
    }
}

impl<C> FossilCollector<C>
where
    C: Eq + Hash,
{
    pub fn add_reference(&mut self, chunk: C) {
        self.unreferenced_chunks.remove(&chunk);
        self.referenced_chunks.insert(chunk);
    }

    pub fn add_chunk(&mut self, chunk: C) {
        if !self.referenced_chunks.contains(&chunk) {
            self.unreferenced_chunks.insert(chunk);
        }
    }
}

impl<C> Default for FossilCollector<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, C> IntoIterator for &'a FossilCollector<C> {
    type Item = &'a C;

    type IntoIter = <&'a HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.unreferenced_chunks.iter()
    }
}

impl<C> IntoIterator for FossilCollector<C> {
    type Item = C;

    type IntoIter = <HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.unreferenced_chunks.into_iter()
    }
}

pub trait Archive {
    type ClientID;

    fn creator(&self) -> &Self::ClientID;

    fn finished_timestamp(&self) -> Instant;

    fn into_creator(self) -> Self::ClientID;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FossilCollectionError<E, A> {
    Repository(E),
    Archive(A),
}

impl<E, A> Display for FossilCollectionError<E, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Repository(_) => "error while accessing repository",
            Self::Archive(_) => "error while accessing archive",
        }
        .fmt(f)
    }
}

impl<E, A> Error for FossilCollectionError<E, A>
where
    E: Error + 'static,
    A: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Repository(ref e) => Some(e),
            Self::Archive(ref e) => Some(e),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FossilDeletionError<E, A> {
    Uncollectible,
    Repository(E),
    Archive(A),
}

impl<E, A> Display for FossilDeletionError<E, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Uncollectible => "fossil deletion impossible: client without new backup exists",
            Self::Repository(_) => "error while accessing repository",
            Self::Archive(_) => "error while accessing archive",
        }
        .fmt(f)
    }
}

impl<E, A> Error for FossilDeletionError<E, A>
where
    E: Error + 'static,
    A: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Uncollectible => None,
            Self::Repository(ref e) => Some(e),
            Self::Archive(ref e) => Some(e),
        }
    }
}
