//! Implementation of Lock-Free Deduplication in Rust.
//!
//! This crates provides an implementation of the algorithm used by tools
//! such as [Duplicacy](https://duplicacy.com) as described in their
//! [paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).

use core::error::Error;
use core::fmt::{Display, Formatter};
use core::hash::Hash;
use core::iter::FusedIterator;
use std::collections::HashSet;
use std::time::Instant;

pub mod sync;

/// A trait which denotes a type as an archive.
///
/// Archives (called "manifests" in the paper) are objects stored in a repository.
/// To allow for deduplication they are stored as a ordered sequence of chunks which
/// are assigned IDs based of their content.
/// When two archives contain chunks with the same ID they are deduplicated by storing
/// only one chunk per IDs in the repository and referencing them from both archives.
pub trait Archive {
    /// Type of a unique ID for each client using the repository.
    type ClientID;

    /// Client which created this archive.
    fn creator(&self) -> &Self::ClientID;

    /// Timestamp indicating the time of creation.
    fn creation_timestamp(&self) -> Instant;

    /// Transform an owned archive into its creator ID.
    fn into_creator(self) -> Self::ClientID;
}

/// A trait which allows fossils to give information about which chunk they represent.
///
/// A fossil is a chunk which has been renamed in preparation for deletion.
/// Because of the optimistic nature of Lock-Free Deduplication fossils may have to
/// be restored when they are referenced by a new archive.
///
/// # Examples
/// ```
/// use vinculum::Fossil;
///
/// #[derive(Clone, Copy)]
/// pub struct ChunkID {
///     hash: [u8; 32]
/// }
///
/// pub struct FossilID {
///     original_chunk: ChunkID
/// }
///
/// impl Fossil for FossilID {
///     type ChunkID = ChunkID;
///
///     fn original_chunk(&self) -> Self::ChunkID {
///         self.original_chunk
///     }
/// }
/// ```
pub trait Fossil {
    /// The type of the original chunk.
    type ChunkID;

    /// Calculate the original chunk from which this fossil was created.
    fn original_chunk(&self) -> Self::ChunkID;
}

/// Collection of fossils awaiting deletion.
///
/// Since fossil collection may run in parallel to other clients creating
/// archives they can't be instantly deleted.
/// Instead, their deletion has to be postponed until it can be verified that
/// they can't be referenced by new archives.
/// Should they become referenced again they are restored, if not they are deleted.
#[derive(Clone, Debug)]
pub struct FossilCollection<F, A> {
    collection_timestamp: Instant,

    fossils: Box<[F]>,

    seen_archives: Box<[A]>,
}

impl<F, A> FossilCollection<F, A> {
    /// Create a new collection from the fossils collected and the archives considered.
    ///
    /// The collected fossils are chunks which where determined to be no longer
    /// referenced by any archives, making them candidates for deletion.
    /// The seen archives are the archives considered when calculating the set
    /// of referenced chunks.
    pub fn new(fossils: Box<[F]>, seen_archives: Box<[A]>) -> FossilCollection<F, A> {
        FossilCollection::with_timestamp(Instant::now(), fossils, seen_archives)
    }

    /// Create a new collection with a custom timestamp.
    ///
    /// Since [`FossilCollection::new`] assumes the fossils to have been collected
    /// immediately before its call its timestamp can delay fossil deletion when this is not the case.
    /// This function allows to supply a custom timestamp which has to represent a point in time after
    /// the fossil collection finished.
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

    /// Timestamp of the fossil collection being finished.
    pub fn collection_timestamp(&self) -> Instant {
        self.collection_timestamp
    }

    /// Collected fossils.
    pub fn fossils(&self) -> CollectedFossilsIter<'_, F> {
        CollectedFossilsIter {
            inner: self.fossils.iter(),
        }
    }

    /// Archives considered during fossil collection.
    pub fn seen_archives(&self) -> SeenArchivesIter<'_, A> {
        SeenArchivesIter {
            inner: self.seen_archives.iter(),
        }
    }

    /// Extract the contained collections of fossils and seen archives.
    pub fn into_inner(self) -> (Box<[F]>, Box<[A]>) {
        (self.fossils, self.seen_archives)
    }
}

/// Iterator of fossils contained in a [`FossilCollection`] created by [`FossilCollection::fossils`].
#[derive(Clone, Debug)]
pub struct CollectedFossilsIter<'a, F> {
    inner: std::slice::Iter<'a, F>,
}

impl<'a, F> Iterator for CollectedFossilsIter<'a, F> {
    type Item = &'a F;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, F> ExactSizeIterator for CollectedFossilsIter<'a, F> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<'a, F> FusedIterator for CollectedFossilsIter<'a, F> {}

/// Iterator of fossils contained in a [`FossilCollection`] created by [`FossilCollection::seen_archives`].
#[derive(Clone, Debug)]
pub struct SeenArchivesIter<'a, A> {
    inner: std::slice::Iter<'a, A>,
}

impl<'a, A> Iterator for SeenArchivesIter<'a, A> {
    type Item = &'a A;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, A> ExactSizeIterator for SeenArchivesIter<'a, A> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<'a, A> FusedIterator for SeenArchivesIter<'a, A> {}

/// Set of clients which can't produce backups referencing chunks renamed during fossil collection.
///
/// Before fossil deletion every client has to be contained in this set.
#[derive(Clone, Debug)]
pub struct ValidClients<C> {
    collection_timestamp: Instant,

    clients: HashSet<C>,
}

impl<C> ValidClients<C> {
    /// Initialize an empty set from the timestamp of a fossil collection being finished.
    ///
    /// The timestamp can be aquired by calling [`FossilCollection::collection_timestamp`].
    pub fn new(collection_timestamp: Instant) -> ValidClients<C> {
        ValidClients {
            collection_timestamp,
            clients: HashSet::new(),
        }
    }

    /// Variant of [`ValidClients::new`] which allows to reduce allocations by specifying a initial capacity.
    pub fn with_capacity(capacity: usize, collection_timestamp: Instant) -> ValidClients<C> {
        ValidClients {
            collection_timestamp,
            clients: HashSet::with_capacity(capacity),
        }
    }

    /// The timestamp supplied during creation.
    pub fn collection_timestamp(&self) -> Instant {
        self.collection_timestamp
    }

    /// The clients currently in the set.
    pub fn iter(&self) -> ValidClientsIter<'_, C> {
        ValidClientsIter {
            inner: self.clients.iter(),
        }
    }
}

impl<C> ValidClients<C>
where
    C: Eq + Hash,
{
    /// Check whether a client is contained in the set.
    pub fn contains(&self, value: &C) -> bool {
        self.clients.contains(value)
    }

    /// Consider the creator of an owned archive for inclusion in the set.
    ///
    /// The creator of the archive is included in the set if the archive
    /// has been created after the timestamp supplied during creation.
    pub fn add_owned_archive<A>(&mut self, archive: A)
    where
        A: Archive<ClientID = C>,
    {
        if archive.creation_timestamp() > self.collection_timestamp() {
            self.clients.insert(archive.into_creator());
        }
    }
}

impl<'a, C> ValidClients<&'a C>
where
    C: Eq + Hash,
{
    /// Variant of [`ValidClients::add_owned_archive`] which takes a borrowed archive.
    pub fn add_borrowed_archive<A>(&mut self, archive: &'a A)
    where
        A: Archive<ClientID = C>,
    {
        if archive.creation_timestamp() > self.collection_timestamp() {
            self.clients.insert(archive.creator());
        }
    }
}

impl<'a, C> IntoIterator for &'a ValidClients<C> {
    type Item = &'a C;

    type IntoIter = ValidClientsIter<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<C> IntoIterator for ValidClients<C> {
    type Item = C;

    type IntoIter = <HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.clients.into_iter()
    }
}

/// Iterator of clients contained in a [`ValidClients`] set created by [`ValidClients::iter`].
#[derive(Clone, Debug)]
pub struct ValidClientsIter<'a, C> {
    inner: std::collections::hash_set::Iter<'a, C>,
}

impl<'a, C> Iterator for ValidClientsIter<'a, C> {
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, C> ExactSizeIterator for ValidClientsIter<'a, C> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<'a, C> FusedIterator for ValidClientsIter<'a, C> {}

/// Helper for determining the set of unreferenced chunks.
#[derive(Clone, Debug)]
pub struct FossilCollector<C> {
    referenced_chunks: HashSet<C>,

    unreferenced_chunks: HashSet<C>,
}

impl<'a, C> FossilCollector<C> {
    /// Create a new empty instance.
    ///
    /// # Examples
    /// ```
    /// use vinculum::FossilCollector;
    ///
    /// let collector: FossilCollector<[u8; 32]> = FossilCollector::new();
    /// assert_eq!(collector.fossil_candidates().next(), None);
    /// ```
    pub fn new() -> FossilCollector<C> {
        FossilCollector {
            referenced_chunks: HashSet::new(),
            unreferenced_chunks: HashSet::new(),
        }
    }

    /// Variant of [`FossilCollector::new`] which allows to reduce allocations
    /// by specifiying initial chunk capacities.
    pub fn with_capacity(referenced: usize, unreferenced: usize) -> FossilCollector<C> {
        FossilCollector {
            referenced_chunks: HashSet::with_capacity(referenced),
            unreferenced_chunks: HashSet::with_capacity(unreferenced),
        }
    }

    /// Candidates for fossil collection.
    pub fn fossil_candidates(&'a self) -> FossilCandidatesIter<'_, C> {
        FossilCandidatesIter {
            inner: self.unreferenced_chunks.iter(),
        }
    }
}

impl<C> FossilCollector<C>
where
    C: Eq + Hash,
{
    /// Add a reference to a chunk, preventing it from being a fossil candidate.
    ///
    /// If the chunk has already been added as unreferenced it will be changed to referenced.
    pub fn add_reference(&mut self, chunk: C) {
        self.unreferenced_chunks.remove(&chunk);
        self.referenced_chunks.insert(chunk);
    }

    /// Add a unreferenced chunk, making it a fossil candidate.
    ///
    /// If the chunk has already been added as referenced it will remain that way.
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

    type IntoIter = FossilCandidatesIter<'a, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.fossil_candidates()
    }
}

impl<C> IntoIterator for FossilCollector<C> {
    type Item = C;

    type IntoIter = <HashSet<C> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.unreferenced_chunks.into_iter()
    }
}

/// Iterator of fossil candidates determined by [`FossilCollector`] created by [`FossilCollector::fossil_candidates`].
#[derive(Clone, Debug)]
pub struct FossilCandidatesIter<'a, C> {
    inner: std::collections::hash_set::Iter<'a, C>,
}

impl<'a, C> Iterator for FossilCandidatesIter<'a, C> {
    type Item = &'a C;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, C> FusedIterator for FossilCandidatesIter<'a, C> {}

/// Error produced during fossil collection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FossilCollectionError<E, A> {
    /// An error occured while accessing the repository.
    Repository(E),

    /// An error occured while accessing an archive.
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

/// Error produced during fossil deletion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FossilDeletionError<E, A> {
    /// The specified fossil collection can not be deleted yet.
    Uncollectible,

    /// An error occured while accessing the repository.
    Repository(E),

    /// An error occured while accessing an archive.
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
