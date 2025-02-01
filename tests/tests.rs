#[cfg(feature = "files")]
mod fossil_collection;

#[cfg(feature = "files")]
mod files_backend;

use futures::stream::TryStreamExt;
use std::collections::HashSet;
use std::ffi::OsString;
use std::hash::Hash;
use vinculum::ChunkBackend;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl From<&ID> for OsString {
    fn from(value: &ID) -> Self {
        hex::encode(value.inner).into()
    }
}

fn assert_eq_unordered<A, B>(a: A, b: B)
where
    A: IntoIterator,
    B: IntoIterator<Item = A::Item>,
    A::Item: Eq + Hash + std::fmt::Debug,
{
    let items_a: Vec<_> = a.into_iter().collect();
    let items_b: Vec<_> = b.into_iter().collect();

    assert_eq!(
        items_a.len(),
        items_b.len(),
        "Iterators have different lengths: {} != {}",
        items_a.len(),
        items_b.len()
    );

    let set_a: HashSet<A::Item> = items_a.into_iter().collect();
    let set_b: HashSet<B::Item> = items_b.into_iter().collect();
    assert_eq!(set_a, set_b);
}

async fn assert_chunks<R, C, B>(repository: &R, chunks: B)
where
    R: ChunkBackend<C>,
    B: IntoIterator<Item = C>,
    C: Eq + Hash + std::fmt::Debug,
{
    let items: Vec<C> = repository
        .chunks()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq_unordered(items, chunks);
}

async fn assert_fossils<R, C, B>(repository: &R, fossils: B)
where
    R: ChunkBackend<C>,
    B: IntoIterator<Item = C>,
    C: Eq + Hash + std::fmt::Debug,
{
    let items: Vec<C> = repository
        .fossils()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq_unordered(items, fossils);
}
