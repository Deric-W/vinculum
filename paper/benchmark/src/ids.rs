//! ID types

use serde::{Deserialize, Serialize};
use std::ffi::OsString;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChunkID {
    inner: [u8; 32],
}

impl ChunkID {
    pub fn new(hash: [u8; 32]) -> ChunkID {
        ChunkID { inner: hash }
    }
}

impl TryFrom<&[u8]> for ChunkID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut buf = [0; 32];
        match hex::decode_to_slice(value, &mut buf) {
            Ok(()) => Ok(ChunkID::new(buf)),
            Err(_) => Err(()),
        }
    }
}

impl From<&ChunkID> for OsString {
    fn from(value: &ChunkID) -> Self {
        hex::encode(value.inner).into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ID {
    inner: String,
}

impl ID {
    pub fn new(inner: String) -> ID {
        ID { inner }
    }
}

impl TryFrom<&[u8]> for ID {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match std::str::from_utf8(value) {
            Ok(string) => Ok(ID::new(string.into())),
            Err(_) => Err(()),
        }
    }
}

impl From<&ID> for OsString {
    fn from(value: &ID) -> Self {
        value.inner.clone().into()
    }
}
