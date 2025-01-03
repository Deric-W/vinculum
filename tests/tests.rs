#[cfg(feature = "files")]
mod fossil_collection;

#[cfg(feature = "files")]
mod files_backend;

use std::ffi::OsString;

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
