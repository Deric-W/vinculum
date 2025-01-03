//! Utilities for creating custom backends.

use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

/// Convert a timestamp to a platform-independent representation.
///
/// This function produces an error should the timestamp be before [`UNIX_EPOCH`].
pub fn timestamp_to_bytes(timestamp: SystemTime) -> Result<[u8; 12], SystemTimeError> {
    let duration = timestamp.duration_since(UNIX_EPOCH)?;
    let mut buf = [0u8; 12];
    buf[..8].copy_from_slice(&duration.as_secs().to_be_bytes());
    buf[8..].copy_from_slice(&duration.subsec_nanos().to_be_bytes());
    Ok(buf)
}

/// Inverse of [`timestamp_to_bytes`].
///
/// This function produces an error should the timestamp be out of bounds
/// containing the parsed seconds and nanoseconds since [`UNIX_EPOCH`].
pub fn timestamp_from_bytes(buffer: [u8; 12]) -> Result<SystemTime, (u64, u32)> {
    let secs = u64::from_be_bytes(buffer[..8].try_into().unwrap());
    let nsecs = u32::from_be_bytes(buffer[8..].try_into().unwrap());
    // prevent panic when using Duration::new
    let duration = match Duration::from_secs(secs).checked_add(Duration::from_nanos(nsecs.into())) {
        Some(d) => d,
        None => return Err((secs, nsecs)),
    };
    match UNIX_EPOCH.checked_add(duration) {
        Some(timestamp) => Ok(timestamp),
        None => Err((secs, nsecs)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_round_trip() {
        for timestamp in [SystemTime::now(), UNIX_EPOCH] {
            let bytes = timestamp_to_bytes(timestamp).unwrap();
            assert_eq!(Ok(timestamp), timestamp_from_bytes(bytes));
        }
    }

    #[test]
    fn timestamp_no_panic_on_overflow() {
        let mut buffer = [0; 12];
        buffer[..8].copy_from_slice(&u64::MAX.to_be_bytes());
        buffer[8..].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(timestamp_from_bytes(buffer), Err((u64::MAX, u32::MAX)));
    }
}
