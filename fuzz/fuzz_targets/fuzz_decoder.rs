#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

// Fuzz the pgoutput binary protocol decoder.
// The decoder parses untrusted binary data from the Postgres WAL stream.
// We want to ensure it never panics, regardless of input — only Ok or Err.
fuzz_target!(|data: &[u8]| {
    let bytes = Bytes::copy_from_slice(data);
    // Should never panic — only Ok or Err
    let _ = replication::decoder::decode(bytes);
});
