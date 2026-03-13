//! Generate seed corpus files for the WAL decoder fuzzer.
//!
//! Run with: `cargo run --bin generate_seeds` from the fuzz/ directory.

use std::fs;
use std::path::Path;

fn main() {
    let corpus_dir = Path::new("corpus/fuzz_decoder");
    fs::create_dir_all(corpus_dir).unwrap();

    let mut count = 0;
    let mut write = |name: &str, data: &[u8]| {
        fs::write(corpus_dir.join(name), data).unwrap();
        count += 1;
    };

    fn cstring(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    fn tuple(cols: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
        let mut buf = (cols.len() as u16).to_be_bytes().to_vec();
        for &(tag, data) in cols {
            buf.extend_from_slice(tag);
            if let Some(d) = data {
                buf.extend_from_slice(&(d.len() as u32).to_be_bytes());
                buf.extend_from_slice(d);
            }
        }
        buf
    }
    {
        let mut buf = vec![b'B'];
        buf.extend_from_slice(&100u64.to_be_bytes());
        buf.extend_from_slice(&200i64.to_be_bytes());
        buf.extend_from_slice(&42u32.to_be_bytes());
        write("begin", &buf);
    }

    {
        let mut buf = vec![b'C', 0];
        buf.extend_from_slice(&100u64.to_be_bytes());
        buf.extend_from_slice(&200u64.to_be_bytes());
        buf.extend_from_slice(&300i64.to_be_bytes());
        write("commit", &buf);
    }

    {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.extend_from_slice(&cstring("public"));
        buf.extend_from_slice(&cstring("users"));
        buf.push(b'd');
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.push(1); // part of key
        buf.extend_from_slice(&cstring("id"));
        buf.extend_from_slice(&23u32.to_be_bytes());
        buf.extend_from_slice(&(-1i32).to_be_bytes());
        write("relation_simple", &buf);
    }

    {
        let mut buf = vec![b'R'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.extend_from_slice(&cstring("public"));
        buf.extend_from_slice(&cstring("users"));
        buf.push(b'd');
        buf.extend_from_slice(&3u16.to_be_bytes());
        for (flags, name, oid) in [(1u8, "id", 23u32), (0, "name", 25), (0, "email", 25)] {
            buf.push(flags);
            buf.extend_from_slice(&cstring(name));
            buf.extend_from_slice(&oid.to_be_bytes());
            buf.extend_from_slice(&(-1i32).to_be_bytes());
        }
        write("relation_multi_col", &buf);
    }

    // Insert
    {
        let td = tuple(&[(b"t", Some(b"42")), (b"t", Some(b"alice"))]);
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);
        write("insert_simple", &buf);
    }

    // Insert with null + unchanged
    {
        let td = tuple(&[(b"n", None), (b"u", None), (b"t", Some(b"alice"))]);
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);
        write("insert_null_unchanged", &buf);
    }

    // Insert with binary column
    {
        let td = tuple(&[(b"b", Some(b"\x00\x01\x02\x03"))]);
        let mut buf = vec![b'I'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);
        write("insert_binary", &buf);
    }

    // Update without old
    {
        let td = tuple(&[(b"t", Some(b"new"))]);
        let mut buf = vec![b'U'];
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(b'N');
        buf.extend_from_slice(&td);
        write("update_no_old", &buf);
    }

    {
        let td = tuple(&[(b"t", Some(b"42"))]);
        let mut buf = vec![b'D'];
        buf.extend_from_slice(&16384u32.to_be_bytes());
        buf.push(b'K');
        buf.extend_from_slice(&td);
        write("delete_simple", &buf);
    }

    {
        let mut buf = vec![b'T'];
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.push(0x01);
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(&200u32.to_be_bytes());
        write("truncate", &buf);
    }

    write("empty", b"");
    write("unknown_tag", &[0xFF]);

    println!("Generated {count} seed files in {}", corpus_dir.display());
}
