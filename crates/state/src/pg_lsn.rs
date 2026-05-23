use byteorder::{NetworkEndian, WriteBytesExt};
use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::SqlType;
use diesel::{AsExpression, FromSqlRow};

/// Native PostgreSQL `pg_lsn` SQL type. Wire format is 8 bytes big-endian
/// representing a u64 (the LSN).
#[derive(SqlType)]
#[diesel(postgres_type(name = "pg_lsn"))]
pub struct PgLsn;

/// Rust-side wrapper for `pg_lsn`. Stored and exposed as `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = PgLsn)]
pub struct Lsn(pub u64);

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}

impl From<Lsn> for u64 {
    fn from(l: Lsn) -> u64 {
        l.0
    }
}

impl ToSql<PgLsn, Pg> for Lsn {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        out.write_u64::<NetworkEndian>(self.0)?;
        Ok(IsNull::No)
    }
}

impl FromSql<PgLsn, Pg> for Lsn {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let raw = bytes.as_bytes();
        if raw.len() != 8 {
            return Err(format!("expected 8 bytes for pg_lsn, got {}", raw.len()).into());
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(raw);
        Ok(Lsn(u64::from_be_bytes(buf)))
    }
}
