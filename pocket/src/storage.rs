use rusqlite::{Connection, OptionalExtension, Result};
use std::path::Path;

pub struct Storage {
    con: Connection,
}

impl Storage {
    pub fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let con = Connection::open(path)?;
        con.pragma_update(None, "journal_mode", "WAL")?;
        con.pragma_update(None, "synchronous", "NORMAL")?;
        con.pragma_update(None, "busy_timeout", "100")?;
        con.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self { con })
    }
    pub fn init(path: impl AsRef<Path>) -> Result<Self> {
        let me = Self::connect(path)?;
        me.con.execute(
            r#"
            create table prefs (
                actor text not null,
                aud   text not null,
                pref  text not null,
                primary key (actor, aud)
            ) strict"#,
            (),
        )?;
        Ok(me)
    }
    pub fn put(&self, actor: &str, aud: &str, pref: &str) -> Result<()> {
        self.con.execute(
            r#"insert into prefs (actor, aud, pref)
               values (?1, ?2, ?3)
               on conflict do update set pref = excluded.pref"#,
            [actor, aud, pref],
        )?;
        Ok(())
    }
    pub fn get(&self, actor: &str, aud: &str) -> Result<Option<String>> {
        self.con
            .query_one(
                r#"select pref from prefs
                   where actor = ?1 and aud = ?2"#,
                [actor, aud],
                |row| row.get(0),
            )
            .optional()
    }
}
