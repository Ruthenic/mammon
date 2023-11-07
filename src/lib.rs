// mammon - a storage engine
// `store(key: ToString, val: Iter<u8>): Result<()>` and `retrieve(key: ToString): Result<Iter<u8>>` and maybe `defragement(): Result<>`

use std::collections::HashMap;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Index {
    pub offset: u64,
    pub length: u64,
}

pub struct Store {
    pub indexes: HashMap<String, Index>,
    pub empties: Vec<Index>,
    pub blob_file: File,
    pub empty_file: File,
    pub db_file: File,
}
/// open a file with r/w permissions.
fn open_file<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())
}

/// create a file with r/w permissions, truncating it if it exists
fn create_file<P: AsRef<Path>>(path: P) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path.as_ref())
}

impl Store {
    /// creates a *new* Mammon::Store in the given directory. cannot be used to open an existing store.
    pub fn new(directory: PathBuf) -> Result<Self> {
        if !directory.exists() {
            create_dir_all(&directory)?;
        } else if !directory.is_dir() {
            bail!("{:?} is not a directory", directory);
        }

        let blob_file = create_file(directory.join("mammon_blobs.bin"))?;
        let empty_file = create_file(directory.join("mammon_empties.cbor"))?;
        let db_file = create_file(directory.join("mammon.cbor"))?;

        Ok(Self {
            indexes: HashMap::new(),
            empties: vec![],
            blob_file,
            empty_file,
            db_file,
        })
    }
    /// opens an existing Mammon::Store in a given directory. cannot be used to create a new store.
    pub fn open(directory: PathBuf) -> Result<Self> {
        if !directory.exists() {
            bail!("{:?} does not exist", directory);
        }

        let blob_file = open_file(directory.join("mammon_blobs.bin"))?;
        let empty_file = open_file(directory.join("mammon_empties.cbor"))?;
        let db_file = open_file(directory.join("mammon.cbor"))?;

        let indexes: HashMap<String, Index> = ciborium::from_reader(&db_file)?;
        let empties: Vec<Index> = ciborium::from_reader(&empty_file)?;

        Ok(Self {
            indexes,
            empties,
            blob_file,
            empty_file,
            db_file,
        })
    }

    /// store a blob in the store, returning Ok(()) on success
    pub fn store(&mut self, key: impl ToString, val: Vec<u8>) -> Result<()> {
        let offset = self.blob_file.seek(std::io::SeekFrom::End(0))?;
        let length = val.len() as u64;

        self.blob_file.write_all(val.as_slice())?;

        self.indexes
            .insert(key.to_string(), Index { offset, length });

        ciborium::into_writer(&self.indexes, &self.db_file)?;

        Ok(())
    }

    /// retrieve a blob from the store
    pub fn retrieve(&mut self, key: impl ToString) -> Result<Vec<u8>> {
        let index = self
            .indexes
            .get(&key.to_string())
            .ok_or_else(|| anyhow::anyhow!("key not found"))?;

        self.blob_file
            .seek(std::io::SeekFrom::Start(index.offset))?;
        let mut buf = vec![0; index.length as usize];
        self.blob_file.read_exact(&mut buf)?;

        return Ok(buf.clone());
    }

    /// delete a blob from the store
    pub fn delete(&mut self, key: impl ToString) -> Result<()> {
        let index = self
            .indexes
            .get(&key.to_string())
            .ok_or_else(|| anyhow::anyhow!("key not found"))?;

        self.empties.push(Index {
            offset: index.offset,
            length: index.length,
        }); // sigh emoji

        self.indexes.remove(&key.to_string());

        ciborium::into_writer(&self.indexes, &self.db_file)?;
        ciborium::into_writer(&self.empties, &self.empty_file)?;

        Ok(())
    }

    // FIXME: implement defragmentation, to avoid the file growing forever
}
