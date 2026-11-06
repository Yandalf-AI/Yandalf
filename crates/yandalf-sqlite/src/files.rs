use anyhow::{Context, Result};
use std::{
    fs::{DirBuilder, File, OpenOptions},
    io::ErrorKind,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub(super) fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}

pub(super) fn prepare_database(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let mut directory = DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        directory.mode(0o700);
        directory
            .create(parent)
            .context("create SQLite store directory")?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error).context("create SQLite store file"),
    }
}

pub(super) fn open_private(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    restrict(&file)?;
    Ok(file)
}

pub(super) fn restrict_database(path: &Path) -> Result<()> {
    for path in [
        path.to_path_buf(),
        with_suffix(path, "-wal"),
        with_suffix(path, "-shm"),
        with_suffix(path, "-journal"),
    ] {
        match File::open(&path) {
            Ok(file) => {
                restrict(&file).with_context(|| format!("secure SQLite file {}", path.display()))?
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("open SQLite file for permission check"),
        }
    }
    Ok(())
}

fn restrict(file: &File) -> std::io::Result<()> {
    #[cfg(unix)]
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    #[cfg(not(unix))]
    let _ = file;
    Ok(())
}
