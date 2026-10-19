use std::collections::HashMap;
use std::fs::Permissions;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use anyhow::{Context, bail, ensure};
use backon::{ConstantBuilder, Retryable};
use bytesize::ByteSize;
use futures::StreamExt;
use futures::future::{BoxFuture, FutureExt, Shared};
use harmonia_file_nar::{NarWriteError, parse_nar, restore};
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, Hash};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::bytes::Bytes;
use tokio_util::io::{InspectReader, ReaderStream, StreamReader};
use tracing::{info, warn};

use crate::closure::Entry;
use crate::narinfo::Compression;
use crate::oci::{self, Repository};
use crate::store_path;
use crate::transport;

use super::fs::{Scratch, rename_tree, set_canonical_time};
use super::{Store, write_unsynced};

/// Restarts a NAR download that the transport couldn't resume.
const NAR_RESTARTS: ConstantBuilder = ConstantBuilder::new().with_max_times(2);

pub(super) type Fetch = Shared<BoxFuture<'static, Result<(), Arc<String>>>>;

/// The closure that a volume or a prefetch asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wanted {
    /// The closure of these store paths, from the caches.
    StorePaths(Vec<StorePath>),
    /// The closure that an artifact lists, from its registry.
    Artifact(oci::Reference),
}

impl std::fmt::Display for Wanted {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::StorePaths(roots) => {
                let roots: Vec<_> = roots.iter().map(ToString::to_string).collect();
                f.write_str(&roots.join(" "))
            }
            Self::Artifact(reference) => reference.fmt(f),
        }
    }
}

/// A closure in the store.
pub struct Ensured {
    /// The store paths that the volume or artifact names.
    pub roots: Vec<StorePath>,
    /// Every store path in the closure, sorted.
    pub paths: Arc<[StorePath]>,
}

/// Where a NAR comes from.
enum Source {
    Cache(usize),
    Registry(Arc<Repository>),
}

impl Store {
    /// Starts fetching the paths in the closure of `roots` that the store
    /// lacks. Resolution and the fetches run in a task of their own, so they
    /// carry on if the caller stops waiting. A closure over the store's limit
    /// fails with [`TooBig`] before any fetch starts. Paths that a
    /// [`Store::prefetch`] holds don't count towards the limit.
    pub fn ensure(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        self.start_ensure(Wanted::StorePaths(roots.to_vec()), false)
    }

    /// Like [`Store::ensure`], for any [`Wanted`] closure.
    pub fn ensure_wanted(self: &Arc<Self>, wanted: Wanted) -> Ensuring {
        self.start_ensure(wanted, false)
    }

    /// Like [`Store::ensure`] with no limit. Until its hold on the closure
    /// ends, the closure doesn't count towards the limit of other ensures.
    pub fn prefetch(self: &Arc<Self>, roots: &[StorePath]) -> Ensuring {
        self.start_ensure(Wanted::StorePaths(roots.to_vec()), true)
    }

    fn start_ensure(self: &Arc<Self>, wanted: Wanted, prefetch: bool) -> Ensuring {
        let this = self.clone();
        let progress = Arc::<OnceLock<_>>::default();
        let set = progress.clone();
        let (tx, rx) = oneshot::channel();
        self.ensures.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let result = this.ensure_closure(&wanted, prefetch, &set).await;
            this.ensures.fetch_sub(1, Ordering::Relaxed);
            // From here on, only the caller's hold makes collection spare the
            // closure.
            drop(set);
            if let Err(Err(e)) = tx.send(result) {
                warn!(closure = %wanted, "no publish is waiting any more: {e:#}");
            }
        });
        Ensuring {
            result: rx,
            progress,
        }
    }

    async fn ensure_closure(
        self: &Arc<Self>,
        wanted: &Wanted,
        prefetch: bool,
        progress: &OnceLock<Progress>,
    ) -> anyhow::Result<Ensured> {
        let installs = self.installs.load(Ordering::SeqCst);
        let (roots, closure) = match wanted {
            Wanted::StorePaths(roots) => (roots.clone(), self.caches.resolve(roots).await?),
            Wanted::Artifact(reference) => {
                let system = store_path::system();
                let store_dir = self.caches.store_dir();
                let artifact = self.registries.fetch(reference, &system, store_dir).await?;
                self.caches.resolve_artifact(artifact)?
            }
        };
        let total: u64 = closure.values().map(|e| e.info.nar_size()).sum();
        let paths: Arc<[StorePath]> = closure.keys().cloned().collect();
        // Under the guard, a collection either sees the hold or finishes
        // before this checks the limit and the store for the paths.
        let (held, missing) = {
            let _gc = self.gc_guard().await;
            let held = self.hold(&paths, prefetch);
            if let Some(limit) = self.max_closure_size.filter(|_| !prefetch) {
                let counted = {
                    let prefetched = self.prefetched.lock().unwrap();
                    (closure.iter())
                        .filter(|(path, _)| !prefetched.contains_key(*path))
                        .map(|(_, e)| e.info.nar_size())
                        .sum()
                };
                if counted > limit {
                    return Err(TooBig {
                        size: total,
                        counted,
                        limit,
                    }
                    .into());
                }
            }
            let (this, all) = (self.clone(), paths.clone());
            let missing = tokio::task::spawn_blocking(move || this.touch_present(&all)).await??;
            (held, missing)
        };
        let fetches: Vec<_> = (missing.iter())
            .map(|path| self.fetch_shared(closure[path].clone()))
            .collect();
        let fetching: u64 = missing.iter().map(|p| closure[p].info.nar_size()).sum();
        // Fetching can take a while, and each fetch holds the entry it needs.
        drop(closure);
        let _ = progress.set(Progress {
            present: total - fetching,
            total,
            fetches: fetches.iter().map(|(_, bytes)| bytes.clone()).collect(),
            _held: held,
        });
        // Returns at the first failure, and the other fetches carry on.
        futures::future::try_join_all(
            fetches
                .into_iter()
                .map(|(fetch, _)| async move { fetch.await.map_err(|e| anyhow::anyhow!("{e}")) }),
        )
        .await?;
        // Only fetches change the node store's narinfos.
        if self.installs.load(Ordering::SeqCst) != installs {
            let (this, all) = (self.clone(), paths.clone());
            tokio::task::spawn_blocking(move || this.check_references(&all)).await??;
        }
        Ok(Ensured { roots, paths })
    }

    /// Fails unless `paths` holds every reference of each store object in it.
    /// A store object that another ensure fetched, or that was fetched again
    /// after collection, may come from a narinfo with other references. A
    /// retry resolves the closure from the node store's narinfo, which matches.
    fn check_references(&self, paths: &[StorePath]) -> anyhow::Result<()> {
        let store_dir = self.caches.store_dir();
        for path in paths {
            let info = (self.caches.local_info(path)?)
                .with_context(|| format!("{} has no narinfo", store_dir.display(path)))?;
            let mut references = info.references().iter();
            if let Some(missing) = references.find(|r| paths.binary_search(r).is_err()) {
                bail!(
                    "{} came from a narinfo that references {}, unlike the one the closure was \
                     resolved from; the next try resolves it again",
                    store_dir.display(path),
                    store_dir.display(missing)
                );
            }
        }
        Ok(())
    }

    /// Makes collection spare `paths` until the result drops. A prefetch's
    /// paths also don't count towards the limit until then.
    fn hold(self: &Arc<Self>, paths: &Arc<[StorePath]>, prefetch: bool) -> Held {
        for holds in self.holds(prefetch) {
            let mut holds = holds.lock().unwrap();
            for path in paths.iter() {
                *holds.entry(path.clone()).or_default() += 1;
            }
        }
        Held {
            store: self.clone(),
            paths: paths.clone(),
            prefetch,
        }
    }

    /// The maps that count a hold: `ensuring`, and for a prefetch, `prefetched`.
    fn holds(&self, prefetch: bool) -> impl Iterator<Item = &Mutex<HashMap<StorePath, usize>>> {
        std::iter::once(&self.ensuring).chain(prefetch.then_some(&self.prefetched))
    }

    /// Records a use of the paths the store has, and returns the rest.
    fn touch_present(&self, paths: &[StorePath]) -> anyhow::Result<Vec<StorePath>> {
        let (present, missing): (Vec<_>, Vec<_>) =
            paths.iter().partition(|path| self.present(path));
        self.touch(present)?;
        Ok(missing.into_iter().cloned().collect())
    }

    /// Runs the fetch of `entry`'s path in its own task, so it finishes even
    /// if no publish waits for it any more. Every publish that needs the path
    /// shares the fetch. Also returns the NAR bytes the fetch has read so far.
    /// The fetch ends once the path is in store/, and the path's sync runs
    /// after that.
    fn fetch_shared(self: &Arc<Self>, entry: Arc<Entry>) -> (Fetch, Arc<AtomicU64>) {
        let path = entry.info.path().clone();
        let mut fetching = self.fetching.lock().unwrap();
        let (fetch, bytes) = fetching.entry(path.clone()).or_insert_with(|| {
            let (this, bytes) = (self.clone(), Arc::<AtomicU64>::default());
            let read = bytes.clone();
            let task = tokio::spawn(async move {
                let leave = Leave(this.clone(), path.clone());
                let result = this.install(entry, &read).await;
                drop(leave);
                if let Ok(true) = result {
                    tokio::spawn(async move {
                        if let Err(e) = this.sync(&path).await {
                            warn!("{e:#}");
                        }
                    });
                }
                result.map(drop).map_err(|e| Arc::new(format!("{e:#}")))
            });
            let fetch = async move { task.await.unwrap_or_else(|e| Err(Arc::new(e.to_string()))) };
            (fetch.boxed().shared(), bytes)
        });
        (fetch.clone(), bytes.clone())
    }

    /// Counts the NAR bytes it reads in `bytes`, and returns whether it
    /// fetched the path.
    async fn install(
        self: &Arc<Self>,
        mut entry: Arc<Entry>,
        bytes: &Arc<AtomicU64>,
    ) -> anyhow::Result<bool> {
        let path = entry.info.path().clone();
        let _permit = self.jobs.acquire().await?;
        // Another fetch may have finished while this one waited.
        if self.present(&path) {
            bytes.store(entry.info.nar_size(), Ordering::Relaxed);
            return Ok(false);
        }
        let mut tried = Vec::new();
        loop {
            let source = match (&entry.registry, entry.cache) {
                (Some(repository), _) => Source::Registry(repository.clone()),
                (None, Some(cache)) => {
                    tried.push(cache);
                    Source::Cache(cache)
                }
                // The node store had the narinfo but no longer has the path.
                (None, None) => {
                    entry = (self.caches.next(&entry, &tried).await)
                        .and_then(|next| next.context("no cache has it"))
                        .with_context(|| format!("fetching {path}"))?;
                    continue;
                }
            };
            let started = Instant::now();
            let fetched = self.fetch(&source, &entry, bytes).await;
            let label = match &source {
                Source::Cache(cache) => self.caches.name(*cache),
                Source::Registry(repository) => &repository.host,
            };
            (self.metrics).nar_fetch(label, fetched.is_ok(), started, entry.info.nar_size());
            let error = match fetched {
                Ok(()) => return Ok(true),
                Err(e) => e.context(format!("fetching {path}")),
            };
            match self.caches.next(&entry, &tried).await {
                Ok(Some(next)) => {
                    warn!("{error:#}; trying the next cache");
                    entry = next;
                }
                Ok(None) => return Err(error),
                Err(e) => {
                    warn!("looking for {path} in the other caches: {e:#}");
                    return Err(error);
                }
            }
        }
    }

    /// Unpacks the store object into tmp/, seals it, and moves it into store/.
    /// Its narinfo stays unsynced until [`Store::sync`].
    async fn fetch(
        self: &Arc<Self>,
        source: &Source,
        entry: &Arc<Entry>,
        bytes: &Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        let info = &entry.info;
        let name = match source {
            Source::Cache(cache) => self.caches.name(*cache),
            Source::Registry(repository) => &repository.name,
        };
        info!(cache = %name, url = %info.url, size = info.nar_size(), "fetching NAR");
        let failed_syncs = self.failed_syncs.load(Ordering::SeqCst);
        let base = info.path().to_string();
        let part = (|| async {
            let body = match source {
                Source::Cache(cache) => self.caches.stream(*cache, &info.url).await?,
                Source::Registry(repository) => repository.stream(&info.url).await?,
            };
            let part = Scratch::new_in(&self.tmp)?;
            let dest = part.path().join(&base);
            let unpacked = self.sandbox.spawn(unpack_nar(
                relay(body),
                info.compression,
                info.nar_hash(),
                info.nar_size(),
                dest,
                bytes.clone(),
            ));
            match unpacked.await.map_err(anyhow::Error::from).flatten() {
                Ok(()) => Ok(part),
                Err(e) => {
                    // Deleting a partly unpacked tree can take a while.
                    self.discard([part]);
                    Err(e)
                }
            }
        })
        .retry(NAR_RESTARTS)
        .when(transport::broke_off)
        .notify(|e, _| warn!(url = %info.url, "fetching NAR: {e:#}; starting over"))
        .await
        .with_context(|| format!("fetching {} from {name}", info.url))?;

        let unpacked = part.path().join(&base);
        let unsynced = self.narinfos.unsynced(info.path());
        let text = info.to_text();
        let dest = self.unpacked(info.path());
        (self.unsynced.lock().unwrap()).insert(info.path().clone(), failed_syncs);
        let moved = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _part = part;
            seal(&unpacked).with_context(|| format!("sealing {}", unpacked.display()))?;
            // The narinfo goes in before the path, so every path in store/
            // has one. Its modification time is the path's last use, and a
            // fetch counts as a use.
            write_unsynced(&unsynced, &text)?;
            rename_tree(&unpacked, &dest)
                .with_context(|| format!("moving {} into the node store", unpacked.display()))
        })
        .await
        .map_err(anyhow::Error::from)
        .flatten();
        self.installs.fetch_add(1, Ordering::SeqCst);
        match moved {
            // Setting the path's mode is the only step that can fail after
            // the move, and the path is whole without it.
            Err(e) if self.present(info.path()) => warn!("{e:#}"),
            Err(e) => {
                self.unsynced.lock().unwrap().remove(info.path());
                return Err(e);
            }
            Ok(()) => {}
        }
        self.size.fetch_add(info.nar_size(), Ordering::Relaxed);
        Ok(())
    }
}

/// Takes a fetch out of [`Store::fetching`] when it ends, even by a panic, so
/// a later publish starts a new one.
struct Leave(Arc<Store>, StorePath);

impl Drop for Leave {
    fn drop(&mut self) {
        self.0.fetching.lock().unwrap().remove(&self.1);
    }
}

/// A closure on its way into the store, from [`Store::ensure`] or
/// [`Store::prefetch`].
pub struct Ensuring {
    result: oneshot::Receiver<anyhow::Result<Ensured>>,
    progress: Arc<OnceLock<Progress>>,
}

/// Counts of a closure's NAR bytes, set once the closure is resolved.
struct Progress {
    /// NAR bytes of the paths the store already had.
    present: u64,
    total: u64,
    /// The NAR bytes that each fetch of the other paths has read.
    fetches: Vec<Arc<AtomicU64>>,
    /// Lasts until both the fetches and the caller are done with it, so
    /// collection spares the closure until the caller has it in a view.
    _held: Held,
}

/// Paths that collection spares, from [`Store::hold`].
struct Held {
    store: Arc<Store>,
    paths: Arc<[StorePath]>,
    prefetch: bool,
}

impl Drop for Held {
    fn drop(&mut self) {
        for holds in self.store.holds(self.prefetch) {
            let mut holds = holds.lock().unwrap();
            for path in self.paths.iter() {
                if let Some(n) = holds.get_mut(path) {
                    *n -= 1;
                    if *n == 0 {
                        holds.remove(path);
                    }
                }
            }
        }
    }
}

impl Ensuring {
    /// Returns the closure's paths once they're all in the store.
    pub async fn done(&mut self) -> anyhow::Result<Arc<[StorePath]>> {
        self.closure().await.map(|closure| closure.paths)
    }

    /// Like [`Ensuring::done`], with the closure's roots.
    pub async fn closure(&mut self) -> anyhow::Result<Ensured> {
        (&mut self.result).await?
    }

    /// Returns the NAR bytes of the closure in the store so far, and the
    /// total, once the closure is resolved.
    pub fn progress(&self) -> Option<(u64, u64)> {
        let progress = self.progress.get()?;
        let fetched: u64 = (progress.fetches.iter())
            .map(|bytes| bytes.load(Ordering::Relaxed))
            .sum();
        Some((progress.present + fetched, progress.total))
    }
}

/// A closure whose NARs, other than those that a prefetch holds, add up to
/// more than the store's limit.
#[derive(Debug)]
pub struct TooBig {
    /// The NAR size of the whole closure.
    pub size: u64,
    /// The part of `size` that counts towards the limit.
    pub counted: u64,
    pub limit: u64,
}

impl std::fmt::Display for TooBig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "the closure is {}", ByteSize(self.size))?;
        if self.counted < self.size {
            let counted = ByteSize(self.counted);
            write!(f, ", or {counted} without prefetched store paths")?;
        }
        write!(f, ", over the limit of {}", ByteSize(self.limit))
    }
}

impl std::error::Error for TooBig {}

/// Gives the tree Nix's canonical modes and modification time.
fn seal(path: &Path) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.is_symlink() {
        return set_canonical_time(path);
    }
    if meta.is_dir() {
        for entry in std::fs::read_dir(path)? {
            seal(&entry?.path())?;
        }
    }
    let mode = if meta.mode() & 0o100 == 0 {
        0o444
    } else {
        0o555
    };
    std::fs::set_permissions(path, Permissions::from_mode(mode))?;
    set_canonical_time(path)
}

/// Unpacks the NAR at `dest` as it streams in, and fails unless its size and
/// hash match. The caller moves the tree into the store only after that.
/// `bytes` counts the NAR bytes read so far.
async fn unpack_nar(
    body: impl AsyncBufRead + Send + Unpin + 'static,
    compression: Compression,
    nar_hash: Hash,
    nar_size: u64,
    dest: PathBuf,
    bytes: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let mut size = 0;
    // Reading one byte past the expected size catches a NAR that's too long.
    let (body, hashed) = decode_apart(compression.decoder(body).take(nar_size + 1));
    let mut nar = InspectReader::new(body, |b| {
        size += b.len() as u64;
        bytes.store(size, Ordering::Relaxed);
    });
    // `restore` rejects entries that would leave `dest`, so a NAR that isn't
    // verified yet can't write elsewhere.
    let mut parse_error = None;
    let events = parse_nar(&mut nar).map(|event| {
        event.map_err(|err| {
            // `restore` only takes its own error type, so the parser's error
            // goes in `parse_error`.
            parse_error = Some(err);
            NarWriteError::create_file_error(dest.clone(), io::ErrorKind::InvalidData.into())
        })
    });
    let restored = restore(events, &dest).await;
    if let Some(err) = parse_error {
        return Err(anyhow::Error::from(err).context("parsing the NAR"));
    }
    restored?;
    // Bytes after the end of the archive count towards the size too.
    tokio::io::copy(&mut nar, &mut tokio::io::sink()).await?;
    drop(nar);
    ensure!(size <= nar_size, "NAR is longer than {nar_size} bytes");
    ensure!(
        size == nar_size,
        "NAR has {size} bytes, expected {nar_size}"
    );
    let hash = hashed.await?;
    ensure!(
        hash == nar_hash,
        "NAR hash mismatch: got {hash:x}, expected {nar_hash:x}"
    );
    Ok(())
}

/// Reads `body` in a task on the caller's runtime. A sandboxed thread can't
/// read it, since the transport may read credentials and resume the
/// download.
fn relay(body: impl AsyncRead + Send + Unpin + 'static) -> impl AsyncBufRead + Send + Unpin {
    pump(body, None)
}

/// Reads `decoded` in one task and hashes it in another, so decompression,
/// hashing and parsing each get a core. Returns a reader of the data and a task
/// that yields its SHA-256. The hash covers all the data once the reader has
/// read to the end. On a 4-vCPU host, tokio workers fetched cold about 15%
/// faster than blocking threads.
fn decode_apart(
    decoded: impl AsyncRead + Send + 'static,
) -> (
    impl AsyncBufRead + Send + Unpin,
    tokio::task::JoinHandle<Hash>,
) {
    let (to_hash, mut hashing) = tokio::sync::mpsc::channel::<Bytes>(4);
    let hashed = tokio::spawn(async move {
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        while let Some(chunk) = hashing.recv().await {
            hasher.update(&chunk);
        }
        hasher.finish()
    });
    (pump(decoded, Some(to_hash)), hashed)
}

/// Reads `src` in a task, and sends each chunk to the returned reader and to
/// `tee`.
fn pump(
    src: impl AsyncRead + Send + 'static,
    tee: Option<tokio::sync::mpsc::Sender<Bytes>>,
) -> impl AsyncBufRead + Send + Unpin {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let mut chunks = std::pin::pin!(ReaderStream::with_capacity(src, 256 * 1024));
        while let Some(chunk) = chunks.next().await {
            if let (Some(tee), Ok(chunk)) = (&tee, &chunk)
                && tee.send(chunk.clone()).await.is_err()
            {
                break;
            }
            if tx.send(chunk).await.is_err() {
                break;
            }
        }
    });
    StreamReader::new(ReceiverStream::new(rx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::tests::sample;
    use crate::narinfo::tests::{
        file_nar, file_store, fixture_narinfo, fixture_path, fixture_root, fixtures, without_lines,
        write_narinfos,
    };
    use crate::store::test_utils::*;

    fn sha256(data: &[u8]) -> Hash {
        Algorithm::SHA256.digest(data)
    }

    async fn unpack(
        compressed: &[u8],
        compression: Compression,
        hash: &Hash,
        size: u64,
    ) -> anyhow::Result<(Scratch, PathBuf)> {
        let tmp = scratch();
        let dest = tmp.path().join("out");
        let body = std::io::Cursor::new(compressed.to_vec());
        let bytes = Arc::default();
        unpack_nar(body, compression, *hash, size, dest.clone(), bytes).await?;
        Ok((tmp, dest))
    }

    #[tokio::test]
    async fn unpacks_other_compressions() {
        use async_compression::tokio::bufread::{BrotliEncoder, BzEncoder, GzipEncoder, XzEncoder};
        use tokio::io::AsyncRead;

        async fn encode(mut encoder: impl AsyncRead + Unpin) -> Vec<u8> {
            let mut out = Vec::new();
            encoder.read_to_end(&mut out).await.unwrap();
            out
        }

        let contents = b"not really a program".repeat(100);
        let plain = file_nar(&contents);
        for (compression, compressed) in [
            (Compression::Xz, encode(XzEncoder::new(&plain[..])).await),
            (Compression::Bzip2, encode(BzEncoder::new(&plain[..])).await),
            (
                Compression::Gzip,
                encode(GzipEncoder::new(&plain[..])).await,
            ),
            (
                Compression::Brotli,
                encode(BrotliEncoder::new(&plain[..])).await,
            ),
        ] {
            let (_tmp, dest) = unpack(
                &compressed,
                compression,
                &sha256(&plain),
                plain.len() as u64,
            )
            .await
            .unwrap();
            assert_eq!(std::fs::read(dest).unwrap(), contents, "{compression:?}");
        }
    }

    #[tokio::test]
    async fn rejects_bad_nars() {
        let plain = file_nar(b"0123456789");
        let hash = sha256(&plain);
        let size = plain.len() as u64;
        for (what, hash, size) in [
            ("wrong hash", sha256(b"other"), size),
            ("too short", hash, size + 1),
            ("too long", hash, size - 1),
        ] {
            assert!(
                unpack(&plain, Compression::None, &hash, size)
                    .await
                    .is_err(),
                "{what}"
            );
        }

        // The parser's error comes back, not the one `restore` got instead.
        let garbage = b"not a NAR".repeat(8);
        let size = garbage.len() as u64;
        let Err(err) = unpack(&garbage, Compression::None, &sha256(&garbage), size).await else {
            panic!("unpacked garbage");
        };
        assert!(format!("{err:#}").contains("parsing the NAR"), "{err:#}");
    }

    #[tokio::test]
    async fn unpacks_closure() {
        let Some(dir) = fixtures() else {
            return;
        };
        let hello = fixture_root(&dir, "hello.path");
        let tmp = scratch();
        // A copy of the cache, since the test deletes it below.
        let cache = tmp.path().join("cache");
        copy_cache(&dir.join("cache-zstd"), &cache);
        let state = tmp.path().join("state");

        let store = Arc::new(Store::new(config(&dir, &cache, &state)).unwrap());
        let err = Store::new(config(&dir, &cache, &state)).err().unwrap();
        assert!(
            format!("{err:#}").contains("another nix-store-csi"),
            "{err:#}"
        );
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        let paths = ensuring.done().await.unwrap();
        assert_eq!(paths.len(), 5);
        let (done, total) = ensuring.progress().unwrap();
        assert!(done == total && total > 0, "{done} of {total}");
        drop(ensuring);
        assert_eq!(count(store.dir()), 5);
        assert_eq!(count(state.join("narinfo")), 5);
        assert_eq!(count(state.join("tmp")), 0);
        let hello_dir = store.unpacked(&hello);
        for (file, mode) in [
            (hello_dir.clone(), 0o555),
            (hello_dir.join("bin/hello"), 0o555),
            (hello_dir.join("share/man/man1/hello.1.gz"), 0o444),
        ] {
            let meta = std::fs::symlink_metadata(&file).unwrap();
            assert_eq!(meta.mode() & 0o7777, mode, "{}", file.display());
            assert_eq!(meta.mtime(), 1, "{}", file.display());
        }

        // A new instance needs no cache for the paths it has.
        std::fs::remove_dir_all(&cache).unwrap();
        let store = reopen(store, || config(&dir, &cache, &state)).await;
        let mut info = fixture_narinfo(&dir, "cache-zstd", &hello);
        info.check_signatures(&[]);
        let stored = std::fs::read(store.narinfos.synced(&hello)).unwrap();
        assert_eq!(stored, info.to_text());
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }

    /// A closure over the limit fails before any fetch starts. The paths the
    /// store already has count towards it, unless a prefetch holds them.
    #[tokio::test]
    async fn limits_closure_size() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let limited = |limit| {
            let mut config = zstd_config(&dir, tmp.path());
            config.max_closure_size = Some(limit);
            config
        };
        let ensure = |store: &Arc<Store>| store.ensure(std::slice::from_ref(&hello));

        let store = Arc::new(Store::new(limited(1)).unwrap());
        let err = ensure(&store).done().await.unwrap_err();
        let &TooBig {
            size,
            counted,
            limit,
        } = err.downcast_ref().unwrap();
        assert_eq!((counted, limit), (size, 1));
        settled(&store).await;
        assert!(store.ensuring.lock().unwrap().is_empty());
        assert_eq!(count(store.dir()), 0);
        assert_eq!(count(tmp.path().join("narinfo")), 0);

        // A prefetch's closure stops counting once it is resolved, before its
        // fetches end, and counts again once its hold ends.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let glibc = store.prefetch(&[fixture_path(&dir, "glibc")]);
        resolved(&glibc).await;
        let err = ensure(&store).done().await.unwrap_err();
        let too_big: &TooBig = err.downcast_ref().unwrap();
        let info = fixture_narinfo(&dir, "cache-zstd", &hello);
        assert_eq!((too_big.size, too_big.counted), (size, info.nar_size()));
        assert!(format!("{err:#}").contains("without prefetched store paths"));
        drop((jobs, glibc));
        settled(&store).await;
        assert!(store.prefetched.lock().unwrap().is_empty());

        let store = reopen(store, || limited(size)).await;
        assert_eq!(ensure(&store).done().await.unwrap().len(), 5);

        let store = reopen(store, || limited(size - 1)).await;
        let err = ensure(&store).done().await.unwrap_err();
        assert!(err.is::<TooBig>(), "{err:#}");
    }

    #[tokio::test]
    async fn counts_fetches() {
        let tmp = scratch();
        let Some((store, _, paths)) = hello_store(tmp.path()).await else {
            return;
        };
        let registry = crate::metrics::registry(&store);
        let cache = store.caches.name(0);
        let metric = |name: &str| sample(&registry, &format!("nix_store_csi_{name}"));
        let fetched = f64::from(u32::try_from(paths.len()).unwrap());
        let count = format!("nar_fetch_duration_seconds_count{{cache=\"{cache}\",result=\"ok\"}}");
        assert_eq!(metric(&count), Some(fetched));
        let failed = count.replace("\"ok\"", "\"error\"");
        assert_eq!(metric(&failed), Some(0.0));
        let published = "publish_duration_seconds_count{grpc_code=\"OK\"}";
        assert_eq!(metric(published), Some(0.0));
        let bytes = format!("nar_fetched_bytes_total{{cache=\"{cache}\"}}");
        assert!(metric(&bytes).unwrap() > 0.0);
        assert_eq!(metric("store_paths"), Some(fetched));
        let size: u64 = (paths.iter())
            .map(|path| store.caches.local_info(path).unwrap().unwrap().nar_size())
            .sum();
        #[allow(clippy::cast_precision_loss)]
        let size = size as f64;
        assert_eq!(metric("store_size_bytes"), Some(size));
        assert_eq!(metric("closures_in_flight"), Some(0.0));
        assert_eq!(metric("unsynced_paths"), Some(0.0));
        assert_eq!(metric("nar_fetches_in_flight"), Some(0.0));
        assert_eq!(metric("failed_syncs_total"), Some(0.0));
        assert!(metric("free_bytes").unwrap() > 0.0);
        // What the test config keeps free.
        assert_eq!(metric("ensure_free_bytes"), Some(f64::from(1u32 << 30)));
        // Both inode gauges are there on a filesystem that limits inodes.
        assert_eq!(
            metric("free_inodes").is_some(),
            metric("ensure_free_inodes").is_some()
        );
        assert_eq!(
            metric(&format!("cache_paused{{cache=\"{cache}\"}}")),
            Some(0.0)
        );
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            metric(&format!("build_info{{version=\"{version}\"}}")),
            Some(1.0)
        );
    }

    /// Ensures whose closures share paths share their fetches.
    #[tokio::test]
    async fn shares_fetches() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let glibc = fixture_path(&dir, "glibc");
        let store = Arc::new(Store::new(zstd_config(&dir, tmp.path())).unwrap());
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let mut a = store.ensure(std::slice::from_ref(&hello));
        let mut b = store.ensure(std::slice::from_ref(&glibc));
        resolved(&a).await;
        resolved(&b).await;
        let fetches = |e: &Ensuring| e.progress.get().unwrap().fetches.clone();
        let (a_fetches, b_fetches) = (fetches(&a), fetches(&b));
        assert!(!b_fetches.is_empty() && b_fetches.len() < a_fetches.len());
        for fetch in &b_fetches {
            assert!(a_fetches.iter().any(|a| Arc::ptr_eq(a, fetch)));
        }
        assert_eq!(store.fetching.lock().unwrap().len(), a_fetches.len());

        drop(jobs);
        assert_eq!(a.done().await.unwrap().len(), a_fetches.len());
        assert_eq!(b.done().await.unwrap().len(), b_fetches.len());
        // Each fetch leaves the map when it ends, so a later failure starts
        // a new one.
        assert!(store.fetching.lock().unwrap().is_empty());
    }

    /// An ensure carries on after its caller stops waiting. When kubelet gives
    /// up on a publish, the retry joins its fetches.
    #[tokio::test]
    async fn carries_on_without_the_caller() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let store = Arc::new(Store::new(zstd_config(&dir, tmp.path())).unwrap());
        // The caller leaves before the closure is resolved. A big closure can
        // take longer to resolve than kubelet waits.
        drop(store.ensure(&[fixture_path(&dir, "glibc")]));
        settled(&store).await;
        let glibc = count(store.dir());
        assert!(glibc > 0);

        // The caller leaves while the fetches wait for jobs.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let fetches = |e: &Ensuring| e.progress.get().unwrap().fetches.clone();
        let first = store.ensure(std::slice::from_ref(&hello));
        resolved(&first).await;
        let started = fetches(&first);
        drop(first);
        let retry = store.ensure(std::slice::from_ref(&hello));
        resolved(&retry).await;
        for fetch in fetches(&retry) {
            assert!(started.iter().any(|s| Arc::ptr_eq(s, &fetch)));
        }

        // With no caller left, the fetches and their syncs still finish.
        drop(retry);
        drop(jobs);
        settled(&store).await;
        assert_eq!(count(store.dir()), glibc + started.len());
        let synced = |name: &String| name.ends_with(".narinfo");
        let narinfos = file_names(tmp.path().join("narinfo"));
        assert_eq!(narinfos.len(), glibc + started.len());
        assert!(narinfos.iter().all(synced));
    }

    /// A NAR that doesn't match its `NarHash` fails over to the next cache.
    #[tokio::test]
    async fn falls_back_after_a_corrupt_nar() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        let corrupt = tmp.path().join("corrupt");
        copy_cache(&dir.join("cache-none"), &corrupt);
        let info = fixture_narinfo(&dir, "cache-none", &hello);
        let nar = corrupt.join(&info.url);
        let mut bytes = std::fs::read(&nar).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        std::fs::write(&nar, bytes).unwrap();

        let state = tmp.path().join("state");
        let mut config = config(&dir, &corrupt, &state);
        config.stores[0].push_str("?priority=10");
        config.stores.push(file_store(&dir.join("cache-zstd")));
        let store = Arc::new(Store::new(config).unwrap());
        let paths = store
            .ensure(std::slice::from_ref(&hello))
            .done()
            .await
            .unwrap();
        settled(&store).await;
        assert_eq!(count(store.dir()), paths.len());
        let narinfo = std::fs::read_to_string(store.narinfos.synced(&hello)).unwrap();
        assert!(narinfo.contains("Compression: zstd"), "{narinfo}");
        assert_eq!(count(state.join("tmp")), 0);
    }

    /// An ensure that joins another ensure's fetch, started from a narinfo
    /// with different references, fails rather than leave out what the store
    /// object needs.
    #[tokio::test]
    async fn checks_references_of_shared_fetches() {
        let Some(dir) = fixtures() else {
            return;
        };
        let tmp = scratch();
        let hello = fixture_root(&dir, "hello.path");
        // The better cache has a narinfo for hello without references.
        let no_refs = tmp.path().join("no-refs");
        copy_cache(&dir.join("cache-zstd"), &no_refs);
        write_narinfos(&dir, "cache-zstd", &no_refs, &[&hello], |t| {
            without_lines(&t, "References:")
        });
        let mut config = config(&dir, &no_refs, &tmp.path().join("state"));
        config.trusted_keys = None;
        config.stores[0].push_str("?priority=10");
        let zstd = file_store(&dir.join("cache-zstd"));
        config.stores.push(format!("{zstd}?priority=20"));
        let store = Arc::new(Store::new(config).unwrap());

        // Another ensure's fetch gets hello from the narinfo with references.
        let jobs = store.jobs.acquire_many(2).await.unwrap();
        let info = fixture_narinfo(&dir, "cache-zstd", &hello);
        let (fetch, _) = store.fetch_shared(Arc::new(Entry {
            cache: Some(1),
            registry: None,
            info,
        }));
        let mut ensuring = store.ensure(std::slice::from_ref(&hello));
        resolved(&ensuring).await;
        drop(jobs);
        fetch.await.unwrap();
        let err = ensuring.done().await.unwrap_err();
        assert!(format!("{err:#}").contains("the next try"), "{err:#}");
        // The next try finds hello in the node store, with its references.
        assert_eq!(store.ensure(&[hello]).done().await.unwrap().len(), 5);
    }

    /// A store for artifacts from `registry`, with `cache` as its cache.
    fn artifact_store(
        state: &Path,
        cache: &Path,
        registry: &oci::test_registry::Registry,
        key: Option<&harmonia_utils_signature::SecretKey>,
    ) -> Arc<Store> {
        let config = crate::store::Config {
            stores: vec![file_store(cache)],
            trusted_keys: key.map(|k| vec![k.to_public_key()]),
            state_dir: state.to_owned(),
            jobs: 2,
            netrc_file: None,
            max_closure_size: None,
            ensure_free: crate::store::EnsureFree::Bytes(0),
            closure_registries: vec![registry.host.clone()],
            insecure_registries: vec!["127.0.0.1".to_owned()],
            registry_credentials: None,
        };
        Arc::new(Store::new(config).unwrap())
    }

    fn artifact(store: &Store, reference: &str) -> Wanted {
        Wanted::Artifact(store.artifact(reference).unwrap())
    }

    #[tokio::test]
    async fn ensures_an_artifact() {
        use oci::test_registry::{Login, Registry};
        let key = oci::tests::key();
        let system = store_path::system();
        let registry = Registry::start(Login::None).await;
        let reference =
            oci::tests::push_closure(&registry, None, &[&system], Compression::Zstd, &key).await;
        let (tmp, cache) = (scratch(), crate::narinfo::tests::empty_cache());
        let store = artifact_store(tmp.path(), cache.path(), &registry, Some(&key));
        let wanted = artifact(&store, &reference);
        let blobs = || registry.requests("GET /v2/envs/app/blobs/").len();
        let before = blobs();

        let closure = store.ensure_wanted(wanted.clone()).closure().await.unwrap();
        let objects = oci::tests::objects(&system);
        assert_eq!(closure.roots, [objects[2].path.clone()]);
        let mut want: Vec<_> = objects.iter().map(|o| o.path.clone()).collect();
        want.sort();
        assert_eq!(&*closure.paths, want);
        let contents = std::fs::read(store.dir().join(objects[2].path.to_string())).unwrap();
        assert_eq!(contents, format!("app for {system}").as_bytes());
        // The config and the three NARs, with no narinfo lookups.
        assert_eq!(blobs() - before, 4);
        assert!(registry.requests(".narinfo").is_empty());
        let metrics = crate::metrics::registry(&store);
        let fetched = format!(
            "nix_store_csi_nar_fetch_duration_seconds_count{{cache=\"oci://{}\",result=\"ok\"}}",
            registry.host
        );
        assert_eq!(sample(&metrics, &fetched), Some(3.0));

        // Another volume fetches only the manifest and the config.
        let again = store.ensure_wanted(wanted).closure().await.unwrap();
        assert_eq!(again.paths, closure.paths);
        assert_eq!(blobs() - before, 5);
    }

    #[tokio::test]
    async fn needs_trusted_signatures_on_artifacts() {
        use oci::test_registry::{Login, Registry};
        let (trusted, other) = (oci::tests::key(), oci::tests::key());
        let system = store_path::system();
        let cache = crate::narinfo::tests::empty_cache();
        let registry = Registry::start(Login::None).await;
        let signed =
            oci::tests::push_closure(&registry, None, &[&system], Compression::None, &trusted)
                .await;
        let by_other =
            oci::tests::push_closure(&registry, None, &[&system], Compression::None, &other).await;
        // Someone who can push changes a path's NarHash, which its signature
        // covers.
        let changed = oci::tests::edit_manifest(&registry, &signed, |m| {
            let hash = m["layers"][0]["annotations"][oci::NAR_HASH]
                .as_str()
                .unwrap();
            let hash = hash.replace(&hash[10..11], if &hash[10..11] == "0" { "1" } else { "0" });
            m["layers"][0]["annotations"][oci::NAR_HASH] = hash.into();
            m["layers"][0]["mediaType"] = format!("{}+zstd", oci::NAR_TYPE).into();
        });
        for (reference, store_key, want) in [
            (&by_other, Some(&trusted), "has no trusted signature for:"),
            (&changed, Some(&trusted), "has no trusted signature for:"),
            // A pod picks the registry, so without keys nothing passes.
            (&signed, None, "closure volumes need trusted public keys"),
        ] {
            let tmp = scratch();
            let store = artifact_store(tmp.path(), cache.path(), &registry, store_key);
            let before = registry.requests("GET").len();
            let err = (store.ensure_wanted(artifact(&store, reference)).done())
                .await
                .unwrap_err();
            assert!(format!("{err:#}").contains(want), "{want}: {err:#}");
            // Only the manifest and the config, no NAR.
            assert_eq!(registry.requests("GET").len() - before, 2, "{want}");
            assert_eq!(count(store.dir()), 0);
        }
    }

    /// A NAR that doesn't match its `NarHash` fails, compressed or not, and
    /// falls back only to a cache with the same `NarHash`.
    #[tokio::test]
    async fn checks_nars_from_artifacts() {
        use oci::test_registry::{Login, Registry};
        let key = oci::tests::key();
        let system = store_path::system();
        let objects = oci::tests::objects(&system);
        let shared = &objects[0];
        for compression in [Compression::None, Compression::Zstd] {
            let registry = Registry::start(Login::None).await;
            let reference =
                oci::tests::push_closure(&registry, None, &[&system], compression, &key).await;
            // The registry serves other bytes for `shared`'s layer.
            let evil = oci::tests::compress(&file_nar(b"evil!!"), compression).await;
            let blob = format!(
                "/v2/{}/blobs/{}",
                oci::tests::NAME,
                oci::tests::digest_of(&oci::tests::compress(&shared.nar, compression).await)
            );
            assert!(registry.get(&blob).is_some());
            registry.put(blob, "application/octet-stream", evil);
            let tmp = scratch();
            let cache = crate::narinfo::tests::empty_cache();
            let store = artifact_store(tmp.path(), cache.path(), &registry, Some(&key));
            let err = (store.ensure_wanted(artifact(&store, &reference)).done())
                .await
                .unwrap_err();
            assert!(format!("{err:#}").contains("NAR hash mismatch"), "{err:#}");
            assert!(!store.present(&shared.path));

            // A cache with another NAR for the path doesn't stand in for the
            // registry. One with the same NAR does.
            let other = oci::tests::Object {
                path: shared.path.clone(),
                nar: file_nar(b"another shared"),
                references: Vec::new(),
            };
            let narinfo = cache.path().join(format!("{}.narinfo", shared.path.hash()));
            std::fs::create_dir_all(cache.path().join("nar")).unwrap();
            for (object, works) in [(&other, false), (shared, true)] {
                let text = oci::tests::narinfo_text(object, Compression::None, Some(&key));
                std::fs::write(&narinfo, text).unwrap();
                let file = format!("nar/{}.nar.none", shared.path.hash());
                std::fs::write(cache.path().join(file), &object.nar).unwrap();
                let done = store
                    .ensure_wanted(artifact(&store, &reference))
                    .done()
                    .await;
                assert_eq!(done.is_ok(), works, "{compression:?} {done:?}");
            }
            assert!(store.present(&shared.path));
        }
    }
}
