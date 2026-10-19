//! Closures from OCI registries. A volume's `closure` attribute pins an
//! artifact by digest. Its config names the roots, and it has a layer for each
//! store path in the closure: the path's NAR, with the rest of its narinfo as
//! annotations. [`docs/OCI.md`](../../../docs/OCI.md) describes the format.

#[cfg(test)]
pub(crate) mod test_registry;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use anyhow::{Context, bail, ensure};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::Algorithm;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncBufRead;
use url::Url;

use crate::narinfo::{Compression, NarInfo};
use crate::store_path;
use crate::transport::{self, Transport};

pub const ARTIFACT_TYPE: &str = "application/vnd.nix-store-csi.closure.v1";
pub const CONFIG_TYPE: &str = "application/vnd.nix-store-csi.closure.config.v1+json";
/// An uncompressed NAR. A compressed one adds the compression's name in
/// Nix's spelling, like `+xz` or `+zstd`.
pub const NAR_TYPE: &str = "application/vnd.nix-store-csi.nar.v1";
pub const MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";

pub const STORE_PATH: &str = "nix-store-csi.store-path";
pub const NAR_HASH: &str = "nix-store-csi.nar-hash";
pub const NAR_SIZE: &str = "nix-store-csi.nar-size";
pub const REFERENCES: &str = "nix-store-csi.references";
pub const SIGNATURES: &str = "nix-store-csi.signatures";

/// The size that the OCI distribution spec asks registries to accept for a
/// manifest. A closure of a few thousand paths fits.
const MAX_MANIFEST: u64 = 4 << 20;

/// Far more than a config's few roots need.
const MAX_CONFIG: u64 = 1 << 20;

/// An artifact that a volume pins, like
/// `oci://registry.example.com/envs/web@sha256:<digest>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    /// `host` or `host:port`.
    host: String,
    name: String,
    digest: String,
    /// The repository's URL, `https://<host>/v2/<name>/`, or `http://` for a
    /// host in `--insecure-registries`.
    base: Url,
}

impl std::fmt::Display for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "oci://{}/{}@{}", self.host, self.name, self.digest)
    }
}

/// The settings for reading artifacts from registries.
pub struct Registries {
    /// `host[:port][/<repository prefix>]` that `closure` volumes may name.
    allowed: Vec<String>,
    /// Hosts, with or without a port, that serve plain HTTP.
    insecure: Vec<String>,
}

/// A repository that NARs come from.
pub struct Repository {
    /// `oci://<host>/<name>`, for messages.
    pub name: Url,
    /// `oci://<host>`, the label that the metrics count NAR fetches under, so
    /// that each repository doesn't add a series.
    pub host: Url,
    transport: Transport,
}

impl Repository {
    /// A reader for the blob at `path`, like `blobs/sha256:<digest>`, which
    /// holds a NAR with `compression`.
    pub async fn stream(
        &self,
        path: &str,
        compression: Compression,
    ) -> anyhow::Result<Box<dyn AsyncBufRead + Send + Unpin>> {
        self.transport.stream(path, compression).await
    }
}

/// A closure as an artifact lists it. The narinfos still carry their
/// signatures, which the caller checks.
pub struct Artifact {
    pub repository: Arc<Repository>,
    pub roots: Vec<StorePath>,
    /// One for each store path in the closure, each with the URL of its
    /// layer, relative to the repository.
    pub layers: Vec<NarInfo>,
}

/// A manifest.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Document {
    #[serde(default)]
    schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<Descriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    layers: Vec<Descriptor>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Descriptor {
    media_type: String,
    digest: String,
    size: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    annotations: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    roots: Vec<String>,
    store_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
}

/// The parts of `oci://<host>/<name>[:<tag>][@<digest>]`, without the tag.
struct Parts<'a> {
    host: &'a str,
    name: &'a str,
    digest: Option<&'a str>,
}

fn parts(reference: &str) -> anyhow::Result<Parts<'_>> {
    let rest = (reference.strip_prefix("oci://"))
        .with_context(|| format!("{reference:?} is not an oci:// reference"))?;
    let (rest, digest) = match rest.rsplit_once('@') {
        Some((rest, digest)) => (rest, Some(digest)),
        None => (rest, None),
    };
    let (host, path) =
        (rest.split_once('/')).with_context(|| format!("{reference:?} has no repository name"))?;
    let (name, tag) = match path.rsplit_once(':') {
        Some((name, tag)) => (name, Some(tag)),
        None => (path, None),
    };
    ensure!(
        !host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-.:[]".contains(c)),
        "{reference:?} has a bad host {host:?}"
    );
    check_name(name).with_context(|| format!("parsing {reference:?}"))?;
    if let Some(tag) = tag {
        let ok = tag.len() <= 128
            && (tag.bytes().next()).is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
            && (tag.bytes()).all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b));
        ensure!(ok, "{reference:?} has a bad tag {tag:?}");
    }
    if let Some(digest) = digest {
        check_digest(digest).with_context(|| format!("parsing {reference:?}"))?;
    }
    Ok(Parts { host, name, digest })
}

impl Registries {
    pub fn new(allowed: Vec<String>, insecure: Vec<String>) -> Self {
        Self { allowed, insecure }
    }

    fn insecure(&self, host: &str) -> bool {
        let name = host.rsplit_once(':').map_or(host, |(name, _)| name);
        (self.insecure.iter()).any(|h| h.eq_ignore_ascii_case(host) || h.eq_ignore_ascii_case(name))
    }

    /// `https://<host>/v2/<name>/`, or `http://` for an insecure host.
    fn base(&self, host: &str, name: &str) -> anyhow::Result<Url> {
        let scheme = if self.insecure(host) { "http" } else { "https" };
        let base = Url::parse(&format!("{scheme}://{host}/v2/{name}/"))
            .with_context(|| format!("bad host {host:?}"))?;
        ensure!(
            base.has_host() && base.path() == format!("/v2/{name}/"),
            "bad host {host:?}"
        );
        Ok(base)
    }

    fn repository(host: &str, name: &str, base: &Url) -> anyhow::Result<Repository> {
        Ok(Repository {
            name: Url::parse(&format!("oci://{host}/{name}"))?,
            host: Url::parse(&format!("oci://{host}"))?,
            transport: Transport::registry(base)?,
        })
    }

    /// Parses a `closure` attribute,
    /// `oci://<host>[:<port>]/<name>@sha256:<digest>`. The registry has to be
    /// one that `--closure-registries` allows. A tag before the digest, as in
    /// `<name>:v1@sha256:<digest>`, tells tools like Renovate which tag to
    /// follow, and the plugin ignores it.
    pub fn parse(&self, reference: &str) -> anyhow::Result<Reference> {
        let reference = reference.trim();
        let parts = parts(reference)?;
        let Some(digest) = parts.digest else {
            bail!("{reference:?} has no digest; pin it like oci://<host>/<name>@sha256:<digest>");
        };
        let (host, name) = (parts.host, parts.name);
        ensure!(
            !self.allowed.is_empty(),
            "closure volumes are off, since --closure-registries lists no registry"
        );
        let allowed = self.allowed.iter().any(|entry| {
            let (entry_host, prefix) = entry.split_once('/').unwrap_or((entry, ""));
            let prefix = prefix.trim_end_matches('/');
            entry_host.eq_ignore_ascii_case(host)
                && (prefix.is_empty()
                    || name == prefix
                    || name
                        .strip_prefix(prefix)
                        .is_some_and(|r| r.starts_with('/')))
        });
        ensure!(allowed, "{host}/{name} is not in --closure-registries");
        Ok(Reference {
            host: host.to_owned(),
            name: name.to_owned(),
            digest: digest.to_owned(),
            base: self.base(host, name)?,
        })
    }

    /// Fetches the manifest that `reference` pins, and its config. Each must
    /// have the digest that names it, and the config must be for `system`.
    /// Each layer becomes a narinfo for its store path.
    pub async fn fetch(
        &self,
        reference: &Reference,
        system: &str,
        store_dir: &StoreDir,
    ) -> anyhow::Result<Artifact> {
        let fetched = self.fetch_manifest(reference, system, store_dir).await;
        fetched.with_context(|| format!("reading {reference}"))
    }

    async fn fetch_manifest(
        &self,
        reference: &Reference,
        system: &str,
        store_dir: &StoreDir,
    ) -> anyhow::Result<Artifact> {
        let repository = Self::repository(&reference.host, &reference.name, &reference.base)?;
        let (content_type, manifest) = (get_document(&repository.transport, &reference.digest))
            .await
            .map_err(|e| hint(&reference.base, e))?;
        let media_type = (manifest.media_type.as_deref())
            .or(content_type.as_deref())
            .map(|t| t.split(';').next().unwrap_or_default().trim());
        ensure!(
            media_type == Some(MANIFEST_TYPE),
            "the artifact is a {}, not a {MANIFEST_TYPE}",
            media_type.unwrap_or("document with no media type")
        );
        if let Some(t) = &manifest.artifact_type {
            ensure!(
                t == ARTIFACT_TYPE,
                "the artifact is a {t}, not a {ARTIFACT_TYPE}"
            );
        }
        let config = manifest
            .config
            .as_ref()
            .context("the manifest has no config")?;
        ensure!(
            config.media_type == CONFIG_TYPE,
            "the manifest's config is a {}, not a {CONFIG_TYPE}",
            config.media_type
        );
        let config = get_blob(&repository.transport, config, MAX_CONFIG).await?;
        let config: Config = serde_json::from_slice(&config).context("parsing the config")?;
        ensure!(
            config.store_dir == store_dir.to_str(),
            "the artifact holds paths for {}, not {store_dir}",
            config.store_dir
        );
        if let Some(theirs) = &config.system {
            ensure!(
                theirs == system,
                "the artifact is for {theirs}, not this node's {system}"
            );
        }
        let roots = (config.roots.iter())
            .map(|root| store_path::parse(store_dir, root).with_context(|| format!("root {root}")))
            .collect::<anyhow::Result<Vec<_>>>()?;
        ensure!(!roots.is_empty(), "the config lists no roots");

        let layers = (manifest.layers.iter())
            .map(|layer| narinfo(store_dir, layer))
            .collect::<anyhow::Result<Vec<_>>>()?;
        check_closure(store_dir, &roots, &layers)?;
        Ok(Artifact {
            repository: Arc::new(repository),
            roots,
            layers,
        })
    }
}

/// Explains a request to a registry over HTTPS that couldn't connect, since
/// the registry may serve only plain HTTP.
fn hint(base: &Url, e: anyhow::Error) -> anyhow::Error {
    if base.scheme() == "https" && transport::failed_to_connect(&e) {
        e.context("a registry that serves plain HTTP has to be in --insecure-registries")
    } else {
        e
    }
}

/// The manifest with `digest`, which must be its SHA-256.
async fn get_document(
    transport: &Transport,
    digest: &str,
) -> anyhow::Result<(Option<String>, Document)> {
    let path = format!("manifests/{digest}");
    let (body, content_type) = match transport
        .get_typed(&path, MANIFEST_TYPE, MAX_MANIFEST)
        .await
    {
        Err(e) if transport::lacks_file(&e) => {
            return Err(e.context(format!("the registry has no manifest {digest}")));
        }
        got => got?,
    };
    check_content(digest, &body).context("checking the manifest")?;
    let document =
        serde_json::from_slice(&body).with_context(|| format!("parsing manifest {digest}"))?;
    Ok((content_type, document))
}

/// The blob that `descriptor` names, of at most `max` bytes.
async fn get_blob(
    transport: &Transport,
    descriptor: &Descriptor,
    max: u64,
) -> anyhow::Result<Vec<u8>> {
    let digest = &descriptor.digest;
    check_digest(digest)?;
    ensure!(descriptor.size <= max, "blob {digest} is over {max} bytes");
    let (body, _) = transport
        .get_typed(&format!("blobs/{digest}"), "*/*", max)
        .await?;
    check_content(digest, &body)?;
    ensure!(
        body.len() as u64 == descriptor.size,
        "blob {digest} has {} bytes, not {}",
        body.len(),
        descriptor.size
    );
    Ok(body)
}

/// The SHA-256 of `content` as a digest, `sha256:<hex>`.
fn digest(content: &[u8]) -> String {
    format!("{:x}", Algorithm::SHA256.digest(content))
}

/// Fails unless `digest` is the SHA-256 of `content`.
fn check_content(digest: &str, content: &[u8]) -> anyhow::Result<()> {
    let got = self::digest(content);
    ensure!(
        got == digest,
        "the registry sent content with digest {got} for {digest}"
    );
    Ok(())
}

/// Accepts only `sha256:` and 64 lowercase hex digits, which keeps the digest
/// from changing the URL it goes in.
fn check_digest(digest: &str) -> anyhow::Result<()> {
    let hex = (digest.strip_prefix("sha256:"))
        .with_context(|| format!("digest {digest:?} is not sha256:<hex>"))?;
    ensure!(
        hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "digest {digest:?} is not sha256: and 64 lowercase hex digits"
    );
    Ok(())
}

/// Accepts the repository names of the OCI distribution spec, components of
/// lowercase letters, digits and separators.
fn check_name(name: &str) -> anyhow::Result<()> {
    for component in name.split('/') {
        let ok = (component.bytes().next()).is_some_and(|b| b.is_ascii_alphanumeric())
            && component
                .bytes()
                .last()
                .is_some_and(|b| b.is_ascii_alphanumeric())
            && (component.bytes())
                .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-'));
        ensure!(ok, "bad repository name {name:?}");
    }
    Ok(())
}

/// The compression of a NAR layer of `media_type`.
fn compression(media_type: &str) -> anyhow::Result<Compression> {
    match media_type.strip_prefix(NAR_TYPE) {
        Some("") => Ok(Compression::None),
        Some(suffix) if suffix.starts_with('+') && suffix != "+none" => {
            Compression::parse(&suffix[1..])
        }
        _ => bail!("a {media_type} is not a {NAR_TYPE} layer"),
    }
}

/// The narinfo that a layer's annotations describe. Its URL is the layer's
/// blob. An uncompressed layer holds the NAR itself, so its digest has to be
/// the `NarHash`. A compressed one needs the `NarHash` and `NarSize` from its
/// annotations, which the fetch checks after decompressing.
fn narinfo(store_dir: &StoreDir, layer: &Descriptor) -> anyhow::Result<NarInfo> {
    let digest = &layer.digest;
    let path = (layer.annotations.get(STORE_PATH))
        .with_context(|| format!("layer {digest} has no {STORE_PATH} annotation"))?;
    let context = || format!("layer {digest} for {path}");
    let compression = compression(&layer.media_type).with_context(context)?;
    check_digest(digest).with_context(context)?;
    let field = |key: &str| -> anyhow::Result<Option<&str>> {
        let value = layer.annotations.get(key).map(String::as_str);
        // A line break would add a line to the narinfo.
        ensure!(
            value.is_none_or(|v| !v.contains(['\n', '\r'])),
            "{key} has a line break"
        );
        Ok(value)
    };
    let mut text = String::new();
    (|| -> anyhow::Result<()> {
        let size = layer.size.to_string();
        let (nar_hash, nar_size) = match compression {
            Compression::None => (
                field(NAR_HASH)?.unwrap_or(digest),
                field(NAR_SIZE)?.unwrap_or(&size).to_owned(),
            ),
            _ => (
                field(NAR_HASH)?.with_context(|| format!("a compressed layer needs {NAR_HASH}"))?,
                (field(NAR_SIZE)?)
                    .with_context(|| format!("a compressed layer needs {NAR_SIZE}"))?
                    .to_owned(),
            ),
        };
        writeln!(text, "StorePath: {path}")?;
        writeln!(text, "URL: blobs/{digest}")?;
        writeln!(text, "Compression: {}", compression.name())?;
        writeln!(text, "NarHash: {nar_hash}")?;
        writeln!(text, "NarSize: {nar_size}")?;
        let references = field(REFERENCES)?.unwrap_or_default().trim();
        if !references.is_empty() {
            writeln!(text, "References: {references}")?;
        }
        for sig in field(SIGNATURES)?.unwrap_or_default().split_whitespace() {
            writeln!(text, "Sig: {sig}")?;
        }
        ensure!(
            !path.contains(['\n', '\r']),
            "{STORE_PATH} has a line break"
        );
        Ok(())
    })()
    .with_context(context)?;
    let info = NarInfo::parse(store_dir, &text).with_context(context)?;
    if compression == Compression::None {
        let nar_hash = format!("{:x}", info.nar_hash());
        ensure!(
            nar_hash == *digest,
            "{}: its NarHash is {nar_hash}",
            context()
        );
        ensure!(
            info.nar_size() == layer.size,
            "{}: its NarSize is {}",
            context(),
            info.nar_size()
        );
    }
    Ok(info)
}

/// Fails unless the layers hold each root and each reference once.
fn check_closure(
    store_dir: &StoreDir,
    roots: &[StorePath],
    layers: &[NarInfo],
) -> anyhow::Result<()> {
    let mut paths = BTreeSet::new();
    for info in layers {
        ensure!(
            paths.insert(info.path()),
            "two layers hold {}",
            store_dir.display(info.path())
        );
    }
    for root in roots {
        ensure!(
            paths.contains(root),
            "no layer holds the root {}",
            store_dir.display(root)
        );
    }
    for info in layers {
        if let Some(missing) = info.references().iter().find(|r| !paths.contains(r)) {
            bail!(
                "no layer holds {}, which {} references",
                store_dir.display(missing),
                store_dir.display(info.path())
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use harmonia_utils_hash::HashFormat;
    use harmonia_utils_signature::SecretKey;
    use serde_json::{Value, json};
    use tokio::io::AsyncReadExt;

    use super::test_registry::Registry;
    use super::*;
    use crate::narinfo::tests::{file_nar, sign};

    pub const NAME: &str = "envs/app";

    pub fn store_path(name: &str, fill: char) -> StorePath {
        StorePath::from_base_path(&format!("{}-{name}", fill.to_string().repeat(32))).unwrap()
    }

    /// A store object for the test artifacts.
    pub struct Object {
        pub path: StorePath,
        pub nar: Vec<u8>,
        pub references: Vec<StorePath>,
    }

    /// `app`, which references itself and `lib`, and `lib`. Each system gets
    /// its own store paths, except `shared`, which `lib` references.
    pub fn objects(system: &str) -> Vec<Object> {
        let fill = if system == "x86_64-linux" { '1' } else { '2' };
        let shared = store_path("shared", '9');
        let lib = store_path("lib", fill);
        let app = store_path("app", char::from(fill as u8 + 2));
        vec![
            Object {
                path: shared.clone(),
                nar: file_nar(b"shared"),
                references: Vec::new(),
            },
            Object {
                path: lib.clone(),
                nar: file_nar(format!("lib for {system}").as_bytes()),
                references: vec![shared],
            },
            Object {
                nar: file_nar(format!("app for {system}").as_bytes()),
                references: vec![app.clone(), lib],
                path: app,
            },
        ]
    }

    pub async fn compress(nar: &[u8], compression: Compression) -> Vec<u8> {
        use async_compression::tokio::bufread::{XzEncoder, ZstdEncoder};
        let mut out = Vec::new();
        match compression {
            Compression::None => out.extend_from_slice(nar),
            Compression::Xz => XzEncoder::new(nar)
                .read_to_end(&mut out)
                .await
                .map(drop)
                .unwrap(),
            Compression::Zstd => ZstdEncoder::new(nar)
                .read_to_end(&mut out)
                .await
                .map(drop)
                .unwrap(),
            _ => unimplemented!(),
        }
        out
    }

    /// The narinfo text of `object` in a cache whose NAR has `compression`,
    /// signed by `key` if given.
    pub fn narinfo_text(
        object: &Object,
        compression: Compression,
        key: Option<&SecretKey>,
    ) -> String {
        let store_dir = StoreDir::default();
        let references: Vec<_> = object.references.iter().map(ToString::to_string).collect();
        let mut text = format!(
            "StorePath: {}\nURL: nar/{}.nar.{}\nCompression: {}\nNarHash: {}\nNarSize: {}\n",
            store_dir.display(&object.path),
            object.path.hash(),
            compression.name(),
            compression.name(),
            Algorithm::SHA256.digest(&object.nar).as_base32(),
            object.nar.len()
        );
        if !references.is_empty() {
            writeln!(text, "References: {}", references.join(" ")).unwrap();
        }
        if let Some(key) = key {
            let info = NarInfo::parse(&store_dir, &text).unwrap();
            writeln!(text, "Sig: {}", sign(&info, key)).unwrap();
        }
        text
    }

    /// The media type of a NAR layer with `compression`.
    pub fn nar_type(compression: Compression) -> String {
        match compression {
            Compression::None => NAR_TYPE.to_owned(),
            c => format!("{NAR_TYPE}+{}", c.name()),
        }
    }

    /// Puts `content` in `registry` as a blob and returns its descriptor.
    fn put_blob(registry: &Registry, content: Vec<u8>, media_type: &str) -> Descriptor {
        let digest = super::digest(&content);
        let size = content.len() as u64;
        let path = format!("/v2/{NAME}/blobs/{digest}");
        registry.put(path, "application/octet-stream", content);
        Descriptor {
            media_type: media_type.to_owned(),
            digest,
            size,
            annotations: BTreeMap::new(),
        }
    }

    /// Puts `document` in `registry` under its digest and `latest`, and
    /// returns its descriptor.
    fn put_manifest(registry: &Registry, document: &Document, media_type: &str) -> Descriptor {
        let body = serde_json::to_vec(document).unwrap();
        let digest = super::digest(&body);
        for reference in [digest.as_str(), "latest"] {
            let path = format!("/v2/{NAME}/manifests/{reference}");
            registry.put(path, media_type, body.clone());
        }
        Descriptor {
            media_type: media_type.to_owned(),
            digest,
            size: body.len() as u64,
            annotations: BTreeMap::new(),
        }
    }

    /// Puts the closure of `app` for each of `systems`, of which there can be
    /// only one, in `registry` as a manifest, with NARs of `compression` and
    /// signatures by `key`, and returns the reference.
    pub async fn put_closure(
        registry: &Registry,
        systems: &[&str],
        compression: Compression,
        key: &SecretKey,
    ) -> String {
        let store_dir = StoreDir::default();
        let mut manifests = Vec::new();
        for system in systems {
            let objects = objects(system);
            let mut layers = Vec::new();
            for object in &objects {
                let text = narinfo_text(object, compression, Some(key));
                let info = NarInfo::parse(&store_dir, &text).unwrap();
                let references: Vec<_> =
                    info.references().iter().map(ToString::to_string).collect();
                let blob = compress(&object.nar, compression).await;
                let mut layer = put_blob(registry, blob, &nar_type(compression));
                layer.annotations = [
                    (STORE_PATH, store_dir.display(&object.path).to_string()),
                    (NAR_HASH, info.nar_hash().as_base32().to_string()),
                    (NAR_SIZE, info.nar_size().to_string()),
                    (REFERENCES, references.join(" ")),
                    (SIGNATURES, sign(&info, key)),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect();
                layers.push(layer);
            }
            let config = serde_json::to_vec(&Config {
                roots: vec![store_dir.display(&objects[2].path).to_string()],
                store_dir: store_dir.to_str().to_owned(),
                system: Some((*system).to_owned()),
            })
            .unwrap();
            let manifest = Document {
                schema_version: 2,
                media_type: Some(MANIFEST_TYPE.to_owned()),
                artifact_type: Some(ARTIFACT_TYPE.to_owned()),
                config: Some(put_blob(registry, config, CONFIG_TYPE)),
                layers,
            };
            manifests.push(put_manifest(registry, &manifest, MANIFEST_TYPE));
        }
        let Ok([manifest]) = <[_; 1]>::try_from(manifests) else {
            panic!("a manifest holds one system");
        };
        let digest = manifest.digest;
        format!("oci://{}/{NAME}@{digest}", registry.host)
    }

    /// Changes the manifest of `reference` with `edit`, puts it under its new
    /// digest, and returns the reference to it.
    pub fn edit_manifest(
        registry: &Registry,
        reference: &str,
        edit: impl FnOnce(&mut Value),
    ) -> String {
        let digest = reference.rsplit_once('@').unwrap().1;
        let path = format!("/v2/{NAME}/manifests/{digest}");
        let mut manifest: Value = serde_json::from_slice(&registry.get(&path).unwrap()).unwrap();
        edit(&mut manifest);
        let body = manifest.to_string().into_bytes();
        let d = super::digest(&body);
        registry.put(format!("/v2/{NAME}/manifests/{d}"), MANIFEST_TYPE, body);
        format!("oci://{}/{NAME}@{d}", registry.host)
    }

    pub fn digest_of(content: &[u8]) -> String {
        super::digest(content)
    }

    pub fn key() -> SecretKey {
        SecretKey::generate("test-1".into()).unwrap()
    }

    /// Registries that allow `registry`, over plain HTTP.
    pub fn registries(registry: &Registry) -> Registries {
        Registries::new(vec![registry.host.clone()], vec!["127.0.0.1".to_owned()])
    }

    async fn fetch(
        registries: &Registries,
        reference: &str,
        system: &str,
    ) -> anyhow::Result<Artifact> {
        let reference = registries.parse(reference)?;
        registries
            .fetch(&reference, system, &StoreDir::default())
            .await
    }

    async fn fetch_err(registries: &Registries, reference: &str, system: &str) -> String {
        match fetch(registries, reference, system).await {
            Ok(_) => panic!("{reference} was fetched"),
            Err(e) => format!("{e:#}"),
        }
    }

    #[test]
    fn parses_references() {
        let d = format!("sha256:{}", "a".repeat(64));
        let registries = Registries::new(
            vec![
                "ghcr.io/org/envs".into(),
                "registry.local".into(),
                "other:5000".into(),
            ],
            vec!["registry.local".into(), "other:5000".into()],
        );
        let r = registries
            .parse(&format!("oci://ghcr.io/org/envs/web@{d}"))
            .unwrap();
        assert_eq!(r.base.as_str(), "https://ghcr.io/v2/org/envs/web/");
        assert_eq!(r.to_string(), format!("oci://ghcr.io/org/envs/web@{d}"));
        assert!(
            registries
                .parse(&format!("oci://ghcr.io/org/envs@{d}"))
                .is_ok()
        );
        // A tag next to the digest is only for tools that follow tags.
        let tagged = registries
            .parse(&format!("oci://registry.local/env:v1@{d}"))
            .unwrap();
        assert_eq!(tagged.to_string(), format!("oci://registry.local/env@{d}"));

        // Plain HTTP only for the hosts listed, with or without their port.
        for (host, scheme) in [
            ("registry.local", "http"),
            ("REGISTRY.local", "http"),
            ("other:5000", "http"),
        ] {
            let r = registries.parse(&format!("oci://{host}/env@{d}")).unwrap();
            assert_eq!(r.base.scheme(), scheme, "{host}");
        }
        let anywhere = Registries::new(vec!["other".into()], vec!["other:5000".into()]);
        let r = anywhere.parse(&format!("oci://other/env@{d}")).unwrap();
        assert_eq!(r.base.scheme(), "https");

        // Only the registries and prefixes listed.
        for (reference, want) in [
            (
                format!("oci://ghcr.io/org/envsx/web@{d}"),
                "is not in --closure-registries",
            ),
            (
                format!("oci://ghcr.io/org/web@{d}"),
                "is not in --closure-registries",
            ),
            (
                format!("oci://registry.local:5000/env@{d}"),
                "is not in --closure-registries",
            ),
            (
                format!("oci://registry.local.evil/env@{d}"),
                "is not in --closure-registries",
            ),
            (
                "oci://registry.local/env:latest".to_owned(),
                "has no digest",
            ),
        ] {
            let e = format!("{:#}", registries.parse(&reference).unwrap_err());
            assert!(e.contains(want), "{reference}: {e}");
        }
        let off = Registries::new(Vec::new(), Vec::new());
        let e = format!(
            "{:#}",
            off.parse(&format!("oci://ghcr.io/env@{d}")).unwrap_err()
        );
        assert!(e.contains("closure volumes are off"), "{e}");

        for bad in [
            format!("https://registry.local/env@{d}"),
            format!("oci://registry.local/env@sha256:{}", "A".repeat(64)),
            format!("oci://registry.local/env@sha256:{}", "a".repeat(63)),
            format!("oci://registry.local/env@sha512:{}", "a".repeat(128)),
            format!("oci://registry.local@{d}"),
            format!("oci://registry.local/../env@{d}"),
            format!("oci://registry.local/env//x@{d}"),
            format!("oci://registry.local/Env@{d}"),
            format!("oci://user@registry.local/env@{d}"),
            format!("oci:///env@{d}"),
            format!("oci://registry.local/env?x@{d}"),
        ] {
            assert!(registries.parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn names_nar_layers() {
        for compression in [
            Compression::None,
            Compression::Xz,
            Compression::Zstd,
            Compression::Brotli,
        ] {
            assert_eq!(
                super::compression(&nar_type(compression)).unwrap(),
                compression
            );
        }
        assert_eq!(
            nar_type(Compression::Xz),
            "application/vnd.nix-store-csi.nar.v1+xz"
        );
        for bad in [
            "application/vnd.nix-store-csi.nar.v1+none",
            "application/vnd.nix-store-csi.nar.v1+lz4",
            "application/vnd.nix-store-csi.nar.v1xz",
            "application/vnd.oci.image.layer.v1.tar",
        ] {
            assert!(super::compression(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn reads_a_manifest() {
        let key = key();
        let system = "aarch64-linux";
        for compression in [Compression::None, Compression::Zstd, Compression::Xz] {
            let registry = Registry::start().await;
            let reference = put_closure(&registry, &[system], compression, &key).await;
            let tag = registry
                .get(&format!("/v2/{NAME}/manifests/latest"))
                .unwrap();
            assert_eq!(reference.rsplit_once('@').unwrap().1, super::digest(&tag));
            let artifact = fetch(&registries(&registry), &reference, system)
                .await
                .unwrap();
            let objects = objects(system);
            assert_eq!(artifact.roots, [objects[2].path.clone()]);
            assert_eq!(
                artifact.repository.name.as_str(),
                format!("oci://{}/{NAME}", registry.host)
            );
            let mut layers = artifact.layers;
            layers.sort_by(|a, b| a.path().cmp(b.path()));
            let mut objects = objects;
            objects.sort_by(|a, b| a.path.cmp(&b.path));
            for (info, object) in layers.iter_mut().zip(&objects) {
                assert_eq!(info.path(), &object.path);
                assert_eq!(info.compression, compression);
                assert_eq!(info.nar_size(), object.nar.len() as u64);
                assert_eq!(info.nar_hash(), Algorithm::SHA256.digest(&object.nar));
                let blob = registry.get(&format!("/v2/{NAME}/{}", info.url())).unwrap();
                assert_eq!(
                    blob,
                    compress(&object.nar, compression).await,
                    "{compression:?}"
                );
                let mut want = object.references.clone();
                want.sort();
                assert_eq!(info.references().iter().cloned().collect::<Vec<_>>(), want);
                assert!(info.check_signatures(&[key.to_public_key()]).is_ok());
            }
            // The manifest and the config, and no narinfos.
            assert_eq!(registry.requests("GET").len(), 2, "{:?}", registry.log);
        }
    }

    #[tokio::test]
    async fn rejects_wrong_digests() {
        let key = key();
        let system = "aarch64-linux";
        let registry = Registry::start().await;
        let reference = put_closure(&registry, &[system], Compression::None, &key).await;
        let digest = reference.rsplit_once('@').unwrap().1;
        let registries = registries(&registry);

        // The registry serves the manifest under another digest.
        let wrong = format!("sha256:{}", "b".repeat(64));
        let manifest = registry
            .get(&format!("/v2/{NAME}/manifests/{digest}"))
            .unwrap();
        registry.put(
            format!("/v2/{NAME}/manifests/{wrong}"),
            MANIFEST_TYPE,
            manifest.clone(),
        );
        let msg = fetch_err(
            &registries,
            &format!("oci://{}/{NAME}@{wrong}", registry.host),
            system,
        )
        .await;
        assert!(
            msg.contains(&format!(
                "the registry sent content with digest {digest} for {wrong}"
            )),
            "{msg}"
        );
        let missing = format!("sha256:{}", "c".repeat(64));
        let msg = fetch_err(
            &registries,
            &format!("oci://{}/{NAME}@{missing}", registry.host),
            system,
        )
        .await;
        assert!(
            msg.contains(&format!("the registry has no manifest {missing}")),
            "{msg}"
        );

        // A config that doesn't match its digest.
        let manifest: Value = serde_json::from_slice(&manifest).unwrap();
        let config = format!(
            "/v2/{NAME}/blobs/{}",
            manifest["config"]["digest"].as_str().unwrap()
        );
        let text = String::from_utf8(registry.get(&config).unwrap()).unwrap();
        registry.put(
            config,
            "application/json",
            text.replace("aarch64", "x86_64").into_bytes(),
        );
        let msg = fetch_err(&registries, &reference, system).await;
        assert!(msg.contains("sent content with digest"), "{msg}");
    }

    #[tokio::test]
    async fn checks_layers() {
        type Edit = fn(&mut Value);
        let key = key();
        let system = "aarch64-linux";
        let cases: [(Compression, &str, Edit); 9] = [
            // An uncompressed layer's digest is its NarHash.
            (Compression::None, "its NarHash is", |m| {
                let other = m["layers"][1]["annotations"][NAR_HASH].clone();
                m["layers"][0]["annotations"][NAR_HASH] = other;
            }),
            (Compression::None, "its NarSize is", |m| {
                m["layers"][0]["annotations"][NAR_SIZE] = json!("1");
            }),
            (
                Compression::Zstd,
                "a compressed layer needs nix-store-csi.nar-hash",
                |m| {
                    m["layers"][0]["annotations"]
                        .as_object_mut()
                        .unwrap()
                        .remove(NAR_HASH);
                },
            ),
            (
                Compression::Zstd,
                "a compressed layer needs nix-store-csi.nar-size",
                |m| {
                    m["layers"][0]["annotations"]
                        .as_object_mut()
                        .unwrap()
                        .remove(NAR_SIZE);
                },
            ),
            (Compression::None, "no layer holds", |m| {
                m["layers"].as_array_mut().unwrap().remove(0);
            }),
            (Compression::None, "two layers hold", |m| {
                let copy = m["layers"][0].clone();
                m["layers"].as_array_mut().unwrap().push(copy);
            }),
            (Compression::None, "has a line break", |m| {
                m["layers"][0]["annotations"][REFERENCES] = json!("\nURL: ../../x");
            }),
            (
                Compression::None,
                "is not a application/vnd.nix-store-csi.nar.v1 layer",
                |m| {
                    m["layers"][0]["mediaType"] = json!("application/vnd.oci.image.layer.v1.tar");
                },
            ),
            (Compression::None, "is not sha256:<hex>", |m| {
                m["layers"][0]["digest"] = json!("md5:x");
            }),
        ];
        for (compression, want, edit) in cases {
            let registry = Registry::start().await;
            let reference = put_closure(&registry, &[system], compression, &key).await;
            let reference = edit_manifest(&registry, &reference, edit);
            let msg = fetch_err(&registries(&registry), &reference, system).await;
            assert!(msg.contains(want), "{want}: {msg}");
        }

        // The artifact must be for this node's system.
        let registry = Registry::start().await;
        let reference = put_closure(&registry, &["x86_64-linux"], Compression::None, &key).await;
        let msg = fetch_err(&registries(&registry), &reference, system).await;
        assert!(
            msg.contains("is for x86_64-linux, not this node's aarch64-linux"),
            "{msg}"
        );
    }

    /// The plain HTTP registry works only once its host is listed. Over
    /// HTTPS, the request would retry its TLS handshake for a minute, so this
    /// checks the URL it goes to.
    #[tokio::test]
    async fn refuses_plain_http_unless_allowed() {
        let registry = Registry::start().await;
        let reference = put_closure(&registry, &["aarch64-linux"], Compression::None, &key()).await;
        let secure = Registries::new(vec![registry.host.clone()], Vec::new());
        let secure = secure.parse(&reference).unwrap();
        assert_eq!(
            secure.base.as_str(),
            format!("https://{}/v2/{NAME}/", registry.host)
        );
        assert!(
            fetch(&registries(&registry), &reference, "aarch64-linux")
                .await
                .is_ok()
        );

        // Over HTTPS, a failure to connect gets the hint, and other errors
        // don't.
        let refused = || async {
            let e = reqwest::get("http://127.0.0.1:1/").await.unwrap_err();
            anyhow::Error::from(e).context("fetching")
        };
        let msg = format!("{:#}", hint(&secure.base, refused().await));
        assert!(
            msg.starts_with(
                "a registry that serves plain HTTP has to be in --insecure-registries: fetching"
            ),
            "{msg}"
        );
        let insecure = registries(&registry).parse(&reference).unwrap();
        assert!(!format!("{:#}", hint(&insecure.base, refused().await)).contains("insecure"));
        let other = anyhow::anyhow!("the registry sent content with digest x");
        assert!(!format!("{:#}", hint(&secure.base, other)).contains("insecure"));
    }
}
