//! Writes the closures of store paths from the caches into a registry, as the
//! artifact that a `closure` volume reads.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Context, bail, ensure};
use futures::{StreamExt, TryStreamExt};
use harmonia_store_path::StorePath;
use harmonia_utils_hash::{Algorithm, HashFormat};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::{Method, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tracing::info;

use super::{
    ARTIFACT_TYPE, CONFIG_TYPE, Config, Descriptor, Document, INDEX_TYPE, MANIFEST_TYPE, NAR_HASH,
    NAR_SIZE, Platform, REFERENCES, Registries, Repository, SIGNATURES, STORE_PATH, nar_type,
    oci_arch, parts,
};
use crate::closure::{Caches, Entry};

/// NARs that go up at once.
const UPLOADS: usize = 4;

/// Pushes the closures of `roots`, each a Nix system and a store path, to
/// `target`, `oci://<host>/<name>` with an optional `:<tag>`. One system gets a
/// manifest, and several get an index of one manifest for each. Each NAR goes
/// up as the cache stores it, so a compressed NAR stays compressed. Returns the
/// reference that pins what it pushed. `tmp` holds each NAR while it goes up.
pub async fn push(
    caches: &Caches,
    registries: &Registries,
    target: &str,
    roots: &[(String, StorePath)],
    tmp: &Path,
) -> anyhow::Result<String> {
    ensure!(
        caches.checks_signatures(),
        "push needs trusted public keys, since closure volumes accept only signed paths"
    );
    let parts = parts(target.trim())?;
    ensure!(
        parts.digest.is_none(),
        "push to oci://<host>/<name> or oci://<host>/<name>:<tag>, not to a digest"
    );
    let base = registries.base(parts.host, parts.name)?;
    let repository = registries.repository(parts.host, parts.name, &base, "pull,push")?;
    let mut systems: BTreeMap<&str, Vec<StorePath>> = BTreeMap::new();
    for (system, root) in roots {
        oci_arch(system)?;
        systems.entry(system).or_default().push(root.clone());
    }
    ensure!(!systems.is_empty(), "nothing to push");

    let mut layers = HashMap::new();
    let mut manifests = Vec::new();
    for (system, roots) in &systems {
        let closure = (caches.resolve(roots, &AtomicUsize::default()).await)
            .with_context(|| format!("resolving {system}"))?;
        let missing: Vec<_> = (closure.values())
            .filter(|e| !layers.contains_key(e.info.path()))
            .cloned()
            .collect();
        let pushed: Vec<_> = futures::stream::iter(missing)
            .map(|entry| push_nar(caches, &repository, entry, tmp))
            .buffer_unordered(UPLOADS)
            .try_collect()
            .await?;
        layers.extend(pushed);
        let store_dir = caches.store_dir();
        let config = serde_json::to_vec(&Config {
            roots: roots
                .iter()
                .map(|r| store_dir.display(r).to_string())
                .collect(),
            store_dir: store_dir.to_str().to_owned(),
            system: Some((*system).to_owned()),
        })?;
        let config = push_blob(&repository, config, CONFIG_TYPE).await?;
        let manifest = Document {
            schema_version: 2,
            media_type: Some(MANIFEST_TYPE.to_owned()),
            artifact_type: Some(ARTIFACT_TYPE.to_owned()),
            config: Some(config),
            layers: (closure.keys()).map(|path| layers[path].clone()).collect(),
            manifests: Vec::new(),
        };
        let tag = parts.tag.filter(|_| systems.len() == 1);
        let mut descriptor = put_manifest(&repository, &manifest, MANIFEST_TYPE, tag).await?;
        descriptor.artifact_type = Some(ARTIFACT_TYPE.to_owned());
        descriptor.platform = Some(Platform {
            architecture: oci_arch(system)?.to_owned(),
            os: "linux".to_owned(),
        });
        manifests.push(descriptor);
    }
    let digest = match <[_; 1]>::try_from(manifests) {
        Ok([manifest]) => manifest.digest,
        Err(manifests) => {
            let index = Document {
                schema_version: 2,
                media_type: Some(INDEX_TYPE.to_owned()),
                artifact_type: Some(ARTIFACT_TYPE.to_owned()),
                manifests,
                ..Document::default()
            };
            (put_manifest(&repository, &index, INDEX_TYPE, parts.tag).await?).digest
        }
    };
    Ok(format!("oci://{}/{}@{digest}", parts.host, parts.name))
}

/// Pushes the NAR of `entry` from its cache, unless the registry has it, and
/// returns its layer. It checks the NAR's hash and size first, so that the
/// registry gets only NARs that the plugin would accept.
async fn push_nar(
    caches: &Caches,
    repository: &Repository,
    entry: Arc<Entry>,
    tmp: &Path,
) -> anyhow::Result<(StorePath, Descriptor)> {
    let info = &entry.info;
    let store_dir = caches.store_dir();
    let path = store_dir.display(info.path()).to_string();
    let cache = entry
        .cache
        .with_context(|| format!("no cache has {path}"))?;
    let file = tmp.join(info.path().to_string());
    let fetched = async {
        // The file hash goes in the layer's digest, and the NAR hash is
        // checked after decompressing.
        let mut body = caches.stream(cache, info.url(), info.compression).await?;
        let mut out = tokio::fs::File::create(&file).await?;
        let mut hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        let mut buf = vec![0; 256 * 1024];
        let mut size = 0;
        loop {
            let n = body.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n]).await?;
            size += n as u64;
        }
        out.flush().await?;
        let mut nar = info
            .compression
            .decoder(BufReader::new(tokio::fs::File::open(&file).await?));
        let mut nar_hasher = harmonia_utils_hash::Context::new(Algorithm::SHA256);
        let mut nar_size = 0;
        loop {
            let n = nar.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            nar_hasher.update(&buf[..n]);
            nar_size += n as u64;
        }
        ensure!(
            nar_size == info.nar_size() && nar_hasher.finish() == info.nar_hash(),
            "the NAR does not match its narinfo's NarHash and NarSize"
        );
        anyhow::Ok((format!("{:x}", hasher.finish()), size))
    };
    let result = async {
        let (digest, size) = fetched
            .await
            .with_context(|| format!("fetching {} from {}", info.url(), caches.name(cache)))?;
        let references: Vec<_> = info.references().iter().map(ToString::to_string).collect();
        let annotations = [
            (STORE_PATH, path.clone()),
            (NAR_HASH, info.nar_hash().as_base32().to_string()),
            (NAR_SIZE, info.nar_size().to_string()),
            (REFERENCES, references.join(" ")),
            (SIGNATURES, info.trusted_signatures().join(" ")),
        ];
        let descriptor = Descriptor {
            media_type: nar_type(info.compression),
            digest,
            size,
            annotations: (annotations.into_iter())
                .map(|(k, v)| (k.to_owned(), v))
                .collect(),
            artifact_type: None,
            platform: None,
        };
        if has_blob(repository, &descriptor.digest).await? {
            info!(store_path = %path, repository = %repository.name, "the registry has the NAR already");
        } else {
            upload(repository, &descriptor.digest, size, || async {
                let file = tokio::fs::File::open(&file).await?;
                Ok(reqwest::Body::wrap_stream(
                    tokio_util::io::ReaderStream::new(file),
                ))
            })
            .await
            .with_context(|| format!("pushing {path}"))?;
            info!(store_path = %path, repository = %repository.name, "pushed the NAR");
        }
        Ok((info.path().clone(), descriptor))
    }
    .await;
    let _ = tokio::fs::remove_file(&file).await;
    result
}

/// Pushes a small blob, like a config, and returns its descriptor.
async fn push_blob(
    repository: &Repository,
    content: Vec<u8>,
    media_type: &str,
) -> anyhow::Result<Descriptor> {
    let digest = super::digest(&content);
    let size = content.len() as u64;
    if !has_blob(repository, &digest).await? {
        let content = tokio_util::bytes::Bytes::from(content);
        upload(repository, &digest, size, || {
            let content = content.clone();
            async move { Ok(reqwest::Body::from(content)) }
        })
        .await?;
    }
    Ok(Descriptor {
        media_type: media_type.to_owned(),
        digest,
        size,
        annotations: BTreeMap::new(),
        artifact_type: None,
        platform: None,
    })
}

/// Whether the repository has the blob `digest`.
async fn has_blob(repository: &Repository, digest: &str) -> anyhow::Result<bool> {
    let path = format!("blobs/{digest}");
    let resp = (repository.authorized(|| async {
        let (_, request) = repository.transport.registry_request(Method::HEAD, &path)?;
        expect(
            request.send().await?,
            &[StatusCode::OK, StatusCode::NOT_FOUND],
        )
        .await
    }))
    .await?;
    Ok(resp.status() == StatusCode::OK)
}

/// Uploads a blob in one request, as the distribution spec's monolithic
/// upload: a POST for an upload URL, then a PUT of the whole blob to it.
async fn upload<F, Fut>(
    repository: &Repository,
    digest: &str,
    size: u64,
    body: F,
) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<reqwest::Body>>,
{
    repository
        .authorized(|| async {
            let (_, request) =
                (repository.transport).registry_request(Method::POST, "blobs/uploads/")?;
            let resp = expect(request.send().await?, &[StatusCode::ACCEPTED]).await?;
            let location = (resp.headers().get(LOCATION))
                .and_then(|l| l.to_str().ok())
                .context("the registry's upload has no Location")?;
            let separator = if location.contains('?') { '&' } else { '?' };
            let location = format!("{location}{separator}digest={digest}");
            let (_, request) = repository
                .transport
                .registry_request(Method::PUT, &location)?;
            let request = (request.header(CONTENT_TYPE, "application/octet-stream"))
                .header(CONTENT_LENGTH, size)
                .body(body().await?);
            expect(request.send().await?, &[StatusCode::CREATED]).await?;
            Ok(())
        })
        .await
}

/// Puts `document` under its digest, and under `tag` if given, and returns
/// its descriptor.
async fn put_manifest(
    repository: &Repository,
    document: &Document,
    media_type: &str,
    tag: Option<&str>,
) -> anyhow::Result<Descriptor> {
    let body = serde_json::to_vec(document)?;
    let digest = super::digest(&body);
    for reference in [Some(digest.as_str()), tag].into_iter().flatten() {
        let path = format!("manifests/{reference}");
        (repository.authorized(|| async {
            let (_, request) = repository.transport.registry_request(Method::PUT, &path)?;
            let request = request.header(CONTENT_TYPE, media_type).body(body.clone());
            expect(request.send().await?, &[StatusCode::CREATED]).await
        }))
        .await
        .with_context(|| format!("putting manifest {reference}"))?;
    }
    Ok(Descriptor {
        media_type: media_type.to_owned(),
        digest,
        size: body.len() as u64,
        annotations: BTreeMap::new(),
        artifact_type: None,
        platform: None,
    })
}

/// Fails unless the response's status is one of `ok`. A 401 fails with the
/// challenge, which [`Repository::authorized`] answers.
async fn expect(resp: reqwest::Response, ok: &[StatusCode]) -> anyhow::Result<reqwest::Response> {
    if ok.contains(&resp.status()) {
        return Ok(resp);
    }
    match crate::transport::check_response(resp).await {
        Ok(resp) => bail!("the registry answered {}", resp.status()),
        Err(e) => Err(e),
    }
}
