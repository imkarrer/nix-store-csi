# Closures from OCI registries

A volume can name a closure by the digest of an OCI artifact, in place of
store paths. The plugin then reads the artifact's manifest from a registry and
fetches the NARs from the same registry. It verifies them as it does a
cache's: each path needs a narinfo signature by a key in `trustedPublicKeys`,
and each NAR has to match its `NarHash`. The shared node store, views,
`.roots/` links, collection and `maxClosureSize` work as for `storePaths`, and
the paths that `prefetch` holds don't count towards the limit. `prefetch`
itself lists only store paths.

The feature is off unless `closureRegistries` lists a registry, and
`storePaths` volumes work as before.

## Why

`storePaths` already pins what a pod runs, since a store path doesn't change.
A digest in a registry pins it too, and adds what clusters already have for
registries:

- **One reference for a closure.** The pod spec names the registry and the
  digest, and one manifest request resolves the closure, instead of a narinfo
  lookup for each path.
- **One reference for every system.** An index has a manifest for each
  system, so one `closure` attribute replaces a `storePaths.<system>` for each.
- **Mirrors and air gaps.** `crane copy`, `oras copy`, `skopeo` and registry
  replication copy an artifact as a unit, and the digest stays the same. A
  cluster can then run from its own registry without reaching a binary cache.
- **Admission policy.** ValidatingAdmissionPolicy, Kyverno and Gatekeeper can
  check the registry and the digest in a pod spec, so a pod from the wrong
  place is refused when it's created, not when its volume fails to mount.
- **Tags for automation.** Renovate, Flux image automation and Argo CD Image
  Updater follow a tag and update the digest in git, with no Nix in the loop.
- **Registry tooling.** `crane`, `oras` and `cosign` inspect, copy and sign
  closures as they do images.

## Prior art

[oranc](https://github.com/linyinfeng/oranc),
[aeroflare](https://github.com/itzemoji/aeroflare) and
[nixcache-oci](https://github.com/cmspam/nixcache-oci) serve a binary cache
from a registry, with a tag or an index entry for each store path. Nix reads
them as substituters. None has one digest that pins a whole closure, which is
what a pod spec, a mirror tool or a policy needs. The format below is that
digest, with each path's NAR and narinfo fields in its layers.

## Reference

```
oci://<host>[:<port>]/<name>[:<tag>]@sha256:<digest>
```

The reference has to have a digest. A tag before it, like `:v1@sha256:...`,
tells tools like Renovate which tag to follow, and the plugin ignores it.

## Format

The digest names either an image index or a manifest.

An **index** (`application/vnd.oci.image.index.v1+json`) has a manifest for
each system. The plugin takes the entry whose `platform` is `os: linux` and
the node's `architecture`, `amd64` for `x86_64-linux` and `arm64` for
`aarch64-linux`. An entry may set `artifactType`, which then has to be
`application/vnd.nix-store-csi.closure.v1`.

A **manifest** (`application/vnd.oci.image.manifest.v1+json`) has
`artifactType: application/vnd.nix-store-csi.closure.v1`, a config, and a
layer for each store path in the closure.

The **config** (`application/vnd.nix-store-csi.closure.config.v1+json`) names
the roots, which get the `.roots/` links:

```json
{
  "roots": ["/nix/store/846h582z2d4mifn4km7axlqllcyn6zdg-hello-2.12.3"],
  "storeDir": "/nix/store",
  "system": "aarch64-linux"
}
```

Each **layer** is the path's NAR, as a cache stores it. Its media type is
`application/vnd.nix-store-csi.nar.v1` for a NAR without compression, and
adds the compression's narinfo name otherwise: `+xz`, `+zstd`, `+bzip2`,
`+gzip` or `+br`, so a producer can copy a cache's NAR files byte for byte.
The annotations carry the rest of the narinfo:

| Annotation | Value |
|---|---|
| `nix-store-csi.store-path` | the store path |
| `nix-store-csi.nar-hash` | `NarHash`, as `sha256:<nix32>` |
| `nix-store-csi.nar-size` | `NarSize` |
| `nix-store-csi.references` | `References`, space-separated base names |
| `nix-store-csi.signatures` | the `Sig` lines, space-separated |

For an uncompressed layer, the digest is the SHA-256 of the NAR, so it has to
be the `NarHash`, and the size has to be the `NarSize`. A compressed layer
needs both annotations, since its digest and size are those of the
compressed file.

```json
{
  "mediaType": "application/vnd.nix-store-csi.nar.v1+zstd",
  "digest": "sha256:cf918a2b75bdbf8098a45fca3f0bd379609f71e660ec71717512bde0873b1742",
  "size": 77097,
  "annotations": {
    "nix-store-csi.nar-hash": "sha256:1akpk014pgd44rxrdcrp92jhd1li209xp1i4nhxvsfndm1d0hnz1",
    "nix-store-csi.nar-size": "294440",
    "nix-store-csi.references": "846h582z2d4mifn4km7axlqllcyn6zdg-hello-2.12.3 f8q4w2hbjvwy7qqwpnvbf5f4qwyww6cp-glibc-2.42-67",
    "nix-store-csi.signatures": "cache.nixos.org-1:MRJI9rrPDtmQyTQoJ8hQs2nO+9zTwJ3BbwUTmoN17KhwhbZBkYvkKMJkA05U3H/l5TL96tL0yFcV+ixlaODZCQ==",
    "nix-store-csi.store-path": "/nix/store/846h582z2d4mifn4km7axlqllcyn6zdg-hello-2.12.3"
  }
}
```

## Security

The registry is a transport the plugin doesn't trust, like a cache:

1. The manifest or index has the digest that the volume pins, a manifest
   from an index has the digest in its entry, and the config has the digest
   in the manifest.
2. The layers hold the roots and every reference, each path once.
3. A key in `trustedPublicKeys` signed each path. The signature covers the
   store path, `NarHash`, `NarSize` and references, so the same signatures
   serve a cache and an artifact, and nobody who can push to the registry can
   change a path's content.
4. Each NAR has its `NarHash` and `NarSize` once unpacked, as from a cache.
   For an uncompressed layer that is also its digest.

A pod picks the registry, while the node store serves each path to every pod
on the node. Without step 3, a pod could put its own content under a store
path that another pod uses. So with `--no-require-sigs` the plugin refuses
every `closure` volume.

`closureRegistries` limits the registries and repositories that pods may
name. Plain HTTP needs the host in `insecureRegistries` too. If a NAR from a
registry fails, the plugin looks for the path in `substituters`, and takes it
only from a narinfo with the same `NarHash`.

## Logins

The plugin reads repositories that need no login. It answers a
`WWW-Authenticate: Bearer` challenge with an anonymous token from the
registry's token service, as public repositories on ghcr.io need.
