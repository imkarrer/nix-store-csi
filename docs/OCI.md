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
A digest in a registry pins it too, and the tools that read image references
can read it:

- **One reference for a closure.** The pod spec names the registry and the
  digest. The plugin resolves the closure with one manifest request, instead
  of a narinfo request for each path.
- **Copies keep the digest.** `crane copy` copies the artifact to another
  registry with the same digest, so a cluster can run from its own registry,
  with `substituters` empty and no binary cache in reach.
- **Admission policy.** A ValidatingAdmissionPolicy can check the registry and
  the digest in the pod spec, so the API server refuses a pod from the wrong
  place when it's created. A `storePaths` pod with a path that no cache has is
  admitted, and waits in `ContainerCreating`.
- **Tags.** Renovate's docker datasource follows a tag in the registry and
  updates the digest in the pod spec, without evaluating Nix.
- **Registry tools.** `crane manifest`, `oras discover` and `cosign sign` and
  `verify` work on the artifact's digest.

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

The digest names a **manifest** (`application/vnd.oci.image.manifest.v1+json`) has
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

1. The manifest has the digest that the volume pins, and the config has the
   digest in the manifest.
2. The layers hold the roots and every reference, each path once.
3. A key in `trustedPublicKeys` signed each path. The signature covers the
   store path, `NarHash`, `NarSize` and references, so the same signatures
   serve a cache and an artifact. Someone who can push to the registry can't
   change what a store path holds without a trusted key.
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

The plugin reads repositories that need no login.
