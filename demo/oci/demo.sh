#!/usr/bin/env bash
# What `closure` volumes add over `storePaths`, step by step, on a kind cluster.
#
#   nix build .#csi-image && docker load < result   # nix-store-csi:<version>
#   docker tag nix-store-csi:<version> nix-store-csi:oci-closure
#   nix shell nixpkgs#{kind,kubectl,kubernetes-helm,crane,oras,cosign,renovate,openssl,apacheHttpd,jq,git} \
#     -c demo/oci/demo.sh
#
# It creates the kind cluster `nsc-demo` unless it exists, runs three
# registries in it with TLS from a throwaway CA and htpasswd logins, and
# installs the plugin from this checkout with no substituters at all.
# `CLEAN=1` deletes the cluster at the end.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
cluster=${CLUSTER:-nsc-demo}
image=${IMAGE:-nix-store-csi:oci-closure}
k() { kubectl --context "kind-$cluster" "$@"; }
work=$(mktemp -d)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  pkill -f "port-forward svc/registry-" 2>/dev/null || true
  rm -rf "$work"
  if [[ ${CLEAN:-} == 1 ]]; then kind delete cluster --name "$cluster"; fi
}
trap cleanup EXIT

# Store paths in cache.nixos.org, from nixpkgs at the time of writing:
#   nix eval --raw nixpkgs#legacyPackages.<system>.{hello,jq.bin,figlet}
hello_x86=/nix/store/lcxc2z1h2g8wq8dxfpizy1di85kyxnqx-hello-2.12.3
jq_x86=/nix/store/25pcd19xk5ff8ybjslsips4g7yv06fxl-jq-1.8.2-bin
figlet_x86=/nix/store/mxf46pw5223bwaini3wdjk3b3dn8qhgf-figlet-2.2.5
hello_arm=/nix/store/846h582z2d4mifn4km7axlqllcyn6zdg-hello-2.12.3
jq_arm=/nix/store/idak61zxiv384i26xpk1rgq17bwfpk74-jq-1.8.2-bin
figlet_arm=/nix/store/ly0ayqb9rjx9dblarrjq9ss7sa73ayfw-figlet-2.2.5
nixos_key=cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=

bold=$'\e[1m' green=$'\e[32m' yellow=$'\e[33m' red=$'\e[31m' reset=$'\e[0m'
step() { printf '\n%s== %s ==%s\n' "$bold" "$*" "$reset"; }
# Shows each command on the script's own output, also when its output goes
# into a pipe.
exec 3>&1
run() { printf '%s$ %s%s\n' "$yellow" "$*" "$reset" >&3; "$@"; }
unlocked() { printf '%sUnlocked:%s %s\n' "$green$bold" "$reset" "$*"; }
before() { printf '%sWith storePaths:%s %s\n' "$red$bold" "$reset" "$*"; }
rows=()
row() { rows+=("$1|$2|$3"); }

a=registry-a.registries.svc b=registry-b.registries.svc c=registry-c.registries.svc
password=demo-password

step "0. Set up: a cluster, three private registries, and the plugin without substituters"
if ! kind get clusters | grep -qx "$cluster"; then kind create cluster --name "$cluster"; fi
kind load docker-image --name "$cluster" "$image" >/dev/null
binary=$(docker inspect "$image" --format '{{index .Config.Entrypoint 0}}')

# A throwaway CA, and one certificate for every registry, as the pods and
# the port forwards on this machine name them.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=nsc-demo-ca \
  -keyout "$work/ca.key" -out "$work/ca.crt" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=registries -keyout "$work/tls.key" \
  -out "$work/tls.csr" 2>/dev/null
printf 'subjectAltName=DNS:%s,DNS:%s,DNS:%s,DNS:localhost,IP:127.0.0.1\n' "$a" "$b" "$c" >"$work/san"
openssl x509 -req -in "$work/tls.csr" -CA "$work/ca.crt" -CAkey "$work/ca.key" \
  -CAcreateserial -days 2 -extfile "$work/san" -out "$work/tls.crt" 2>/dev/null
htpasswd -Bbn demo "$password" >"$work/htpasswd" 2>/dev/null

k create namespace registries --dry-run=client -o yaml | k apply -f - >/dev/null
k -n registries create secret tls registry-tls --cert "$work/tls.crt" --key "$work/tls.key" \
  --dry-run=client -o yaml | k apply -f - >/dev/null
k -n registries create secret generic registry-htpasswd --from-file=htpasswd="$work/htpasswd" \
  --dry-run=client -o yaml | k apply -f - >/dev/null
for r in a b c; do sed "s/NAME/$r/g" "$here/registries.yaml" | k apply -f - >/dev/null; done
# A run before this one may have left registries with an older certificate.
k -n registries rollout restart deploy >/dev/null
for r in a b c; do k -n registries rollout status deploy/registry-$r --timeout=180s >/dev/null; done

# Logins as a dockerconfigjson Secret, like an image pull Secret. The one for
# registry C has the wrong password.
auth() { printf '%s' "demo:$1" | base64; }
cat >"$work/config.json" <<EOF
{"auths": {
  "$a": {"auth": "$(auth "$password")"},
  "$b": {"auth": "$(auth "$password")"},
  "$c": {"auth": "$(auth wrong-password)"},
  "localhost:5443": {"auth": "$(auth "$password")"},
  "localhost:5444": {"auth": "$(auth "$password")"},
  "localhost:5445": {"auth": "$(auth "$password")"}
}}
EOF
for ns in kube-system default; do
  k -n $ns create secret generic nsc-registries --type=kubernetes.io/dockerconfigjson \
    --from-file=.dockerconfigjson="$work/config.json" --dry-run=client -o yaml | k apply -f - >/dev/null
  k -n $ns create configmap nsc-demo-ca --from-file=ca.crt="$work/ca.crt" \
    --dry-run=client -o yaml | k apply -f - >/dev/null
done

cat >"$work/values.yaml" <<EOF
image:
  repository: docker.io/library/${image%%:*}
  tag: ${image##*:}
  pullPolicy: IfNotPresent
# No binary cache at all: the nodes get every store path from the registries.
substituters: []
trustedPublicKeys: [$nixos_key]
closureRegistries: [$a, $b, $c]
registryCredentialsSecret: nsc-registries
extraCACertificates: nsc-demo-ca
EOF
run helm --kube-context "kind-$cluster" upgrade --install nix-store-csi "${CHART:-$repo/deploy/helm/nix-store-csi}" \
  -n kube-system -f "$work/values.yaml" --wait >/dev/null
k -n kube-system rollout restart ds/nix-store-csi-node >/dev/null
k -n kube-system rollout status ds/nix-store-csi-node --timeout=180s >/dev/null
k delete namespace demo --ignore-not-found >/dev/null
k delete validatingadmissionpolicybinding closures-from-registry-b --ignore-not-found >/dev/null
echo "plugin values:"; sed 's/^/  /' "$work/values.yaml"

# Port forwards, so the tools on this machine reach the registries.
port=5443
for r in a b c; do
  # Restarted if it stops, like when a registry's pod is replaced.
  (while true; do k -n registries port-forward svc/registry-$r $port:443 >/dev/null 2>&1; sleep 1; done) &
  pids+=($!)
  port=$((port + 1))
done
export DOCKER_CONFIG=$work SSL_CERT_FILE=$work/ca.crt NODE_EXTRA_CA_CERTS=$work/ca.crt
# Go ignores SSL_CERT_FILE on macOS, so there the Go tools skip verifying the
# port forwards' certificate. The plugin and `push` verify it in the cluster.
crane=(crane) insecure=() cosign_insecure=()
if [[ $(uname) == Darwin ]]; then
  crane+=(--insecure) insecure=(--plain-http=false --insecure) cosign_insecure=(--allow-insecure-registry)
fi
for port in 5443 5444 5445; do
  until curl -fsS --cacert "$work/ca.crt" -u "demo:$password" https://localhost:$port/v2/ >/dev/null 2>&1; do sleep 1; done
done
k create namespace demo --dry-run=client -o yaml | k apply -f - >/dev/null

# Runs `nix-store-csi push` in the cluster and prints the reference it pushed.
push() {
  local name=$1 target=$2
  shift 2
  k -n default delete job "$name" --ignore-not-found >/dev/null
  k -n default apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: push
          image: $image
          command: [$binary, push, $target$(printf ', %s' "$@")]
          env:
            - name: NIX_STORE_CSI_SUBSTITUTERS
              value: https://cache.nixos.org
            - name: NIX_STORE_CSI_REGISTRY_CREDENTIALS
              value: /etc/registries/.dockerconfigjson
            - name: SSL_CERT_FILE
              value: /etc/ssl/certs/ca-bundle.crt
            - name: SSL_CERT_DIR
              value: /etc/demo-ca
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ALL]
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            - name: registries
              mountPath: /etc/registries
            - name: ca
              mountPath: /etc/demo-ca
      volumes:
        - name: tmp
          emptyDir: {}
        - name: registries
          secret:
            secretName: nsc-registries
        - name: ca
          configMap:
            name: nsc-demo-ca
EOF
  until [[ -n $(k -n default get job "$name" -o jsonpath='{.status.completionTime}{.status.failed}') ]]; do
    sleep 2
  done
  if [[ -z $(k -n default get job "$name" -o jsonpath='{.status.completionTime}') ]]; then
    k -n default logs "job/$name" >&2
    return 1
  fi
  local log
  log=$(k -n default logs "job/$name")
  printf '  push: uploaded %s NARs, %s were in the registry already\n' \
    "$(grep -c ' pushed /nix/store/' <<<"$log")" "$(grep -c ' already$' <<<"$log")" >&2
  k -n default logs "job/$name" | grep '^oci://' 
}

# A pod that runs hello and jq from a volume with `closure`, or with
# another attribute, like `storePaths`, as the third argument.
pod() {
  local name=$1 closure=$2 attribute=${3:-closure}
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: demo
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: tools
      image: public.ecr.aws/docker/library/busybox:1.37
      command:
        - sh
        - -c
        - >-
          PATH=/nix/store/.roots/hello-2.12.3/bin:/nix/store/.roots/jq-1.8.2-bin/bin:\$PATH;
          hello -g "hello from \$(uname -m)";
          echo '{"tool":"jq"}' | jq -c '.version = "1.8.2"'
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: [ALL]
      volumeMounts:
        - name: nix
          mountPath: /nix/store
  volumes:
    - name: nix
      csi:
        driver: nix-store-csi
        readOnly: true
        volumeAttributes:
          $attribute: $closure
EOF
}

# Applies a pod and prints its log once it ran, or else its last mount error.
try_pod() {
  local name=$1
  k -n demo delete pod "$name" --ignore-not-found --wait >/dev/null
  pod "$@" | k apply -f -
  for _ in $(seq 1 60); do
    case $(k -n demo get pod "$name" -o jsonpath='{.status.phase}') in
      Succeeded) k -n demo logs "$name" | sed 's/^/  log: /'; return ;;
      Failed) k -n demo logs "$name" | sed 's/^/  log: /'; return 1 ;;
    esac
    local event
    event=$(k -n demo get events --field-selector "involvedObject.name=$name,reason=FailedMount" \
      -o jsonpath='{.items[-1:].message}' 2>/dev/null || true)
    if [[ -n $event ]]; then
      echo "  event: ${event#*desc = }"
      return
    fi
    sleep 2
  done
  echo "  still waiting" >&2
  return 1
}

step "1. Push the closure of hello and jq for two systems into registry A"
ref1=$(push push-1 "oci://$a/envs/tools:latest" \
  x86_64-linux=$hello_x86 x86_64-linux=$jq_x86 aarch64-linux=$hello_arm aarch64-linux=$jq_arm)
echo "pushed: $ref1"
digest1=${ref1##*@}
run "${crane[@]}" manifest "localhost:5443/envs/tools@$digest1" | jq '{mediaType, artifactType, manifests: [.manifests[] | {platform, digest}]}'
arm1=$("${crane[@]}" manifest "localhost:5443/envs/tools@$digest1" | jq -r '.manifests[] | select(.platform.architecture == "arm64") | .digest')
run "${crane[@]}" manifest "localhost:5443/envs/tools@$arm1" |
  jq '{config: .config.mediaType, layers: [.layers[] | {mediaType, path: .annotations["nix-store-csi.store-path"]}]}'

step "2. One digest for every architecture"
echo "The pod pins one reference, and each node takes the manifest for its own architecture:"
echo "  closure: oci://$a/envs/tools:latest@$digest1"
echo "With storePaths, the same pod lists a store path per system:"
echo "  storePaths.x86_64-linux: $hello_x86 $jq_x86"
echo "  storePaths.aarch64-linux: $hello_arm $jq_arm"
try_pod tools-a "oci://$a/envs/tools:latest@$digest1"
echo "The plugin's log for the volume, with the artifact it read the closure from:"
k -n kube-system logs ds/nix-store-csi-node -c nix-store-csi |
  grep -E 'fetched the closure|published' | tail -2 | sed 's/^[^ ]* *INFO //' | cut -c1-260 | sed 's/^/  /'
unlocked "one immutable reference covers aarch64-linux and x86_64-linux; adding a system changes the artifact, not the pod spec."
before "a storePaths.<system> attribute per system, each a different store path to keep in sync."
row "Architectures" "one storePaths.<system> list per system" "one index digest for every system"

step "3. Promotion and an air gap: copy the artifact to registry B"
run "${crane[@]}" copy "localhost:5443/envs/tools@$digest1" "localhost:5444/envs/tools:latest" 2>&1 | tail -1
run "${crane[@]}" digest "localhost:5444/envs/tools:latest"
echo "Only the host in the pod spec changes; the digest stays:"
echo "  oci://$a/envs/tools@$digest1"
echo "  oci://$b/envs/tools@$digest1"
try_pod tools-b "oci://$b/envs/tools:latest@$digest1"
echo "The plugin has no binary cache configured:"
k -n kube-system get ds nix-store-csi-node -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="NIX_STORE_CSI_SUBSTITUTERS")]}'; echo
unlocked "any registry mirror tool (crane copy, oras copy, skopeo, Harbor replication) moves a whole closure into an air-gapped registry as one unit, digest unchanged."
before "every cluster needs a binary cache that holds those paths and is signed by a key it trusts (nix copy per path, cache and key per environment)."
row "Promotion, air gap" "nix copy into a reachable cache; nodes need that cache" "crane copy A to B; same digest; no cache on the nodes"

step "4. Refusals, each as the pod's event"
echo "A registry that closureRegistries doesn't list:"
try_pod not-allowed "oci://ghcr.io/example/tools@$digest1"
echo "A tag without a digest:"
try_pod tag-only "oci://$a/envs/tools:latest"
echo "Registry C, for which the plugin's Secret has the wrong password:"
run "${crane[@]}" copy "localhost:5443/envs/tools@$digest1" "localhost:5445/envs/tools:latest" 2>/dev/null
try_pod wrong-login "oci://$c/envs/tools@$digest1"
echo "Content changed in the registry by someone who may push, here jq's NarHash:"
"${crane[@]}" manifest "localhost:5443/envs/tools@$arm1" |
  jq -c '(.layers[] | select(.annotations["nix-store-csi.store-path"] | endswith("-jq-1.8.2-bin")) | .annotations["nix-store-csi.nar-hash"]) |= sub("sha256:(?<c>.)"; "sha256:" + (if .c == "0" then "1" else "0" end))' \
    >"$work/tampered.json"
tampered=$(curl -fsS --cacert "$work/ca.crt" -u "demo:$password" -X PUT \
  -H 'Content-Type: application/vnd.oci.image.manifest.v1+json' --data-binary @"$work/tampered.json" \
  -D - -o /dev/null "https://localhost:5443/v2/envs/tools/manifests/tampered" |
  tr -d '\r' | awk -F': ' 'tolower($1) == "docker-content-digest" {print $2}')
try_pod tampered "oci://$a/envs/tools@$tampered"
unlocked "the registry is an untrusted transport: the digest pins the artifact, and each path still needs a trusted narinfo signature."

step "5. Policy at admission: only registry B, only by digest"
k apply -f "$here/policy.yaml" >/dev/null
sleep 5
echo "A pod from registry A, which the policy refuses when it's applied:"
printf '%s$ kubectl apply -f pod-from-a.yaml%s\n' "$yellow" "$reset"
pod policy-a "oci://$a/envs/tools@$digest1" | k apply -f - 2>&1 | sed 's/^/  /' || true
echo "The same pod from registry B:"
try_pod policy-b "oci://$b/envs/tools:latest@$digest1"
echo "A storePaths pod for a path that no cache has. The API server admits it, and it waits:"
k -n demo delete pod store-paths --ignore-not-found --wait >/dev/null
pod store-paths /nix/store/00000000000000000000000000000000-hello-2.12.3 storePaths | k apply -f -
sleep 10
k -n demo get pod store-paths --no-headers | sed 's/^/  /'
k -n demo get events --field-selector involvedObject.name=store-paths,reason=FailedMount \
  -o jsonpath='{.items[-1:].message}' | sed 's/.*desc = /  event: /'; echo
unlocked "admission policies, Kyverno and Gatekeeper read the registry and digest in the pod spec; Argo CD shows the refusal as a sync error at once."
before "the API server admits any store path; a wrong one shows up as a pod stuck in ContainerCreating (Argo CD: Progressing until it times out)."
row "Policy" "admitted; fails later as a mount error" "refused at admission by registry and digest"

step "6. Automated bumps: Renovate follows the tag and updates the digest"
git -C "$work" init -q repo
pod tools "oci://$a/envs/tools:latest@$digest1" >"$work/repo/pod.yaml"
# Renovate runs on this machine, so it reaches registry A through the port
# forward, and its regex manager reads the tag and digest from the pod spec.
cat >"$work/repo/renovate.json" <<EOF
{
  "customManagers": [{
    "customType": "regex",
    "managerFilePatterns": ["/pod\\\\.yaml$/"],
    "matchStrings": ["closure: oci://(?<registry>[^/]+)/(?<depName>[^:@\\\\s]+):(?<currentValue>[^@\\\\s]+)@(?<currentDigest>sha256:[a-f0-9]{64})"],
    "datasourceTemplate": "docker",
    "registryUrlTemplate": "https://localhost:5443"
  }],
  "hostRules": [{"matchHost": "localhost", "username": "demo", "password": "$password"}]
}
EOF
git -C "$work/repo" add . && git -C "$work/repo" -c user.name=demo -c user.email=demo@example.com commit -qm init
echo "CI pushes a new version of the closure, with figlet added, to the same tag:"
ref2=$(push push-2 "oci://$a/envs/tools:latest" \
  x86_64-linux=$hello_x86 x86_64-linux=$jq_x86 x86_64-linux=$figlet_x86 \
  aarch64-linux=$hello_arm aarch64-linux=$jq_arm aarch64-linux=$figlet_arm)
echo "pushed: $ref2"
printf '%s$ renovate --platform=local --dry-run=lookup%s\n' "$yellow" "$reset"
(cd "$work/repo" && LOG_LEVEL=debug LOG_FORMAT=json RENOVATE_CONFIG_FILE=renovate.json \
  renovate --platform=local --dry-run=lookup --onboarding=false --require-config=optional 2>&1) >"$work/renovate.log" || true
jq -r 'select(.msg == "packageFiles with updates") | .config.regex[]?.deps[]? |
  "  \(.depName):\(.currentValue) \(.currentDigest[0:19])... -> \(.updates[]? | .newDigest[0:19])... (\(.updates[]?.updateType))"' \
  "$work/renovate.log" | sort -u
unlocked "Renovate, Flux image automation and Argo CD Image Updater bump closures from the registry's tags, with no Nix in the loop."
before "CI evaluates Nix and fills the paths into manifests (deploy/manifests.nix and envsubst) to bump anything."
row "Bumps" "custom CI that evaluates Nix and envsubsts paths" "Renovate (docker datasource) bumps the digest from the tag"

step "7. Registry tooling works on closures"
COSIGN_PASSWORD= cosign generate-key-pair --output-key-prefix "$work/cosign" >/dev/null 2>&1
COSIGN_PASSWORD= run cosign sign "${cosign_insecure[@]}" --yes --key "$work/cosign.key" \
  --tlog-upload=false --use-signing-config=false "localhost:5443/envs/tools@$digest1" 2>&1 | tail -1
run cosign verify "${cosign_insecure[@]}" --key "$work/cosign.pub" --insecure-ignore-tlog=true \
  "localhost:5443/envs/tools@$digest1" 2>&1 | grep -v -e '^$' -e '^\[' | tail -3
run oras discover "${insecure[@]}" --format tree "localhost:5443/envs/tools@$digest1"
unlocked "signing, verification and inspection tools work on the closure as on any image: cosign, oras, crane."
row "Inspection, signing" "nix path-info against a cache; narinfo signatures only" "crane, oras, cosign on the digest; narinfo signatures still checked"

step "Side by side"
printf '%-22s | %-55s | %s\n' "" "storePaths" "closure (OCI)"
printf '%s\n' "----------------------------------------------------------------------------------------------------------------------------------"
printf '%-22s | %-55s | %s\n' "Pinning" "an immutable store path in the pod spec" "an immutable digest in the pod spec"
for r in "${rows[@]}"; do
  IFS='|' read -r what old new <<<"$r"
  printf '%-22s | %-55s | %s\n' "$what" "$old" "$new"
done
