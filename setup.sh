#!/bin/bash
set -eo pipefail
exec 1> >(stdbuf -oL cat) 2>&1

###############################################
# ENVIRONMENT
###############################################
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "[setup] Waiting for k3s node..."
until kubectl get nodes 2>/dev/null | grep -q " Ready"; do sleep 2; done
echo "[setup] k3s Ready."

mkdir -p /home/ubuntu/.kube
cp /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config
chown -R ubuntu:ubuntu /home/ubuntu/.kube
chmod 600 /home/ubuntu/.kube/config

###############################################
# IMPORT IMAGES
###############################################
echo "[setup] Importing container images..."
CTR="ctr --address /run/k3s/containerd/containerd.sock -n k8s.io"
until [ -S /run/k3s/containerd/containerd.sock ]; do sleep 2; done
sleep 3
for img in /var/lib/rancher/k3s/agent/images/*.tar; do
  $CTR images import "$img" 2>/dev/null || true
done

###############################################
# SCALE DOWN UNRELATED WORKLOADS
###############################################
echo "[setup] Scaling down unrelated workloads..."
for ns in bleater monitoring observability harbor argocd mattermost; do
  for dep in $(kubectl get deployments -n "$ns" -o name 2>/dev/null); do
    kubectl scale "$dep" -n "$ns" --replicas=0 2>/dev/null || true
  done
  for sts in $(kubectl get statefulsets -n "$ns" -o name 2>/dev/null); do
    kubectl scale "$sts" -n "$ns" --replicas=0 2>/dev/null || true
  done
done

# Also scale down the pre-existing glitchtip Helm release workloads
# (postgresql, web/worker, the bundled valkey-primary). The task
# operates only on its own valkey-runtime-state StatefulSet — leaving
# the bundled stack running starves the node of CPU / memory / PVC
# slots and causes WaitForPodScheduled storms when the multi-replica
# StatefulSet boots.
for dep in $(kubectl get deployments -n glitchtip -o name 2>/dev/null); do
  kubectl scale "$dep" -n glitchtip --replicas=0 2>/dev/null || true
done
for sts in $(kubectl get statefulsets -n glitchtip -o name 2>/dev/null); do
  case "${sts##*/}" in
    valkey-runtime-state) ;;  # ours, leave alone
    *) kubectl scale "$sts" -n glitchtip --replicas=0 2>/dev/null || true ;;
  esac
done

echo "[setup] Waiting for k3s API to stabilize after scale-down..."
sleep 15
until kubectl get nodes >/dev/null 2>&1; do sleep 3; done
sleep 10
NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
kubectl taint node "$NODE_NAME" node.kubernetes.io/unreachable- 2>/dev/null || true
kubectl taint node "$NODE_NAME" node.kubernetes.io/not-ready- 2>/dev/null || true
kubectl delete validatingwebhookconfiguration ingress-nginx-admission 2>/dev/null || true

###############################################
# NAMESPACE
###############################################
kubectl create namespace glitchtip 2>/dev/null || true
# Mark the namespace's security profile via labels. We keep the
# enforce mode permissive so existing-but-non-conformant workloads
# (saboteur CronJobs, Valkey StatefulSet, restore-probe pods) are
# not blocked, but the warn/audit modes broadcast the expectation
# so agents discover it via `kubectl get ns glitchtip -o yaml`.
kubectl label namespace glitchtip \
  "pod-security.kubernetes.io/warn=restricted" \
  "pod-security.kubernetes.io/audit=restricted" \
  --overwrite 2>/dev/null || true
kubectl annotate namespace glitchtip \
  "contract.glitchtip.io/backup-pod-security=restricted" \
  "contract.glitchtip.io/backup-pod-security-applies-to=workloads with label job=valkey-backup" \
  --overwrite 2>/dev/null || true

###############################################
# DEPLOY VALKEY STATEFULSET
# — 1 replica, PVC-backed /data, persistence policy tuned so it will NOT
#   automatically BGSAVE during the task run (the stale on-disk RDB
#   persists until something forces a new save).
###############################################
echo "[setup] Deploying Valkey StatefulSet..."

VALKEY_AUTH_PASSWORD=$(head -c 16 /dev/urandom | od -A n -t x1 | tr -d ' \n')

kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: valkey-runtime-state-auth
  namespace: glitchtip
type: Opaque
stringData:
  password: "${VALKEY_AUTH_PASSWORD}"
---
apiVersion: v1
kind: Service
metadata:
  name: valkey-runtime-state
  namespace: glitchtip
  labels:
    app: valkey-runtime-state
    component: cache
spec:
  clusterIP: None
  selector:
    app: valkey-runtime-state
  ports:
  - name: resp
    port: 6379
    targetPort: 6379
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: valkey-runtime-state-config
  namespace: glitchtip
data:
  valkey.conf: |
    # Runtime cache for GlitchTip coordination state.
    # Master save policy: one write every hour OR 100k writes per 15
    # minutes — snapshots are rare unless a backup forces them.
    save 3600 1
    save 900 100000
    appendonly no
    dir /data
    dbfilename dump.rdb
    bind 0.0.0.0
    protected-mode no
    maxmemory 64mb
    maxmemory-policy allkeys-lru
    # Replication topology: this StatefulSet runs 3 pods. Pod ord=0 is
    # the writable master; ord>=1 are read-only replicas configured via
    # the entrypoint (see startup.sh). Replicas have an aggressive
    # local snapshot policy so their on-disk RDB looks recent — but
    # the keyset they hold lags the master for any unreplicated writes.
    replica-priority 0
    replica-read-only yes
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
  startup.sh: |
    #!/bin/sh
    # Pod ordinal is the trailing integer of the pod name. We rely on
    # the Kubernetes downward API (POD_NAME env var injected from
    # metadata.name) rather than \$HOSTNAME (busybox sh doesn't auto-set
    # it) or \`hostname\` command (returns empty in this minimal alpine
    # image). The StatefulSet pod-template injects POD_NAME below.
    # NOTE: every \$ in this block is escaped because the surrounding
    # heredoc in setup.sh is unquoted — without escaping, the *outer*
    # shell would expand POD_NAME (to empty) before kubectl writes the
    # ConfigMap, baking the broken value into every pod's startup.
    if [ -z "\${POD_NAME}" ]; then
      echo "[startup] FATAL: POD_NAME env var not set — cannot determine ordinal"
      exit 2
    fi
    ORD="\${POD_NAME##*-}"
    MASTER_HOST="valkey-runtime-state-0.valkey-runtime-state.glitchtip.svc.cluster.local"
    echo "[startup] pod=\${POD_NAME} ord=\${ORD}"
    if [ "\${ORD}" = "0" ]; then
      echo "[startup] role=master (ord=0)"
      exec valkey-server /config/valkey.conf
    fi
    echo "[startup] role=replica (ord=\${ORD}) replicaof \${MASTER_HOST}:6379"
    # Replicas snapshot every 60s with a single write so their on-disk
    # dump.rdb mtime is recent — a naive backup that captures from a
    # replica will look fresh but is missing whatever has not yet
    # replicated from master at the moment of capture.
    exec valkey-server /config/valkey.conf \\
      --replicaof "\${MASTER_HOST}" 6379 \\
      --save "60 1"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: valkey-runtime-state
  namespace: glitchtip
  labels:
    app: valkey-runtime-state
    component: cache
spec:
  serviceName: valkey-runtime-state
  # 2 replicas is enough for the role-pinning anti-agent mechanic
  # (master at ord=0, read-only replica at ord=1). 3 replicas
  # over-stressed the single-node k3s cluster after we left the bundled
  # glitchtip stack running. Both topologies make agents who target the
  # headless service fail role:master verification ~50% of the time.
  replicas: 2
  podManagementPolicy: OrderedReady
  selector:
    matchLabels:
      app: valkey-runtime-state
  template:
    metadata:
      labels:
        app: valkey-runtime-state
        component: cache
    spec:
      containers:
      - name: valkey
        image: docker.io/valkey/valkey:7.2-alpine
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "/config/startup.sh"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 6379
          name: resp
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /config
          readOnly: true
        readinessProbe:
          exec:
            command: ["valkey-cli", "PING"]
          initialDelaySeconds: 3
          periodSeconds: 5
        resources:
          requests: {cpu: "10m", memory: "32Mi"}
          limits: {cpu: "200m", memory: "128Mi"}
      volumes:
      - name: config
        configMap:
          name: valkey-runtime-state-config
          defaultMode: 0755
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: local-path
      resources:
        requests:
          storage: 256Mi
---
# Platform durable-archive PVC. Pre-provisioned with a known name so
# backup workloads can route oversized artifacts here. The namespace
# ResourceQuota does NOT budget additional PVCs for backup workloads.
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: glitchtip-valkey-backup-archive
  namespace: glitchtip
  labels:
    app: glitchtip
    component: backup-archive
  annotations:
    description: "Durable archive PVC for Valkey backup artifacts. RWO local-path."
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: local-path
  resources:
    requests:
      storage: 256Mi
---
# Platform-managed quota: caps ConfigMap count (so wildcards don't
# drive away storage) and PVC count to the budgeted slots — Valkey's
# StatefulSet PVCs + the archive PVC + a small slack for grader probes.
apiVersion: v1
kind: ResourceQuota
metadata:
  name: glitchtip-platform-quota
  namespace: glitchtip
  labels:
    app: glitchtip
    component: platform-policy
spec:
  hard:
    count/configmaps: "32"
    count/secrets: "20"
    count/persistentvolumeclaims: "5"
---
# Trust anchor for handoff signing keys. The annotations carry the
# canonical contract — recovery tooling and the cluster admission
# policy both read these to enforce signing semantics. Agents
# inspect these annotations to learn what the consumer expects.
apiVersion: v1
kind: ConfigMap
metadata:
  name: glitchtip-valkey-backup-trust-anchor
  namespace: glitchtip
  labels:
    app: glitchtip
    component: backup-trust-anchor
  annotations:
    contract.glitchtip.io/registry-field: "public_key_fingerprints"
    contract.glitchtip.io/algorithm: "ed25519"
    contract.glitchtip.io/public-key-format: "raw-32-bytes-base64"
    contract.glitchtip.io/signature-format: "raw-64-bytes-base64"
    contract.glitchtip.io/fingerprint-format: "sha256-hex-of-raw-public-key"
    contract.glitchtip.io/registry-encoding: "newline-separated-fingerprints"
    contract.glitchtip.io/key-rotation-policy: "per-run-fresh-keypair"
    contract.glitchtip.io/canonical-payload-format: "json-sort-keys-true-separators-comma-colon-no-space"
    contract.glitchtip.io/canonical-payload-fields: "backup_id,snapshot_epoch,artifact_sha256,artifact_source_pod,artifact_source_role"
    contract.glitchtip.io/registration-protocol: "append-fingerprint-line-before-publish"
data:
  public_key_fingerprints: ""
EOF

# Content-shape admission policy on the runtime handoff ConfigMap.
# Applied via a SEPARATE quoted heredoc so the regex backslashes
# survive shell processing intact for CEL's string parser.
kubectl apply -f - <<'POLICY_EOF'
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: glitchtip-handoff-shape
  annotations:
    contract.glitchtip.io/applies-to: "handoff CM in glitchtip namespace"
    contract.glitchtip.io/purpose: "reject ill-formed handoff publishes"
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - apiGroups: [""]
      apiVersions: ["v1"]
      operations: ["CREATE","UPDATE"]
      resources: ["configmaps"]
  matchConditions:
  - name: only-handoff-cm
    expression: |
      object.metadata.namespace == 'glitchtip' &&
      object.metadata.name == 'glitchtip-valkey-backup-restore-handoff'
  - name: has-handoff-key
    expression: |
      has(object.data) && 'handoff.json' in object.data
  validations:
  - expression: |
      object.data['handoff.json'].matches('"safe_for_restore"\\s*:\\s*(true|false)')
    message: "handoff.json must declare a boolean safe_for_restore"
  - expression: |
      object.data['handoff.json'].matches('"safe_for_restore"\\s*:\\s*false')
      ||
      (
        object.data['handoff.json'].matches('"backup_id"\\s*:\\s*"[^"]+"')
        && object.data['handoff.json'].matches('"snapshot_epoch"\\s*:\\s*[0-9]{10,}')
        && object.data['handoff.json'].matches('"artifact_bytes"\\s*:\\s*[0-9]+')
        && object.data['handoff.json'].matches('"artifact_sha256"\\s*:\\s*"[0-9a-f]{64}"')
        && object.data['handoff.json'].matches('"artifact_location"\\s*:\\s*\\{')
        && object.data['handoff.json'].matches('"artifact_source_pod"\\s*:\\s*"[^"]+"')
        && object.data['handoff.json'].matches('"artifact_source_role"\\s*:\\s*"master"')
        && object.data['handoff.json'].matches('"master_replid"\\s*:\\s*"[0-9a-f]{40}"')
        && object.data['handoff.json'].matches('"master_repl_offset"\\s*:\\s*[0-9]+')
        && object.data['handoff.json'].matches('"signature"\\s*:\\s*\\{')
        && object.data['handoff.json'].matches('"alg"\\s*:\\s*"ed25519"')
        && object.data['handoff.json'].matches('"public_key"\\s*:\\s*"[A-Za-z0-9+/=]+"')
        && object.data['handoff.json'].matches('"sig"\\s*:\\s*"[A-Za-z0-9+/=]+"')
        && object.data['handoff.json'].matches('"signed_at_epoch"\\s*:\\s*[0-9]{10,}')
        && object.data['handoff.json'].matches('"restore_proof"\\s*:\\s*\\{')
        && object.data['handoff.json'].matches('"key_count_at_snapshot"\\s*:\\s*[0-9]+')
      )
    message: "successful handoff (safe_for_restore:true) must carry the full attestation: backup_id, snapshot_epoch, artifact_bytes, artifact_sha256, artifact_location object, artifact_source_pod, artifact_source_role:master, master_replid (40-hex), master_repl_offset, signature object {alg:ed25519, public_key, sig}, signed_at_epoch, restore_proof.key_count_at_snapshot"
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: glitchtip-handoff-shape-binding
spec:
  policyName: glitchtip-handoff-shape
  validationActions: [Deny, Audit]
POLICY_EOF

echo "[setup] Waiting for Valkey StatefulSet (2 replicas: 1 master + 1 read-only)..."
# local-path-provisioner can take a moment after the heavy scale-down.
# Make sure it is actually running before we wait on the pod.
kubectl rollout status deployment/local-path-provisioner -n kube-system --timeout=60s 2>/dev/null || \
  kubectl scale deployment local-path-provisioner -n kube-system --replicas=1 2>/dev/null || true

for i in $(seq 1 120); do
  READY=$(kubectl get statefulset/valkey-runtime-state -n glitchtip \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  if [ "${READY}" = "2" ]; then
    echo "[setup] Valkey ready (2/2) after $((i*5))s."
    break
  fi
  if [ $((i % 6)) = 0 ]; then
    echo "[setup] Valkey not ready yet (t=$((i*5))s, ready=${READY:-0}/2). Pod / PVC status:"
    kubectl get pod -n glitchtip -l app=valkey-runtime-state 2>&1 | head -5 || true
    kubectl get pvc -n glitchtip 2>&1 | head -5 || true
    kubectl get events -n glitchtip --sort-by='.lastTimestamp' 2>&1 | tail -10 || true
  fi
  sleep 5
done
kubectl wait --for=condition=ready pod -l app=valkey-runtime-state -n glitchtip --timeout=180s || true

# All writes go to ord=0 (master); replicas (-1, -2) PSYNC for reads.
VALKEY_POD="valkey-runtime-state-0"

###############################################
# POPULATE LIVE VALKEY WITH RUNTIME STATE
# — realistic short-horizon coordination data operators expect to
#   survive a recovery drill.
###############################################
echo "[setup] Populating Valkey with live runtime state..."

vk() {
  kubectl exec -n glitchtip "${VALKEY_POD}" -- valkey-cli "$@"
}

vk SET "mute:project-1:error-feature-x" '{"until":"2026-04-27T10:00:00Z","reason":"launch-noise"}' EX 86400 >/dev/null
vk SET "mute:project-1:spam-ruleset-v3" '{"until":"2026-04-26T18:00:00Z","reason":"rules-review"}' EX 36000 >/dev/null
vk SET "mute:project-2:deploy-spam" '{"until":"2026-04-25T04:00:00Z","reason":"deploy-window"}' EX 18000 >/dev/null
vk SET "throttle:api:user-abc" 45 EX 300 >/dev/null
vk SET "throttle:api:user-def" 12 EX 300 >/dev/null
vk SET "throttle:api:user-ghi" 78 EX 300 >/dev/null
vk SET "throttle:ingest:token-pqr" 3200 EX 600 >/dev/null
vk SET "coord:alert-lock:incident-42" "on-call-rotation-pager-A" EX 900 >/dev/null
vk SET "coord:alert-lock:incident-47" "on-call-rotation-pager-B" EX 900 >/dev/null
vk SET "coord:dedupe:event-fingerprint-81ab22" 1 EX 600 >/dev/null
vk SET "coord:dedupe:event-fingerprint-9c0f11" 1 EX 600 >/dev/null
vk HSET "stats:project-1:today" events_last_hour 1247 suppressions 3 alerts_sent 19 >/dev/null
vk HSET "stats:project-2:today" events_last_hour 308 suppressions 0 alerts_sent 4 >/dev/null
vk HSET "stats:project-3:today" events_last_hour 0 suppressions 0 alerts_sent 0 >/dev/null
vk ZADD "rollup:ingest-backlog-24h" 1745300100 "incident-42" 1745303600 "incident-47" 1745308200 "incident-51" >/dev/null
vk ZADD "rollup:resolved-last-24h" 1745291000 "incident-39" 1745296500 "incident-40" >/dev/null
vk SET "feature-flag:canary:issue-search-v2" '{"enabled":true,"rollout":0.15}' >/dev/null
vk SET "feature-flag:canary:symbolicator-queue" '{"enabled":false,"rollout":0.0}' >/dev/null
vk SET "idempotency:worker-ingest:shard-3" "run-id-0e4c9a-partial" EX 1800 >/dev/null
vk SET "idempotency:worker-symbols:shard-1" "run-id-2a77fd-complete" EX 1800 >/dev/null
vk LPUSH "queue:retries:symbolicator-slow" '{"event":"e-123","attempt":2}' '{"event":"e-124","attempt":1}' >/dev/null
vk LPUSH "queue:retries:ingest-partial" '{"event":"e-555","attempt":1}' >/dev/null

LIVE_DBSIZE=$(vk DBSIZE | tr -d '[:space:]')
echo "[setup] Live Valkey DBSIZE after seeding: ${LIVE_DBSIZE}"

# Force an initial on-disk RDB, then age it so it is OBVIOUSLY stale
vk BGSAVE >/dev/null || true
for _ in $(seq 1 30); do
  LAST=$(vk LASTSAVE | tr -d '[:space:]')
  if [ -n "${LAST}" ] && [ "${LAST}" != "0" ]; then break; fi
  sleep 1
done
# Backdate the RDB mtime to 7 days ago.
# Compute the stamp in the setup container (GNU date) and apply it
# inside the alpine Valkey pod using busybox `touch -t` (no GNU -d).
STALE_STAMP=$(date -u -d "@$(( $(date -u +%s) - 7*24*3600 ))" '+%Y%m%d%H%M.%S')
kubectl exec -n glitchtip "${VALKEY_POD}" -- touch -t "${STALE_STAMP}" /data/dump.rdb
STALE_MTIME=$(kubectl exec -n glitchtip "${VALKEY_POD}" -- stat -c '%Y' /data/dump.rdb)
echo "[setup] On-disk dump.rdb backdated to epoch ${STALE_MTIME} (stale by design)."

###############################################
# LIVE TRAFFIC GENERATOR
# — emits short-TTL coordination keys every 30s. Two effects:
#   (1) live DBSIZE drifts every minute, so a backup whose RDB was
#       captured a few minutes ago will not match the live keyspace
#       at restore-probe time (forces fresh BGSAVE on every run).
#   (2) writes interleave with BGSAVE, so a naive backup that doesn't
#       quiesce writes risks torn snapshots.
###############################################
echo "[setup] Installing live traffic generator CronJob..."

kubectl apply -f - <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: valkey-runtime-traffic
  namespace: glitchtip
  labels:
    app: glitchtip
    component: traffic-generator
  annotations:
    description: "Simulates live coordination traffic against Valkey"
spec:
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 50
      template:
        spec:
          restartPolicy: Never
          containers:
          - name: writer
            image: docker.io/valkey/valkey:7.2-alpine
            imagePullPolicy: IfNotPresent
            command: ["/bin/sh", "-c"]
            args:
            - |
              VK="valkey-cli -h valkey-runtime-state.glitchtip.svc.cluster.local -p 6379 --no-auth-warning"
              TS=$(date -u +%s)
              # Short-horizon coordination state — TTL between 60s and 300s.
              # Enough churn that a stale backup is detectable, but the
              # base keyset (mute:/throttle:/coord:/stats:/...) persists.
              for i in 1 2 3; do
                $VK SET "throttle:tg-$$-${i}-${TS}" "${RANDOM}" EX 240 >/dev/null
                $VK SET "coord:tg-$$-${i}-${TS}" "lock-${RANDOM}" EX 180 >/dev/null
              done
              $VK INCRBY "stats:traffic-gen:tick-count" 1 >/dev/null
              $VK SET "stats:traffic-gen:last-tick" "${TS}" EX 600 >/dev/null
              echo "traffic-gen tick: ${TS}"
EOF

###############################################
# BROKEN BACKUP SCRIPT + CRONJOB
# — runs valkey-cli --rdb against the service, but:
#   (a) does not validate the result (ignores failures, accepts 0-byte
#       output, does not check REDIS magic bytes)
#   (b) publishes status/handoff as SUCCESS unconditionally, with no
#       backup_id / snapshot_epoch / restore_proof
#   (c) stores the artifact only in pod-local emptyDir — nothing durable
###############################################
echo "[setup] Creating broken backup script + CronJob..."

kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: valkey-state-backup-script
  namespace: glitchtip
data:
  backup.sh: |
    #!/bin/sh
    # --- approved backup script (v1, count-based validation) ---
    export PATH="/tools:${PATH}"
    TS=$(date +%Y%m%d_%H%M%S)
    OUT=/backups/dump-${TS}.rdb
    mkdir -p /backups

    echo "[backup] fetching Valkey snapshot via --rdb..."
    valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning --rdb "${OUT}" 2>/dev/null || true

    # No validation of the file — just publish success.
    SIZE=0
    [ -f "${OUT}" ] && SIZE=$(wc -c < "${OUT}" | tr -d '[:space:]')

    TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    STATUS="# Valkey backup\nstatus=SUCCESS\ntimestamp=${NOW}\n"
    STATUS_ESC=$(printf '%s' "$STATUS" | sed ':a;N;$!ba;s/\\/\\\\/g;s/"/\\"/g;s/\n/\\n/g')
    wget -qO- --header="Authorization: Bearer ${TOKEN}" \
      --ca-certificate="${CA}" \
      --method=PATCH \
      --header="Content-Type: application/merge-patch+json" \
      --body-data="{\"data\":{\"status.md\":\"${STATUS_ESC}\"}}" \
      "https://kubernetes.default.svc/api/v1/namespaces/glitchtip/configmaps/glitchtip-valkey-backup-status" >/dev/null 2>&1 || true

    HANDOFF='{"latest_run":"'"${NOW}"'","result":"success","safe_for_restore":true,"artifact_bytes":'"${SIZE}"'}'
    HANDOFF_ESC=$(printf '%s' "$HANDOFF" | sed ':a;N;$!ba;s/\\/\\\\/g;s/"/\\"/g;s/\n/\\n/g')
    wget -qO- --header="Authorization: Bearer ${TOKEN}" \
      --ca-certificate="${CA}" \
      --method=PATCH \
      --header="Content-Type: application/merge-patch+json" \
      --body-data="{\"data\":{\"handoff.json\":\"${HANDOFF_ESC}\"}}" \
      "https://kubernetes.default.svc/api/v1/namespaces/glitchtip/configmaps/glitchtip-valkey-backup-restore-handoff" >/dev/null 2>&1 || true

    echo "[backup] done, wrote ${OUT} (${SIZE} bytes)."
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: default
  namespace: glitchtip
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: valkey-backup-legacy-writer
  namespace: glitchtip
rules:
# Deliberately broad so the initial script "works" — the agent must narrow
# this down to just the two runtime ConfigMaps as part of the fix.
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "patch", "update", "create"]
- apiGroups: ["batch"]
  resources: ["cronjobs"]
  verbs: ["get", "patch", "update"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: valkey-backup-legacy-writer-binding
  namespace: glitchtip
subjects:
- kind: ServiceAccount
  name: default
  namespace: glitchtip
roleRef:
  kind: Role
  name: valkey-backup-legacy-writer
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: valkey-state-backup
  namespace: glitchtip
  labels:
    app: glitchtip
    component: valkey-backup
  annotations:
    description: "Backs up GlitchTip's Valkey runtime state. Runs hourly."
    description: "Backs up GlitchTip's Valkey runtime state. Runs hourly."
spec:
  schedule: "17 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 300
      template:
        metadata:
          labels:
            app: glitchtip
            job: valkey-backup
        spec:
          restartPolicy: Never
          initContainers:
          - name: ship-valkey-cli
            image: docker.io/valkey/valkey:7.2-alpine
            imagePullPolicy: IfNotPresent
            command: ["/bin/sh", "-c"]
            args:
            - |
              cp /usr/local/bin/valkey-cli /tools/valkey-cli
              chmod +x /tools/valkey-cli
            volumeMounts:
            - name: tools
              mountPath: /tools
          - name: ship-curl
            image: docker.io/curlimages/curl:8.9.1
            imagePullPolicy: IfNotPresent
            command: ["/bin/sh", "-c"]
            args:
            - |
              # curl is dynamically linked; copy the binary AND every
              # dependent .so into /tools/lib so the main container can
              # load them with LD_LIBRARY_PATH.
              mkdir -p /tools/lib
              cp /usr/bin/curl /tools/curl
              chmod +x /tools/curl
              for lib in $(ldd /usr/bin/curl 2>/dev/null | awk '/=>/ {print $3}'); do
                [ -n "$lib" ] && [ -f "$lib" ] && cp -L "$lib" /tools/lib/ || true
              done
              # Also copy the musl loader if it's referenced as an absolute path.
              ldd /usr/bin/curl 2>/dev/null | awk '/ld-musl/ {print $1}' | while read loader; do
                [ -f "$loader" ] && cp -L "$loader" /tools/lib/ || true
              done
              ls -la /tools/ /tools/lib/
            volumeMounts:
            - name: tools
              mountPath: /tools
          containers:
          - name: backup
            image: docker.io/valkey/valkey:7.2-alpine
            imagePullPolicy: IfNotPresent
            command: ["/bin/sh", "/scripts/backup.sh"]
            env:
            - name: VALKEY_HOST
              value: valkey-runtime-state.glitchtip.svc.cluster.local
            volumeMounts:
            - name: script
              mountPath: /scripts
            - name: storage
              mountPath: /backups
            - name: tools
              mountPath: /tools
          volumes:
          - name: script
            configMap:
              name: valkey-state-backup-script
              defaultMode: 0755
          - name: storage
            emptyDir: {}
          - name: tools
            emptyDir: {}
EOF

###############################################
# STATUS + HANDOFF CONFIGMAPS (pre-populated stale)
###############################################
echo "[setup] Pre-populating stale status + restore-handoff..."

kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: glitchtip-valkey-backup-status
  namespace: glitchtip
  labels:
    app: glitchtip
    component: documentation
data:
  status.md: |
    # Valkey Runtime State Backup
    ## Latest Recorded Run: 2026-03-30T02:17:11Z

    - Status: SUCCESS
    - Backup ID: valkey-20260330_021711
    - Snapshot captured at: 2026-03-30T02:17:11Z
    - Artifact bytes: 214562
    - Restore proof: (not recorded)

    ### Current State
    - Backup CronJob: valkey-state-backup (runs hourly at :17)
    - Artifact storage: ephemeral pod-local scratch (/backups) — NOT durable
    - Validation: snapshot existence only — does NOT prove freshness
    - Retention: 3 successful job history entries

    ### Symptoms reported during the recovery drill
    - Restored runtime state was clearly older than the time the run record
      claimed.
    - On-call could not tell from this note alone whether the captured
      artifact was real, current, or recoverable.
    - The pod-local scratch directory does not survive node turnover.
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: glitchtip-valkey-backup-restore-handoff
  namespace: glitchtip
  labels:
    app: glitchtip
    component: documentation
data:
  handoff.json: |
    {
      "latest_run": "2026-03-30T02:17:11Z",
      "backup_id": "valkey-20260330_021711",
      "result": "success",
      "safe_for_restore": true,
      "artifact_bytes": 214562,
      "notes": [
        "Downstream recovery tooling reads this file before attempting a restore.",
        "This record is stale until the current pipeline refreshes it."
      ]
    }
EOF

###############################################
# SABOTEUR 1 — DRIFT RECONCILER CRONJOB
# Reverts valkey-state-backup-script ConfigMap to the approved version
# every minute. The approved version is itself stored as a ConfigMap
# so the agent can update the baseline as part of the fix.
###############################################
echo "[setup] Installing backup-script drift reconciler..."

ORIG_SCRIPT=$(kubectl get configmap valkey-state-backup-script -n glitchtip \
  -o go-template='{{ index .data "backup.sh" }}')

kubectl create configmap valkey-state-backup-script-baseline -n glitchtip \
  --from-literal="backup.sh=${ORIG_SCRIPT}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f - <<'RBAC_EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: backup-drift-reconciler-sa
  namespace: glitchtip
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: backup-drift-reconciler
  namespace: glitchtip
rules:
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "patch", "update"]
- apiGroups: ["batch"]
  resources: ["cronjobs"]
  verbs: ["get", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: backup-drift-reconciler-binding
  namespace: glitchtip
subjects:
- kind: ServiceAccount
  name: backup-drift-reconciler-sa
  namespace: glitchtip
roleRef:
  kind: Role
  name: backup-drift-reconciler
  apiGroup: rbac.authorization.k8s.io
RBAC_EOF

kubectl apply -f - <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: valkey-backup-drift-reconciler
  namespace: glitchtip
  labels:
    app: glitchtip
    component: backup-lifecycle
  annotations:
    description: "Keeps the Valkey backup script aligned with the approved baseline"
spec:
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 50
      template:
        spec:
          restartPolicy: Never
          serviceAccountName: backup-drift-reconciler-sa
          containers:
          - name: reconciler
            image: docker.io/bitnamilegacy/postgresql:17.0.0-debian-12-r11
            imagePullPolicy: IfNotPresent
            command: ["/bin/bash", "-c"]
            args:
            - |
              APPROVED=$(cat /approved/backup.sh 2>/dev/null || echo "")
              if [ -z "${APPROVED}" ]; then exit 0; fi
              ESC=$(printf '%s' "${APPROVED}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')
              PATCH="{\"data\":{\"backup.sh\":${ESC}}}"
              TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
              CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
              curl -sf --cacert ${CA} \
                -X PATCH \
                -H "Authorization: Bearer ${TOKEN}" \
                -H "Content-Type: application/merge-patch+json" \
                -d "${PATCH}" \
                "https://kubernetes.default.svc/api/v1/namespaces/glitchtip/configmaps/valkey-state-backup-script" >/dev/null 2>&1 || true
              echo "drift reconcile tick: done."
            volumeMounts:
            - name: approved
              mountPath: /approved
          volumes:
          - name: approved
            configMap:
              name: valkey-state-backup-script-baseline
              optional: true
EOF

###############################################
# SABOTEUR 2 — TEMPLATE MANAGER CRONJOB
# Reverts valkey-state-backup CronJob's serviceAccountName to the
# approved baseline every minute.
###############################################
echo "[setup] Installing backup-template manager..."

kubectl create configmap valkey-state-backup-template-baseline -n glitchtip \
  --from-literal=serviceAccountName=default \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f - <<'EOF'
apiVersion: batch/v1
kind: CronJob
metadata:
  name: valkey-backup-template-manager
  namespace: glitchtip
  labels:
    app: glitchtip
    component: backup-template
  annotations:
    description: "Reconciles Valkey backup CronJob runtime identity"
spec:
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 50
      template:
        spec:
          restartPolicy: Never
          serviceAccountName: backup-drift-reconciler-sa
          containers:
          - name: reconciler
            image: docker.io/bitnamilegacy/postgresql:17.0.0-debian-12-r11
            imagePullPolicy: IfNotPresent
            command: ["/bin/bash", "-c"]
            args:
            - |
              SA=$(tr -d '[:space:]' < /approved/serviceAccountName 2>/dev/null || echo "default")
              [ -z "${SA}" ] && SA="default"
              PATCH="{\"spec\":{\"jobTemplate\":{\"spec\":{\"template\":{\"spec\":{\"serviceAccountName\":\"${SA}\"}}}}}}"
              TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
              CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
              curl -sf --cacert ${CA} \
                -X PATCH \
                -H "Authorization: Bearer ${TOKEN}" \
                -H "Content-Type: application/merge-patch+json" \
                -d "${PATCH}" \
                "https://kubernetes.default.svc/apis/batch/v1/namespaces/glitchtip/cronjobs/valkey-state-backup" >/dev/null 2>&1 || true
              echo "template reconcile tick: done."
            volumeMounts:
            - name: approved
              mountPath: /approved
          volumes:
          - name: approved
            configMap:
              name: valkey-state-backup-template-baseline
              optional: true
EOF

###############################################
# VALKEY NETWORK POLICY — scoped, must be preserved
###############################################
kubectl apply -f - <<'EOF'
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: valkey-runtime-state-access
  namespace: glitchtip
  labels:
    app: valkey-runtime-state
  annotations:
    description: "Scoped Valkey access — authorized workloads only"
spec:
  podSelector:
    matchLabels:
      app: valkey-runtime-state
  policyTypes:
  - Ingress
  ingress:
  # Inter-replica traffic for PSYNC (master <-> replicas).
  - from:
    - podSelector:
        matchLabels:
          app: valkey-runtime-state
    ports:
    - protocol: TCP
      port: 6379
  - from:
    - podSelector:
        matchLabels:
          app.kubernetes.io/name: glitchtip
    ports:
    - protocol: TCP
      port: 6379
  - from:
    - podSelector:
        matchLabels:
          job: valkey-backup
    ports:
    - protocol: TCP
      port: 6379
EOF

###############################################
# DECOY DOCUMENTATION
###############################################
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: glitchtip-dr-incident-2024q3
  namespace: glitchtip
  labels:
    app: glitchtip
    component: documentation
  annotations:
    date: "2024-09-22"
data:
  incident-report.md: |
    # GlitchTip DR Drill Incident — Q3 2024
    ## Summary
    Q3 drill showed missing issue data for ~3h after restore.
    ## Root cause
    TLS certificate expired on the PostgreSQL connection — pg_restore
    silently skipped two tables.
    ## Resolution
    - Rotated PostgreSQL TLS certs
    - Added cert expiry alert to PagerDuty
    - Re-ran pg_dump after verification
    ## Follow-up (complete)
    - Quarterly cert rotation
    - Cert expiry alert wired to on-call
    - DR runbook updated with TLS verification step
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: valkey-runtime-tuning-reference
  namespace: glitchtip
  labels:
    app: glitchtip
    component: documentation
data:
  notes.md: |
    # Valkey Runtime Cache Tuning Reference
    The runtime cache is intentionally biased toward uptime over
    persistence: the in-cluster save policy is tuned for long intervals
    because backup is owned by the backup pipeline, not by the server's
    on-disk snapshot heuristic. Operators working on backup changes
    should assume that the server's on-disk dump.rdb may be significantly
    behind live state between scheduled backup runs.

    ## Historical note
    Earlier attempts at making the backup script fresher lasted one run
    and then reverted after the next lifecycle reconciliation cycle.
    Any fix has to survive the surrounding automation, not just the
    immediate run.
EOF

###############################################
# STRIP ANNOTATIONS
###############################################
for res in \
  "configmap/valkey-state-backup-script -n glitchtip" \
  "configmap/valkey-state-backup-script-baseline -n glitchtip" \
  "configmap/valkey-state-backup-template-baseline -n glitchtip" \
  "configmap/valkey-runtime-state-config -n glitchtip" \
  "configmap/glitchtip-valkey-backup-status -n glitchtip" \
  "configmap/glitchtip-valkey-backup-restore-handoff -n glitchtip" \
  "configmap/glitchtip-dr-incident-2024q3 -n glitchtip" \
  "configmap/valkey-runtime-tuning-reference -n glitchtip" \
  "cronjob/valkey-state-backup -n glitchtip" \
  "cronjob/valkey-backup-drift-reconciler -n glitchtip" \
  "cronjob/valkey-backup-template-manager -n glitchtip" \
  "cronjob/valkey-runtime-traffic -n glitchtip" \
  "statefulset/valkey-runtime-state -n glitchtip" \
  "service/valkey-runtime-state -n glitchtip" \
  "secret/valkey-runtime-state-auth -n glitchtip" \
  "networkpolicy/valkey-runtime-state-access -n glitchtip" \
  "configmap/glitchtip-valkey-backup-trust-anchor -n glitchtip" \
  "validatingadmissionpolicy/glitchtip-handoff-shape" \
  "validatingadmissionpolicybinding/glitchtip-handoff-shape-binding"; do
  kubectl annotate ${res} kubectl.kubernetes.io/last-applied-configuration- 2>/dev/null || true
done

###############################################
# SAVE SETUP INFO
###############################################
cat > /root/.setup_info <<SETUP_EOF
NAMESPACE=glitchtip
VALKEY_STATEFULSET=valkey-runtime-state
VALKEY_POD=${VALKEY_POD}
VALKEY_SERVICE=valkey-runtime-state
VALKEY_AUTH_PASSWORD=${VALKEY_AUTH_PASSWORD}
LIVE_DBSIZE=${LIVE_DBSIZE}
STALE_DUMP_MTIME=${STALE_MTIME}
SETUP_EOF
chmod 600 /root/.setup_info

echo "[setup] ============================================"
echo "[setup] Setup complete. Valkey backup freshness gap ready."
echo "[setup] ============================================"
