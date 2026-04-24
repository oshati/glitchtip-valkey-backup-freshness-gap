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
    # Save policy: one write every hour OR 100k writes per 15 minutes.
    # During normal platform operation snapshots are rare, so the on-disk
    # RDB is expected to lag behind live state unless a backup actively
    # forces a fresh snapshot.
    save 3600 1
    save 900 100000
    appendonly no
    dir /data
    dbfilename dump.rdb
    bind 0.0.0.0
    protected-mode no
    maxmemory 64mb
    maxmemory-policy allkeys-lru
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
  replicas: 1
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
        args:
        - /config/valkey.conf
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
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 256Mi
EOF

echo "[setup] Waiting for Valkey (k3s may be under load after scale-down)..."
# Poll instead of failing hard on rollout status — scheduler latency can
# spike right after the big scale-down above.
for i in $(seq 1 60); do
  READY=$(kubectl get statefulset/valkey-runtime-state -n glitchtip \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  if [ "${READY}" = "1" ]; then
    echo "[setup] Valkey ready."
    break
  fi
  sleep 5
done
kubectl wait --for=condition=ready pod -l app=valkey-runtime-state -n glitchtip --timeout=120s || true

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
    - Artifact bytes: 214562
    - Snapshot verified: (not recorded)
    - Restore proof: (not recorded)

    ### Current State
    - Backup CronJob: valkey-state-backup (runs hourly at :17)
    - Artifact storage: ephemeral pod-local scratch (/backups)
    - Validation: snapshot existence only
    - Retention: 3 successful job history entries

    ### Known Gaps
    - The backup script does not prove the captured snapshot is current.
    - The run note has historically lagged behind the actual run.
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
      "result": "success",
      "safe_for_restore": true,
      "artifact_bytes": 214562,
      "notes": [
        "Downstream recovery tooling checks this handoff before restoring.",
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
  "statefulset/valkey-runtime-state -n glitchtip" \
  "service/valkey-runtime-state -n glitchtip" \
  "secret/valkey-runtime-state-auth -n glitchtip" \
  "networkpolicy/valkey-runtime-state-access -n glitchtip"; do
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
