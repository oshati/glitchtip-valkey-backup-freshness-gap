#!/bin/bash
set -e
export KUBECONFIG=/home/ubuntu/.kube/config

NS=glitchtip
CRONJOB=valkey-state-backup
SCRIPT_CM=valkey-state-backup-script
SCRIPT_BASELINE_CM=valkey-state-backup-script-baseline
TEMPLATE_BASELINE_CM=valkey-state-backup-template-baseline
STATUS_CM=glitchtip-valkey-backup-status
HANDOFF_CM=glitchtip-valkey-backup-restore-handoff
TRUST_CM=glitchtip-valkey-backup-trust-anchor
ARCHIVE_PVC=glitchtip-valkey-backup-archive
SA=valkey-backup-publisher

# IN-PLACE FIX: the drift reconciler + template manager CronJobs are
# part of the supported job-management flow, so we leave them running
# and instead overwrite the baselines they reapply each minute (the
# script-baseline + template-baseline ConfigMaps below).
echo "[solution] Writing the fixed backup script..."
cat > /tmp/valkey-backup-fixed.sh <<'SCRIPT_EOF'
#!/bin/sh
# Trustworthy Valkey backup, master-pinned, signed, and replication-attested.
# Stored verbatim in the valkey-state-backup-script ConfigMap (the
# outer heredoc in solution.sh is single-quoted so all $ expansions
# are evaluated by the inner /bin/sh inside the backup pod).
exec 2>&1
export PATH="/tools:${PATH}"
# IMPORTANT: do NOT export LD_LIBRARY_PATH globally. The curl from
# /tools/lib ships its own libcrypto.so.3 / libssl.so.3 which conflict
# with the alpine valkey image's openssl binary at /usr/bin/openssl
# (relocation errors). Use the wrapper functions below instead, which
# scope LD_LIBRARY_PATH to the curl invocation only.
CURL="/tools/curl"
curl_with_libs() {
  LD_LIBRARY_PATH="/tools/lib" "${CURL}" "$@"
}
echo "[backup] pod started at $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(hostname)"

NS=glitchtip
STATUS_CM=glitchtip-valkey-backup-status
HANDOFF_CM=glitchtip-valkey-backup-restore-handoff
TRUST_CM=glitchtip-valkey-backup-trust-anchor
ARCHIVE_MOUNT=/archive
TS=$(date +%Y%m%d-%H%M%S)
BACKUP_ID="valkey-${TS}"
OUT="/backups/dump-${TS}.rdb"
mkdir -p /backups
NOW_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
NOW_EPOCH=$(date +%s)
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

SS_NAME="valkey-runtime-state"
SS_SVC="valkey-runtime-state.${NS}.svc.cluster.local"
MASTER_POD="valkey-runtime-state-0"
MASTER_HOST="${MASTER_POD}.${SS_SVC}"
ARTIFACT_SOURCE_POD="${MASTER_POD}"
ARTIFACT_SOURCE_ROLE="master"

patch_cm() {
  cm="$1"; key="$2"; content_file="$3"
  bf="/tmp/apply-${cm}-$$.yaml"
  {
    printf 'apiVersion: v1\n'
    printf 'kind: ConfigMap\n'
    printf 'metadata:\n'
    printf '  name: %s\n' "${cm}"
    printf '  namespace: %s\n' "${NS}"
    printf 'data:\n'
    printf '  %s: |\n' "${key}"
    sed 's/^/    /' "${content_file}"
  } > "${bf}"
  _rc=0
  _out=$(curl_with_libs -sS --cacert "${CA}" \
    -X PATCH \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/apply-patch+yaml" \
    --data-binary "@${bf}" \
    -w 'HTTP:%{http_code}' \
    "https://kubernetes.default.svc/api/v1/namespaces/${NS}/configmaps/${cm}?fieldManager=valkey-backup&force=true" 2>&1) || _rc=$?
  _http=$(printf '%s' "${_out}" | sed -n 's/.*HTTP:\([0-9]\+\).*/\1/p' | tail -1)
  if [ "${_rc}" != "0" ] || { [ "${_http}" != "200" ] && [ "${_http}" != "201" ]; }; then
    echo "[backup] patch_cm ${cm}/${key} FAIL rc=${_rc} http=${_http}"
    return 1
  fi
  rm -f "${bf}" 2>/dev/null || true
  return 0
}

publish_failure() {
  reason="$1"
  reason_clean=$(printf '%s' "${reason}" | tr '"\\\\' "''" | tr '\n' ' ' | head -c 250)
  cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"failed","safe_for_restore":false,"snapshot_epoch":0,"artifact_bytes":0,"artifact_sha256":null,"artifact_location":null,"artifact_source_pod":null,"artifact_source_role":null,"master_replid":null,"master_repl_offset":null,"signature":null,"signed_at_epoch":0,"restore_proof":null,"reason":"${reason_clean}"}
HEOF
  patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || true
  cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: FAILED**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Reason: ${reason_clean}
SEOF
  patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || true
  echo "[backup] FAILED: ${reason_clean}"
  exit 1
}

echo "[backup] step: PING master"
i=0
while [ $i -lt 30 ]; do
  if valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning PING 2>/dev/null \
       | grep -q PONG; then break; fi
  sleep 2; i=$((i+1))
done
[ $i -ge 30 ] && publish_failure "master pod did not answer PING within 60s"

echo "[backup] step: INFO replication (verify role + capture replid/offset)"
INFO_OUT=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning INFO replication 2>&1)
[ $? -ne 0 ] && publish_failure "INFO replication failed"
ROLE_LINE=$(printf '%s' "${INFO_OUT}" | grep '^role:' | tr -d '\r')
[ "${ROLE_LINE}" = "role:master" ] || publish_failure "${MASTER_POD} not master (${ROLE_LINE})"
MASTER_REPLID=$(printf '%s' "${INFO_OUT}" | grep '^master_replid:' | head -1 | cut -d: -f2 | tr -d '\r ')
MASTER_REPL_OFFSET=$(printf '%s' "${INFO_OUT}" | grep '^master_repl_offset:' | head -1 | cut -d: -f2 | tr -d '\r ')
case "${MASTER_REPLID}" in
  *[!0-9a-f]*|"") publish_failure "could not parse master_replid (got '${MASTER_REPLID}')" ;;
esac
case "${MASTER_REPL_OFFSET}" in
  ''|*[!0-9]*) publish_failure "could not parse master_repl_offset" ;;
esac
echo "[backup] step: master_replid=${MASTER_REPLID} offset=${MASTER_REPL_OFFSET}"

echo "[backup] step: CLIENT PAUSE WRITE for atomic point-in-time capture"
# Pause incoming writes on master for 30s (read commands and admin
# commands like BGSAVE / SYNC are unaffected). With writes paused,
# the keyspace is frozen for the duration of the backup window:
#   - BGSAVE forks at T1 with state S0
#   - dump.rdb on disk = S0
#   - --rdb forks at T2 (T2 > T1) but state is still S0 (no writes
#     accepted in [T1, T2])
#   - both forks capture identical state — no gap, no torn writes
# We always issue UNPAUSE on script exit (trap) so a crash doesn't
# leave master frozen.
trap 'valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning CLIENT UNPAUSE >/dev/null 2>&1 || true' EXIT INT TERM
valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning CLIENT PAUSE 30000 WRITE >/dev/null 2>&1 || \
  publish_failure "CLIENT PAUSE failed — point-in-time capture cannot be guaranteed"

echo "[backup] step: BGSAVE"
PREV_LASTSAVE=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
[ -z "${PREV_LASTSAVE}" ] && PREV_LASTSAVE=0
BGSAVE_OUT=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning BGSAVE 2>&1)
BGSAVE_RC=$?
if [ "${BGSAVE_RC}" != "0" ] && ! printf '%s' "${BGSAVE_OUT}" | grep -qi "in progress"; then
  publish_failure "BGSAVE failed (rc=${BGSAVE_RC}): ${BGSAVE_OUT}"
fi

echo "[backup] step: wait for LASTSAVE advance (BGSAVE completes; dump.rdb on disk)"
NEW_LASTSAVE=${PREV_LASTSAVE}
i=0
while [ $i -lt 25 ]; do
  CURR=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
  if [ -n "${CURR}" ] && [ "${CURR}" -gt "${PREV_LASTSAVE}" ] 2>/dev/null; then
    NEW_LASTSAVE=${CURR}; break
  fi
  sleep 1; i=$((i+1))
done
[ "${NEW_LASTSAVE}" = "${PREV_LASTSAVE}" ] && publish_failure "LASTSAVE did not advance after BGSAVE"

echo "[backup] step: --rdb fetch (writes paused; --rdb captures same frozen state)"
valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning --rdb "${OUT}" >/tmp/rdb.log 2>&1
RDB_RC=$?
[ "${RDB_RC}" != "0" ] && publish_failure "valkey-cli --rdb failed (rc=${RDB_RC}): $(head -c 200 /tmp/rdb.log)"
[ -f "${OUT}" ] || publish_failure "RDB file not written"

echo "[backup] step: CLIENT UNPAUSE (release writes)"
valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning CLIENT UNPAUSE >/dev/null 2>&1 || true
trap - EXIT INT TERM
MAGIC=$(head -c 5 "${OUT}" 2>/dev/null || true)
SIZE=$(wc -c < "${OUT}" | tr -d '[:space:]')
RDB_SHA256=$(sha256sum "${OUT}" 2>/dev/null | awk '{print $1}')
[ "${MAGIC}" = "REDIS" ] || publish_failure "RDB does not start with REDIS"
[ "${SIZE}" -lt 200 ] 2>/dev/null && publish_failure "RDB too small (${SIZE})"
[ -z "${RDB_SHA256}" ] && publish_failure "could not compute sha256"

echo "[backup] step: DBSIZE"
DBSIZE=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning DBSIZE 2>/dev/null | tr -d '[:space:]')
[ -z "${DBSIZE}" ] || [ "${DBSIZE}" = "0" ] 2>/dev/null && publish_failure "DBSIZE 0"

echo "[backup] step: persist artifact to archive PVC at ${ARCHIVE_MOUNT}"
ARTIFACT_PATH="/dump-${TS}.rdb"
cp "${OUT}" "${ARCHIVE_MOUNT}${ARTIFACT_PATH}" || \
  publish_failure "could not copy artifact to archive PVC mount ${ARCHIVE_MOUNT}"
sync || true

echo "[backup] step: generate fresh Ed25519 keypair (rotates per run)"
openssl genpkey -algorithm Ed25519 -out /tmp/sign.pem 2>/tmp/genpkey.err || \
  publish_failure "openssl genpkey Ed25519 failed: $(head -c 200 /tmp/genpkey.err)"
# DER-encoded SubjectPublicKeyInfo for Ed25519 is 44 bytes; the last 32
# are the raw public key. base64 those as the wire format.
openssl pkey -in /tmp/sign.pem -pubout -outform DER -out /tmp/sign.pub.der 2>/dev/null
PK_RAW_FILE=/tmp/sign.pub.raw
tail -c 32 /tmp/sign.pub.der > "${PK_RAW_FILE}"
PK_B64=$(base64 -w0 < "${PK_RAW_FILE}" 2>/dev/null || base64 < "${PK_RAW_FILE}" | tr -d '\n')
PK_FP=$(sha256sum "${PK_RAW_FILE}" | awk '{print $1}')

echo "[backup] step: build canonical payload + sign"
# Canonical JSON: keys sorted lexicographically with compact separators.
# The order is: artifact_sha256, artifact_source_pod, artifact_source_role, backup_id, snapshot_epoch
CANON=/tmp/canon.bin
printf '{"artifact_sha256":"%s","artifact_source_pod":"%s","artifact_source_role":"%s","backup_id":"%s","snapshot_epoch":%s}' \
  "${RDB_SHA256}" "${ARTIFACT_SOURCE_POD}" "${ARTIFACT_SOURCE_ROLE}" "${BACKUP_ID}" "${NEW_LASTSAVE}" > "${CANON}"
SIG_FILE=/tmp/sign.sig
openssl pkeyutl -sign -inkey /tmp/sign.pem -rawin -in "${CANON}" -out "${SIG_FILE}" 2>/tmp/sign.err || \
  publish_failure "openssl sign failed: $(head -c 200 /tmp/sign.err)"
SIG_B64=$(base64 -w0 < "${SIG_FILE}" 2>/dev/null || base64 < "${SIG_FILE}" | tr -d '\n')

echo "[backup] step: register public-key fingerprint in trust anchor"
# Read existing fingerprints and append ours (newline-separated).
EXISTING=$(curl_with_libs -sS --cacert "${CA}" \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://kubernetes.default.svc/api/v1/namespaces/${NS}/configmaps/${TRUST_CM}" 2>/dev/null \
  | sed -n 's/.*"public_key_fingerprints"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
# The CM stores it as a single string with embedded \n escapes; turn back into newlines.
EXISTING_PLAIN=$(printf '%b' "${EXISTING}" | tr -d '\r')
NEW_LIST_FILE=/tmp/trust-fps.txt
{
  printf '%s\n' "${EXISTING_PLAIN}" | grep -v '^$' || true
  printf '%s\n' "${PK_FP}"
} > "${NEW_LIST_FILE}"
patch_cm "${TRUST_CM}" "public_key_fingerprints" "${NEW_LIST_FILE}" || \
  publish_failure "could not register public-key fingerprint in trust anchor"

echo "[backup] step: publish status + signed handoff"
cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: SUCCESS**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot captured at: ${NOW_UTC} (epoch=${NEW_LASTSAVE})
- Artifact bytes: ${SIZE}
- Artifact sha256: ${RDB_SHA256}
- Artifact location: pvc/${ARCHIVE_PVC:-glitchtip-valkey-backup-archive}${ARTIFACT_PATH}
- Artifact source pod: ${ARTIFACT_SOURCE_POD} (role=${ARTIFACT_SOURCE_ROLE})
- master_replid: ${MASTER_REPLID}
- master_repl_offset: ${MASTER_REPL_OFFSET}
- Signing key fingerprint: ${PK_FP}
- Restore proof: key_count_at_snapshot=${DBSIZE}, lastsave=${PREV_LASTSAVE}->${NEW_LASTSAVE}
SEOF
patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || publish_failure "failed to publish status"

cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"success","safe_for_restore":true,"snapshot_epoch":${NEW_LASTSAVE},"artifact_bytes":${SIZE},"artifact_sha256":"${RDB_SHA256}","artifact_location":{"type":"pvc","claim":"glitchtip-valkey-backup-archive","path":"${ARTIFACT_PATH}","namespace":"${NS}"},"artifact_source_pod":"${ARTIFACT_SOURCE_POD}","artifact_source_role":"${ARTIFACT_SOURCE_ROLE}","master_replid":"${MASTER_REPLID}","master_repl_offset":${MASTER_REPL_OFFSET},"signature":{"alg":"ed25519","public_key":"${PK_B64}","sig":"${SIG_B64}"},"signed_at_epoch":${NOW_EPOCH},"restore_proof":{"key_count_at_snapshot":${DBSIZE},"rdb_magic_ok":true,"lastsave_before":${PREV_LASTSAVE},"lastsave_after":${NEW_LASTSAVE}},"reason":"signed master-snapshot, fresh BGSAVE, REDIS-magic verified"}
HEOF
patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || publish_failure "failed to publish handoff"

echo "[backup] ${BACKUP_ID}: SUCCESS (size=${SIZE}, sha=${RDB_SHA256:0:12}, replid=${MASTER_REPLID:0:12}, fp=${PK_FP:0:12})"
SCRIPT_EOF

# Order matters: update the BASELINEs first so the next reconciler tick
# reapplies our fixed script + scoped SA, then update the active
# resources for immediate effect.
echo "[solution] Updating script-baseline (drift reconciler source) first..."
kubectl create configmap ${SCRIPT_BASELINE_CM} -n ${NS} \
  --from-file=backup.sh=/tmp/valkey-backup-fixed.sh \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[solution] Updating template-baseline (template manager source)..."
kubectl create configmap ${TEMPLATE_BASELINE_CM} -n ${NS} \
  --from-literal=serviceAccountName=${SA} \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[solution] Installing fixed script as the active script ConfigMap..."
kubectl create configmap ${SCRIPT_CM} -n ${NS} \
  --from-file=backup.sh=/tmp/valkey-backup-fixed.sh \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[solution] Creating scoped ServiceAccount and Role for the backup job..."
kubectl apply -f - <<RBAC_EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SA}
  namespace: ${NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ${SA}-writer
  namespace: ${NS}
rules:
# Narrow patch access — only the two runtime status surfaces and the
# trust-anchor CM (for public_key_fingerprints registration).
- apiGroups: [""]
  resources: ["configmaps"]
  resourceNames: ["${STATUS_CM}", "${HANDOFF_CM}", "${TRUST_CM}"]
  verbs: ["get", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ${SA}-writer-binding
  namespace: ${NS}
subjects:
- kind: ServiceAccount
  name: ${SA}
  namespace: ${NS}
roleRef:
  kind: Role
  name: ${SA}-writer
  apiGroup: rbac.authorization.k8s.io
RBAC_EOF

echo "[solution] Patching the CronJob: scoped SA + PSS-restricted securityContext on backup container + archive PVC mount + writable emptyDirs..."
# Pod-level: fsGroup only (so the PVC mount is writable by the
# non-root backup container). Init containers continue at the image
# default user (root) to copy binaries into /tools — they don't
# touch the PVC.
# Backup container: full PSS-restricted profile.
kubectl patch cronjob ${CRONJOB} -n ${NS} --type strategic -p "$(cat <<PATCH
{
  "spec": {
    "jobTemplate": {
      "spec": {
        "template": {
          "spec": {
            "serviceAccountName": "${SA}",
            "securityContext": {
              "fsGroup": 1001
            },
            "containers": [
              {
                "name": "backup",
                "securityContext": {
                  "runAsNonRoot": true,
                  "runAsUser": 1001,
                  "runAsGroup": 1001,
                  "allowPrivilegeEscalation": false,
                  "readOnlyRootFilesystem": true,
                  "capabilities": {"drop": ["ALL"]},
                  "seccompProfile": {"type": "RuntimeDefault"}
                },
                "volumeMounts": [
                  {"name": "script", "mountPath": "/scripts"},
                  {"name": "storage", "mountPath": "/backups"},
                  {"name": "tools", "mountPath": "/tools"},
                  {"name": "tmpdir", "mountPath": "/tmp"},
                  {"name": "archive", "mountPath": "/archive"}
                ]
              }
            ],
            "volumes": [
              {"name": "script", "configMap": {"name": "${SCRIPT_CM}", "defaultMode": 493}},
              {"name": "storage", "emptyDir": {}},
              {"name": "tools", "emptyDir": {}},
              {"name": "tmpdir", "emptyDir": {}},
              {"name": "archive", "persistentVolumeClaim": {"claimName": "${ARCHIVE_PVC}"}}
            ]
          }
        }
      }
    }
  }
}
PATCH
)"

echo "[solution] Done. Backup CronJob: PSS-restricted, master-pinned, signed handoff, archive-PVC artifact, replid+offset attestation, key rotation per run."
