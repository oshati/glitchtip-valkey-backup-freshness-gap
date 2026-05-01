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
SA=valkey-backup-publisher

# IN-PLACE FIX: the drift reconciler + template manager CronJobs are
# part of the supported job-management flow, so we leave them running
# and instead overwrite the baselines they reapply each minute (the
# script-baseline + template-baseline ConfigMaps below). That way the
# next reconcile tick keeps publishing OUR fixed script + scoped SA
# instead of reverting them.
echo "[solution] Writing the fixed backup script..."
cat > /tmp/valkey-backup-fixed.sh <<'SCRIPT_EOF'
#!/bin/sh
# Trustworthy Valkey backup, master-pinned:
#  - resolves the writable master pod via INFO replication (StatefulSet
#    ord=0 is the master; replicas are read-only and lag),
#  - connects only to that master pod's stable DNS,
#  - forces a fresh snapshot (BGSAVE + LASTSAVE advance),
#  - validates REDIS magic bytes, size, and sha256,
#  - persists the artifact as a binaryData ConfigMap (durable in-cluster),
#  - refreshes status.md and handoff.json with real metadata
#    including artifact_source_pod / artifact_source_role,
#  - fails closed on every error path with safe_for_restore=false.
exec 2>&1
export PATH="/tools:${PATH}"
export LD_LIBRARY_PATH="/tools/lib:${LD_LIBRARY_PATH:-}"
# Intentionally NOT using `set -e`: this script's contract is to fail
# CLOSED by publishing a truthful failure handoff/status, not to die
# silently. With `set -e` a failing command substitution kills the
# script BEFORE publish_failure can run.
echo "[backup] pod started at $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(hostname)"
echo "[backup] PATH=${PATH}  VALKEY_HOST=${VALKEY_HOST}"

# RFC 1123 subdomain (lowercase, hyphens only — no underscores) so that
# `valkey-backup-${TS}` is a valid ConfigMap name.
TS=$(date +%Y%m%d-%H%M%S)
BACKUP_ID="valkey-${TS}"
OUT="/backups/dump-${TS}.rdb"
mkdir -p /backups
NOW_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
NS=glitchtip
STATUS_CM=glitchtip-valkey-backup-status
HANDOFF_CM=glitchtip-valkey-backup-restore-handoff

# Pod-stable DNS for the master ordinal. The headless service +
# StatefulSet contract guarantees pod-N has FQDN
# valkey-runtime-state-N.valkey-runtime-state.glitchtip.svc.cluster.local
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
  _out=$(curl -sS --cacert "${CA}" \
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
  reason_clean=$(printf '%s' "${reason}" | tr '"\\' "''" | tr '\n' ' ' | head -c 250)
  cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"failed","safe_for_restore":false,"snapshot_epoch":0,"artifact_bytes":0,"artifact_sha256":null,"artifact_location":null,"artifact_source_pod":null,"artifact_source_role":null,"restore_proof":null,"reason":"${reason_clean}"}
HEOF
  patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || true
  cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: FAILED**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot timestamp: (not produced)
- Artifact bytes: 0
- Artifact source: (not produced)
- Restore proof: (not produced)
- Reason: ${reason_clean}
SEOF
  patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || true
  echo "[backup] FAILED: ${reason_clean}"
  exit 1
}

echo "[backup] step: PING master ${MASTER_HOST}"
i=0
while [ $i -lt 30 ]; do
  if valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning PING 2>/dev/null \
       | grep -q PONG; then
    break
  fi
  sleep 2
  i=$((i+1))
done
if [ $i -ge 30 ]; then
  publish_failure "master pod ${MASTER_POD} did not answer PING within 60s"
fi
echo "[backup] step: PING ready after ${i} attempts"

echo "[backup] step: verifying master role via INFO replication"
INFO_OUT=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning INFO replication 2>&1)
INFO_RC=$?
if [ "${INFO_RC}" != "0" ]; then
  publish_failure "INFO replication failed on ${MASTER_POD} (rc=${INFO_RC})"
fi
ROLE_LINE=$(printf '%s' "${INFO_OUT}" | grep -E '^role:' | tr -d '\r')
echo "[backup] step: ${MASTER_POD} reports ${ROLE_LINE}"
if [ "${ROLE_LINE}" != "role:master" ]; then
  publish_failure "${MASTER_POD} is not master (${ROLE_LINE}) — refusing to back up from a replica"
fi

echo "[backup] step: reading PREV_LASTSAVE"
PREV_LASTSAVE=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
[ -z "${PREV_LASTSAVE}" ] && PREV_LASTSAVE=0
echo "[backup] step: PREV_LASTSAVE=${PREV_LASTSAVE}"

echo "[backup] step: BGSAVE on ${MASTER_POD}"
BGSAVE_OUT=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning BGSAVE 2>&1)
BGSAVE_RC=$?
echo "[backup] step: BGSAVE rc=${BGSAVE_RC} out=${BGSAVE_OUT}"
if [ "${BGSAVE_RC}" != "0" ] && ! printf '%s' "${BGSAVE_OUT}" | grep -qi "in progress"; then
  publish_failure "BGSAVE failed (rc=${BGSAVE_RC}): ${BGSAVE_OUT}"
fi

echo "[backup] step: waiting for LASTSAVE advance"
NEW_LASTSAVE=${PREV_LASTSAVE}
i=0
while [ $i -lt 45 ]; do
  CURR=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
  if [ -n "${CURR}" ] && [ "${CURR}" -gt "${PREV_LASTSAVE}" ] 2>/dev/null; then
    NEW_LASTSAVE=${CURR}
    break
  fi
  sleep 1
  i=$((i+1))
done
echo "[backup] step: LASTSAVE advanced: ${PREV_LASTSAVE} -> ${NEW_LASTSAVE} (after ${i}s)"
if [ "${NEW_LASTSAVE}" = "${PREV_LASTSAVE}" ]; then
  publish_failure "LASTSAVE did not advance after BGSAVE within 45s (prev=${PREV_LASTSAVE})"
fi

echo "[backup] step: fetching RDB via --rdb from ${MASTER_POD} into ${OUT}"
RDB_LOG=/tmp/rdb.log
valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning --rdb "${OUT}" >"${RDB_LOG}" 2>&1
RDB_RC=$?
echo "[backup] step: --rdb rc=${RDB_RC} log='$(head -c 200 "${RDB_LOG}" 2>/dev/null)'"
if [ "${RDB_RC}" != "0" ]; then
  publish_failure "valkey-cli --rdb failed (rc=${RDB_RC}): $(head -c 200 "${RDB_LOG}" 2>/dev/null)"
fi
[ -f "${OUT}" ] || publish_failure "RDB file was not written"

echo "[backup] step: validate magic + size + sha256"
MAGIC=$(head -c 5 "${OUT}" 2>/dev/null || true)
SIZE=$(wc -c < "${OUT}" | tr -d '[:space:]')
RDB_SHA256=$(sha256sum "${OUT}" 2>/dev/null | awk '{print $1}')
[ -z "${RDB_SHA256}" ] && RDB_SHA256=$(openssl dgst -sha256 -r "${OUT}" 2>/dev/null | awk '{print $1}')
echo "[backup] step: magic='${MAGIC}' size=${SIZE} sha256=${RDB_SHA256}"
[ "${MAGIC}" = "REDIS" ] || publish_failure "RDB does not start with REDIS magic bytes"
if [ "${SIZE}" -lt 200 ] 2>/dev/null; then
  publish_failure "RDB too small (${SIZE} bytes)"
fi
if [ -z "${RDB_SHA256}" ]; then
  publish_failure "could not compute artifact sha256"
fi

echo "[backup] step: reading DBSIZE on master"
DBSIZE=$(valkey-cli -h "${MASTER_HOST}" -p 6379 --no-auth-warning DBSIZE 2>/dev/null | tr -d '[:space:]')
echo "[backup] step: DBSIZE=${DBSIZE}"
if [ -z "${DBSIZE}" ] || [ "${DBSIZE}" = "0" ] 2>/dev/null; then
  publish_failure "Valkey DBSIZE is 0 — nothing to back up"
fi

echo "[backup] step: base64-encoding RDB"
ARTIFACT_CM="valkey-backup-${TS}"
RDB_B64=$(base64 -w 0 "${OUT}" 2>/dev/null || base64 "${OUT}" | tr -d '\n')

echo "[backup] step: building body file"
ART_BODY_FILE=/tmp/art-body-${TS}.json
printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"%s","labels":{"app":"glitchtip","component":"valkey-backup-artifact","backup-id":"%s"}},"binaryData":{"dump.rdb":"%s"}}' \
  "${ARTIFACT_CM}" "${NS}" "${BACKUP_ID}" "${RDB_B64}" > "${ART_BODY_FILE}"

echo "[backup] step: POST artifact CM ${ARTIFACT_CM}"
ART_RESPONSE_FILE=/tmp/art-response-${TS}.txt
ART_ERR_FILE=/tmp/art-err-${TS}.txt
HTTP_CODE=0; CURL_RC=0
HTTP_CODE=$(curl -sS --cacert "${CA}" \
  -X POST \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary "@${ART_BODY_FILE}" \
  -o "${ART_RESPONSE_FILE}" \
  -w '%{http_code}' \
  "https://kubernetes.default.svc/api/v1/namespaces/${NS}/configmaps" \
  2>"${ART_ERR_FILE}") || CURL_RC=$?
echo "[backup] step: POST curl_rc=${CURL_RC} http_code=${HTTP_CODE}"
if [ "${CURL_RC}" != "0" ] || { [ "${HTTP_CODE}" != "201" ] && [ "${HTTP_CODE}" != "200" ]; }; then
  SNIPPET="curl_rc=${CURL_RC} http=${HTTP_CODE} stderr=$(head -c 300 "${ART_ERR_FILE}" 2>/dev/null) body=$(head -c 200 "${ART_RESPONSE_FILE}" 2>/dev/null)"
  publish_failure "artifact POST failed: ${SNIPPET}"
fi
echo "[backup] step: artifact persisted"

# Publish truthful status + handoff with master-pinning evidence.
cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: SUCCESS**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot captured at: ${NOW_UTC} (epoch=${NEW_LASTSAVE})
- Artifact bytes: ${SIZE}
- Artifact sha256: ${RDB_SHA256}
- Artifact location: configmap/${ARTIFACT_CM}/dump.rdb
- Artifact source pod: ${ARTIFACT_SOURCE_POD} (role=${ARTIFACT_SOURCE_ROLE})
- Restore proof: key_count_at_snapshot=${DBSIZE}, rdb_magic_ok=true, lastsave_advanced=${PREV_LASTSAVE}->${NEW_LASTSAVE}
SEOF
patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || publish_failure "failed to publish status"

cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"success","safe_for_restore":true,"snapshot_epoch":${NEW_LASTSAVE},"artifact_bytes":${SIZE},"artifact_sha256":"${RDB_SHA256}","artifact_location":{"type":"configmap","name":"${ARTIFACT_CM}","key":"dump.rdb","namespace":"${NS}"},"artifact_source_pod":"${ARTIFACT_SOURCE_POD}","artifact_source_role":"${ARTIFACT_SOURCE_ROLE}","restore_proof":{"key_count_at_snapshot":${DBSIZE},"rdb_magic_ok":true,"lastsave_before":${PREV_LASTSAVE},"lastsave_after":${NEW_LASTSAVE}},"reason":"backup produced a fresh snapshot from master with verified magic bytes and sha256-checksummed bytes"}
HEOF
patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || publish_failure "failed to publish handoff"

echo "[backup] ${BACKUP_ID}: SUCCESS (size=${SIZE}, sha=${RDB_SHA256:0:12}, keys=${DBSIZE}, lastsave ${PREV_LASTSAVE}->${NEW_LASTSAVE}, source=${ARTIFACT_SOURCE_POD}/${ARTIFACT_SOURCE_ROLE})"
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
# Narrow patch access — only the two runtime status surfaces.
- apiGroups: [""]
  resources: ["configmaps"]
  resourceNames: ["${STATUS_CM}", "${HANDOFF_CM}"]
  verbs: ["get", "patch", "update"]
# Allowed to create artifact ConfigMaps (valkey-backup-<id>) in this namespace.
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["create"]
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

echo "[solution] Patching the CronJob to use the scoped ServiceAccount..."
kubectl patch cronjob ${CRONJOB} -n ${NS} --type merge -p "$(cat <<PATCH
{"spec":{"jobTemplate":{"spec":{"template":{"spec":{"serviceAccountName":"${SA}"}}}}}}
PATCH
)"

echo "[solution] Done. Backup CronJob now: pins to master pod, validates role:master, captures fresh BGSAVE, persists durable artifact, publishes truthful handoff with artifact_source_pod/role and strict-monotonic snapshot_epoch."
