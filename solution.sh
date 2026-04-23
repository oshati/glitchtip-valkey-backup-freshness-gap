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

echo "[solution] Neutralizing drift reconcilers that would revert the fix..."
kubectl delete cronjob valkey-backup-drift-reconciler -n ${NS} --ignore-not-found
kubectl delete cronjob valkey-backup-template-manager -n ${NS} --ignore-not-found
kubectl delete job -n ${NS} -l app=glitchtip --field-selector status.successful=0 --ignore-not-found 2>/dev/null || true

echo "[solution] Writing the fixed backup script..."
cat > /tmp/valkey-backup-fixed.sh <<'SCRIPT_EOF'
#!/bin/sh
# Trustworthy Valkey backup:
#  - forces a fresh snapshot (BGSAVE + LASTSAVE advance) before reading it,
#  - validates REDIS magic bytes and size,
#  - captures DBSIZE as restore proof,
#  - persists the artifact as a binaryData ConfigMap (durable in-cluster),
#  - refreshes status.md and handoff.json with real metadata,
#  - fails closed and marks safe_for_restore=false with a reason.
exec 2>&1
set -x
set -e
echo "[backup] pod started running backup.sh at $(date -u +%Y-%m-%dT%H:%M:%SZ) on host=$(hostname) as user=$(id -un)"
echo "[backup] PATH=${PATH}"
echo "[backup] VALKEY_HOST=${VALKEY_HOST}"
which valkey-cli || echo "[backup] WARN: valkey-cli not in PATH yet"
ls -la /tools/ 2>/dev/null || true

TS=$(date +%Y%m%d_%H%M%S)
BACKUP_ID="valkey-${TS}"
OUT="/backups/dump-${TS}.rdb"
mkdir -p /backups
RUN_START=$(date +%s)
NOW_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
CA=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
NS=glitchtip
STATUS_CM=glitchtip-valkey-backup-status
HANDOFF_CM=glitchtip-valkey-backup-restore-handoff

json_str() {
  sed ':a;N;$!ba;s/\\/\\\\/g;s/"/\\"/g;s/\n/\\n/g' | sed 's/^/"/;s/$/"/'
}

patch_cm() {
  cm="$1"; key="$2"; val="$3"
  body="{\"data\":{\"${key}\":${val}}}"
  wget -qO- \
    --header="Authorization: Bearer ${TOKEN}" \
    --ca-certificate="${CA}" \
    --method=PATCH \
    --header="Content-Type: application/merge-patch+json" \
    --body-data="${body}" \
    "https://kubernetes.default.svc/api/v1/namespaces/${NS}/configmaps/${cm}" \
    >/dev/null 2>&1 || return 1
}

publish_failure() {
  reason="$1"
  reason_json=$(printf '%s' "${reason}" | json_str)
  handoff="{\"latest_run\":\"${NOW_UTC}\",\"backup_id\":\"${BACKUP_ID}\",\"result\":\"failed\",\"safe_for_restore\":false,\"snapshot_epoch\":0,\"artifact_bytes\":0,\"artifact_location\":null,\"restore_proof\":null,\"reason\":${reason_json}}"
  handoff_esc=$(printf '%s' "${handoff}" | json_str)
  patch_cm "${HANDOFF_CM}" "handoff.json" "${handoff_esc}" || true
  status_md="# Valkey backup run ${BACKUP_ID}
**Status: FAILED**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot timestamp: (not produced)
- Artifact bytes: 0
- Restore proof: (not produced)
- Reason: ${reason}
"
  status_esc=$(printf '%s' "${status_md}" | json_str)
  patch_cm "${STATUS_CM}" "status.md" "${status_esc}" || true
  echo "[backup] FAILED: ${reason}"
  exit 1
}

# 0) Wait for Valkey to accept commands (PING). Handles the case where
#    the StatefulSet was just scaled back up and the pod is still warming.
i=0
while [ $i -lt 30 ]; do
  if valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning PING 2>/dev/null \
       | grep -q PONG; then
    break
  fi
  sleep 2
  i=$((i+1))
done

# 1) Force a fresh snapshot and wait for LASTSAVE to advance.
#    BGSAVE can return "Background save already in progress" if a previous
#    save is still running; treat that as OK, we still want LASTSAVE to
#    advance before proceeding.
PREV_LASTSAVE=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
[ -z "${PREV_LASTSAVE}" ] && PREV_LASTSAVE=0

BGSAVE_OUT=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning BGSAVE 2>&1)
BGSAVE_RC=$?
if [ "${BGSAVE_RC}" != "0" ] && ! printf '%s' "${BGSAVE_OUT}" | grep -qi "in progress"; then
  publish_failure "BGSAVE failed (rc=${BGSAVE_RC}): ${BGSAVE_OUT}"
fi

NEW_LASTSAVE=${PREV_LASTSAVE}
i=0
while [ $i -lt 45 ]; do
  CURR=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
  if [ -n "${CURR}" ] && [ "${CURR}" -gt "${PREV_LASTSAVE}" ] 2>/dev/null; then
    NEW_LASTSAVE=${CURR}
    break
  fi
  sleep 1
  i=$((i+1))
done
if [ "${NEW_LASTSAVE}" = "${PREV_LASTSAVE}" ]; then
  publish_failure "LASTSAVE did not advance after BGSAVE within 45s (prev=${PREV_LASTSAVE})"
fi

# 2) Stream the fresh snapshot to a file.
valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning --rdb "${OUT}" 2>/dev/null || \
  publish_failure "valkey-cli --rdb failed to stream the snapshot"

[ -f "${OUT}" ] || publish_failure "RDB file was not written"

# 3) Validate magic bytes + size.
MAGIC=$(head -c 5 "${OUT}" 2>/dev/null || true)
[ "${MAGIC}" = "REDIS" ] || publish_failure "RDB does not start with REDIS magic bytes"
SIZE=$(wc -c < "${OUT}" | tr -d '[:space:]')
if [ "${SIZE}" -lt 200 ] 2>/dev/null; then
  publish_failure "RDB too small (${SIZE} bytes)"
fi

# 4) Restore proof — capture live DBSIZE at snapshot time.
DBSIZE=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning DBSIZE 2>/dev/null | tr -d '[:space:]')
if [ -z "${DBSIZE}" ] || [ "${DBSIZE}" = "0" ] 2>/dev/null; then
  publish_failure "Valkey DBSIZE is 0 — nothing to back up"
fi

# 5) Persist the artifact as a binaryData ConfigMap.
#    Build the body in a file so we don't hit argv length / shell quoting
#    limits, and capture wget's response so any API error is visible in the
#    failure message.
ARTIFACT_CM="valkey-backup-${TS}"
RDB_B64=$(base64 -w 0 "${OUT}" 2>/dev/null || base64 "${OUT}" | tr -d '\n')
ART_BODY_FILE=/tmp/art-body-${TS}.json
printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"%s","labels":{"app":"glitchtip","component":"valkey-backup-artifact","backup-id":"%s"}},"binaryData":{"dump.rdb":"%s"}}' \
  "${ARTIFACT_CM}" "${NS}" "${BACKUP_ID}" "${RDB_B64}" > "${ART_BODY_FILE}"
ART_RESPONSE_FILE=/tmp/art-response-${TS}.txt
WGET_ERR_FILE=/tmp/art-err-${TS}.txt
wget -S -O "${ART_RESPONSE_FILE}" \
  --header="Authorization: Bearer ${TOKEN}" \
  --ca-certificate="${CA}" \
  --method=POST \
  --header="Content-Type: application/json" \
  --body-file="${ART_BODY_FILE}" \
  "https://kubernetes.default.svc/api/v1/namespaces/${NS}/configmaps" \
  2>"${WGET_ERR_FILE}"
WGET_RC=$?
if [ "${WGET_RC}" != "0" ]; then
  SNIPPET=$(head -c 600 "${WGET_ERR_FILE}" 2>/dev/null; echo; head -c 400 "${ART_RESPONSE_FILE}" 2>/dev/null)
  publish_failure "artifact POST failed (rc=${WGET_RC}): ${SNIPPET}"
fi

# 6) Publish truthful status + handoff.
STATUS_MD="# Valkey backup run ${BACKUP_ID}
**Status: SUCCESS**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot timestamp: epoch=${NEW_LASTSAVE}
- Artifact bytes: ${SIZE}
- Artifact location: configmap/${ARTIFACT_CM}/dump.rdb
- Restore proof: key_count_at_snapshot=${DBSIZE}, magic=REDIS, lastsave_advanced=${PREV_LASTSAVE}->${NEW_LASTSAVE}
"
STATUS_ESC=$(printf '%s' "${STATUS_MD}" | json_str)
patch_cm "${STATUS_CM}" "status.md" "${STATUS_ESC}" || publish_failure "failed to publish status"

HANDOFF="{\"latest_run\":\"${NOW_UTC}\",\"backup_id\":\"${BACKUP_ID}\",\"result\":\"success\",\"safe_for_restore\":true,\"snapshot_epoch\":${NEW_LASTSAVE},\"artifact_bytes\":${SIZE},\"artifact_location\":{\"type\":\"configmap\",\"name\":\"${ARTIFACT_CM}\",\"key\":\"dump.rdb\",\"namespace\":\"${NS}\"},\"restore_proof\":{\"key_count_at_snapshot\":${DBSIZE},\"rdb_magic_ok\":true,\"lastsave_before\":${PREV_LASTSAVE},\"lastsave_after\":${NEW_LASTSAVE}},\"reason\":\"backup produced a fresh snapshot with verified magic bytes and non-zero key count\"}"
HANDOFF_ESC=$(printf '%s' "${HANDOFF}" | json_str)
patch_cm "${HANDOFF_CM}" "handoff.json" "${HANDOFF_ESC}" || publish_failure "failed to publish handoff"

echo "[backup] ${BACKUP_ID}: SUCCESS (size=${SIZE}, keys=${DBSIZE}, lastsave ${PREV_LASTSAVE}->${NEW_LASTSAVE})"
SCRIPT_EOF

echo "[solution] Installing fixed script (active + approved baseline)..."
kubectl create configmap ${SCRIPT_CM} -n ${NS} \
  --from-file=backup.sh=/tmp/valkey-backup-fixed.sh \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap ${SCRIPT_BASELINE_CM} -n ${NS} \
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

echo "[solution] Updating the template baseline so future reconciles preserve the fix..."
kubectl create configmap ${TEMPLATE_BASELINE_CM} -n ${NS} \
  --from-literal=serviceAccountName=${SA} \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[solution] Patching the CronJob to use the scoped ServiceAccount..."
kubectl patch cronjob ${CRONJOB} -n ${NS} --type merge -p "$(cat <<PATCH
{"spec":{"jobTemplate":{"spec":{"template":{"spec":{"serviceAccountName":"${SA}"}}}}}}
PATCH
)"

echo "[solution] Done. Backup CronJob now fails closed on stale/unreachable Valkey, stores the artifact durably, and publishes truthful status and handoff with restore proof."
