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
export PATH="/tools:${PATH}"
export LD_LIBRARY_PATH="/tools/lib:${LD_LIBRARY_PATH:-}"
# Intentionally NOT using `set -e`: the script's whole contract is to fail
# CLOSED by publishing a truthful failure handoff/status, not to die silently.
# With `set -e`, a failing command substitution (e.g. BGSAVE_OUT=$(valkey-cli
# ... 2>&1) when Valkey is scaled to 0) kills the script BEFORE the explicit
# error check + publish_failure can run, leaving a stale handoff.
echo "[backup] pod started at $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(hostname)"
echo "[backup] PATH=${PATH}  VALKEY_HOST=${VALKEY_HOST}"
which valkey-cli || true
which curl || true
ls -la /tools/ 2>/dev/null || true
echo "[backup] curl version:"
curl --version 2>&1 | head -2 || echo "[backup] curl failed to run"

# RFC 1123 subdomain (lowercase, hyphens only — no underscores) so that
# `valkey-backup-${TS}` is a valid ConfigMap name.
TS=$(date +%Y%m%d-%H%M%S)
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

# Send a server-side-apply patch using application/apply-patch+yaml.
# YAML's `|` block literal preserves the inner content verbatim, which
# sidesteps every nested-JSON escaping nightmare that merge-patch+json
# imposes on a busybox/alpine shell. The `content_file` is the raw
# value (markdown or JSON) that ends up as `data.<key>`.
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
    # Indent every line of the value by 4 spaces (YAML block literal).
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
    _body_head=$(head -c 400 "${bf}" 2>/dev/null)
    _resp_head=$(printf '%s' "${_out}" | head -c 400)
    echo "[backup] patch_cm ${cm}/${key} FAIL rc=${_rc} http=${_http}"
    echo "[backup]   body head: ${_body_head}"
    echo "[backup]   response head: ${_resp_head}"
    return 1
  fi
  rm -f "${bf}" 2>/dev/null || true
  return 0
}

publish_failure() {
  reason="$1"
  # Sanitize the reason so we can embed it in a JSON string literal
  # without any escaping — strip quotes/backslashes/newlines.
  reason_clean=$(printf '%s' "${reason}" | tr '"\\' "''" | tr '\n' ' ' | head -c 250)
  cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"failed","safe_for_restore":false,"snapshot_epoch":0,"artifact_bytes":0,"artifact_location":null,"restore_proof":null,"reason":"${reason_clean}"}
HEOF
  patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || true
  cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: FAILED**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot timestamp: (not produced)
- Artifact bytes: 0
- Restore proof: (not produced)
- Reason: ${reason_clean}
SEOF
  patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || true
  echo "[backup] FAILED: ${reason_clean}"
  exit 1
}

echo "[backup] step: PING wait"
i=0
while [ $i -lt 30 ]; do
  if valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning PING 2>/dev/null \
       | grep -q PONG; then
    break
  fi
  sleep 2
  i=$((i+1))
done
echo "[backup] step: PING ready after ${i} attempts"

echo "[backup] step: reading PREV_LASTSAVE"
PREV_LASTSAVE=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning LASTSAVE 2>/dev/null | tr -d '[:space:]')
[ -z "${PREV_LASTSAVE}" ] && PREV_LASTSAVE=0
echo "[backup] step: PREV_LASTSAVE=${PREV_LASTSAVE}"

echo "[backup] step: BGSAVE"
BGSAVE_OUT=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning BGSAVE 2>&1)
BGSAVE_RC=$?
echo "[backup] step: BGSAVE rc=${BGSAVE_RC} out=${BGSAVE_OUT}"
if [ "${BGSAVE_RC}" != "0" ] && ! printf '%s' "${BGSAVE_OUT}" | grep -qi "in progress"; then
  publish_failure "BGSAVE failed (rc=${BGSAVE_RC}): ${BGSAVE_OUT}"
fi

echo "[backup] step: waiting for LASTSAVE advance"
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
echo "[backup] step: LASTSAVE advanced: ${PREV_LASTSAVE} -> ${NEW_LASTSAVE} (after ${i}s)"
if [ "${NEW_LASTSAVE}" = "${PREV_LASTSAVE}" ]; then
  publish_failure "LASTSAVE did not advance after BGSAVE within 45s (prev=${PREV_LASTSAVE})"
fi

echo "[backup] step: fetching RDB via --rdb into ${OUT}"
RDB_LOG=/tmp/rdb.log
valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning --rdb "${OUT}" >"${RDB_LOG}" 2>&1
RDB_RC=$?
echo "[backup] step: --rdb rc=${RDB_RC} log='$(head -c 200 "${RDB_LOG}" 2>/dev/null)'"
if [ "${RDB_RC}" != "0" ]; then
  publish_failure "valkey-cli --rdb failed (rc=${RDB_RC}): $(head -c 200 "${RDB_LOG}" 2>/dev/null)"
fi
if [ ! -f "${OUT}" ]; then
  publish_failure "RDB file was not written"
fi

echo "[backup] step: validate magic + size"
MAGIC=$(head -c 5 "${OUT}" 2>/dev/null || true)
SIZE=$(wc -c < "${OUT}" | tr -d '[:space:]')
echo "[backup] step: magic='${MAGIC}' size=${SIZE}"
[ "${MAGIC}" = "REDIS" ] || publish_failure "RDB does not start with REDIS magic bytes"
if [ "${SIZE}" -lt 200 ] 2>/dev/null; then
  publish_failure "RDB too small (${SIZE} bytes)"
fi

echo "[backup] step: reading DBSIZE"
DBSIZE=$(valkey-cli -h "${VALKEY_HOST}" -p 6379 --no-auth-warning DBSIZE 2>/dev/null | tr -d '[:space:]')
echo "[backup] step: DBSIZE=${DBSIZE}"
if [ -z "${DBSIZE}" ] || [ "${DBSIZE}" = "0" ] 2>/dev/null; then
  publish_failure "Valkey DBSIZE is 0 — nothing to back up"
fi

echo "[backup] step: base64-encoding RDB"
ARTIFACT_CM="valkey-backup-${TS}"
RDB_B64=$(base64 -w 0 "${OUT}" 2>/dev/null || base64 "${OUT}" | tr -d '\n')
echo "[backup] step: base64 size=${#RDB_B64}"

echo "[backup] step: building body file"
ART_BODY_FILE=/tmp/art-body-${TS}.json
printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"%s","labels":{"app":"glitchtip","component":"valkey-backup-artifact","backup-id":"%s"}},"binaryData":{"dump.rdb":"%s"}}' \
  "${ARTIFACT_CM}" "${NS}" "${BACKUP_ID}" "${RDB_B64}" > "${ART_BODY_FILE}"
BODY_SIZE=$(wc -c < "${ART_BODY_FILE}")
echo "[backup] step: body file=${ART_BODY_FILE} size=${BODY_SIZE}"

echo "[backup] step: POST artifact CM ${ARTIFACT_CM}"
ART_RESPONSE_FILE=/tmp/art-response-${TS}.txt
ART_ERR_FILE=/tmp/art-err-${TS}.txt
HTTP_CODE=0
CURL_RC=0
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
echo "[backup] step: POST stderr: $(head -c 400 "${ART_ERR_FILE}" 2>/dev/null)"
echo "[backup] step: POST response head: $(head -c 300 "${ART_RESPONSE_FILE}" 2>/dev/null)"
if [ "${CURL_RC}" != "0" ] || { [ "${HTTP_CODE}" != "201" ] && [ "${HTTP_CODE}" != "200" ]; }; then
  SNIPPET="curl_rc=${CURL_RC} http=${HTTP_CODE} stderr=$(head -c 300 "${ART_ERR_FILE}" 2>/dev/null) body=$(head -c 200 "${ART_RESPONSE_FILE}" 2>/dev/null)"
  publish_failure "artifact POST failed: ${SNIPPET}"
fi
echo "[backup] step: artifact persisted"

# 6) Publish truthful status + handoff.
# Write success surfaces as plain files; YAML server-side-apply (see
# patch_cm) embeds them verbatim via `|` block literal.
cat > /tmp/status-body.md <<SEOF
# Valkey backup run ${BACKUP_ID}
**Status: SUCCESS**
- Timestamp: ${NOW_UTC}
- Backup ID: ${BACKUP_ID}
- Snapshot timestamp: epoch=${NEW_LASTSAVE}
- Artifact bytes: ${SIZE}
- Artifact location: configmap/${ARTIFACT_CM}/dump.rdb
- Restore proof: key_count_at_snapshot=${DBSIZE}, magic=REDIS, lastsave_advanced=${PREV_LASTSAVE}->${NEW_LASTSAVE}
SEOF
patch_cm "${STATUS_CM}" "status.md" "/tmp/status-body.md" || publish_failure "failed to publish status"

cat > /tmp/handoff-body.json <<HEOF
{"latest_run":"${NOW_UTC}","backup_id":"${BACKUP_ID}","result":"success","safe_for_restore":true,"snapshot_epoch":${NEW_LASTSAVE},"artifact_bytes":${SIZE},"artifact_location":{"type":"configmap","name":"${ARTIFACT_CM}","key":"dump.rdb","namespace":"${NS}"},"restore_proof":{"key_count_at_snapshot":${DBSIZE},"rdb_magic_ok":true,"lastsave_before":${PREV_LASTSAVE},"lastsave_after":${NEW_LASTSAVE}},"reason":"backup produced a fresh snapshot with verified magic bytes and non-zero key count"}
HEOF
patch_cm "${HANDOFF_CM}" "handoff.json" "/tmp/handoff-body.json" || publish_failure "failed to publish handoff"

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
