#!/usr/bin/env python3
"""
Grader for glitchtip-valkey-backup-freshness-gap.

4 functional subscores, weight 0.25 each:

1. backup_enforces_snapshot_freshness
   - Trigger a backup run. Valkey LASTSAVE must advance. Backup artifact
     mtime and snapshot_epoch in handoff must be >= run start.
   - Force-failure: scale Valkey replicas to 0. A triggered backup must
     fail closed and the handoff must mark restore unsafe.

2. backup_artifact_durable_and_reusable
   - Handoff declares artifact_location of type configmap/secret/pvc.
   - The artifact is reachable post-run, starts with the "REDIS" RDB magic,
     has non-trivial size, and records a key_count_at_snapshot > 0 that
     is a reasonable fraction of live Valkey DBSIZE.

3. status_and_handoff_surface_truth
   - Both status.md and handoff.json are refreshed per run (markers
     overwritten) with backup_id, snapshot timestamp, artifact size, and
     restore-proof detail. Success sets safe_for_restore=true; forced
     failure sets it false with a reason.

4. repair_survives_reconciliation_and_scoped_rbac
   - After the drift reconciler fires (wait ~90s), a fresh backup still
     produces a valid run. The backup ServiceAccount RBAC is narrow
     (patch status/handoff ConfigMaps only, no cronjob edit, no
     MutatingWebhookConfiguration edit, no broad ConfigMap access).
"""

import json
import os
import re
import shlex
import subprocess
import time


try:
    from apex_arena._types import GradingResult
except ImportError:
    class GradingResult:
        def __init__(self, score, subscores=None, weights=None, feedback=None):
            self.score = score
            self.subscores = subscores or {}
            self.weights = weights or {}
            self.feedback = (
                json.dumps(feedback) if isinstance(feedback, dict) else (feedback or "")
            )


NAMESPACE = "glitchtip"
VALKEY_STATEFULSET = "valkey-runtime-state"
VALKEY_POD = "valkey-runtime-state-0"
VALKEY_SERVICE = "valkey-runtime-state"
BACKUP_CRONJOB = "valkey-state-backup"
STATUS_CM = "glitchtip-valkey-backup-status"
STATUS_KEY = "status.md"
HANDOFF_CM = "glitchtip-valkey-backup-restore-handoff"
HANDOFF_KEY = "handoff.json"
RUNTIME_CM_TARGETS = {STATUS_CM, HANDOFF_CM}


# ─────────────────────────────────────────────
# Shell helpers
# ─────────────────────────────────────────────
def run_cmd(cmd, timeout=30):
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return 1, "", "timeout"
    except Exception as e:
        return 1, "", str(e)


def load_setup_info():
    info = {}
    try:
        with open("/root/.setup_info") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    info[key] = value
    except FileNotFoundError:
        pass
    return info


def kget_json(resource, ns=None, name=None):
    ns_arg = f"-n {ns}" if ns else ""
    name_arg = name or ""
    rc, out, _ = run_cmd(
        f"kubectl get {resource} {name_arg} {ns_arg} -o json 2>/dev/null"
    )
    if rc != 0 or not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def valkey_cli(cmd, timeout=10):
    rc, out, _ = run_cmd(
        f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- valkey-cli {cmd}",
        timeout=timeout,
    )
    return out.strip() if rc == 0 else ""


def write_marker_key(marker_name, marker_value):
    """Write a freshness-proof key directly into Valkey via kubectl exec.
    The key MUST appear in any RDB captured AFTER this call."""
    safe_value = marker_value.replace('"', '\\"')
    rc, _, _ = run_cmd(
        f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- valkey-cli "
        f"SET {shlex.quote(marker_name)} {shlex.quote(safe_value)}",
        timeout=10,
    )
    return rc == 0


def rdb_contains_marker(rdb_bytes, marker_name, marker_value):
    """RDB serializes string keys+values literally as length-prefixed bytes.
    A simple substring search on the raw bytes is sufficient to prove the
    marker was present at BGSAVE time — agents cannot fake this without
    actually capturing post-marker state."""
    if not rdb_bytes:
        return False
    try:
        name_b = marker_name.encode("utf-8")
        value_b = marker_value.encode("utf-8")
    except Exception:
        return False
    return name_b in rdb_bytes and value_b in rdb_bytes


def restore_probe_keyset(rdb_bytes, expected_keys):
    """Spin up an isolated Valkey probe pod, load the agent's RDB, run
    KEYS *. Returns (matched_keys, missing_keys, error_or_None).
    Caps probe pod boot at 60s — flaky infra fails fairly with diagnostic."""
    import base64 as _b64
    ts = int(time.time())
    probe = f"grader-restore-probe-{ts}"
    rdb_b64 = _b64.b64encode(rdb_bytes).decode("ascii")
    cm_name = f"grader-restore-rdb-{ts}"
    cm_yaml = (
        f"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {cm_name}\n  "
        f"namespace: {NAMESPACE}\nbinaryData:\n  dump.rdb: {rdb_b64}\n"
    )
    rc, _, err = run_cmd(
        f"echo {shlex.quote(cm_yaml)} | kubectl apply -f -",
        timeout=20,
    )
    if rc != 0:
        return set(), set(expected_keys), f"failed to stage RDB CM: {err[:200]}"
    pod_yaml = f"""apiVersion: v1
kind: Pod
metadata:
  name: {probe}
  namespace: {NAMESPACE}
  labels:
    app: grader-restore-probe
spec:
  restartPolicy: Never
  securityContext:
    runAsUser: 0
    runAsGroup: 0
    fsGroup: 0
  containers:
  - name: probe
    image: docker.io/valkey/valkey:7.2-alpine
    securityContext:
      runAsUser: 0
      runAsNonRoot: false
    command: ["sh", "-c"]
    args:
    - |
      set -e
      echo "[probe] copying RDB..."
      cp /rdb/dump.rdb /data/dump.rdb
      chmod 644 /data/dump.rdb
      ls -la /data/
      echo "[probe] starting valkey-server..."
      valkey-server --dir /data --dbfilename dump.rdb --port 6390 \\
        --daemonize no --bind 127.0.0.1 --protected-mode no \\
        --logfile /tmp/valkey.log &
      VPID=$!
      echo "[probe] valkey pid=$VPID — waiting up to 30s for ready..."
      for i in $(seq 1 30); do
        if valkey-cli -p 6390 PING 2>/dev/null | grep -q PONG; then
          echo "[probe] PONG after ${{i}}s"
          break
        fi
        sleep 1
      done
      echo "[probe] DBSIZE:"
      valkey-cli -p 6390 DBSIZE 2>/dev/null || echo "DBSIZE-fail"
      echo "[probe] === KEYS BEGIN ==="
      valkey-cli -p 6390 KEYS '*' 2>/dev/null || echo "KEYS-fail"
      echo "[probe] === KEYS END ==="
      echo "[probe] valkey-server log tail:"
      tail -40 /tmp/valkey.log 2>/dev/null || true
      sleep 60
    volumeMounts:
    - name: rdb
      mountPath: /rdb
    - name: data
      mountPath: /data
  volumes:
  - name: rdb
    configMap:
      name: {cm_name}
  - name: data
    emptyDir: {{}}
"""
    rc, _, err = run_cmd(
        f"echo {shlex.quote(pod_yaml)} | kubectl apply -f -",
        timeout=15,
    )
    if rc != 0:
        run_cmd(f"kubectl delete cm {cm_name} -n {NAMESPACE} --ignore-not-found",
                timeout=10)
        return set(), set(expected_keys), f"failed to launch probe pod: {err[:200]}"
    matched = set()
    missing = set(expected_keys)
    err_msg = None
    last_logs = ""
    try:
        for _ in range(20):
            time.sleep(4)
            rc, phase, _ = run_cmd(
                f"kubectl get pod {probe} -n {NAMESPACE} "
                f"-o jsonpath='{{.status.phase}}' 2>/dev/null",
                timeout=10,
            )
            phase = phase.strip().strip("'")
            if phase in ("Running", "Succeeded"):
                rc, logs, _ = run_cmd(
                    f"kubectl logs {probe} -n {NAMESPACE} 2>/dev/null",
                    timeout=15,
                )
                last_logs = logs
                # Extract only the lines between KEYS BEGIN / KEYS END
                keyset = set()
                in_keys = False
                for ln in logs.splitlines():
                    s = ln.strip()
                    if "=== KEYS BEGIN ===" in s:
                        in_keys = True
                        continue
                    if "=== KEYS END ===" in s:
                        in_keys = False
                        break
                    if in_keys and s:
                        keyset.add(s)
                if "=== KEYS END ===" in logs:
                    matched = {k for k in expected_keys if k in keyset}
                    missing = {k for k in expected_keys if k not in keyset}
                    break
            if phase == "Failed":
                err_msg = "probe pod entered Failed phase"
                break
        if not matched and not err_msg:
            err_msg = (
                "probe pod did not reach KEYS-output stage in time. "
                f"log tail: {last_logs[-400:]}"
            )
    finally:
        run_cmd(f"kubectl delete pod {probe} -n {NAMESPACE} --grace-period=0 "
                f"--force --ignore-not-found 2>/dev/null", timeout=15)
        run_cmd(f"kubectl delete cm {cm_name} -n {NAMESPACE} --ignore-not-found",
                timeout=10)
    return matched, missing, err_msg


def get_lastsave():
    out = valkey_cli("LASTSAVE")
    try:
        return int(out)
    except ValueError:
        return 0


def get_dbsize():
    out = valkey_cli("DBSIZE")
    try:
        return int(out)
    except ValueError:
        return 0


def trigger_backup(job_name, wait_seconds=300):
    run_cmd(
        f"kubectl delete job {job_name} -n {NAMESPACE} --ignore-not-found "
        "--wait=false >/dev/null 2>&1",
        timeout=20,
    )
    time.sleep(2)
    rc, out, err = run_cmd(
        f"kubectl create job {job_name} --from=cronjob/{BACKUP_CRONJOB} "
        f"-n {NAMESPACE} 2>&1",
        timeout=30,
    )
    if rc != 0:
        return False, "", f"create-job failed: {(err or out)[:200]}"
    completed = False
    failed_flag = ""
    for _ in range(wait_seconds // 5):
        rc, status, _ = run_cmd(
            f"kubectl get job {job_name} -n {NAMESPACE} "
            "-o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null"
        )
        parts = status.strip("'").split("/")
        succeeded = parts[0] if len(parts) > 0 else ""
        failed_flag = parts[1] if len(parts) > 1 else ""
        if succeeded == "1":
            completed = True
            break
        if failed_flag.isdigit() and int(failed_flag) > 0:
            break
        time.sleep(5)
    _, logs, _ = run_cmd(
        f"kubectl logs -n {NAMESPACE} -l job-name={job_name} "
        "--all-containers --tail=300 2>/dev/null",
        timeout=30,
    )
    if not logs.strip():
        _, pods, _ = run_cmd(
            f"kubectl get pods -n {NAMESPACE} -l job-name={job_name} "
            "-o wide 2>&1",
            timeout=20,
        )
        _, ev, _ = run_cmd(
            f"kubectl get events -n {NAMESPACE} "
            "--sort-by='.lastTimestamp' 2>&1 | tail -15",
            timeout=20,
        )
        logs = f"[no container logs]\npods:\n{pods}\n---\nrecent events:\n{ev}"
    return completed, logs, ""


def cleanup_job(name):
    run_cmd(
        f"kubectl delete job {name} -n {NAMESPACE} --ignore-not-found "
        "--wait=false >/dev/null 2>&1",
        timeout=20,
    )


def get_status_doc():
    rc, out, _ = run_cmd(
        f"kubectl get configmap {STATUS_CM} -n {NAMESPACE} "
        f"-o go-template='{{{{ index .data \"{STATUS_KEY}\" }}}}' 2>/dev/null"
    )
    return out.strip("'") if rc == 0 else ""


def get_handoff_doc():
    rc, out, _ = run_cmd(
        f"kubectl get configmap {HANDOFF_CM} -n {NAMESPACE} "
        f"-o go-template='{{{{ index .data \"{HANDOFF_KEY}\" }}}}' 2>/dev/null"
    )
    return out.strip("'") if rc == 0 else ""


def parse_handoff(text):
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def resolve_artifact_location(parsed):
    """
    Canonical-only resolver. The task contract documents exactly one shape:
      artifact_location: {"type": "configmap"|"secret"|"pvc", ...}
    No aliasing. If an agent publishes their own field name, recovery
    tooling won't find the artifact — the grader simulates that.
    """
    if not isinstance(parsed, dict):
        return None
    cand = parsed.get("artifact_location")
    if not isinstance(cand, dict):
        return None
    return cand


def resolve_snapshot_epoch(parsed):
    """Canonical-only: snapshot_epoch must be an int under that exact key."""
    if not isinstance(parsed, dict):
        return None
    v = parsed.get("snapshot_epoch")
    return v if isinstance(v, int) else None


def resolve_sha256(parsed):
    """Canonical-only: artifact_sha256 must be a 64-hex string."""
    if not isinstance(parsed, dict):
        return None
    v = parsed.get("artifact_sha256")
    if isinstance(v, str) and len(v) == 64:
        return v
    return None


def verify_artifact_source_role(parsed):
    """
    Verify that the handoff declares the backup ran against the Valkey
    *master* (not a replica), and that the declared pod is in fact the
    master per `INFO replication`. Returns (ok, reason).

    The Valkey StatefulSet runs 3 pods; only ord=0 is the writable
    master, the rest are read-only replicas. A backup captured from a
    replica may be missing recently-written keys that have not yet
    replicated, so the handoff must commit to which pod produced the
    artifact and the grader confirms its role at probe time.

    Required handoff fields:
      artifact_source_role: "master"
      artifact_source_pod:  "<statefulset-pod-name>"   e.g. valkey-runtime-state-0
    """
    if not isinstance(parsed, dict):
        return False, "handoff not a dict"
    role = parsed.get("artifact_source_role")
    if role != "master":
        return False, (
            f"artifact_source_role={role!r} (must be 'master' — "
            "backup must capture from the writable master, not a replica)"
        )
    pod = parsed.get("artifact_source_pod")
    if not isinstance(pod, str) or not pod.strip():
        return False, "artifact_source_pod missing or not a string"
    # Confirm the declared pod actually presents as master in INFO replication.
    rc, out, _ = run_cmd(
        f"kubectl exec -n {NAMESPACE} {shlex.quote(pod)} -- "
        "valkey-cli INFO replication 2>/dev/null", timeout=15)
    if rc != 0 or not out:
        return False, (
            f"could not exec INFO replication on declared pod "
            f"{pod!r} (rc={rc})"
        )
    role_line = ""
    for ln in out.splitlines():
        if ln.startswith("role:"):
            role_line = ln.strip()
            break
    if role_line != "role:master":
        return False, (
            f"pod {pod!r} reports {role_line!r} (expected role:master) — "
            "backup ran against a replica or wrong pod"
        )
    return True, f"pod {pod} confirmed role:master"


def handoff_has_real_success_shape(parsed, run_start_epoch):
    """
    A handoff is a 'real' success record only if:
      * it's a parseable dict
      * safe_for_restore is True
      * snapshot_epoch is an int >= run_start - 5 (within 5s slack)
      * artifact_location is a dict whose declared resource resolves
      * artifact_source_role is 'master' AND the declared pod confirms it
    Otherwise it's a fake-success record (cosmetic safe_for_restore=true).
    Returns (ok, reason).
    """
    if not isinstance(parsed, dict):
        return False, "handoff not a dict"
    if parsed.get("safe_for_restore") != True:
        return False, "safe_for_restore != true"
    epoch = resolve_snapshot_epoch(parsed)
    if epoch is None:
        return False, "no recognised snapshot_epoch field (int)"
    if epoch < run_start_epoch - 5:
        return False, f"snapshot_epoch={epoch} < run_start={run_start_epoch}"
    location = resolve_artifact_location(parsed)
    if not isinstance(location, dict):
        return False, "no recognised artifact_location reference"
    loc_type = (location.get("type") or "").lower()
    ns = location.get("namespace") or NAMESPACE
    if loc_type in ("configmap", "cm"):
        name = location.get("name") or ""
        if not name:
            return False, "configmap location missing name"
        rc, _, _ = run_cmd(
            f"kubectl get configmap {shlex.quote(name)} -n "
            f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
        if rc != 0:
            return False, f"configmap/{name} does not exist"
    elif loc_type == "secret":
        name = location.get("name") or ""
        if not name:
            return False, "secret location missing name"
        rc, _, _ = run_cmd(
            f"kubectl get secret {shlex.quote(name)} -n "
            f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
        if rc != 0:
            return False, f"secret/{name} does not exist"
    elif loc_type == "pvc":
        name = location.get("claim") or ""
        if not name:
            return False, "pvc location missing claim"
        rc, _, _ = run_cmd(
            f"kubectl get pvc {shlex.quote(name)} -n "
            f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
        if rc != 0:
            return False, f"pvc/{name} does not exist"
    else:
        return False, f"artifact_location.type='{loc_type}' invalid"
    role_ok, role_reason = verify_artifact_source_role(parsed)
    if not role_ok:
        return False, role_reason
    return True, "real success shape"


def set_status_marker(marker):
    payload = json.dumps({"data": {STATUS_KEY: marker}})
    run_cmd(
        f"kubectl patch configmap {STATUS_CM} -n {NAMESPACE} "
        f"--type merge -p {shlex.quote(payload)}",
        timeout=20,
    )


def set_handoff_marker(marker):
    payload = json.dumps({"data": {HANDOFF_KEY: marker}})
    run_cmd(
        f"kubectl patch configmap {HANDOFF_CM} -n {NAMESPACE} "
        f"--type merge -p {shlex.quote(payload)}",
        timeout=20,
    )


def wait_for_doc_change(getter, marker, timeout_seconds=60):
    end = time.time() + timeout_seconds
    while time.time() < end:
        current = getter()
        if current and marker not in current:
            return current
        time.sleep(5)
    return getter()


def scale_valkey(replicas):
    """
    Scale the Valkey StatefulSet to `replicas` and wait for all of
    them to become Ready. With OrderedReady pod-management each replica
    must boot, PSYNC from master, then ack ready, so allow up to 240s.
    """
    run_cmd(
        f"kubectl scale statefulset/{VALKEY_STATEFULSET} -n {NAMESPACE} "
        f"--replicas={replicas}",
        timeout=30,
    )
    deadline = time.time() + 240
    while time.time() < deadline:
        rc, ready, _ = run_cmd(
            f"kubectl get statefulset/{VALKEY_STATEFULSET} -n {NAMESPACE} "
            "-o jsonpath='{.status.readyReplicas}' 2>/dev/null"
        )
        current = ready.strip("'") or "0"
        if str(current) == str(replicas):
            time.sleep(3)
            return True
        time.sleep(3)
    return False


# ─────────────────────────────────────────────
# Artifact readers (configmap / secret / pvc)
# ─────────────────────────────────────────────
def read_artifact_bytes(location):
    """
    location is the dict parsed out of handoff.artifact_location:
      {"type": "configmap|secret", "name": "...", "key": "...", "namespace": "..."}
      {"type": "pvc", "claim": "...", "path": "..."}
    Returns (bytes, error_message).
    """
    if not isinstance(location, dict):
        return b"", "artifact_location is not a dict"
    kind = (location.get("type") or "").lower()
    ns = location.get("namespace") or NAMESPACE

    if kind in ("configmap", "cm"):
        name = location.get("name") or ""
        key = location.get("key")
        encoding = (location.get("encoding") or "").lower()
        if not name:
            return b"", "configmap artifact_location missing name"
        # 1. Try binaryData (auto-base64 by k8s).
        rc, out, _ = run_cmd(
            f"kubectl get configmap {name} -n {ns} "
            f"-o jsonpath='{{.binaryData.{key.replace('.', chr(92)+chr(46))}}}' 2>/dev/null"
        )
        b64 = out.strip("'")
        if b64:
            try:
                import base64 as _b64
                return _b64.b64decode(b64), ""
            except Exception as e:
                return b"", f"failed to decode binaryData: {e}"
        # 2. Try data — accept both raw text and base64-encoded text
        # (when handoff or CM annotation declares encoding=base64).
        rc, out, _ = run_cmd(
            f"kubectl get configmap {name} -n {ns} "
            f"-o jsonpath='{{.data.{key.replace('.', chr(92)+chr(46))}}}' 2>/dev/null"
        )
        raw = out.strip("'")
        if raw:
            # Per-CM annotation can declare base64 encoding too.
            if encoding != "base64":
                rc2, ann_out, _ = run_cmd(
                    f"kubectl get configmap {name} -n {ns} "
                    "-o jsonpath='{.metadata.annotations.encoding}' 2>/dev/null"
                )
                if ann_out.strip("'").lower() == "base64":
                    encoding = "base64"
            if encoding == "base64":
                try:
                    import base64 as _b64
                    return _b64.b64decode(raw), ""
                except Exception as e:
                    return b"", f"failed to decode data (base64): {e}"
            return raw.encode("utf-8", errors="replace"), ""
        return b"", f"configmap {name} has no {key}"

    if kind in ("secret",):
        name = location.get("name") or ""
        key = location.get("key")
        rc, out, _ = run_cmd(
            f"kubectl get secret {name} -n {ns} "
            f"-o jsonpath='{{.data.{key.replace('.', chr(92)+chr(46))}}}' 2>/dev/null"
        )
        b64 = out.strip("'")
        if not b64:
            return b"", f"secret {name} has no {key}"
        try:
            import base64 as _b64
            return _b64.b64decode(b64), ""
        except Exception as e:
            return b"", f"failed to decode secret data: {e}"

    if kind == "pvc":
        claim = location.get("claim") or ""
        path = location.get("path") or ""
        if not claim or not path:
            return b"", "pvc artifact_location missing claim or path"
        ts = int(time.time())
        probe = f"grader-pvc-probe-{ts}"
        pod_spec = f"""apiVersion: v1
kind: Pod
metadata:
  name: {probe}
  namespace: {ns}
  labels:
    app: grader-probe
spec:
  restartPolicy: Never
  containers:
  - name: probe
    image: docker.io/valkey/valkey:7.2-alpine
    imagePullPolicy: IfNotPresent
    command: ["/bin/sh", "-c", "sleep 600"]
    volumeMounts:
    - name: backup
      mountPath: /probe
      readOnly: true
  volumes:
  - name: backup
    persistentVolumeClaim:
      claimName: {claim}
      readOnly: true
"""
        proc = subprocess.run(
            "kubectl apply -f - 2>&1",
            input=pod_spec, shell=True, capture_output=True, text=True, timeout=30,
        )
        if proc.returncode != 0:
            return b"", f"probe pod apply failed: {proc.stdout[:200]}"
        try:
            deadline = time.time() + 90
            ready = False
            while time.time() < deadline:
                rc, phase, _ = run_cmd(
                    f"kubectl get pod {probe} -n {ns} "
                    "-o jsonpath='{.status.phase}' 2>/dev/null"
                )
                if phase.strip("'") == "Running":
                    ready = True
                    break
                time.sleep(3)
            if not ready:
                return b"", "probe pod didn't become Running (PVC maybe bound RWO elsewhere)"
            rel = path.lstrip("/")
            rc, b64, _ = run_cmd(
                f"kubectl exec -n {ns} {probe} -- sh -c "
                f"'[ -f /probe/{rel} ] && base64 /probe/{rel} | tr -d \"\\n\"' 2>/dev/null",
                timeout=60,
            )
            if not b64:
                return b"", f"artifact not found at {path} on PVC {claim}"
            try:
                import base64 as _b64
                return _b64.b64decode(b64), ""
            except Exception as e:
                return b"", f"pvc read decode failed: {e}"
        finally:
            run_cmd(
                f"kubectl delete pod {probe} -n {ns} --ignore-not-found "
                "--force --grace-period=0 --wait=false >/dev/null 2>&1",
                timeout=15,
            )

    return b"", f"unsupported artifact_location type: {kind}"


# ─────────────────────────────────────────────
# CHECK 1: backup_enforces_snapshot_freshness
# ─────────────────────────────────────────────
def check_backup_enforces_snapshot_freshness(setup_info):
    """
    Partial-credit subscore: 4 independent quarter-credit components.

    A. Backup job completes on a clean run and refreshes the handoff
       (handoff is parseable JSON with safe_for_restore=true).
    B. Valkey LASTSAVE actually advanced during the run (BGSAVE fired)
       AND handoff.snapshot_epoch is an int >= run_start.
    C. The agent's captured RDB physically contains a freshness marker
       key written by the grader between marker-set and backup-trigger.
    D. Forced-failure run (Valkey scaled to 0) leaves an unsafe handoff
       (safe_for_restore=false with a non-empty reason).
    """
    if not scale_valkey(2):
        return 0.0, "could not ensure Valkey is scaled to 2 replicas"
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)

    lastsave_before = get_lastsave()
    run_start = int(time.time())
    ts = int(time.time())
    marker = f"GRADER-FRESH-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)

    rdb_marker_name = f"grader:freshness:{ts}"
    rdb_marker_value = f"PROOF-{ts}-NONCE-{os.urandom(8).hex()}"
    marker_written = write_marker_key(rdb_marker_name, rdb_marker_value)

    job = f"grader-fresh-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    score = 0.0
    notes = []

    parsed = parse_handoff(handoff) if handoff else None

    # ── Component A: clean run completes with a minimum success record ──
    # Not just safe_for_restore=true — the handoff must declare the
    # success-shape fields the rest of the pipeline relies on so that
    # agents who publish a bare `{"safe_for_restore": true}` don't pass.
    component_a_ok = False
    if err:
        notes.append(f"A:err={err}")
    elif not completed:
        notes.append("A:job-did-not-complete")
    elif parsed is None:
        notes.append("A:handoff-not-parseable")
    elif parsed.get("safe_for_restore") != True:
        notes.append("A:safe_for_restore!=true")
    else:
        backup_id = parsed.get("backup_id")
        snap_epoch = resolve_snapshot_epoch(parsed)
        location = resolve_artifact_location(parsed)
        missing_shape = []
        if not isinstance(backup_id, str) or not backup_id.strip():
            missing_shape.append("backup_id")
        if snap_epoch is None:
            missing_shape.append("snapshot_epoch")
        if not isinstance(location, dict):
            missing_shape.append("artifact_location")
        if "artifact_source_role" not in parsed:
            missing_shape.append("artifact_source_role")
        if "artifact_source_pod" not in parsed:
            missing_shape.append("artifact_source_pod")
        if missing_shape:
            notes.append(
                f"A:safe_for_restore=true but handoff missing minimum "
                f"success-shape fields {missing_shape}"
            )
        else:
            role_ok, role_reason = verify_artifact_source_role(parsed)
            if not role_ok:
                notes.append(f"A:{role_reason}")
            else:
                component_a_ok = True
                notes.append(f"A:clean-run-ok ({role_reason})")
    if component_a_ok:
        score += 0.25

    # ── Component B: STRICTLY MONOTONIC snapshot_epoch across two
    # consecutive backup runs. The first run's epoch must be >= run_start;
    # then a SECOND grader-triggered backup must produce an epoch
    # strictly greater than the first. This defeats agents who reuse the
    # on-disk RDB or skip BGSAVE on subsequent runs — every run must
    # produce a genuinely new snapshot. ──
    lastsave_after = get_lastsave()
    snapshot_epoch_run1 = parsed.get("snapshot_epoch") if parsed else None
    component_b_ok = False
    component_b_reason = ""
    if lastsave_after <= lastsave_before:
        component_b_reason = (
            f"LASTSAVE did not advance ({lastsave_before}->{lastsave_after}) "
            "— BGSAVE never fired in run 1"
        )
    elif not isinstance(snapshot_epoch_run1, int):
        component_b_reason = (
            f"handoff.snapshot_epoch not int in run 1 (got {snapshot_epoch_run1})"
        )
    elif snapshot_epoch_run1 < run_start - 5:
        component_b_reason = (
            f"run 1 snapshot_epoch={snapshot_epoch_run1} < run_start={run_start}"
        )
    else:
        # Second consecutive run must produce strictly greater epoch.
        # Sleep at least 2s so the grader can detect a re-used dump.rdb
        # (LASTSAVE granularity is seconds).
        time.sleep(3)
        ts_b2 = int(time.time())
        marker_b2 = f"GRADER-FRESH-B2-{ts_b2}"
        set_status_marker(marker_b2)
        set_handoff_marker(marker_b2)
        run_start_b2 = int(time.time())
        job_b2 = f"grader-fresh-b2-{ts_b2}"
        completed_b2, _, err_b2 = trigger_backup(job_b2, wait_seconds=240)
        handoff_b2 = wait_for_doc_change(get_handoff_doc, marker_b2)
        cleanup_job(job_b2)
        parsed_b2 = parse_handoff(handoff_b2)
        snapshot_epoch_run2 = (
            parsed_b2.get("snapshot_epoch")
            if isinstance(parsed_b2, dict) else None
        )
        if err_b2 or not completed_b2:
            component_b_reason = (
                f"run 2 did not complete (err={err_b2 or 'timeout'}) — "
                "monotonic-epoch check requires two successful runs"
            )
        elif not isinstance(snapshot_epoch_run2, int):
            component_b_reason = (
                f"run 2 handoff.snapshot_epoch not int "
                f"(got {snapshot_epoch_run2})"
            )
        elif snapshot_epoch_run2 <= snapshot_epoch_run1:
            component_b_reason = (
                f"snapshot_epoch not strictly monotonic across runs: "
                f"run1={snapshot_epoch_run1} run2={snapshot_epoch_run2} — "
                "looks like a reused RDB / no fresh BGSAVE on run 2"
            )
        elif snapshot_epoch_run2 < run_start_b2 - 5:
            component_b_reason = (
                f"run 2 snapshot_epoch={snapshot_epoch_run2} < "
                f"run 2 start={run_start_b2}"
            )
        else:
            component_b_ok = True
            component_b_reason = (
                f"strictly monotonic: "
                f"run1={snapshot_epoch_run1} -> run2={snapshot_epoch_run2}"
            )
    if component_b_ok:
        score += 0.25
    notes.append(f"B:{component_b_reason}")

    # ── Component C: captured RDB contains the freshness marker key ──
    component_c_ok = False
    component_c_reason = "skipped (no artifact_location to read)"
    if not marker_written:
        component_c_reason = "grader could not write freshness marker into Valkey"
    elif parsed is not None:
        location = resolve_artifact_location(parsed)
        if not isinstance(location, dict):
            component_c_reason = "no recognised artifact_location reference"
        else:
            rdb_bytes, art_err = read_artifact_bytes(location)
            if art_err or not rdb_bytes:
                component_c_reason = (
                    f"could not read artifact for marker check: "
                    f"{art_err or 'no bytes'}"
                )
            elif not rdb_contains_marker(
                rdb_bytes, rdb_marker_name, rdb_marker_value
            ):
                component_c_reason = (
                    f"freshness marker '{rdb_marker_name}' not in captured RDB"
                )
            else:
                component_c_ok = True
                component_c_reason = "marker present in RDB"
    if component_c_ok:
        score += 0.25
        notes.append(f"C:{component_c_reason}")
    else:
        notes.append(f"C:{component_c_reason}")

    # ── Component D: forced-failure run leaves unsafe handoff with reason ──
    ts2 = int(time.time())
    fail_marker = f"GRADER-FRESH-FAIL-{ts2}"
    set_status_marker(fail_marker)
    set_handoff_marker(fail_marker)
    scale_valkey(0)
    component_d_ok = False
    component_d_reason = "forced-failure not evaluated"
    try:
        fail_job = f"grader-fresh-fail-{ts2}"
        completed2, _logs2, _ = trigger_backup(fail_job, wait_seconds=180)
        fail_handoff = wait_for_doc_change(get_handoff_doc, fail_marker)
        cleanup_job(fail_job)
        if completed2:
            component_d_reason = (
                "forced-failure run succeeded — pipeline not validating Valkey"
            )
        else:
            parsed_fail = parse_handoff(fail_handoff)
            if parsed_fail is None:
                component_d_reason = "handoff not JSON after forced-failure"
            elif parsed_fail.get("safe_for_restore") != False:
                component_d_reason = (
                    "handoff.safe_for_restore != false on forced-failure"
                )
            else:
                reason_field = parsed_fail.get("reason") or ""
                if not isinstance(reason_field, str) or not reason_field.strip():
                    component_d_reason = "handoff.reason missing/empty"
                else:
                    component_d_ok = True
                    component_d_reason = "fail-closed surface ok"
    finally:
        scale_valkey(2)
    if component_d_ok:
        score += 0.25
    notes.append(f"D:{component_d_reason}")

    return score, "; ".join(notes)


# ─────────────────────────────────────────────
# CHECK 2: backup_artifact_durable_and_reusable
# ─────────────────────────────────────────────
def check_backup_artifact_durable_and_reusable(setup_info):
    """
    Partial-credit subscore: 4 independent quarter-credit components.

    A. handoff.artifact_location declares a durable in-cluster resource
       (configmap/secret/pvc) that the grader can resolve.
    B. The retrieved bytes start with the REDIS RDB magic and exceed a
       minimum size (real RDB, not a placeholder).
    C. handoff.restore_proof.key_count_at_snapshot is a sane integer
       and handoff.artifact_bytes matches the retrieved size.
    D. End-to-end restore probe: a clean Valkey pod loaded with the
       agent's RDB recovers a meaningful share of the live key set.
    """
    if not scale_valkey(2):
        return 0.0, "could not ensure Valkey is scaled to 2 replicas"
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)
    live_dbsize = get_dbsize()

    ts = int(time.time())
    marker = f"GRADER-ART-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)

    job = f"grader-art-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    score = 0.0
    notes = []
    parsed = parse_handoff(handoff) if handoff else None

    # ── Component A: artifact reference declared and resolvable ──
    # Accepts any of the reasonable shapes that resolve_artifact_location
    # recognises (artifact_location dict, artifact_pvc+artifact_path,
    # artifact_configmap, artifact_uri, etc).
    component_a_ok = False
    component_a_reason = ""
    location = None
    loc_type = ""
    if err:
        component_a_reason = f"err={err}"
    elif not completed:
        component_a_reason = "artifact run did not complete"
    elif parsed is None:
        component_a_reason = "handoff not parseable"
    elif parsed.get("safe_for_restore") != True:
        component_a_reason = "safe_for_restore != true on clean run"
    else:
        location = resolve_artifact_location(parsed)
        if not isinstance(location, dict):
            component_a_reason = (
                "no recognised artifact reference in handoff "
                "(expected artifact_location dict, artifact_pvc+path, "
                "artifact_configmap, or artifact_uri)"
            )
        else:
            loc_type = (location.get("type") or "").lower()
            if loc_type not in ("configmap", "cm", "secret", "pvc"):
                component_a_reason = (
                    f"artifact_location.type='{loc_type}' — must resolve "
                    "to configmap / secret / pvc"
                )
            else:
                ns = location.get("namespace") or NAMESPACE
                if loc_type in ("configmap", "cm"):
                    name = location.get("name") or ""
                    rc, _, _ = run_cmd(
                        f"kubectl get configmap {shlex.quote(name)} -n "
                        f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
                elif loc_type == "secret":
                    name = location.get("name") or ""
                    rc, _, _ = run_cmd(
                        f"kubectl get secret {shlex.quote(name)} -n "
                        f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
                else:  # pvc
                    name = location.get("claim") or ""
                    rc, _, _ = run_cmd(
                        f"kubectl get pvc {shlex.quote(name)} -n "
                        f"{shlex.quote(ns)} -o name 2>/dev/null", timeout=10)
                if rc != 0:
                    component_a_reason = (
                        f"{loc_type}/{name} declared but does not exist"
                    )
                else:
                    component_a_ok = True
                    component_a_reason = (
                        f"declared {loc_type}/{name} exists"
                    )
    if component_a_ok:
        score += 0.25
    notes.append(f"A:{component_a_reason or 'location-ok'}")

    # ── Component B: bytes have REDIS magic + non-trivial size ──
    component_b_ok = False
    component_b_reason = "skipped (location not resolvable)"
    artifact = b""
    if component_a_ok:
        artifact, read_err = read_artifact_bytes(location)
        if read_err:
            component_b_reason = f"could not read artifact bytes: {read_err}"
        elif len(artifact) < 200:
            component_b_reason = (
                f"artifact only {len(artifact)} bytes — too small for an RDB"
            )
        elif not artifact.startswith(b"REDIS"):
            component_b_reason = (
                f"artifact does not start with REDIS magic (got {artifact[:8]!r})"
            )
        else:
            component_b_ok = True
            component_b_reason = (
                f"REDIS magic OK, {len(artifact)} bytes"
            )
    if component_b_ok:
        score += 0.25
    notes.append(f"B:{component_b_reason}")

    # ── Component C: restore_proof + artifact_bytes shape correct ──
    component_c_ok = False
    component_c_reason = "skipped"
    if parsed is not None:
        restore_proof = parsed.get("restore_proof")
        artifact_bytes_reported = parsed.get("artifact_bytes")
        if not isinstance(restore_proof, dict):
            component_c_reason = "handoff.restore_proof missing"
        else:
            key_count = restore_proof.get("key_count_at_snapshot")
            if not isinstance(key_count, int) or key_count <= 0:
                component_c_reason = (
                    "restore_proof.key_count_at_snapshot <= 0"
                )
            elif (not isinstance(artifact_bytes_reported, int)
                  or artifact_bytes_reported <= 0):
                component_c_reason = (
                    "handoff.artifact_bytes missing or <= 0"
                )
            elif (artifact and
                  abs(artifact_bytes_reported - len(artifact))
                  > max(32, int(len(artifact) * 0.1))):
                component_c_reason = (
                    f"artifact_bytes={artifact_bytes_reported} != "
                    f"retrieved={len(artifact)}"
                )
            else:
                component_c_ok = True
                component_c_reason = (
                    f"restore_proof.key_count={key_count}, "
                    f"artifact_bytes={artifact_bytes_reported} matches"
                )
    if component_c_ok:
        score += 0.25
    notes.append(f"C:{component_c_reason}")

    # ── Component D: end-to-end restore probe matches the STABLE
    # subset of the live key set. The traffic generator CronJob writes
    # short-TTL keys with predictable prefixes ("throttle:tg-",
    # "coord:tg-", "stats:traffic-gen:") and the grader's own
    # freshness markers are prefixed "grader:". We exclude both classes
    # from the comparison so an agent's backup is graded only on the
    # stable coordination keyset that operators expect to survive a
    # recovery drill — not on transient writes that landed between
    # BGSAVE and the grader's live-keys read. ──
    def _is_grader_or_traffic(k):
        return (
            k.startswith("grader:")
            or k.startswith("throttle:tg-")
            or k.startswith("coord:tg-")
            or k.startswith("stats:traffic-gen:")
        )
    component_d_ok = False
    component_d_reason = "skipped (no usable artifact)"
    if component_b_ok:
        rc, live_keys_raw, _ = run_cmd(
            f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- valkey-cli KEYS '*'",
            timeout=15,
        )
        live_keys = {
            ln.strip() for ln in live_keys_raw.splitlines() if ln.strip()
        }
        live_keys = {k for k in live_keys if not _is_grader_or_traffic(k)}
        if not live_keys:
            component_d_reason = (
                "could not read live stable Valkey key set for comparison"
            )
        else:
            matched, missing, probe_err = restore_probe_keyset(
                artifact, live_keys
            )
            if probe_err and not matched:
                component_d_reason = f"restore probe failed: {probe_err}"
            else:
                # Filter probe-side keyset against the same predicate so
                # any churn keys that happened to be in the RDB don't
                # spuriously inflate the "matched" count.
                matched = {k for k in matched if not _is_grader_or_traffic(k)}
                missing = {k for k in missing if not _is_grader_or_traffic(k)}
                # Strict: require 100% recovery on the stable keyset.
                missed_count = len(live_keys) - len(matched)
                if missed_count > 0:
                    component_d_reason = (
                        f"restore probe recovered "
                        f"{len(matched)}/{len(live_keys)} stable keys — "
                        f"need full match. missing sample: "
                        f"{sorted(list(missing))[:5]}"
                    )
                else:
                    component_d_ok = True
                    component_d_reason = (
                        f"restore probe matched "
                        f"{len(matched)}/{len(live_keys)} stable "
                        f"(missed={missed_count})"
                    )
    if component_d_ok:
        score += 0.25
    notes.append(f"D:{component_d_reason}")

    return score, "; ".join(notes)


# ─────────────────────────────────────────────
# CHECK 3: status_and_handoff_surface_truth
# ─────────────────────────────────────────────
def check_status_and_handoff_surface_truth(setup_info):
    """
    Partial-credit subscore: 4 independent quarter-credit components.

    A. Both surfaces refreshed (markers replaced) on a clean run.
    B. Handoff has the core success-shape (required fields present,
       safe_for_restore=true, sha256 matches actual artifact bytes).
    C. Status surfaces numeric epoch + artifact-bytes that agree with
       handoff (so the human-readable doc isn't drifted from machine).
    D. Forced-failure leaves a refreshed unsafe surface (status flags
       failure, handoff.safe_for_restore=false with a reason).
    """
    if not scale_valkey(2):
        return 0.0, "could not ensure Valkey is scaled to 2 replicas"
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)

    ts = int(time.time())
    marker = f"GRADER-SURF-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)

    job = f"grader-surf-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    status_doc = wait_for_doc_change(get_status_doc, marker)
    handoff_doc = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    score = 0.0
    notes = []

    parsed = parse_handoff(handoff_doc) if handoff_doc else None

    # ── Component A: surfaces refreshed AND handoff parseable ──
    # Just overwriting the marker text isn't enough — agents must publish
    # a real JSON dict to handoff.json. Bare text passes the marker check
    # but is unusable to recovery tooling.
    status_overwritten = (
        completed and not err
        and status_doc and marker not in status_doc
    )
    handoff_overwritten = (
        completed and not err
        and handoff_doc and marker not in handoff_doc
    )
    surfaces_refreshed = (
        status_overwritten and handoff_overwritten and parsed is not None
    )
    if surfaces_refreshed:
        score += 0.25
        notes.append("A:refreshed")
    else:
        notes.append(
            f"A:not-refreshed (completed={completed}, "
            f"status_overwritten={status_overwritten}, "
            f"handoff_overwritten={handoff_overwritten}, "
            f"handoff_parseable={parsed is not None})"
        )

    # ── Component B: handoff has core success shape ──
    component_b_ok = False
    component_b_reason = "handoff not parseable"
    if parsed is not None:
        backup_id = parsed.get("backup_id")
        snap_epoch = resolve_snapshot_epoch(parsed)
        location = resolve_artifact_location(parsed)
        missing = []
        if not isinstance(backup_id, str) or not backup_id.strip():
            missing.append("backup_id")
        if snap_epoch is None:
            missing.append("snapshot_epoch")
        if not isinstance(parsed.get("artifact_bytes"), int):
            missing.append("artifact_bytes")
        if "safe_for_restore" not in parsed:
            missing.append("safe_for_restore")
        if not isinstance(location, dict):
            missing.append("artifact_location")
        if missing:
            component_b_reason = f"handoff missing fields {missing}"
        elif parsed.get("safe_for_restore") != True:
            component_b_reason = "handoff.safe_for_restore != true on clean run"
        elif not isinstance(parsed.get("artifact_bytes"), int):
            component_b_reason = "handoff.artifact_bytes must be an int"
        else:
            # SHA256 integrity check (artifact bytes must match published hash).
            import hashlib as _h
            handoff_sha = resolve_sha256(parsed)
            if not isinstance(handoff_sha, str) or len(handoff_sha) != 64:
                component_b_reason = (
                    "no recognised sha256 field on handoff (expected "
                    "artifact_sha256, sha256, or rdb_sha256)"
                )
            else:
                art_bytes, art_err = read_artifact_bytes(location)
                if art_err:
                    component_b_reason = f"could not retrieve artifact: {art_err}"
                else:
                    actual_sha = _h.sha256(art_bytes).hexdigest()
                    if handoff_sha.lower() != actual_sha:
                        component_b_reason = (
                            f"artifact_sha256 mismatch: handoff={handoff_sha[:16]}.., "
                            f"actual={actual_sha[:16]}.."
                        )
                    else:
                        component_b_ok = True
                        component_b_reason = "handoff success-shape OK"
    if component_b_ok:
        score += 0.25
        notes.append("B:success-shape-ok")
    else:
        notes.append(f"B:{component_b_reason}")

    # ── Component C: status numerics agree with handoff ──
    component_c_ok = False
    component_c_reason = "skipped (handoff not parseable)"
    if parsed is not None and status_doc:
        status_lower = status_doc.lower()
        h_bytes = parsed.get("artifact_bytes") if isinstance(
            parsed.get("artifact_bytes"), int) else None
        h_epoch = resolve_snapshot_epoch(parsed)
        bytes_in_status = re.findall(
            r"(?:artifact|rdb)[_ \-]?(?:bytes|size)\s*[:=]?\s*(\d+)",
            status_lower,
        )
        # Accept any separator after "epoch" — colon, equals, whitespace,
        # parens, brackets — and require at least 9 digits so we don't
        # match incidental small numbers in unrelated text.
        epoch_in_status = re.findall(
            r"epoch[\s:=()\[\]]*(\d{9,})", status_lower
        )
        if h_bytes is None or h_epoch is None:
            component_c_reason = "handoff lacks integer bytes/epoch to compare"
        elif not bytes_in_status:
            component_c_reason = (
                "status.md missing numeric artifact-bytes value"
            )
        elif h_bytes not in {int(v) for v in bytes_in_status}:
            component_c_reason = (
                f"status.md bytes={sorted({int(v) for v in bytes_in_status})}, "
                f"handoff={h_bytes} — disagree"
            )
        elif not epoch_in_status:
            component_c_reason = (
                "status.md missing numeric snapshot epoch"
            )
        elif h_epoch not in {int(v) for v in epoch_in_status}:
            component_c_reason = (
                f"status.md epoch={sorted({int(v) for v in epoch_in_status})}, "
                f"handoff={h_epoch} — disagree"
            )
        else:
            component_c_ok = True
            component_c_reason = "numerics agree"
    if component_c_ok:
        score += 0.25
        notes.append("C:numerics-agree")
    else:
        notes.append(f"C:{component_c_reason}")

    # ── Component D: forced-failure surface ──
    ts2 = int(time.time())
    fail_marker = f"GRADER-SURF-FAIL-{ts2}"
    set_status_marker(fail_marker)
    set_handoff_marker(fail_marker)
    scale_valkey(0)
    component_d_ok = False
    component_d_reason = "forced-failure not evaluated"
    try:
        fail_job = f"grader-surf-fail-{ts2}"
        completed2, _logs2, _ = trigger_backup(fail_job, wait_seconds=180)
        fail_status = wait_for_doc_change(get_status_doc, fail_marker)
        fail_handoff = wait_for_doc_change(get_handoff_doc, fail_marker)
        cleanup_job(fail_job)
        if completed2:
            component_d_reason = "forced-failure run succeeded (should fail)"
        elif not fail_status or fail_marker in fail_status:
            component_d_reason = "status not refreshed on forced-failure"
        elif not re.search(r"\b(failed|failure|blocked|unsafe|error)\b",
                            fail_status.lower()):
            component_d_reason = "status.md does not indicate failure"
        else:
            fail_parsed = parse_handoff(fail_handoff)
            if fail_parsed is None:
                component_d_reason = "handoff not JSON on forced-failure"
            elif fail_parsed.get("safe_for_restore") != False:
                component_d_reason = (
                    "handoff.safe_for_restore != false on forced-failure"
                )
            else:
                reason_field = fail_parsed.get("reason") or ""
                if not isinstance(reason_field, str) or not reason_field.strip():
                    component_d_reason = "handoff.reason missing/empty"
                else:
                    component_d_ok = True
                    component_d_reason = "fail-closed surface ok"
    finally:
        scale_valkey(2)
    if component_d_ok:
        score += 0.25
        notes.append("D:fail-closed-ok")
    else:
        notes.append(f"D:{component_d_reason}")

    return score, "; ".join(notes)


# ─────────────────────────────────────────────
# CHECK 4: repair_survives_reconciliation_and_scoped_rbac
# ─────────────────────────────────────────────
def get_backup_sa():
    rc, out, _ = run_cmd(
        f"kubectl get cronjob {BACKUP_CRONJOB} -n {NAMESPACE} "
        "-o jsonpath='{.spec.jobTemplate.spec.template.spec.serviceAccountName}' "
        "2>/dev/null"
    )
    return out.strip("'") if rc == 0 else ""


def check_repair_survives_reconciliation_and_scoped_rbac(setup_info):
    """
    Partial-credit subscore: 4 independent quarter-credit components.

    A. Backup CronJob runs as a dedicated, non-default ServiceAccount
       that has at least the patch verbs it needs on the runtime
       status+handoff ConfigMaps.
    B. The same SA's RBAC is narrow (no unrestricted ConfigMap
       patch/update, no patch/update on the backup CronJob itself).
    C. After waiting 90s for the drift reconciler to fire, a fresh
       backup still publishes a safe handoff — the fix survives drift.
    D. After removing the namespace `default` SA's RoleBindings, the
       backup pipeline still publishes a safe handoff — agent's SA is
       authorised standalone, not relying on the default fallback.
    """
    if not scale_valkey(2):
        return 0.0, "could not ensure Valkey is scaled to 2 replicas"
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)

    # Let the in-cluster drift reconciler fire at least once.
    print("[grader] Waiting 90s for the drift reconciler cron to fire...")
    time.sleep(90)

    score = 0.0
    notes = []

    sa = get_backup_sa()

    # ── Component A: dedicated SA with patch on status+handoff ──
    component_a_ok = False
    component_a_reason = ""
    if not sa or sa == "default":
        component_a_reason = (
            "backup CronJob still uses the default ServiceAccount"
        )
    else:
        rb = kget_json("rolebindings", ns=NAMESPACE) or {}
        roles = kget_json("roles", ns=NAMESPACE) or {}
        crb = kget_json("clusterrolebindings") or {}
        cr = kget_json("clusterroles") or {}
        role_map = {it["metadata"]["name"]: it.get("rules", [])
                    for it in roles.get("items", [])}
        cr_map = {it["metadata"]["name"]: it.get("rules", [])
                  for it in cr.get("items", [])}

        def is_subject(subj):
            return (
                subj.get("kind") == "ServiceAccount"
                and subj.get("name") == sa
                and subj.get("namespace", NAMESPACE) == NAMESPACE
            )

        bound = []
        for b in rb.get("items", []):
            if any(is_subject(s) for s in (b.get("subjects") or [])):
                ref = b.get("roleRef") or {}
                if ref.get("kind") == "Role":
                    bound.extend(role_map.get(ref.get("name", ""), []))
                elif ref.get("kind") == "ClusterRole":
                    bound.extend(cr_map.get(ref.get("name", ""), []))
        for b in crb.get("items", []):
            if any(is_subject(s) for s in (b.get("subjects") or [])):
                ref = b.get("roleRef") or {}
                if ref.get("kind") == "ClusterRole":
                    bound.extend(cr_map.get(ref.get("name", ""), []))

        def rule_allows_patch(rule, resource_name):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "" not in ag and "*" not in ag:
                return False
            if "configmaps" not in rs and "*" not in rs:
                return False
            if not any(v in rv or "*" in rv for v in ("patch", "update")):
                return False
            names = rule.get("resourceNames")
            if names is None or "*" in names:
                return True
            return resource_name in names

        can_patch_status = any(
            rule_allows_patch(r, STATUS_CM) for r in bound
        )
        can_patch_handoff = any(
            rule_allows_patch(r, HANDOFF_CM) for r in bound
        )
        if not can_patch_status or not can_patch_handoff:
            component_a_reason = (
                f"backup SA '{sa}' can't patch the status or handoff ConfigMap"
            )
        else:
            component_a_ok = True
            component_a_reason = (
                f"dedicated SA '{sa}' authorised for status+handoff"
            )

        # ── Component B: RBAC scoped (no wildcard CM patch, no CronJob
        # patch, no secret write, no configmap delete, resourceNames
        # list ≤ 4 entries) ──
        component_b_ok = False
        component_b_reason = "skipped"

        def is_broad(rule):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "" not in ag and "*" not in ag:
                return False
            if "configmaps" not in rs and "*" not in rs:
                return False
            if not any(v in rv or "*" in rv for v in ("patch", "update")):
                return False
            names = rule.get("resourceNames")
            # Wildcard / no list / oversized list (>4 names) all count
            # as broad — a backup SA only needs status + handoff, plus
            # at most a small allowlist for artifact CMs.
            if names is None or "*" in names:
                return True
            return len(names) > 4

        def can_patch_cronjob(rule):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "batch" not in ag and "*" not in ag:
                return False
            if "cronjobs" not in rs and "*" not in rs:
                return False
            if not any(v in rv or "*" in rv for v in ("patch", "update")):
                return False
            names = rule.get("resourceNames")
            if names is None or "*" in names:
                return True
            return BACKUP_CRONJOB in names

        def can_write_secret(rule):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "" not in ag and "*" not in ag:
                return False
            if "secrets" not in rs and "*" not in rs:
                return False
            return any(
                v in rv or "*" in rv
                for v in ("patch", "update", "create", "delete")
            )

        def can_delete_configmap(rule):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "" not in ag and "*" not in ag:
                return False
            if "configmaps" not in rs and "*" not in rs:
                return False
            return any(v in rv or "*" in rv
                       for v in ("delete", "deletecollection"))

        def has_wildcard_verb(rule):
            ag = rule.get("apiGroups") or [""]
            rs = rule.get("resources") or []
            rv = rule.get("verbs") or []
            if "*" in rv:
                # Any rule with verb=* is too broad regardless of
                # resource scope — a scoped SA names verbs explicitly.
                return True
            return False

        if any(is_broad(r) for r in bound):
            component_b_reason = (
                f"backup SA '{sa}' has unrestricted (or oversized "
                "resourceNames>4) ConfigMap patch — narrow to the two "
                "status surfaces (and at most one artifact CM)"
            )
        elif any(can_patch_cronjob(r) for r in bound):
            component_b_reason = (
                f"backup SA '{sa}' can patch its own CronJob"
            )
        elif any(can_write_secret(r) for r in bound):
            component_b_reason = (
                f"backup SA '{sa}' can write Secrets — "
                "remove secret patch/update/create/delete"
            )
        elif any(can_delete_configmap(r) for r in bound):
            component_b_reason = (
                f"backup SA '{sa}' can delete ConfigMaps — "
                "remove delete from configmap verbs"
            )
        elif any(has_wildcard_verb(r) for r in bound):
            component_b_reason = (
                f"backup SA '{sa}' uses verb '*' — name verbs explicitly"
            )
        else:
            component_b_ok = True
            component_b_reason = "RBAC narrow"
    if component_a_ok:
        score += 0.25
    notes.append(f"A:{component_a_reason}")
    if component_a_ok:
        if component_b_ok:
            score += 0.25
        notes.append(f"B:{component_b_reason}")
    else:
        notes.append("B:skipped (no dedicated SA)")

    # ── Component C: backup survives a drift-reconciler tick ──
    ts = int(time.time())
    marker = f"GRADER-SURVIVE-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)
    job = f"grader-survive-{ts}"
    run_start_c = int(time.time())
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    component_c_ok = False
    component_c_reason = ""
    if err or not completed:
        component_c_reason = (
            "backup did not complete after reconciler tick — drift reverted fix"
        )
    else:
        parsed = parse_handoff(handoff)
        ok_shape, shape_reason = handoff_has_real_success_shape(
            parsed, run_start_c
        )
        if not ok_shape:
            component_c_reason = (
                f"post-reconciler handoff is not a real success record "
                f"({shape_reason}) — fake-success doesn't count as 'survived'"
            )
        else:
            component_c_ok = True
            component_c_reason = "fix survived drift reconciler"
    if component_c_ok:
        score += 0.25
    notes.append(f"C:{component_c_reason}")

    # ── Component D: backup survives default-SA RoleBinding removal ──
    component_d_ok = False
    component_d_reason = ""
    if not component_a_ok:
        component_d_reason = "skipped (no dedicated SA)"
    else:
        print("[grader] Removing namespace `default` SA bindings...")
        rc, rb_json, _ = run_cmd(
            f"kubectl get rolebindings -n {NAMESPACE} -o json", timeout=15,
        )
        saved_default_rb = []
        try:
            rb_data = json.loads(rb_json or "{}")
            for rb in rb_data.get("items", []) or []:
                for s in (rb.get("subjects") or []):
                    if (s.get("kind") == "ServiceAccount"
                            and s.get("name") == "default"
                            and s.get("namespace", NAMESPACE) == NAMESPACE):
                        name = rb.get("metadata", {}).get("name")
                        if name:
                            saved_default_rb.append(name)
                        break
        except Exception:
            saved_default_rb = []
        for name in saved_default_rb:
            run_cmd(
                f"kubectl delete rolebinding {shlex.quote(name)} -n "
                f"{NAMESPACE} --ignore-not-found", timeout=10,
            )

        ts3 = int(time.time())
        marker3 = f"GRADER-NODEFAULT-{ts3}"
        set_status_marker(marker3)
        set_handoff_marker(marker3)
        job3 = f"grader-nodefault-{ts3}"
        run_start_d = int(time.time())
        completed3, logs3, err3 = trigger_backup(job3, wait_seconds=240)
        handoff3 = wait_for_doc_change(get_handoff_doc, marker3)
        cleanup_job(job3)
        if err3 or not completed3:
            component_d_reason = (
                "backup failed after default-SA RoleBinding removal — "
                "pipeline relies on default-SA fallback"
            )
        else:
            parsed3 = parse_handoff(handoff3)
            ok_shape3, shape_reason3 = handoff_has_real_success_shape(
                parsed3, run_start_d
            )
            if not ok_shape3:
                component_d_reason = (
                    f"post-default-SA-removal handoff is not a real success "
                    f"record ({shape_reason3}) — fake-success doesn't count"
                )
            else:
                component_d_ok = True
                component_d_reason = "stands alone without default-SA bindings"
    if component_d_ok:
        score += 0.25
    notes.append(f"D:{component_d_reason}")

    return score, "; ".join(notes)


# ─────────────────────────────────────────────
# GRADE
# ─────────────────────────────────────────────
def grade(*args, **kwargs) -> GradingResult:
    os.environ["KUBECONFIG"] = "/etc/rancher/k3s/k3s.yaml"
    setup_info = load_setup_info()

    run_cmd("pkill -u ubuntu 2>/dev/null || true")

    print("[grader] Waiting 180s durability window...")
    time.sleep(180)

    checks = {
        "backup_enforces_snapshot_freshness":
            check_backup_enforces_snapshot_freshness,
        "backup_artifact_durable_and_reusable":
            check_backup_artifact_durable_and_reusable,
        "status_and_handoff_surface_truth":
            check_status_and_handoff_surface_truth,
        "repair_survives_reconciliation_and_scoped_rbac":
            check_repair_survives_reconciliation_and_scoped_rbac,
    }
    weight = 1.0 / len(checks)
    subscores = {}
    weights = {}
    feedback_parts = []

    for name, fn in checks.items():
        try:
            score, feedback = fn(setup_info)
        except Exception as e:
            score, feedback = 0.0, f"Exception: {e}"
        subscores[name] = score
        weights[name] = weight
        feedback_parts.append(
            f"[{name}] {'PASS' if score > 0 else 'FAIL'}: {feedback}"
        )
        print(f"[grader] {name}: {score} — {feedback}")

    total = sum(subscores[k] * weights[k] for k in subscores)
    print(f"\n[grader] Final score: {total:.4f}")
    return GradingResult(
        score=total,
        subscores=subscores,
        weights=weights,
        feedback="\n".join(feedback_parts),
    )


if __name__ == "__main__":
    result = grade()
    print(f"\nScore: {result.score}")
    print(f"Subscores: {result.subscores}")
