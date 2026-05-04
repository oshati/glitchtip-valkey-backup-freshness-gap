#!/usr/bin/env python3
"""
Grader for glitchtip-valkey-backup-freshness-gap.

Four binary subscores (1.0 or 0.0 each, weight 0.25 each). Per the
QC reviewer rule, each subscore is all-or-nothing — a single
coherent objective that is either fully met (1.0) or not met (0.0).
Internal sub-checks are AND-gated within each subscore; they are
facets of the same property, not independent objectives.

1. backup_enforces_snapshot_freshness — "fresh point-in-time
   snapshots, not stale or live re-serializations."

2. backup_artifact_durable_and_reusable — "the artifact is durably
   persisted AND can actually be restored."

3. status_and_handoff_surface_truth — "the runtime status surfaces
   truthfully attest each run; no replays, forgeries, or false
   greens/reds."

4. repair_survives_reconciliation_and_scoped_rbac — "the backup
   pipeline's runtime configuration is hardened and durable: correct
   identity, scoped authority, survives reconciliation, auditable
   rotation trail."

Each subscore returns 1.0 only if every facet of its single
objective is satisfied; otherwise 0.0. Feedback strings enumerate
which facet failed, for diagnostic clarity.
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


TRUST_ANCHOR_CM = "glitchtip-valkey-backup-trust-anchor"


def _canonical_handoff_payload(parsed):
    """
    Build the canonical bytes that the handoff signature MUST cover.
    Documented in task.yaml: a sorted-key JSON object containing
    backup_id, snapshot_epoch, artifact_sha256, artifact_source_pod,
    artifact_source_role. No spaces, sort_keys=True. Recovery tooling
    constructs this payload identically.
    """
    payload = {
        "backup_id": parsed.get("backup_id"),
        "snapshot_epoch": parsed.get("snapshot_epoch"),
        "artifact_sha256": parsed.get("artifact_sha256"),
        "artifact_source_pod": parsed.get("artifact_source_pod"),
        "artifact_source_role": parsed.get("artifact_source_role"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _verify_handoff_signature(parsed):
    """
    Verify Ed25519 signature on the canonical handoff payload, and
    confirm the public key fingerprint is registered in the trust
    anchor ConfigMap.

    Returns (ok, reason, fingerprint_hex_or_None).
    """
    import base64 as _b64
    import hashlib as _h
    sig_block = parsed.get("signature") if isinstance(parsed, dict) else None
    if not isinstance(sig_block, dict):
        return False, "no signature block in handoff", None
    if sig_block.get("alg") != "ed25519":
        return False, f"signature.alg={sig_block.get('alg')!r} (must be 'ed25519')", None
    pk_b64 = sig_block.get("public_key")
    sig_b64 = sig_block.get("sig")
    if not isinstance(pk_b64, str) or not isinstance(sig_b64, str):
        return False, "signature.public_key / signature.sig must be base64 strings", None
    try:
        pk = _b64.b64decode(pk_b64)
        sig = _b64.b64decode(sig_b64)
    except Exception as e:
        return False, f"signature base64 decode failed: {e}", None
    if len(pk) != 32:
        return False, f"public_key is {len(pk)}B (must be raw 32-byte Ed25519 key)", None
    if len(sig) != 64:
        return False, f"sig is {len(sig)}B (must be raw 64-byte Ed25519 signature)", None
    # Verify via openssl subprocess — the apex_arena base image's
    # python doesn't ship `cryptography`'s _cffi_backend, but openssl
    # 3.x with Ed25519 support is on PATH. Wrap raw 32-byte pubkey in
    # the 12-byte Ed25519 SubjectPublicKeyInfo DER prefix to feed it to
    # `openssl pkeyutl -verify -pubin -keyform DER`.
    import os as _os
    import tempfile as _tf
    tmpdir = _tf.mkdtemp(prefix="grader-ed25519-")
    try:
        pub_der = bytes.fromhex("302a300506032b6570032100") + pk
        pk_path = _os.path.join(tmpdir, "pub.der")
        sig_path = _os.path.join(tmpdir, "sig.bin")
        canon_path = _os.path.join(tmpdir, "canon.bin")
        with open(pk_path, "wb") as f:
            f.write(pub_der)
        with open(sig_path, "wb") as f:
            f.write(sig)
        with open(canon_path, "wb") as f:
            f.write(_canonical_handoff_payload(parsed))
        rc, out, err = run_cmd(
            f"openssl pkeyutl -verify -pubin -inkey {pk_path} "
            f"-keyform DER -rawin -in {canon_path} -sigfile {sig_path} 2>&1",
            timeout=15,
        )
        combined = (out + " " + err).lower()
        if rc != 0 or "successful" not in combined:
            return False, (
                f"Ed25519 signature does NOT verify against canonical "
                f"payload (openssl rc={rc}, out={out[:120]!r})"
            ), None
    finally:
        try:
            for f in _os.listdir(tmpdir):
                _os.unlink(_os.path.join(tmpdir, f))
            _os.rmdir(tmpdir)
        except Exception:
            pass
    fingerprint = _h.sha256(pk).hexdigest()
    rc, fps_data, _ = run_cmd(
        f"kubectl get configmap {TRUST_ANCHOR_CM} -n {NAMESPACE} "
        f"-o jsonpath='{{.data.public_key_fingerprints}}' 2>/dev/null",
        timeout=10,
    )
    if rc != 0:
        return False, f"could not read trust-anchor CM {TRUST_ANCHOR_CM}", None
    registered = {ln.strip().lower() for ln in (fps_data or "").splitlines() if ln.strip()}
    if fingerprint not in registered:
        return False, (
            f"public key fingerprint {fingerprint[:16]}.. NOT registered in "
            f"{TRUST_ANCHOR_CM}.public_key_fingerprints "
            f"(registered: {len(registered)} keys)"
        ), fingerprint
    return True, "verified + registered", fingerprint


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
            # Resolve `path` against the PVC root robustly.
            # Agents declare the artifact path with one of:
            #   (a) PVC-root-relative (canonical):  "/dump-XXX.rdb"
            #   (b) Container-mountpoint-relative:  "/archive/dump-XXX.rdb"
            #       where the agent's pod mounted the PVC at /archive.
            # We search for the file under all plausible interpretations
            # so the same file written by the agent is reachable
            # regardless of which convention they picked.
            import os as _os
            rel = path.lstrip("/")
            base = _os.path.basename(path)
            candidates = [rel]
            # Strip a known mount-point prefix if the agent included one.
            for prefix in ("archive/", "backups/", "backup/", "data/",
                           "valkey-backup-archive/"):
                if rel.startswith(prefix):
                    candidates.append(rel[len(prefix):])
            # Last-resort: just the basename at PVC root.
            if base and base not in candidates:
                candidates.append(base)
            seen = set()
            ordered = []
            for c in candidates:
                if c and c not in seen:
                    seen.add(c)
                    ordered.append(c)
            b64 = ""
            tried = []
            for cand in ordered:
                cand_q = cand.replace("'", "'\\''")
                rc, out, _ = run_cmd(
                    f"kubectl exec -n {ns} {probe} -- sh -c "
                    f"'[ -f /probe/{cand_q} ] && base64 /probe/{cand_q} | tr -d \"\\n\"' 2>/dev/null",
                    timeout=60,
                )
                tried.append(cand)
                if out and out.strip():
                    b64 = out.strip()
                    break
            if not b64:
                return b"", (
                    f"artifact not found on PVC {claim} (tried: "
                    f"{', '.join('/probe/' + c for c in tried)}; "
                    f"declared path: {path})"
                )
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
    Binary subscore (1.0 or 0.0). Single coherent objective:
        "The backup pipeline produces FRESH point-in-time snapshots
         and rejects stale ones."

    Four sub-checks — each contributes to the same objective and ALL
    must pass for the subscore. They are not independent in meaning;
    they are different facets of "freshness":
      A. Clean run completes and emits a real success record (proves
         a fresh attempt actually ran end-to-end and committed to
         master as the source).
      B. snapshot_epoch is strictly monotonic across two consecutive
         runs (proves a re-published static RDB is detected as stale).
      C. The captured artifact is fork-frozen at BGSAVE-fork time
         (pre-key in RDB; post-BGSAVE-start key NOT in RDB) — proves
         the snapshot is not a live re-serialization at fetch time.
      D. Forced-failure (Valkey scaled to 0) is reported as unsafe —
         proves the freshness contract is enforced even on the sad
         path, not silently inflated to a green record.
    Any failed facet => the freshness objective is not met => 0.0.
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

    # Point-in-time detection: spawn a background thread that waits
    # for either rdb_bgsave_in_progress=1 or LASTSAVE to advance, then
    # writes a unique post-key onto master. A truly fork-frozen RDB
    # captured at fork-time will NOT contain this post-key. A live
    # re-serialize (e.g. sequential `BGSAVE; wait LASTSAVE; --rdb`,
    # where `--rdb` triggers a SECOND fork after the post-key has
    # landed on master) WILL contain it.
    import threading as _thr
    rdb_post_name = f"grader:post-bgsave:{ts}"
    rdb_post_value = f"POST-{ts}-NONCE-{os.urandom(8).hex()}"
    post_written_flag = [False]
    post_stop_flag = [False]

    def _post_key_thread():
        deadline = time.time() + 90
        while time.time() < deadline and not post_stop_flag[0]:
            rc, info_out, _ = run_cmd(
                f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- "
                "valkey-cli INFO persistence 2>/dev/null", timeout=5,
            )
            in_progress = "rdb_bgsave_in_progress:1" in (info_out or "")
            curr_last = 0
            for ln in (info_out or "").splitlines():
                if ln.startswith("rdb_last_save_time:"):
                    try:
                        curr_last = int(ln.split(":", 1)[1].strip())
                    except ValueError:
                        curr_last = 0
                    break
            if in_progress or curr_last > lastsave_before:
                if write_marker_key(rdb_post_name, rdb_post_value):
                    post_written_flag[0] = True
                return
            time.sleep(0.15)
        # Fallback: deadline hit without observing BGSAVE — write
        # anyway so the post-key check has a definite verdict.
        if write_marker_key(rdb_post_name, rdb_post_value):
            post_written_flag[0] = True

    post_thread = _thr.Thread(target=_post_key_thread, daemon=True)
    post_thread.start()

    job = f"grader-fresh-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)
    post_stop_flag[0] = True
    post_thread.join(timeout=5)

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

    # ── Component C: point-in-time RDB ──
    # The captured RDB MUST contain the pre-key freshness marker
    # (proves the snapshot covers state up to the moment of trigger)
    # AND MUST NOT contain the post-BGSAVE-start key (proves the
    # snapshot is fork-frozen at BGSAVE-fork time, not a live
    # re-serialization at fetch time).
    #
    # The post-key was written by the background thread once it
    # observed `rdb_bgsave_in_progress=1` (or LASTSAVE advance) on
    # master. So by the time master's BGSAVE forks, post-key is NOT
    # yet on master; by the time master's BGSAVE completes,
    # post-key IS on master. Any agent that:
    #   - issues `valkey-cli --rdb` AFTER waiting for LASTSAVE to
    #     advance triggers a SECOND fork in the server, and that
    #     fork captures the live keyspace including the post-key →
    #     FAIL.
    #   - reads /data/dump.rdb byte-for-byte (the fork-frozen file)
    #     captures only state at fork-1 time → PASS.
    #   - issues a single `--rdb` call (no separate BGSAVE wait)
    #     atomically queues onto the in-flight fork-1 stream →
    #     PASS, because the post-key write only happens AFTER the
    #     fork has already started.
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
            elif not post_written_flag[0]:
                # Grader could not deterministically write the
                # post-key (rare — Valkey unreachable). Pass on the
                # pre-key alone in that degenerate case.
                component_c_ok = True
                component_c_reason = (
                    "marker present in RDB (post-key check skipped: "
                    "grader could not write post-key)"
                )
            elif rdb_contains_marker(
                rdb_bytes, rdb_post_name, rdb_post_value
            ):
                component_c_reason = (
                    f"point-in-time violation: post-BGSAVE-start key "
                    f"'{rdb_post_name}' IS in captured RDB — snapshot "
                    "was a live re-serialization at fetch time, not "
                    "fork-frozen at BGSAVE-fork time"
                )
            else:
                component_c_ok = True
                component_c_reason = (
                    "marker present in RDB; post-key absent "
                    "(point-in-time fork-frozen)"
                )
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

    return (1.0 if score >= 0.999 else 0.0), "; ".join(notes)


# ─────────────────────────────────────────────
# CHECK 2: backup_artifact_durable_and_reusable
# ─────────────────────────────────────────────
def check_backup_artifact_durable_and_reusable(setup_info):
    """
    Binary subscore (1.0 or 0.0). Single coherent objective:
        "The persisted artifact is a TRUE point-in-time recovery
         target — durably stored AND round-trips the freshness
         contract to recovery time."

    Four facets — all aspects of "is the persisted artifact actually
    usable as a recovery target?":
      A. The artifact_location declared in the handoff resolves to a
         real, durable in-cluster resource (configmap/secret/pvc).
      B. The retrieved bytes start with the REDIS magic and exceed a
         minimum size — proves the bytes are a real RDB.
      C. The restore_proof key_count + artifact_bytes match the
         retrieved bytes — proves the handoff's claims line up with
         what was actually persisted.
      D. End-to-end restore probe: a clean Valkey loaded with the
         persisted bytes (i) recovers the full stable live keyset
         AND (ii) does NOT contain the post-BGSAVE-start key the
         grader writes during the BGSAVE window. The second
         condition distinguishes a true fork-frozen capture (point-
         in-time recovery target — post-key absent) from a live
         re-serialization at fetch time (post-key round-trips
         through restore — NOT a recovery target). S1 tests
         freshness at capture time; S2-D tests the same property
         at restore time.
    Any failed facet => the persisted artifact is not a point-in-
    time recovery target => 0.0.
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

    # Spawn post-BGSAVE-start key writer (same pattern as S1-C) so the
    # restore probe can verify the persisted artifact is a true
    # point-in-time recovery target — the post-key must NOT round-trip
    # through restore. A fork-frozen capture excludes it; a live
    # re-serialization at fetch time captures it.
    import threading as _thr
    rdb_post_name = f"grader:post-bgsave:{ts}"
    rdb_post_value = f"POST-ART-{ts}-NONCE-{os.urandom(8).hex()}"
    post_written_flag = [False]
    post_stop_flag = [False]
    lastsave_before_art = get_lastsave()

    def _post_key_thread_art():
        deadline = time.time() + 90
        while time.time() < deadline and not post_stop_flag[0]:
            rc, info_out, _ = run_cmd(
                f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- "
                "valkey-cli INFO persistence 2>/dev/null", timeout=5,
            )
            in_progress = "rdb_bgsave_in_progress:1" in (info_out or "")
            curr_last = 0
            for ln in (info_out or "").splitlines():
                if ln.startswith("rdb_last_save_time:"):
                    try:
                        curr_last = int(ln.split(":", 1)[1].strip())
                    except ValueError:
                        curr_last = 0
                    break
            if in_progress or curr_last > lastsave_before_art:
                if write_marker_key(rdb_post_name, rdb_post_value):
                    post_written_flag[0] = True
                return
            time.sleep(0.15)
        if write_marker_key(rdb_post_name, rdb_post_value):
            post_written_flag[0] = True

    post_thread = _thr.Thread(target=_post_key_thread_art, daemon=True)
    post_thread.start()

    job = f"grader-art-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)
    post_stop_flag[0] = True
    post_thread.join(timeout=5)

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
            # Probe with the live stable keys PLUS the post-BGSAVE-start
            # key. If the persisted artifact is a true point-in-time
            # recovery target (fork-frozen), the post-key was written to
            # master AFTER the fork started, so it is NOT in the
            # captured RDB and NOT recoverable from the artifact. If the
            # artifact is a live re-serialization (e.g. sequential
            # BGSAVE+--rdb), the post-key landed on master before the
            # second fork and IS in the persisted bytes — round-trips
            # through restore. We require the restored instance to
            # contain every stable key AND to NOT contain the post-key.
            probe_targets = set(live_keys)
            probe_targets.add(rdb_post_name)
            matched, missing, probe_err = restore_probe_keyset(
                artifact, probe_targets
            )
            if probe_err and not matched:
                component_d_reason = f"restore probe failed: {probe_err}"
            else:
                post_key_in_restore = rdb_post_name in matched
                # Filter the keyset comparison back to stable-only.
                matched_stable = {k for k in matched if not _is_grader_or_traffic(k)}
                missing_stable = {k for k in missing if not _is_grader_or_traffic(k)}
                missed_count = len(live_keys) - len(matched_stable)
                if missed_count > 0:
                    component_d_reason = (
                        f"restore probe recovered "
                        f"{len(matched_stable)}/{len(live_keys)} stable keys — "
                        f"need full match. missing sample: "
                        f"{sorted(list(missing_stable))[:5]}"
                    )
                elif post_key_in_restore and post_written_flag[0]:
                    component_d_reason = (
                        f"persisted artifact is NOT a point-in-time recovery "
                        f"target: post-BGSAVE-start key '{rdb_post_name}' "
                        "round-tripped through restore (the captured bytes "
                        "include a write that landed on master AFTER the "
                        "snapshot started — i.e. live re-serialization, not "
                        "fork-frozen)"
                    )
                else:
                    component_d_ok = True
                    component_d_reason = (
                        f"restore probe matched "
                        f"{len(matched_stable)}/{len(live_keys)} stable; "
                        "post-BGSAVE-start key absent (point-in-time "
                        "recovery target)"
                    )
    if component_d_ok:
        score += 0.25
    notes.append(f"D:{component_d_reason}")

    return (1.0 if score >= 0.999 else 0.0), "; ".join(notes)


# ─────────────────────────────────────────────
# CHECK 3: status_and_handoff_surface_truth
# ─────────────────────────────────────────────
def check_status_and_handoff_surface_truth(setup_info):
    """
    Binary subscore (1.0 or 0.0). Single coherent objective:
        "The runtime status surfaces truthfully attest each run —
         no stale replays, no forged signatures, no false greens,
         no false reds."

    Four facets of the same objective. Surface truthfulness is the
    one property; each facet is a different way the surfaces could
    be lying:
      A. resourceVersion progresses + parseable JSON +
         safe_for_restore:true on a clean run — catches "lied via
         stale replay" (handoff bytes never actually re-written).
      B. Ed25519 signature verifies + key rotated between runs —
         catches "lied via forged or re-used signature".
      C. master_replid + master_repl_offset match live cluster —
         catches "lied about which master generation" (failover
         masquerade) and "lied about freshness" (re-published static
         snapshot from earlier offset).
      D. Forced-failure path emits unsafe handoff with non-empty
         reason — catches "lied via false-green-on-error".
    Any failed facet => surface truthfulness objective is not met
    => 0.0.
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

    # Snapshot the handoff CM's resourceVersion BEFORE triggering the
    # backup. After the run we require it to have strictly increased,
    # AND we require the backup to have caused at least 2 increments
    # (set_*_marker call by grader = 1 increment; agent's own publish
    # = 1+ increments). Single increment means the agent never wrote
    # to the handoff CM — they only watched the marker change.
    def _get_handoff_rv():
        rc, out, _ = run_cmd(
            f"kubectl get configmap {HANDOFF_CM} -n {NAMESPACE} "
            "-o jsonpath='{.metadata.resourceVersion}' 2>/dev/null",
            timeout=10,
        )
        out = out.strip().strip("'")
        try:
            return int(out)
        except ValueError:
            return 0
    rv_before = _get_handoff_rv()

    job = f"grader-surf-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    status_doc = wait_for_doc_change(get_status_doc, marker)
    handoff_doc = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    rv_after = _get_handoff_rv()

    score = 0.0
    notes = []

    parsed = parse_handoff(handoff_doc) if handoff_doc else None

    # ── Component A: handoff CM resourceVersion progression ──
    # The previous "marker overwritten" check was dead weight (5/5
    # passing across rollouts) because anything that prints something
    # to handoff.json overwrites the marker. The strict version: the
    # handoff CM's metadata.resourceVersion must strictly increase
    # AND the post-trigger handoff must be parseable JSON with
    # safe_for_restore=true. resourceVersion is a Kubernetes-server-
    # assigned monotonic counter on each CM mutation; it cannot be
    # forged by an agent that simply replays an old handoff body.
    status_overwritten = (
        completed and not err
        and status_doc and marker not in status_doc
    )
    handoff_overwritten = (
        completed and not err
        and handoff_doc and marker not in handoff_doc
    )
    rv_progressed = rv_after > rv_before
    surfaces_refreshed = (
        status_overwritten and handoff_overwritten and parsed is not None
        and rv_progressed
        and parsed.get("safe_for_restore") is True
    )
    if surfaces_refreshed:
        score += 0.25
        notes.append(f"A:refreshed (rv {rv_before}->{rv_after})")
    else:
        notes.append(
            f"A:not-refreshed (completed={completed}, "
            f"status_overwritten={status_overwritten}, "
            f"handoff_overwritten={handoff_overwritten}, "
            f"handoff_parseable={parsed is not None}, "
            f"rv_progressed={rv_progressed} ({rv_before}->{rv_after}), "
            f"safe_for_restore=={parsed.get('safe_for_restore') if parsed else 'N/A'})"
        )

    # ── Component B: Ed25519 signature verifies + key rotation ──
    # Replaces the old plain-sha256 check. Handoff's `signature` block
    # must:
    #   * declare alg="ed25519" with raw 32-byte public_key (b64) and
    #     raw 64-byte sig (b64)
    #   * verify against canonical JSON of the success record
    #   * sha256(public_key) must appear in the trust-anchor ConfigMap
    #   * a SECOND run's public_key fingerprint must be DIFFERENT from
    #     the first run's (key rotation between consecutive backups)
    # Reuses the plain-bytes sha256 sanity check as a fallback fail
    # state. This is the strongest authenticity signal in the grader.
    component_b_ok = False
    component_b_reason = "handoff not parseable"
    run1_fingerprint = None
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
        sig_block = parsed.get("signature")
        if not isinstance(sig_block, dict):
            missing.append("signature")
        if missing:
            component_b_reason = f"handoff missing fields {missing}"
        elif parsed.get("safe_for_restore") != True:
            component_b_reason = "handoff.safe_for_restore != true on clean run"
        else:
            ok, reason, fp = _verify_handoff_signature(parsed)
            if not ok:
                component_b_reason = f"signature: {reason}"
            else:
                # Sanity-check sha256 still matches actual artifact bytes
                # (so signature can't lie about hash either).
                import hashlib as _h
                handoff_sha = resolve_sha256(parsed)
                art_bytes, art_err = read_artifact_bytes(location)
                if art_err:
                    component_b_reason = f"could not retrieve artifact: {art_err}"
                elif _h.sha256(art_bytes).hexdigest() != (handoff_sha or "").lower():
                    component_b_reason = (
                        f"artifact_sha256 mismatch: handoff={handoff_sha[:16] if handoff_sha else 'none'}.., "
                        f"actual={_h.sha256(art_bytes).hexdigest()[:16]}.."
                    )
                else:
                    run1_fingerprint = fp
                    # Trigger a SECOND run and require a DIFFERENT
                    # signing key fingerprint — proves rotation.
                    time.sleep(3)
                    ts_b2 = int(time.time())
                    marker_b2 = f"GRADER-SURF-B2-{ts_b2}"
                    set_status_marker(marker_b2)
                    set_handoff_marker(marker_b2)
                    job_b2 = f"grader-surf-b2-{ts_b2}"
                    completed_b2, _, _ = trigger_backup(job_b2, wait_seconds=240)
                    handoff_b2 = wait_for_doc_change(get_handoff_doc, marker_b2)
                    cleanup_job(job_b2)
                    parsed_b2 = parse_handoff(handoff_b2)
                    if not completed_b2 or parsed_b2 is None:
                        component_b_reason = (
                            "second run for rotation check did not complete"
                        )
                    else:
                        ok2, reason2, fp2 = _verify_handoff_signature(parsed_b2)
                        if not ok2:
                            component_b_reason = (
                                f"run 2 signature: {reason2}"
                            )
                        elif fp2 == run1_fingerprint:
                            component_b_reason = (
                                f"signing key NOT rotated between runs: "
                                f"both fingerprint={fp[:16]}.. "
                                "(must rotate per run)"
                            )
                        else:
                            component_b_ok = True
                            component_b_reason = (
                                f"signature ok + rotated "
                                f"({run1_fingerprint[:12]}.. -> "
                                f"{fp2[:12]}..)"
                            )
    if component_b_ok:
        score += 0.25
        notes.append(f"B:{component_b_reason}")
    else:
        notes.append(f"B:{component_b_reason}")

    # ── Component C: master replication identity attestation ──
    # Replaces the old "status numerics agree" cosmetic check with a
    # consistency probe. Handoff must declare:
    #   - master_replid: the master's 40-hex replication ID from
    #     `INFO replication`. Stable across replication; rotates on
    #     failover. Lets recovery tooling detect "different master
    #     produced this artifact".
    #   - master_repl_offset: integer current replication offset at
    #     capture time. Strictly increases under writes; lets recovery
    #     tooling detect re-published static snapshots.
    # Grader queries live master INFO replication and confirms:
    #   * declared master_replid == live master_replid (no failover
    #     masquerade), 40-hex
    #   * declared master_repl_offset is a non-negative int and is
    #     <= live offset + 1024 (within reasonable drift since capture
    #     plus tolerance for ongoing writes by traffic-gen)
    component_c_ok = False
    component_c_reason = "skipped (handoff not parseable)"
    if parsed is not None:
        h_replid = parsed.get("master_replid")
        h_repl_off = parsed.get("master_repl_offset")
        if not isinstance(h_replid, str) or not re.fullmatch(r"[0-9a-f]{40}", h_replid or ""):
            component_c_reason = (
                f"handoff.master_replid missing or not 40-hex "
                f"(got {h_replid!r})"
            )
        elif not isinstance(h_repl_off, int) or h_repl_off < 0:
            component_c_reason = (
                f"handoff.master_repl_offset not a non-negative int "
                f"(got {h_repl_off!r})"
            )
        else:
            rc, info_out, _ = run_cmd(
                f"kubectl exec -n {NAMESPACE} {VALKEY_POD} -- "
                "valkey-cli INFO replication 2>/dev/null", timeout=15)
            live_replid = ""
            live_offset = -1
            for ln in (info_out or "").splitlines():
                ln = ln.strip()
                if ln.startswith("master_replid:"):
                    live_replid = ln.split(":", 1)[1].strip()
                elif ln.startswith("master_repl_offset:"):
                    try:
                        live_offset = int(ln.split(":", 1)[1].strip())
                    except ValueError:
                        live_offset = -1
            if rc != 0 or not live_replid:
                component_c_reason = (
                    "could not query live master_replid / offset for cross-check"
                )
            elif live_replid.lower() != h_replid.lower():
                component_c_reason = (
                    f"master_replid mismatch: handoff={h_replid[:12]}.., "
                    f"live={live_replid[:12]}.. — backup did not capture "
                    "from the current master generation"
                )
            elif live_offset < 0:
                component_c_reason = "could not parse live master_repl_offset"
            elif h_repl_off > live_offset + 1024:
                component_c_reason = (
                    f"master_repl_offset implausible: handoff={h_repl_off} > "
                    f"live={live_offset} + 1024 tolerance"
                )
            else:
                component_c_ok = True
                component_c_reason = (
                    f"replication identity ok "
                    f"(replid={h_replid[:12]}.., offset={h_repl_off}/{live_offset})"
                )
    if component_c_ok:
        score += 0.25
        notes.append(f"C:{component_c_reason}")
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

    return (1.0 if score >= 0.999 else 0.0), "; ".join(notes)


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
    Binary subscore (1.0 or 0.0). Single coherent objective:
        "The backup pipeline's runtime configuration is hardened
         and durable — correct identity, correctly scoped authority,
         survives in-cluster reconciliation, and produces an
         auditable rotated-key trail."

    Four facets of the same objective (pipeline runtime integrity is
    the property; each facet is a way that integrity can fail):
      A. Identity hardening: dedicated non-default ServiceAccount AND
         pod template satisfies PSS Restricted (runAsNonRoot,
         allowPrivilegeEscalation=false, readOnlyRootFilesystem=true,
         capabilities.drop includes ALL, seccompProfile=RuntimeDefault).
      B. Authority hardening: the same SA's RBAC is narrow (no
         unrestricted ConfigMap patch/update, no CronJob patch/update,
         no Secret writes, no ConfigMap delete, no `*` verb,
         resourceNames list ≤ 4 entries).
      C. Reconciliation durability: after the drift reconciler fires,
         a fresh backup still publishes a real success record — the
         fix survives the supported template-management flow.
      D. Auditable rotation trail: the trust-anchor CM's
         public_key_fingerprints accumulates ≥3 distinct registered
         Ed25519 fingerprints across grader-triggered runs with no
         reuse — proves the rotation contract is actually being
         exercised, not faked once.
    Any failed facet => the runtime-integrity objective is not met
    => 0.0.
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
            # PSS Restricted compliance check on the backup CronJob's
            # pod-template securityContext. The contract (per task.yaml)
            # requires backup workloads to run with the Pod Security
            # Standards "restricted" profile: runAsNonRoot=true,
            # allowPrivilegeEscalation=false, readOnlyRootFilesystem=true,
            # capabilities.drop includes "ALL", seccompProfile.type ∈
            # {RuntimeDefault, Localhost}. The check inspects the
            # CronJob's jobTemplate.spec.template.spec — agent-set
            # values, post-patch — and fails Component A if any
            # required item is missing.
            cj = kget_json("cronjob", ns=NAMESPACE, name=BACKUP_CRONJOB) or {}
            pod_spec = (
                cj.get("spec", {})
                  .get("jobTemplate", {})
                  .get("spec", {})
                  .get("template", {})
                  .get("spec", {})
            )
            pod_sc = pod_spec.get("securityContext") or {}
            backup_container = next(
                (c for c in (pod_spec.get("containers") or [])
                 if c.get("name") == "backup"),
                {},
            )
            ctr_sc = backup_container.get("securityContext") or {}
            def _eff(k):
                v = ctr_sc.get(k)
                return v if v is not None else pod_sc.get(k)
            pss_violations = []
            if _eff("runAsNonRoot") is not True:
                pss_violations.append("runAsNonRoot!=true")
            if _eff("allowPrivilegeEscalation") is not False:
                pss_violations.append("allowPrivilegeEscalation!=false")
            if _eff("readOnlyRootFilesystem") is not True:
                pss_violations.append("readOnlyRootFilesystem!=true")
            caps = (ctr_sc.get("capabilities") or {})
            drop = caps.get("drop") or []
            if "ALL" not in drop and "all" not in drop:
                pss_violations.append("capabilities.drop missing ALL")
            sp = (
                _eff("seccompProfile")
                if isinstance(_eff("seccompProfile"), dict)
                else {}
            )
            if sp.get("type") not in ("RuntimeDefault", "Localhost"):
                pss_violations.append(
                    f"seccompProfile.type={sp.get('type')!r} (need RuntimeDefault/Localhost)"
                )
            if pss_violations:
                component_a_reason = (
                    f"dedicated SA '{sa}' OK but backup CronJob pod spec "
                    f"violates PSS Restricted: {pss_violations}"
                )
            else:
                component_a_ok = True
                component_a_reason = (
                    f"dedicated SA '{sa}' + PSS Restricted compliant"
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

    # ── Component D: trust-anchor history attestation ──
    # Replaces the previous "default-SA RoleBinding removal" check
    # (zero variance — every agent that gets dedicated SA in 4-A also
    # passes 4-D trivially). The new check inspects the trust anchor
    # ConfigMap's `public_key_fingerprints` data field at the END of
    # the grader run. By this point S1-B has run two consecutive
    # backups, S2/S3 have triggered additional backups, and S3-B has
    # explicitly required key rotation. So the trust anchor should
    # have at least N >= 3 distinct 64-hex fingerprints, all unique
    # (no key reused). Agents that hard-code a static signing key,
    # never register the fingerprint, or only rotate once will fail
    # this attestation.
    fps_data = ""
    rc, fps_data, _ = run_cmd(
        f"kubectl get configmap {TRUST_ANCHOR_CM} -n {NAMESPACE} "
        f"-o jsonpath='{{.data.public_key_fingerprints}}' 2>/dev/null",
        timeout=10,
    )
    fps = [
        ln.strip().lower()
        for ln in (fps_data or "").splitlines()
        if ln.strip() and re.fullmatch(r"[0-9a-f]{64}", ln.strip().lower())
    ]
    distinct = sorted(set(fps))
    component_d_ok = False
    if not fps:
        component_d_reason = (
            f"trust anchor {TRUST_ANCHOR_CM}.public_key_fingerprints is "
            "empty — no signing keys ever registered across grader runs"
        )
    elif len(distinct) < 3:
        component_d_reason = (
            f"trust anchor has only {len(distinct)} distinct fingerprints "
            f"after multiple grader-triggered backups (need >=3 — "
            "signing keys must rotate per run)"
        )
    elif len(distinct) != len(fps):
        component_d_reason = (
            f"trust anchor has {len(fps)} entries but only "
            f"{len(distinct)} distinct — signing key REUSED across runs "
            "(rotation requirement violated)"
        )
    else:
        component_d_ok = True
        component_d_reason = (
            f"trust anchor history ok: {len(distinct)} distinct rotated "
            "fingerprints, no reuse"
        )
    if component_d_ok:
        score += 0.25
    notes.append(f"D:{component_d_reason}")

    return (1.0 if score >= 0.999 else 0.0), "; ".join(notes)


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
