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
    run_cmd(
        f"kubectl scale statefulset/{VALKEY_STATEFULSET} -n {NAMESPACE} "
        f"--replicas={replicas}",
        timeout=30,
    )
    deadline = time.time() + 120
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
    Also accepts the legacy/aliased shape `name_or_claim`/`key_or_path`.
    Returns (bytes, error_message).
    """
    if not isinstance(location, dict):
        return b"", "artifact_location is not a dict"
    kind = (location.get("type") or "").lower()
    ns = location.get("namespace") or NAMESPACE

    if kind in ("configmap", "cm"):
        name = location.get("name") or location.get("name_or_claim") or ""
        key = location.get("key") or location.get("key_or_path") or "dump.rdb"
        if not name:
            return b"", "configmap artifact_location missing name"
        rc, out, _ = run_cmd(
            f"kubectl get configmap {name} -n {ns} "
            f"-o jsonpath='{{.binaryData.{key.replace('.', chr(92)+chr(46))}}}' 2>/dev/null"
        )
        b64 = out.strip("'")
        if not b64:
            rc, out, _ = run_cmd(
                f"kubectl get configmap {name} -n {ns} "
                f"-o jsonpath='{{.data.{key.replace('.', chr(92)+chr(46))}}}' 2>/dev/null"
            )
            raw = out.strip("'")
            if raw:
                return raw.encode("utf-8", errors="replace"), ""
            return b"", f"configmap {name} has no {key}"
        try:
            import base64 as _b64
            return _b64.b64decode(b64), ""
        except Exception as e:
            return b"", f"failed to decode binaryData: {e}"

    if kind in ("secret",):
        name = location.get("name") or location.get("name_or_claim") or ""
        key = location.get("key") or location.get("key_or_path") or "dump.rdb"
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
        claim = location.get("claim") or location.get("name_or_claim") or ""
        path = location.get("path") or location.get("key_or_path") or ""
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
    if not scale_valkey(1):
        return 0.0, "could not ensure Valkey is scaled to 1 replica"
    # Wait for valkey to be exec-ready
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)

    # Snapshot the previously-published snapshot_epoch so we can require
    # strict monotonic advance after the clean run. Catches agents who
    # publish a copy of the seeded baseline epoch without producing a
    # genuinely fresh snapshot.
    pre_handoff = parse_handoff(get_handoff_doc()) or {}
    prior_snapshot_epoch = pre_handoff.get("snapshot_epoch")
    if not isinstance(prior_snapshot_epoch, int):
        prior_snapshot_epoch = 0

    lastsave_before = get_lastsave()
    run_start = int(time.time())
    ts = int(time.time())
    marker = f"GRADER-FRESH-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)

    job = f"grader-fresh-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)
    if err:
        return 0.0, err
    if not completed:
        return 0.0, f"clean freshness run did not succeed. logs tail:\n{logs[-400:]}"
    parsed = parse_handoff(handoff)
    if parsed is None:
        return 0.0, f"handoff is not valid JSON after clean run: {handoff[:300]}"
    if parsed.get("safe_for_restore") is not True:
        return 0.0, f"handoff safe_for_restore != true on clean run: {str(parsed)[:300]}"
    snapshot_epoch = parsed.get("snapshot_epoch")
    if not isinstance(snapshot_epoch, int) or snapshot_epoch < run_start - 5:
        return 0.0, (
            f"handoff.snapshot_epoch={snapshot_epoch} not >= run_start={run_start} "
            "(agent didn't force a fresh snapshot)"
        )
    if snapshot_epoch <= prior_snapshot_epoch:
        return 0.0, (
            f"handoff.snapshot_epoch={snapshot_epoch} did not strictly advance "
            f"past prior published epoch={prior_snapshot_epoch} — backup is "
            "republishing a stale value rather than capturing a fresh snapshot"
        )
    lastsave_after = get_lastsave()
    if lastsave_after <= lastsave_before:
        return 0.0, (
            f"Valkey LASTSAVE did not advance "
            f"(before={lastsave_before}, after={lastsave_after}) "
            "— BGSAVE/SYNC never fired"
        )

    # Forced-failure scenario: scale Valkey to 0 and run backup. Must fail closed.
    ts2 = int(time.time())
    fail_marker = f"GRADER-FRESH-FAIL-{ts2}"
    set_status_marker(fail_marker)
    set_handoff_marker(fail_marker)
    scale_valkey(0)
    try:
        fail_job = f"grader-fresh-fail-{ts2}"
        completed2, logs2, err2 = trigger_backup(fail_job, wait_seconds=180)
        fail_handoff = wait_for_doc_change(get_handoff_doc, fail_marker)
        cleanup_job(fail_job)
        if completed2:
            return 0.0, (
                "forced-failure run succeeded even though Valkey was scaled to 0 — "
                "backup pipeline is not validating Valkey reachability"
            )
        parsed_fail = parse_handoff(fail_handoff)
        if parsed_fail is None:
            return 0.0, (
                "handoff not refreshed / not JSON after forced-failure run; "
                f"content: {fail_handoff[:300]}"
            )
        if parsed_fail.get("safe_for_restore") is not False:
            return 0.0, (
                "handoff safe_for_restore is not false after forced-failure run — "
                "pipeline does not fail closed"
            )
        reason = parsed_fail.get("reason") or ""
        if not isinstance(reason, str) or not reason.strip():
            return 0.0, "handoff has no reason on forced-failure run"
    finally:
        scale_valkey(1)

    return 1.0, (
        f"freshness enforced: LASTSAVE advanced {lastsave_before}->{lastsave_after}, "
        f"snapshot_epoch={snapshot_epoch}, forced-failure correctly marked unsafe"
    )


# ─────────────────────────────────────────────
# CHECK 2: backup_artifact_durable_and_reusable
# ─────────────────────────────────────────────
def check_backup_artifact_durable_and_reusable(setup_info):
    if not scale_valkey(1):
        return 0.0, "could not ensure Valkey is scaled to 1 replica"
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
    if err:
        return 0.0, err
    if not completed:
        return 0.0, f"artifact run did not succeed. logs tail:\n{logs[-400:]}"
    parsed = parse_handoff(handoff)
    if parsed is None:
        return 0.0, f"handoff is not JSON after clean run: {handoff[:300]}"
    if parsed.get("safe_for_restore") is not True:
        return 0.0, "handoff says unsafe on a clean run"

    location = parsed.get("artifact_location")
    if not isinstance(location, dict):
        return 0.0, (
            "handoff.artifact_location is missing — durable storage of the "
            "backup artifact is not declared"
        )
    loc_type = (location.get("type") or "").lower()
    if loc_type not in ("configmap", "cm", "secret", "pvc"):
        return 0.0, (
            f"handoff.artifact_location.type='{loc_type}' — must be "
            "configmap / secret / pvc (a durable in-cluster resource)"
        )

    artifact, err = read_artifact_bytes(location)
    if err:
        return 0.0, f"could not read backup artifact from declared location: {err}"
    if len(artifact) < 200:
        return 0.0, (
            f"backup artifact is only {len(artifact)} bytes — too small to be a "
            "real Valkey RDB snapshot"
        )
    if not artifact.startswith(b"REDIS"):
        return 0.0, (
            f"backup artifact does not start with the REDIS RDB magic "
            f"(got {artifact[:8]!r}) — the saved file is not a usable snapshot"
        )

    restore_proof = parsed.get("restore_proof")
    if not isinstance(restore_proof, dict):
        return 0.0, "handoff.restore_proof is missing — no evidence the backup is reusable"
    key_count = restore_proof.get("key_count_at_snapshot")
    if not isinstance(key_count, int) or key_count <= 0:
        return 0.0, (
            "handoff.restore_proof.key_count_at_snapshot <= 0 — "
            "backup did not capture any live state"
        )
    if live_dbsize > 0 and key_count < max(1, live_dbsize // 3):
        return 0.0, (
            f"restore_proof.key_count_at_snapshot={key_count} is far below live "
            f"DBSIZE={live_dbsize} — backup is not capturing the real dataset"
        )
    artifact_bytes_reported = parsed.get("artifact_bytes")
    if not isinstance(artifact_bytes_reported, int) or artifact_bytes_reported <= 0:
        return 0.0, "handoff.artifact_bytes is missing or <= 0"
    if abs(artifact_bytes_reported - len(artifact)) > max(32, int(len(artifact) * 0.1)):
        return 0.0, (
            f"handoff.artifact_bytes={artifact_bytes_reported} does not match the "
            f"retrieved artifact size ({len(artifact)} bytes)"
        )

    return 1.0, (
        f"artifact durable and reusable: {loc_type}, {len(artifact)} bytes, "
        f"restore_proof key_count={key_count} / live DBSIZE={live_dbsize}"
    )


# ─────────────────────────────────────────────
# CHECK 3: status_and_handoff_surface_truth
# ─────────────────────────────────────────────
def check_status_and_handoff_surface_truth(setup_info):
    if not scale_valkey(1):
        return 0.0, "could not ensure Valkey is scaled to 1 replica"
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

    if err or not completed:
        return 0.0, f"surface run did not succeed. logs tail:\n{logs[-400:]}"

    if not status_doc or marker in status_doc:
        return 0.0, "status ConfigMap was not refreshed by the backup run"
    status_lower = status_doc.lower()
    required_in_status = [
        r"backup[_ -]?id",
        r"(snapshot|captured).{0,40}(timestamp|epoch|at)",
        r"(artifact|rdb)[_ -]?(bytes|size)",
        r"restore[_ -]?proof",
    ]
    for pat in required_in_status:
        if not re.search(pat, status_lower):
            return 0.0, (
                f"status.md is missing required run detail matching /{pat}/. "
                f"excerpt: {status_doc[:300]}"
            )

    parsed = parse_handoff(handoff_doc)
    if parsed is None:
        return 0.0, f"handoff.json is not machine-readable: {handoff_doc[:300]}"
    for key in ("backup_id", "snapshot_epoch", "artifact_bytes",
                "safe_for_restore", "restore_proof", "artifact_location"):
        if key not in parsed:
            return 0.0, f"handoff missing field '{key}' on success"
    if parsed.get("safe_for_restore") is not True:
        return 0.0, "handoff.safe_for_restore != true on clean run"

    # Cross-doc consistency: status.md MUST report the same numeric values
    # as handoff.json. Fill-in-the-blanks templates pass the regex above
    # but lie about the numbers; require artifact_bytes (and snapshot
    # epoch when status mentions one) to match handoff to within zero.
    h_bytes = parsed.get("artifact_bytes")
    if isinstance(h_bytes, int):
        bytes_in_status = re.findall(
            r"(?:artifact|rdb)[_ \-]?(?:bytes|size)\s*[:=]?\s*(\d+)",
            status_lower,
        )
        if bytes_in_status:
            int_vals = {int(v) for v in bytes_in_status}
            if h_bytes not in int_vals:
                return 0.0, (
                    f"status.md reports artifact bytes {sorted(int_vals)} but "
                    f"handoff.json reports {h_bytes} — the two surfaces do "
                    "not agree, restore tooling cannot trust either"
                )
    h_epoch = parsed.get("snapshot_epoch")
    if isinstance(h_epoch, int):
        epoch_in_status = re.findall(r"epoch\s*[:=]\s*(\d+)", status_lower)
        if epoch_in_status:
            int_vals = {int(v) for v in epoch_in_status}
            if h_epoch not in int_vals:
                return 0.0, (
                    f"status.md reports snapshot epoch {sorted(int_vals)} but "
                    f"handoff.json reports {h_epoch} — surfaces disagree"
                )

    # Forced-failure: scale Valkey to 0, run again, expect failure surface
    ts2 = int(time.time())
    fail_marker = f"GRADER-SURF-FAIL-{ts2}"
    set_status_marker(fail_marker)
    set_handoff_marker(fail_marker)
    scale_valkey(0)
    try:
        fail_job = f"grader-surf-fail-{ts2}"
        completed2, logs2, _ = trigger_backup(fail_job, wait_seconds=180)
        fail_status = wait_for_doc_change(get_status_doc, fail_marker)
        fail_handoff = wait_for_doc_change(get_handoff_doc, fail_marker)
        cleanup_job(fail_job)
        if completed2:
            return 0.0, "forced-failure run succeeded instead of failing closed"
        if not fail_status or fail_marker in fail_status:
            return 0.0, "status ConfigMap not refreshed on forced-failure run"
        if not re.search(r"\b(failed|failure|blocked|unsafe|error)\b",
                          fail_status.lower()):
            return 0.0, (
                "status.md does not indicate failure after forced-failure run. "
                f"excerpt: {fail_status[:300]}"
            )
        fail_parsed = parse_handoff(fail_handoff)
        if fail_parsed is None:
            return 0.0, f"handoff not JSON on forced-failure: {fail_handoff[:300]}"
        if fail_parsed.get("safe_for_restore") is not False:
            return 0.0, "handoff.safe_for_restore != false on forced-failure run"
        reason = fail_parsed.get("reason") or ""
        if not isinstance(reason, str) or not reason.strip():
            return 0.0, "handoff.reason missing/empty on forced-failure run"
    finally:
        scale_valkey(1)

    return 1.0, (
        "status.md and handoff.json reflect each run truthfully and fail closed "
        "with a reason when the snapshot cannot be captured"
    )


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


def sa_rbac_is_scoped(sa_name):
    if not sa_name or sa_name == "default":
        return False, "backup CronJob still uses the default ServiceAccount"
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
            and subj.get("name") == sa_name
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

    def rule_allows(rule, api_group, resource, verbs, resource_name=None):
        ag = rule.get("apiGroups") or [""]
        rs = rule.get("resources") or []
        rv = rule.get("verbs") or []
        if api_group not in ag and "*" not in ag:
            return False
        if resource not in rs and "*" not in rs:
            return False
        if not any(v in rv or "*" in rv for v in verbs):
            return False
        names = rule.get("resourceNames")
        if resource_name is None:
            return True
        if names is None or "*" in names:
            return True
        return resource_name in names

    can_patch_status = any(
        rule_allows(r, "", "configmaps", ["patch", "update"], STATUS_CM)
        for r in bound
    )
    can_patch_handoff = any(
        rule_allows(r, "", "configmaps", ["patch", "update"], HANDOFF_CM)
        for r in bound
    )
    if not can_patch_status or not can_patch_handoff:
        return False, (
            f"backup SA '{sa_name}' can't patch the status or handoff ConfigMap"
        )

    def is_broad(rule):
        if not rule_allows(rule, "", "configmaps", ["patch", "update"]):
            return False
        names = rule.get("resourceNames")
        if names is None or "*" in names:
            return True
        return not set(names).issubset(RUNTIME_CM_TARGETS)

    if any(is_broad(r) for r in bound):
        return False, (
            f"backup SA '{sa_name}' has broad ConfigMap write access — "
            "it must only patch the status and restore-handoff ConfigMaps"
        )

    def can_patch_cronjob(rule):
        return rule_allows(rule, "batch", "cronjobs", ["patch", "update"],
                            BACKUP_CRONJOB)

    if any(can_patch_cronjob(r) for r in bound):
        return False, (
            f"backup SA '{sa_name}' can patch its own CronJob — this would let "
            "the job rewrite its own template and defeat drift control"
        )

    def can_touch_webhooks(rule):
        return rule_allows(
            rule, "admissionregistration.k8s.io",
            "mutatingwebhookconfigurations", ["patch", "update", "delete"],
        )

    if any(can_touch_webhooks(r) for r in bound):
        return False, (
            f"backup SA '{sa_name}' can modify MutatingWebhookConfigurations "
            "— too broad for a backup runtime identity"
        )

    return True, f"backup SA '{sa_name}' is scoped to status + handoff writes"


def check_repair_survives_reconciliation_and_scoped_rbac(setup_info):
    if not scale_valkey(1):
        return 0.0, "could not ensure Valkey is scaled to 1 replica"
    for _ in range(30):
        if valkey_cli("PING") == "PONG":
            break
        time.sleep(2)

    # Let the in-cluster drift reconciler fire at least once.
    print("[grader] Waiting 90s for the drift reconciler cron to fire...")
    time.sleep(90)

    sa = get_backup_sa()
    ok, reason = sa_rbac_is_scoped(sa)
    if not ok:
        return 0.0, reason

    ts = int(time.time())
    marker = f"GRADER-SURVIVE-{ts}"
    set_status_marker(marker)
    set_handoff_marker(marker)
    job = f"grader-survive-{ts}"
    completed, logs, err = trigger_backup(job, wait_seconds=240)
    handoff = wait_for_doc_change(get_handoff_doc, marker)
    cleanup_job(job)

    if err or not completed:
        return 0.0, (
            "backup run after reconciler tick did not succeed — the fix "
            f"didn't survive the drift cycle. logs tail:\n{logs[-400:]}"
        )
    parsed = parse_handoff(handoff)
    if parsed is None or parsed.get("safe_for_restore") is not True:
        return 0.0, (
            "backup after reconciler tick didn't produce a safe handoff — "
            "drift control reverted the fix"
        )

    return 1.0, (
        f"repair survives drift reconciliation; backup SA '{sa}' is narrow "
        "(status + handoff writes only)"
    )


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
