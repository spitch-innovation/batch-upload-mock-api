#!/usr/bin/env python3
"""
Flag-only client for the Recordings Ingest Mock API.

Operations:
  --operation presign  # presign only (prints JSON; optionally --out presign.json)
  --operation upload   # upload using a presign JSON ( --presign-json )
  --operation create   # create /recordings from a presign JSON + --meta files
  --operation poll     # poll a batch
  --operation demo     # presign -> upload -> create -> poll

Notes:
- For presign & demo: --files must line up with --meta (same count/order) for create.
- For upload/create: provide --presign-json (produced by presign or demo step).
- For append: pass --batch-id to presign/demo; server returns the effective batch_id.
"""

import argparse
import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


def pretty(title: str, obj: Any):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, indent=2, sort_keys=True))


def guess_content_type(path: Path) -> str:
    n = path.name.lower()
    if n.endswith(".wav"): return "audio/wav"
    if n.endswith(".mp3"): return "audio/mpeg"
    if n.endswith(".m4a"): return "audio/mp4"
    if n.endswith(".flac"): return "audio/flac"
    return "application/octet-stream"


def load_json_file(path: Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


def parse_paths(values: List[str]) -> List[Path]:
    out = [Path(v).expanduser() for v in values]
    for p in out:
        if not p.exists():
            raise SystemExit(f"File not found: {p}")
    return out


# ---------------- API calls ----------------

def api_presign(base_url: str, api_key: str, files: List[Path], batch_id: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"items": [{"filename": p.name, "contentType": guess_content_type(p)} for p in files]}
    if batch_id:
        payload["batch_id"] = batch_id
    r = requests.post(
        f"{base_url.rstrip('/')}/uploads/presign",
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json=payload, timeout=30,
    )
    if not r.ok:
        raise SystemExit(f"[presign] HTTP {r.status_code}: {r.text}")
    data = r.json()
    pretty("presign response", data)
    return data


def do_uploads_from_presign(presign: Dict[str, Any], files: List[Path]):
    items = presign["items"]
    if len(items) != len(files):
        raise SystemExit("Presign items count does not match provided --files.")
    for i, file_path in enumerate(files):
        slot = items[i]
        ct = slot["required_headers"].get("Content-Type", guess_content_type(file_path))
        with open(file_path, "rb") as f:
            resp = requests.put(slot["upload_url"], data=f, headers={"Content-Type": ct}, timeout=120)
        if resp.status_code not in (200, 201, 204):
            raise SystemExit(f"[upload] {file_path.name}: HTTP {resp.status_code}: {resp.text}")
        print(f"[upload] OK -> {file_path.name} ({file_path.stat().st_size} bytes)")


def api_create_recordings(base_url: str, api_key: str, batch_id: str, presign: Dict[str, Any],
                          metas: List[Path], idempotency_key: Optional[str]) -> Dict[str, Any]:
    if len(presign["items"]) != len(metas):
        raise SystemExit("Presign items count does not match provided --meta count.")

    manifest_items: List[Dict[str, Any]] = []
    for i, meta_path in enumerate(metas):
        data = load_json_file(meta_path)
        manifest_items.append({
            "client_item_id": f"c{i+1}",
            "blob_ref": presign["items"][i]["blob_ref"],
            "data": data
        })

    payload = {
        "batch_id": batch_id,
        "idempotency_key": idempotency_key or str(uuid.uuid4()),
        "items": manifest_items
    }
    r = requests.post(
        f"{base_url.rstrip('/')}/recordings",
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json=payload, timeout=30,
    )
    if r.status_code != 202:
        raise SystemExit(f"[recordings] HTTP {r.status_code}: {r.text}")
    data = r.json()
    pretty("recordings response (202)", data)
    return data


def api_poll_batch(base_url: str, api_key: str, batch_id: str) -> Dict[str, Any]:
    r = requests.get(f"{base_url.rstrip('/')}/batches/{batch_id}", headers={"X-API-Key": api_key}, timeout=15)
    if not r.ok:
        raise SystemExit(f"[poll] HTTP {r.status_code}: {r.text}")
    data = r.json()
    pretty("batch status", data)
    return data


# ---------------- Operations ----------------

def op_presign(base_url: str, api_key: str, files: List[Path], batch_id: Optional[str], out_path: Optional[Path]):
    res = api_presign(base_url, api_key, files, batch_id)
    if out_path:
        out_path.write_text(json.dumps(res, indent=2))
        print(f"[presign] wrote {out_path}")
    else:
        # raw JSON line for easy capture
        print("\n[PRESIGN_JSON]")
        print(json.dumps(res))


def op_upload(presign_json: Path, files: List[Path]):
    presign = load_json_file(presign_json)
    do_uploads_from_presign(presign, files)


def op_create(base_url: str, api_key: str, presign_json: Path, metas: List[Path],
              batch_id_override: Optional[str], idempotency_key: Optional[str]):
    presign = load_json_file(presign_json)
    batch_id = batch_id_override or presign.get("batch_id")
    if not batch_id:
        raise SystemExit("No batch_id available. Provide --batch-id or presign JSON with batch_id.")
    api_create_recordings(base_url, api_key, batch_id, presign, metas, idempotency_key)


def op_poll(base_url: str, api_key: str, batch_id: str):
    api_poll_batch(base_url, api_key, batch_id)
    print(f"\nOpen UI: {base_url.rstrip('/')}/ui?key={api_key}")


def op_demo(base_url: str, api_key: str, files: List[Path], metas: List[Path],
            batch_id_opt: Optional[str], out_presign: Optional[Path], idempotency_key: Optional[str]):
    # presign
    pres = api_presign(base_url, api_key, files, batch_id_opt)
    if out_presign:
        out_presign.write_text(json.dumps(pres, indent=2))
        print(f"[demo] wrote presign to {out_presign}")
    batch_id = pres["batch_id"]

    # upload
    do_uploads_from_presign(pres, files)

    # create
    res = api_create_recordings(base_url, api_key, batch_id, pres, metas, idempotency_key)

    # poll
    api_poll_batch(base_url, api_key, batch_id=batch_id)
    print(f"\nOpen UI: {base_url.rstrip('/')}/ui?key={api_key}")


def main():
    ap = argparse.ArgumentParser(description="Flag-only client for the mock upload API.")
    ap.add_argument("--operation", choices=["presign", "upload", "create", "poll", "demo"], default="demo",
                    help="Which operation to run (default: demo)")

    # Common/global
    ap.add_argument("--base-url", default="http://localhost:5050", help="API base URL")
    ap.add_argument("--api-key", default="test_12345", help="X-API-Key")
    ap.add_argument("--batch-id", default=None, help="Existing batch to append to (optional)")
    ap.add_argument("--idempotency-key", default=None, help="Override idempotency key")

    # Files & metadata
    ap.add_argument("--files", nargs="*", help="Audio file paths (order matters)")
    ap.add_argument("--meta",  nargs="*", help="Metadata JSON paths, same count/order as --files")

    # Cross-step artifacts
    ap.add_argument("--presign-json", help="Path to presign JSON produced by presign or demo")
    ap.add_argument("--out", help="Where to write presign JSON (for presign/demo)")

    args = ap.parse_args()

    # Convert paths
    files = parse_paths(args.files) if args.files else []
    metas = parse_paths(args.meta) if args.meta else []
    presign_json = Path(args.presign_json).expanduser() if args.presign_json else None
    out_presign = Path(args.out).expanduser() if args.out else None

    op = args.operation
    if op == "presign":
        if not files:
            raise SystemExit("--files required for presign")
        op_presign(args.base_url, args.api_key, files, args.batch_id, out_presign)
    elif op == "upload":
        if not presign_json or not files:
            raise SystemExit("--presign-json and --files required for upload")
        op_upload(presign_json, files)
    elif op == "create":
        if not presign_json or not metas:
            raise SystemExit("--presign-json and --meta required for create")
        op_create(args.base_url, args.api_key, presign_json, metas, args.batch_id, args.idempotency_key)
    elif op == "poll":
        if not args.batch_id:
            raise SystemExit("--batch-id required for poll")
        op_poll(args.base_url, args.api_key, args.batch_id)
    elif op == "demo":
        if not files or not metas:
            raise SystemExit("--files and --meta required for demo")
        op_demo(args.base_url, args.api_key, files, metas, args.batch_id, out_presign, args.idempotency_key)
    else:
        raise SystemExit("Unsupported operation")


if __name__ == "__main__":
    main()
