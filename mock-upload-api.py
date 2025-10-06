#!/usr/bin/env python3
from fastapi import FastAPI, Header, HTTPException, Depends, Path, Request, status, Query
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse, HTMLResponse, Response
from pydantic import BaseModel, Field, AnyUrl, constr
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import hashlib, os, uuid, json, sqlite3
from pathlib import Path as FSPath

app = FastAPI(
    title="Recordings Ingest Mock API",
    version="1.2.0",
    description="""
A **mock** implementation of the two-endpoint flow for uploading audio + sending metadata.

**Flow**

1. POST **/uploads/presign** — returns 1–10 *presigned* PUT URLs (they actually point to this app: `/mock-upload/{upload_id}?token=...`).
   - NEW: Pass optional `batch_id` in the body to add uploads to an **existing** batch; otherwise a **new** batch is created and returned.
2. PUT to each `upload_url` — the server saves bytes under `./storage/` and records the blob.
3. POST **/recordings** — send metadata that references the `blob_ref` values; creates or appends to a **batch** (supply `batch_id` to append).
   - Server verifies blobs exist and belong to the batch.
4. GET **/batches/{batch_id}** — poll status.

**Auth:** `X-API-Key: test_12345`

**Extras**
- Uses **SQLite** (`./mock.db`) to persist batches, recordings, and blobs.
- Adds **/ui** to view batches + recordings with an HTML page (enter `key` in the page to view).
- **/media/{recording_id}** streams the saved audio file.
""",
)

# ------------------------------------------------------------------------------
# "Configuration"
TEST_API_KEY = "test_12345"
PRESIGN_TTL_SECONDS = 600
STORAGE_DIR = FSPath("./storage")
DB_PATH = FSPath("./mock.db")

# ------------------------------------------------------------------------------
# In-memory tokens (short-lived, for presigned mock)
presigned_tokens: Dict[str, dict] = {}  # upload_id -> { token, expires_at, blob_ref, content_type, tenant_id, batch_id }

# ------------------------------------------------------------------------------
# Auth dependency

def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if x_api_key != TEST_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid X-API-Key")
    return {"tenant_id": "tn_demo"}

# ------------------------------------------------------------------------------
# SQLite helpers

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS blobs (
            blob_ref TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            size_bytes INTEGER,
            content_type TEXT,
            uploaded_at TEXT
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id TEXT PRIMARY KEY,
            status TEXT,          -- open, pending, processing, completed, failed
            idem_key TEXT,
            created_at TEXT
        )""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS recordings (
            id TEXT PRIMARY KEY,
            batch_id TEXT,
            tenant_id TEXT,
            client_item_id TEXT,
            blob_ref TEXT,
            status TEXT,
            data_json TEXT,
            created_at TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        )""")
        # NEW: link presigned/uploads to a batch even before /recordings
        conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_uploads (
            blob_ref TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL,
            created_at TEXT
        )""")
        # simple indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_recordings_batch ON recordings(batch_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_uploads_batch ON batch_uploads(batch_id)")
        conn.commit()

@app.on_event("startup")
def on_startup():
    init_db()

# ------------------------------------------------------------------------------
# Schemas

def example_blob_ref():
    return "blob://s3/rec-bucket/tenants/tn_demo/2025/08/11/rec_abc123/call1.wav"

class PresignItemIn(BaseModel):
    filename: constr(strip_whitespace=True, min_length=1) = Field(..., example="call1.wav")
    contentType: constr(strip_whitespace=True, min_length=1) = Field(..., example="audio/wav")

class PresignRequest(BaseModel):
    batch_id: Optional[str] = Field(None, example="rb_01J8X8RJ6H8J9Z", description="Existing batch to add uploads to")
    items: List[PresignItemIn] = Field(..., min_items=1, max_items=10, example={
        "items": [
            {"filename": "call1.wav", "contentType": "audio/wav"},
            {"filename": "call2.mp3", "contentType": "audio/mpeg"},
        ]
    })

class PresignedItemOut(BaseModel):
    temp_id: str = Field(..., example="tmp_b3c2f4a1e2")
    method: str = Field("PUT", example="PUT")
    upload_url: AnyUrl = Field(..., example="http://localhost:8000/mock-upload/upl_abc123?token=eyJ...")
    required_headers: Dict[str, str] = Field(default_factory=dict, example={"Content-Type": "audio/wav"})
    blob_ref: str = Field(..., example=example_blob_ref())

class PresignResponse(BaseModel):
    batch_id: str = Field(..., example="rb_01J8X8RJ6H8J9Z")
    expires_in_seconds: int = Field(PRESIGN_TTL_SECONDS, example=600)
    items: List[PresignedItemOut]

class RecordingItemIn(BaseModel):
    client_item_id: constr(strip_whitespace=True, min_length=1) = Field(..., example="c1")
    blob_ref: constr(strip_whitespace=True, min_length=1) = Field(..., example=example_blob_ref())
    data: Dict[str, Any] = Field(..., example={"agentId": "007", "locale": "en-US"})

class RecordingsRequest(BaseModel):
    batch_id: Optional[str] = Field(None, example="rb_01J8X8RJ6H8J9Z", description="Append to an existing batch")
    idempotency_key: constr(strip_whitespace=True, min_length=8) = Field(..., example="7c0f6f62-2e9e-482f-9b1e-6a6f1b3a2c3b")
    items: List[RecordingItemIn] = Field(..., min_items=1, max_items=10, example={
        "idempotency_key": "7c0f6f62-2e9e-482f-9b1e-6a6f1b3a2c3b",
        "items": [
            { "client_item_id": "c1", "blob_ref": example_blob_ref(), "data": {"agentId": "007", "locale": "en-US"} }
        ]
    })

class RecordingItemOut(BaseModel):
    client_item_id: str = Field(..., example="c1")
    recording_id: str = Field(..., example="rec_01H8X8R6ZK3YQ9A2ZB7J")
    status: str = Field(..., example="queued")

class RecordingsResponse(BaseModel):
    batch_id: str = Field(..., example="rb_01J8X8RJ6H8J9Z")
    status: str = Field(..., example="pending")
    items: List[RecordingItemOut]
    poll: Dict[str, str] = Field(..., example={"href": "/batches/rb_01J8X8RJ6H8J9Z"})

class BatchStatusResponse(BaseModel):
    batch_id: str = Field(..., example="rb_01J8X8RJ6H8J9Z")
    status: str = Field(..., example="pending")
    items: List[RecordingItemOut]

# ------------------------------------------------------------------------------
# Helpers

def safe_filename(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)

def new_id(prefix: str) -> str:
    return f"{prefix}_" + uuid.uuid4().hex

def now_utc():
    return datetime.now(timezone.utc)

def build_blob_ref(tenant_id: str, recording_id: str, filename: str) -> str:
    dt = now_utc()
    y, m, d = dt.year, f"{dt.month:02d}", f"{dt.day:02d}"
    key = f"tenants/{tenant_id}/recordings/{y}/{m}/{d}/{recording_id}/{safe_filename(filename)}"
    return f"blob://s3/rec-bucket/{key}"

def storage_path_for(blob_ref: str) -> FSPath:
    h = hashlib.sha256(blob_ref.encode()).hexdigest()[:32]
    return STORAGE_DIR / f"{h}.bin"

def ensure_open_batch(conn: sqlite3.Connection, batch_id: Optional[str]) -> str:
    """Return a valid batch_id. If given, must exist; else create a new 'open' batch."""
    if batch_id:
        row = conn.execute("SELECT id, status FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="batch_id not found")
        # allow adding while 'open' or 'pending'
        if row["status"] not in ("open", "pending"):
            raise HTTPException(status_code=409, detail=f"batch {batch_id} is not open for new uploads")
        return row["id"]
    # create new
    new_bid = new_id("rb")
    conn.execute("INSERT INTO batches(id, status, idem_key, created_at) VALUES(?,?,?,?)",
                 (new_bid, "open", None, now_utc().isoformat()))
    return new_bid

# ------------------------------------------------------------------------------
# Endpoint: POST /uploads/presign  (batch-aware)

@app.post(
    "/uploads/presign",
    response_model=PresignResponse,
    summary="Issue presigned PUT URLs for audio uploads (optionally link to a batch)",
    tags=["Uploads"],
)
def presign_uploads(req: PresignRequest, request: Request, auth=Depends(require_api_key)):
    tenant_id = auth["tenant_id"]
    base = str(request.base_url).rstrip("/")

    with db() as conn:
        batch_id = ensure_open_batch(conn, req.batch_id)
        items_out: List[PresignedItemOut] = []

        for it in req.items:
            recording_id = new_id("rec")
            blob_ref = build_blob_ref(tenant_id, recording_id, it.filename)
            upload_id = new_id("upl")
            token = new_id("tok")
            expires_at = now_utc() + timedelta(seconds=PRESIGN_TTL_SECONDS)

            # stage link between blob and batch BEFORE upload completes
            conn.execute(
                "INSERT OR REPLACE INTO batch_uploads(blob_ref, batch_id, created_at) VALUES(?,?,?)",
                (blob_ref, batch_id, now_utc().isoformat())
            )
            conn.commit()

            presigned_tokens[upload_id] = {
                "token": token,
                "expires_at": expires_at,
                "blob_ref": blob_ref,
                "content_type": it.contentType,
                "tenant_id": tenant_id,
                "batch_id": batch_id,
            }

            upload_url = f"{base}/mock/mock-upload/{upload_id}?token={token}"

            items_out.append(PresignedItemOut(
                temp_id=new_id("tmp")[:12],
                method="PUT",
                upload_url=upload_url,
                required_headers={"Content-Type": it.contentType},
                blob_ref=blob_ref,
            ))

    return PresignResponse(batch_id=batch_id, expires_in_seconds=PRESIGN_TTL_SECONDS, items=items_out)

# ------------------------------------------------------------------------------
# Endpoint: PUT /mock-upload/{upload_id}  (saves bytes; records blob)

@app.put(
    "/mock-upload/{upload_id}",
    summary="Mock upload sink (accepts bytes; saved to ./storage)",
    tags=["Uploads (Mock)"],
    responses={201: {"description": "Created (blob stored)"}, 400: {"description": "Invalid token or expired"}},
)
async def mock_upload(
    request: Request,
    upload_id: str = Path(..., description="Upload session ID from presign response"),
    token: Optional[str] = None,
):
    entry = presigned_tokens.get(upload_id)
    if not entry or token != entry["token"] or now_utc() > entry["expires_at"]:
        raise HTTPException(status_code=400, detail="Invalid or expired upload URL")

    required_ct = entry["content_type"]
    req_ct = (request.headers.get("content-type") or "").split(";")[0].strip()
    if required_ct and req_ct != required_ct:
        raise HTTPException(status_code=400, detail=f"Content-Type must be {required_ct}")

    # Stream to disk
    blob_ref = entry["blob_ref"]
    path = storage_path_for(blob_ref)
    size = 0
    with open(path, "wb") as f:
        async for chunk in request.stream():
            size += len(chunk)
            f.write(chunk)

    # Persist blob in SQLite
    with db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO blobs(blob_ref, path, size_bytes, content_type, uploaded_at) VALUES(?,?,?,?,?)",
            (blob_ref, str(path), size, required_ct, now_utc().isoformat()),
        )
        conn.commit()

    presigned_tokens.pop(upload_id, None)
    return PlainTextResponse("", status_code=201)

# ------------------------------------------------------------------------------
# Endpoint: POST /recordings  (batch-aware; persists to SQLite)

@app.post(
    "/recordings",
    response_model=RecordingsResponse,
    summary="Create or append to a recording batch with metadata",
    tags=["Recordings"],
)
def create_recordings(req: RecordingsRequest, auth=Depends(require_api_key)):
    tenant_id = auth["tenant_id"]

    with db() as conn:
        # ensure batch (existing or new)
        batch_id = ensure_open_batch(conn, req.batch_id)

        # verify each blob exists and belongs to this batch
        for it in req.items:
            exists = conn.execute("SELECT 1 FROM blobs WHERE blob_ref=?", (it.blob_ref,)).fetchone()
            if not exists:
                raise HTTPException(status_code=409, detail=f"Blob not found for client_item_id={it.client_item_id}")
            link = conn.execute("SELECT batch_id FROM batch_uploads WHERE blob_ref=?", (it.blob_ref,)).fetchone()
            if not link or link["batch_id"] != batch_id:
                raise HTTPException(status_code=409, detail=f"Blob for client_item_id={it.client_item_id} is not linked to batch {batch_id}")

        # idempotency (scoped to this batch)
        request_hash = hashlib.sha256(("|".join([i.client_item_id + i.blob_ref for i in req.items]) + f":{batch_id}").encode()).hexdigest()
        idem_key = f"{tenant_id}:{req.idempotency_key}:{request_hash}"
        existing = conn.execute("SELECT id, status FROM batches WHERE idem_key=?", (idem_key,)).fetchone()
        if existing:
            batch_id = existing["id"]
            rec_rows = conn.execute("SELECT client_item_id, id, status FROM recordings WHERE batch_id=?", (batch_id,)).fetchall()
            items = [RecordingItemOut(client_item_id=r["client_item_id"], recording_id=r["id"], status=r["status"]) for r in rec_rows]
            return RecordingsResponse(batch_id=batch_id, status=existing["status"], items=items, poll={"href": f"/batches/{batch_id}"})

        # mark/keep batch status 'pending' when we have items
        conn.execute("UPDATE batches SET status=?, idem_key=? WHERE id=?", ("pending", idem_key, batch_id))

        out_items: List[RecordingItemOut] = []
        for it in req.items:
            rec_id = new_id("rec")
            conn.execute(
                "INSERT INTO recordings(id, batch_id, tenant_id, client_item_id, blob_ref, status, data_json, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (rec_id, batch_id, tenant_id, it.client_item_id, it.blob_ref, "queued", json.dumps(it.data), now_utc().isoformat())
            )
            out_items.append(RecordingItemOut(client_item_id=it.client_item_id, recording_id=rec_id, status="queued"))

        conn.commit()

    return RecordingsResponse(batch_id=batch_id, status="pending", items=out_items, poll={"href": f"/batches/{batch_id}"})


# Add this endpoint to your mock-upload-api.py file

@app.delete(
    "/batches/{batch_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a batch and all its recordings and files",
    tags=["Recordings"],
)
def delete_batch(batch_id: str = Path(..., description="Batch ID"), auth=Depends(require_api_key)):
    with db() as conn:
        # First, check if the batch exists
        batch_exists = conn.execute("SELECT id FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not batch_exists:
            raise HTTPException(status_code=404, detail="Batch not found")

        # Find all recordings and their blob references for this batch
        recs_to_delete = conn.execute("SELECT id, blob_ref FROM recordings WHERE batch_id=?", (batch_id,)).fetchall()
        blob_refs_to_delete = [r["blob_ref"] for r in recs_to_delete]

        # Find the physical paths of the blobs to delete the files
        if blob_refs_to_delete:
            placeholders = ",".join("?" * len(blob_refs_to_delete))
            blobs_to_delete = conn.execute(f"SELECT path FROM blobs WHERE blob_ref IN ({placeholders})", blob_refs_to_delete).fetchall()
            
            # Delete the actual files from the storage directory
            for blob in blobs_to_delete:
                try:
                    file_path = FSPath(blob["path"])
                    if file_path.exists():
                        os.remove(file_path)
                except Exception as e:
                    # Log error but continue cleanup
                    print(f"Warning: Could not delete file {blob['path']}: {e}")
        
        # In a single transaction, delete all database records
        # The order is important to respect relationships
        if blob_refs_to_delete:
            conn.execute(f"DELETE FROM blobs WHERE blob_ref IN ({placeholders})", blob_refs_to_delete)
            conn.execute(f"DELETE FROM batch_uploads WHERE blob_ref IN ({placeholders})", blob_refs_to_delete)
        
        conn.execute("DELETE FROM recordings WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM batches WHERE id=?", (batch_id,))
        
        conn.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)

# ------------------------------------------------------------------------------
# Endpoint: GET /batches/{batch_id}

@app.get(
    "/batches/{batch_id}",
    response_model=BatchStatusResponse,
    summary="Get batch status",
    tags=["Recordings"],
)
def get_batch(batch_id: str = Path(..., description="Batch ID"), auth=Depends(require_api_key)):
    with db() as conn:
        row = conn.execute("SELECT id, status FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Batch not found")
        rec_rows = conn.execute("SELECT client_item_id, id, status FROM recordings WHERE batch_id=?", (batch_id,)).fetchall()
        items = [RecordingItemOut(client_item_id=r["client_item_id"], recording_id=r["id"], status=r["status"]) for r in rec_rows]
        return BatchStatusResponse(batch_id=row["id"], status=row["status"], items=items)

# ------------------------------------------------------------------------------
# UI (batch-grouped) & media endpoints

@app.get("/ui", include_in_schema=False)
def ui(
    key: Optional[str] = Query(None, description="API key for browser access"),
):
    if not key:
        return HTMLResponse(f"""
        <html><body style="font-family: system-ui; padding: 24px">
          <h2>Enter API key</h2>
          <form method="GET" action="/ui">
            <input type="password" name="key" placeholder="X-API-Key" style="padding:8px" />
            <button type="submit" style="padding:8px 12px">Open</button>
          </form>
          <p>For this mock, use: <code>{TEST_API_KEY}</code></p>
        </body></html>
        """, status_code=200)

    if key != TEST_API_KEY:
        return HTMLResponse(f"""
        <html><body style="font-family: system-ui; padding: 24px">
          <h2>Enter API key</h2>
          <p style="color:#b00">Invalid key</p>
          <form method="GET" action="/ui">
            <input type="password" name="key" placeholder="X-API-Key" style="padding:8px" />
            <button type="submit" style="padding:8px 12px">Open</button>
          </form>
          <p>For this mock, use: <code>{TEST_API_KEY}</code></p>
        </body></html>
        """, status_code=401)

    # Group by batch
    with db() as conn:
        batches_rows = conn.execute("SELECT id, status, created_at FROM batches ORDER BY created_at DESC").fetchall()
        recs_by_batch: Dict[str, List[sqlite3.Row]] = {}
        for b in batches_rows:
            recs = conn.execute("""
                SELECT r.id as rec_id, r.client_item_id, r.status, r.data_json,
                       bl.size_bytes, bl.content_type
                FROM recordings r
                LEFT JOIN blobs bl ON bl.blob_ref = r.blob_ref
                WHERE r.batch_id=?
                ORDER BY r.created_at ASC
            """, (b["id"],)).fetchall()
            recs_by_batch[b["id"]] = recs

    sections = []
    for b in batches_rows:
        rows = recs_by_batch.get(b["id"], [])
        row_html = []
        for r in rows:
            meta = json.loads(r["data_json"] or "{}")
            row_html.append(f"""
            <tr>
              <td>{r['rec_id']}</td>
              <td>{r['client_item_id']}</td>
              <td>{r['status']}</td>
              <td><pre style="margin:0">{json.dumps(meta, indent=2)}</pre></td>
              <td>{r['size_bytes'] or 0}</td>
              <td>{r['content_type'] or ''}</td>
              <td><audio controls src="/mock/media/{r['rec_id']}?key={key}"></audio></td>
            </tr>
            """)
        sections.append(f"""
        <section style="margin: 20px 0; border:1px solid #ddd; border-radius:8px; padding:12px">
          <h2 style="margin:0 0 8px 0">Batch: {b['id']} <small style="color:#666">({b['status']})</small></h2>
          <table style="width:100%; border-collapse:collapse">
            <thead>
              <tr>
                <th style="border:1px solid #ddd; padding:6px">Recording ID</th>
                <th style="border:1px solid #ddd; padding:6px">Client Item</th>
                <th style="border:1px solid #ddd; padding:6px">Status</th>
                <th style="border:1px solid #ddd; padding:6px">Metadata</th>
                <th style="border:1px solid #ddd; padding:6px">Size</th>
                <th style="border:1px solid #ddd; padding:6px">Type</th>
                <th style="border:1px solid #ddd; padding:6px">Preview</th>
              </tr>
            </thead>
            <tbody>
              {''.join(row_html) or '<tr><td colspan="7" style="padding:8px">No recordings in this batch yet.</td></tr>'}
            </tbody>
          </table>
        </section>
        """)

    html = f"""
    <html>
    <head>
      <title>Recordings UI</title>
      <style>
        body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; padding: 16px; }}
        h1 {{ margin-top: 0; }}
      </style>
    </head>
    <body>
      <h1>Recordings (Grouped by Batch)</h1>
      <p><small>Using key: <code>{key}</code></small></p>
      {''.join(sections) or '<p>No batches yet.</p>'}
      <p style="margin-top:16px"><a href="/ui">Change key</a></p>
    </body>
    </html>
    """
    return HTMLResponse(html)

@app.get(
    "/jsonui",
    summary="[JSON API] Get all batches and their recordings",
    # This endpoint is kept for compatibility, but you might rename it to /api/batches
    tags=["UI & Reporting"],
    include_in_schema=True, # It's a real endpoint now, so we can include it in docs
)
def get_batches_as_json(
    key: Optional[str] = Query(None, description="API key for browser access"),
):
    # 1. Authentication check now returns JSON errors
    if not key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-API-Key")
    
    if key != TEST_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid X-API-Key")

    # 2. Database query remains the same
    with db() as conn:
        batches_rows = conn.execute("SELECT id, status, created_at FROM batches ORDER BY created_at DESC").fetchall()
        recs_by_batch: Dict[str, List[sqlite3.Row]] = {}
        for b in batches_rows:
            recs = conn.execute("""
                SELECT r.id as rec_id, r.client_item_id, r.status, r.data_json,
                       bl.size_bytes, bl.content_type
                FROM recordings r
                LEFT JOIN blobs bl ON bl.blob_ref = r.blob_ref
                WHERE r.batch_id=?
                ORDER BY r.created_at ASC
            """, (b["id"],)).fetchall()
            recs_by_batch[b["id"]] = recs
    # 3. Data is now structured into a Python list of dictionaries
    output_batches = []
    for b in batches_rows:
        recordings_list = []
        # Get the recordings for the current batch
        for r in recs_by_batch.get(b["id"], []):
            # Parse the metadata from its JSON string format
            metadata = json.loads(r["data_json"] or "{}")
            
            recordings_list.append({
                "recording_id": r["rec_id"],
                "client_item_id": r["client_item_id"],
                "status": r["status"],
                "metadata": metadata,
                "size_bytes": r["size_bytes"] or 0,
                "content_type": r["content_type"] or "",
                "media_url": f"/media/{r['rec_id']}?key={key}" # Provide a direct URL to the media
            })

        output_batches.append({
            "batch_id": b["id"],
            "status": b["status"],
            "created_at": b["created_at"],
            "recordings": recordings_list
        })
    
    # 4. Return a JSONResponse with the structured data
    return JSONResponse(content=output_batches)

    
@app.get("/media/{recording_id}", include_in_schema=False)
def media(
    recording_id: str,
    key: Optional[str] = Query(None),
):
    if key != TEST_API_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid key")

    with db() as conn:
        row = conn.execute("SELECT blob_ref FROM recordings WHERE id=?", (recording_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Recording not found")
        b = conn.execute("SELECT path, content_type FROM blobs WHERE blob_ref=?", (row["blob_ref"],)).fetchone()
        if not b:
            raise HTTPException(status_code=404, detail="Blob not found")

    path = FSPath(b["path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="File missing on disk")
    return FileResponse(path, media_type=b["content_type"] or "application/octet-stream")

# ------------------------------------------------------------------------------
# Health

@app.get("/healthz", include_in_schema=False)
def health():
    return {"ok": True}
