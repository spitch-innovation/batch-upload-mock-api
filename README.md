# Mock Upload API + Client

A tiny end-to-end playground for uploading **audio recordings** with associated **JSON metadata**, grouping uploads into **batches**, and viewing them in a simple web UI.

* **Server:** `mock-upload-api.py` (FastAPI, SQLite, local file storage)
* **Client:** `mock-client.py` (flag-only CLI that supports presign, upload, create, poll, demo)

> Perfect for wiring up your ingest flow before you plug in S3/MinIO and a real DB.

---

## Requirements

* **Python**: 3.10+
* **pip** (or uv/pipx/poetry if you prefer)
* Optional: **ffmpeg** (to generate sample audio quickly)

---

## Install

Create and activate a virtualenv, then install deps:

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### `requirements.txt`

```txt
fastapi>=0.110
uvicorn[standard]>=0.27
pydantic>=2.5
requests>=2.31
```

SQLite is built into Python; no extra package needed.

---

## Run the mock server

```bash
uvicorn mock-upload-api:app --reload --port 5050
```

* API docs (OpenAPI): `http://localhost:5050/docs`
* Simple UI (requires key): `http://localhost:5050/ui`

  * When prompted, enter the test key: **`test_12345`**
  * Or open directly with: `http://localhost:5050/ui?key=test_12345`

### What the server does

* `POST /uploads/presign`
  Issues presigned **PUT** URLs (they point back to this app) and **creates/uses a `batch_id`** so uploads are grouped.
* `PUT /mock-upload/{upload_id}?token=...`
  Accepts bytes (like `/dev/null`), but **saves** them under `./storage/` and records them in SQLite.
* `POST /recordings`
  Registers metadata (JSON) that references the uploaded `blob_ref`s, **attached to a batch**.
* `GET /batches/{batch_id}`
  Poll batch status.
* `GET /ui?key=test_12345`
  Shows batches grouped with recordings, metadata, and an `<audio>` player for each item.
* `GET /media/{recording_id}?key=test_12345`
  Streams the saved audio file.

### Files created by the server

* `./mock.db` — SQLite database
* `./storage/` — audio blobs saved to disk (hashed filenames)

---

## Client quick start

The client is flag-only; operations are chosen with `--operation`. Use **any** order for flags.

```bash
python mock-client.py --help
```

### 1) All-in-one demo (new batch)

```bash
python mock-client.py \
  --operation demo \
  --base-url http://localhost:5050 \
  --api-key test_12345 \
  --files demo_assets/call1.wav demo_assets/call2.mp3 \
  --meta  demo_assets/call1.json demo_assets/call2.json
```

What it does: `presign → upload → recordings → poll`, then prints a UI link.

### 2) Append to an existing batch (all-in-one)

```bash
python mock-client.py \
  --operation demo \
  --base-url http://localhost:5050 \
  --api-key test_12345 \
  --batch-id rb_<existing_batch_id> \
  --files demo_assets/call3.m4a \
  --meta  demo_assets/call3.json
```

### 3) Step-by-step (presign → upload → create → poll)

**Presign (save output to file):**

```bash
python mock-client.py \
  --operation presign \
  --base-url http://localhost:5050 \
  --api-key test_12345 \
  --files demo_assets/call1.wav demo_assets/call2.mp3 \
  --out /tmp/presign.json
```

**Upload using the presign JSON:**

```bash
python mock-client.py \
  --operation upload \
  --presign-json /tmp/presign.json \
  --files demo_assets/call1.wav demo_assets/call2.mp3
```

**Create recordings with metadata (uses batch from presign.json):**

```bash
python mock-client.py \
  --operation create \
  --base-url http://localhost:5050 \
  --api-key test_12345 \
  --presign-json /tmp/presign.json \
  --meta demo_assets/call1.json demo_assets/call2.json
```

**Poll the batch:**

```bash
BATCH_ID=$(jq -r .batch_id /tmp/presign.json)
python mock-client.py \
  --operation poll \
  --base-url http://localhost:5050 \
  --api-key test_12345 \
  --batch-id "$BATCH_ID"
```

Then open the UI:

```
http://localhost:5050/ui?key=test_12345
```

---

## API keys & auth

* Test key for all endpoints: **`test_12345`**
* For **API requests** (JSON routes), send header: `X-API-Key: test_12345`
* For **UI/Media in the browser**, pass as query param: `?key=test_12345`

---

## Batch semantics (important)

* `POST /uploads/presign`

  * If you **omit `batch_id`**, the server creates a **new** batch and returns `batch_id`.
  * If you **provide `batch_id`**, uploaded blobs are linked to that batch.
* `POST /recordings`

  * Always pass the **same `batch_id`** used for presign/upload.
  * Each item includes `blob_ref` (from presign) and your metadata.
  * The server verifies the blobs exist **and** are linked to the given `batch_id`.

> If you forget to include `batch_id` in `/recordings`, the server will reject with a 409. The client’s **demo** path always carries the correct `batch_id` forward.

---

## Troubleshooting

* **401 on `/ui`**
  Include the key: `http://localhost:5050/ui?key=test_12345`

* **409 “Blob … is not linked to batch …”**
  You presigned/uploads under batch **A** but called `/recordings` for batch **B** (or omitted `batch_id`).
  Fix: reuse the `batch_id` returned by `/uploads/presign` for the same files.

* **Uploads succeed but no audio in UI**
  Ensure the `Content-Type` you PUT matches the one returned by `presign`. The client handles this automatically; manual `curl` must set it.

* **Port changes**
  The server builds presigned URLs from the **request’s base URL**, so use the same `--port` when you upload.

---

* **Repo layout**

```
.
├── mock-upload-api.py
├── mock-client.py
├── requirements.txt
├── README.md
└── demo_assets/
    ├── call1.wav
    ├── call2.mp3
    ├── call2.m4a
    ├── call1.json
    ├── call2.json
    └── call3.json
```
