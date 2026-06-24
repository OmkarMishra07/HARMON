#!/usr/bin/env python3
"""
Velox Music Backend — JioSaavn API & S3-backed audio caching
==============================================================
Cache hierarchy (fastest → slowest):
  1. In-memory dict   — 0 ms  (same session, hot paths)
  2. S3 presigned URL — ~80ms (persists forever, survives restarts)
  3. JioSaavn API     — ~1s   (only on first-ever play of a song)

S3 upload strategy:
  - On cache miss: serve user immediately from JioSaavn audio stream
  - Simultaneously download full audio and upload to S3 in background thread
  - Next request for same song hits S3 → instant presigned URL returned
"""

import os, sys, io, json, time, threading, logging, subprocess, socket
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
load_dotenv()                          # reads .env file

import boto3
from botocore.exceptions import ClientError
import requests as req_lib
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context, redirect
from flask_cors import CORS


# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("velox")

app = Flask(__name__, static_folder=".")
CORS(app)

executor = ThreadPoolExecutor(max_workers=8)


# ── AWS / S3 config ───────────────────────────────────────────────────────────
S3_BUCKET     = os.environ.get("S3_BUCKET")
S3_REGION     = os.environ.get("AWS_REGION")
AWS_ACCESS    = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET    = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3 = None
if S3_BUCKET and S3_REGION and AWS_ACCESS and AWS_SECRET:
    try:
        s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=AWS_ACCESS,
            aws_secret_access_key=AWS_SECRET,
        )
        log.info(f"[S3] Client initialized successfully for bucket '{S3_BUCKET}'")
    except Exception as e:
        log.error(f"[S3] Failed to initialize client: {e}")
        s3 = None
else:
    log.warning("[S3] Environment variables missing. Running in local fallback mode (no S3 cache).")

# JioSaavn API Configuration
JIOSAAVN_API_URL = os.environ.get("JIOSAAVN_API_URL", "http://localhost:3000")


# ── Ensure JioSaavn API is running ────────────────────────────────────────────
def _ensure_jiosaavn_api():
    if "localhost" in JIOSAAVN_API_URL or "127.0.0.1" in JIOSAAVN_API_URL:
        # Check if the port is already listening
        try:
            port = int(JIOSAAVN_API_URL.split(":")[-1].split("/")[0])
        except Exception:
            port = 3000
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.0)
        try:
            s.connect(("127.0.0.1", port))
            s.close()
            log.info(f"[JioSaavn API] Server detected on port {port}")
        except Exception:
            # Not running, start it
            log.info(f"[JioSaavn API] Not running. Launching local API on port {port}...")
            jio_dir = os.path.join(os.path.dirname(__file__), "jiosaavn-api")
            if os.path.exists(jio_dir):
                # Ensure node dependencies are installed
                node_modules_path = os.path.join(jio_dir, "node_modules")
                if not os.path.exists(node_modules_path):
                    log.info("[JioSaavn API] node_modules missing. Installing npm packages...")
                    try:
                        subprocess.run("npm install", cwd=jio_dir, check=True, shell=True)
                    except Exception as e:
                        log.error(f"[JioSaavn API] npm install failed: {e}")

                subprocess.Popen(
                    "npx tsx serve.js",
                    cwd=jio_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=True
                )
                log.info("[JioSaavn API] Started server process 'npx tsx serve.js' in background")
                # Wait briefly for server boot
                time.sleep(2)
            else:
                log.warning("[JioSaavn API] Local 'jiosaavn-api' folder not found. Please self-host it manually.")


import re

def _sanitize_filename(name: str) -> str:
    # Remove characters that aren't alphanumeric, spaces, hyphens, or underscores
    s = re.sub(r"[^\w\s-]", "", name)
    # Replace spaces and multiple underscores/hyphens with a single underscore
    s = re.sub(r"[\s_]+", "_", s)
    return s[:60].strip("_")

def _clean_song_title(title: str) -> str:
    # Remove common tags
    tag_pattern = r"\s*[\(\[][^\]\)]*(?:official|lyrics?|video|audio|visualizer|clip|hd|hq|4k|remix|cover|edit|mv|ft\.|feat\.)[^\]\)]*[\)\]]"
    cleaned = re.sub(tag_pattern, "", title, flags=re.IGNORECASE)
    
    loose_pattern = r"\s*-\s*(?:official|lyrics?|video|audio|visualizer|clip)\b"
    cleaned = re.sub(loose_pattern, "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"\s*[|:\-–—\s]+$", "", cleaned)
    cleaned = re.sub(r"^\s*[|:\-–—\s]+", "", cleaned)
    
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip() or title

def _audio_key(vid: str, title: str | None = None, ext: str = "mp4") -> str:
    if title:
        sanitized = _sanitize_filename(title)
        return f"audio/{sanitized}_{vid}.{ext}"
    return f"audio/{vid}.{ext}"

def _find_audio_key(vid: str, title: str | None = None) -> str:
    # Check for multiple possible extensions to support legacy .webm files as well as new .mp4/.mp3 files
    for ext in ["mp4", "mp3", "webm"]:
        if title:
            key_with_title = _audio_key(vid, title, ext)
            if _s3_exists(key_with_title):
                return key_with_title
        
        key_without_title = _audio_key(vid, None, ext)
        if _s3_exists(key_without_title):
            return key_without_title
            
    # Default to mp4 for new files
    return _audio_key(vid, title, "mp4")

def _meta_key(vid: str)  -> str: return f"meta/{vid}.json"


# ── S3 CORS (browser needs this to play audio directly from S3) ───────────────
def _setup_s3_cors():
    if not s3:
        return
    try:
        s3.put_bucket_cors(
            Bucket=S3_BUCKET,
            CORSConfiguration={"CORSRules": [{
                "AllowedHeaders": ["*"],
                "AllowedMethods": ["GET", "HEAD"],
                "AllowedOrigins": ["*"],
                "ExposeHeaders": [
                    "Content-Type", "Content-Length",
                    "Accept-Ranges", "Content-Range", "ETag",
                ],
                "MaxAgeSeconds": 86400,
            }]},
        )
        log.info(f"[S3] CORS set on bucket '{S3_BUCKET}'")
    except Exception as e:
        log.warning(f"[S3] CORS setup warning (non-fatal): {e}")


# ── S3 helpers ────────────────────────────────────────────────────────────────
def _s3_exists(key: str) -> bool:
    if not s3:
        return False
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False

def _s3_presigned(key: str, expires: int = 3600) -> str:
    if not s3:
        return ""
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )

def _s3_get_meta(vid: str) -> dict | None:
    if not s3:
        return None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=_meta_key(vid))
        return json.loads(obj["Body"].read())
    except Exception:
        return None

def _s3_put_meta(vid: str, meta: dict):
    if not s3:
        return
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=_meta_key(vid),
            Body=json.dumps(meta).encode(),
            ContentType="application/json",
        )
    except Exception as e:
        log.warning(f"[S3] Failed to put meta: {e}")


# ── Background S3 upload ──────────────────────────────────────────────────────
_uploading: set = set()   # song IDs currently being uploaded (dedup)

def _bg_upload(audio_url: str, vid: str, meta: dict):
    """
    Background thread: download audio from JioSaavn → upload to S3.
    User is already listening; this runs silently behind the scenes.
    """
    if not s3:
        return
    if vid in _uploading:
        return
    _uploading.add(vid)
    try:
        title = meta.get("title", "")
        log.info(f"[S3 ↑] downloading {vid} ({title[:30]}) for upload…")
        r = req_lib.get(audio_url, headers=HEADERS, stream=True, timeout=300)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "audio/mp4")

        # Determine the file extension dynamically from the content type or URL
        ext = "mp4"
        if "mpeg" in content_type or "mp3" in content_type:
            ext = "mp3"
        elif "webm" in content_type:
            ext = "webm"
        elif ".mp3" in audio_url.lower():
            ext = "mp3"

        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=131_072):   # 128 KB chunks
            buf.write(chunk)
        size_kb = buf.tell() // 1024
        buf.seek(0)

        # Upload with human readable filename using sanitized title and vid
        key = _audio_key(vid, title, ext)
        s3.upload_fileobj(
            buf, S3_BUCKET, key,
            ExtraArgs={"ContentType": content_type},
        )
        _s3_put_meta(vid, meta)
        log.info(f"[S3 ✓] {vid} ({title[:30]}) {size_kb} KB uploaded")
    except Exception as e:
        log.error(f"[S3 ✗] {vid}: {e}")
    finally:
        _uploading.discard(vid)


# ── In-memory caches ──────────────────────────────────────────────────────────
_mem:     dict = {}   # query_key → result  (expires after CACHE_TTL)
_vid_map: dict = {}   # query_key → song_id (never expires in-session, populated by suggest)
_lock = threading.Lock()

_vid_map_path = "vid_map.json"

def _load_vid_map():
    global _vid_map
    if os.path.exists(_vid_map_path):
        try:
            with open(_vid_map_path, "r", encoding="utf-8") as f:
                _vid_map = json.load(f)
            log.info(f"[Cache] Loaded {len(_vid_map)} song mappings from local file.")
        except Exception as e:
            log.warning(f"[Cache] Failed to load local song map: {e}")

def _save_vid_map():
    try:
        with open(_vid_map_path, "w", encoding="utf-8") as f:
            json.dump(_vid_map, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"[Cache] Failed to save local song map: {e}")

def _update_vid_map(key: str, vid: str):
    k = key.lower().strip()
    if not k:
        return
    with _lock:
        if _vid_map.get(k) != vid:
            _vid_map[k] = vid
            _save_vid_map()

# Load mapping on startup
_load_vid_map()
_inflight: dict = {}
_inflight_lock = threading.Lock()

CACHE_TTL = 3500     # ~58 min — S3 presigned URLs renew every call so they never stale

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Core: fast song-ID lookup ─────────────────────────────────────────────────
def _get_vid_id(query: str) -> str | None:
    """
    Get JioSaavn song ID via fast search endpoint.
    Result cached in _vid_map so repeated calls are instant.
    """
    k = query.lower().strip()
    with _lock:
        if k in _vid_map:
            return _vid_map[k]
    try:
        r = req_lib.get(f"{JIOSAAVN_API_URL}/api/search/songs", params={"query": query})
        if r.status_code == 200:
            data = r.json()
            if data.get("success") and data.get("data", {}).get("results"):
                vid = data["data"]["results"][0].get("id")
                if vid:
                    _update_vid_map(k, vid)
                    return vid
    except Exception as e:
        log.error(f"[VID-ID ERR] {e}")
    return None


# ── Core: main fetch (cache hierarchy) ───────────────────────────────────────
def _fetch_song(query: str, upload_to_s3: bool = True, video_id: str | None = None) -> dict | None:
    """
    Returns a result dict with at minimum:
      url, title, thumbnail, duration, channel, video_id, source, expires
    """
    key = query.lower().strip()

    # ── Layer 1: in-memory cache ──────────────────────────────────
    with _lock:
        e = _mem.get(key)
        if e and time.time() < e.get("expires", 0):
            log.info(f"[MEM ⚡] {key[:55]}")
            return e

    # ── Deduplicate concurrent fetches for same query ──────────────
    with _inflight_lock:
        ev = _inflight.get(key)
        if ev is None:
            ev = threading.Event()
            _inflight[key] = ev
            do_work = True
        else:
            do_work = False

    if not do_work:
        log.info(f"[WAIT] {key[:55]}")
        ev.wait(timeout=90)
        with _lock:
            return _mem.get(key)

    result = None
    try:
        # ── Layer 2: S3 cache ─────────────────────────────────────
        with _lock:
            cached_vid = _vid_map.get(key)
        vid = video_id or cached_vid or _get_vid_id(query)

        if vid:
            s3_meta = _s3_get_meta(vid)
            if s3_meta:
                title = s3_meta.get("title", "")
                audio_key = _find_audio_key(vid, title)
                if _s3_exists(audio_key):
                    url = _s3_presigned(audio_key, expires=3600)
                    result = {
                        **s3_meta,
                        "url":     url,
                        "source":  "s3",
                        "expires": time.time() + CACHE_TTL,
                    }
                    with _lock:
                        _mem[key] = result
                    log.info(f"[S3  ⚡] {vid} ({title[:30]})")
                    return result

        # ── Layer 3: JioSaavn Fetch ───────────────────────────────
        log.info(f"[JioSaavn 🔍] {key[:55]}")
        song_data = None

        if vid:
            # Query song details directly by ID
            r = req_lib.get(f"{JIOSAAVN_API_URL}/api/songs/{vid}")
            if r.status_code == 200:
                data = r.json()
                if data.get("success") and data.get("data"):
                    song_data = data["data"][0]

        if not song_data:
            # Fallback to search if ID lookup fails
            r = req_lib.get(f"{JIOSAAVN_API_URL}/api/search/songs", params={"query": query})
            if r.status_code == 200:
                data = r.json()
                if data.get("success") and data.get("data", {}).get("results"):
                    song_data = data["data"]["results"][0]

        if not song_data:
            return None

        vid = song_data.get("id") or vid or ""
        raw_title = song_data.get("name", query)
        clean_title = _clean_song_title(raw_title)
        
        _update_vid_map(key, vid)
        _update_vid_map(clean_title, vid)

        # Get highest quality stream url
        dl_urls = song_data.get("downloadUrl", [])
        audio_url = ""
        for u in dl_urls:
            if u.get("quality") == "320kbps":
                audio_url = u.get("url")
                break
        if not audio_url and dl_urls:
            audio_url = dl_urls[-1].get("url")

        if not audio_url:
            return None

        # Build thumbnail and artists information
        images = song_data.get("image", [])
        thumb = images[-1].get("url") if images else ""
        
        artists_list = song_data.get("artists", {}).get("primary", [])
        channel = ", ".join([a.get("name", "") for a in artists_list if a.get("name")])

        s3_meta = {
            "title":     clean_title,
            "thumbnail": thumb,
            "duration":  int(song_data.get("duration", 0)),
            "channel":   channel,
            "video_id":  vid,
        }
        result = {
            **s3_meta,
            "url":     audio_url,
            "source":  "jiosaavn",
            "expires": time.time() + CACHE_TTL,
        }
        with _lock:
            _mem[key] = result

        # ── Kick off background S3 upload ─────────────────────────
        if upload_to_s3 and vid and vid not in _uploading:
            log.info(f"[S3 ↑] queuing upload for {vid}")
            executor.submit(_bg_upload, audio_url, vid, s3_meta)

        return result

    except Exception as e:
        log.error(f"[FETCH ERR] {key[:55]}: {e}")
        return None
    finally:
        with _inflight_lock:
            _inflight.pop(key, None)
        ev.set()


def _fetch_suggestions(query: str) -> list:
    try:
        r = req_lib.get(f"{JIOSAAVN_API_URL}/api/search/songs", params={"query": query})
        if r.status_code != 200:
            return []
        data = r.json()
        if not data.get("success") or not data.get("data", {}).get("results"):
            return []
        
        out = []
        for e in data["data"]["results"]:
            if not e:
                continue
            vid = e.get("id", "")
            raw_title = e.get("name", "")
            clean_title = _clean_song_title(raw_title)
            
            artists_list = e.get("artists", {}).get("primary", [])
            channel = ", ".join([a.get("name", "") for a in artists_list if a.get("name")])
            
            # Map both raw and cleaned titles to the song ID
            if vid:
                if raw_title:
                    _vid_map[raw_title.lower().strip()] = vid
                if clean_title:
                    _vid_map[clean_title.lower().strip()] = vid
            
            images = e.get("image", [])
            thumb = images[-1].get("url") if images else ""

            out.append({
                "title":    clean_title,
                "channel":  channel,
                "duration": int(e.get("duration", 0)),
                "thumbnail": thumb,
                "video_id": vid,
            })
        return out
    except Exception as e:
        log.error(f"[SUGGEST ERR] {e}")
        return []


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/suggest")
def suggest():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])

    items = _fetch_suggestions(q)

    # Prefetch top 2 — if they're already in S3 this is instant
    for item in items[:2]:
        k = item["title"].lower().strip()
        with _lock:
            cached = _mem.get(k)
        if not cached or time.time() >= cached.get("expires", 0):
            executor.submit(_fetch_song, item["title"], False)

    return jsonify(items)


@app.route("/api/meta")
def meta():
    """
    Primary play endpoint.
    Returns audio_url which is either:
      - An S3 presigned URL  (source=s3)        → browser plays directly from S3, ~80ms
      - A JioSaavn direct link (source=jiosaavn) → browser plays directly, first play
    """
    q = request.args.get("q", "").strip()
    video_id = request.args.get("video_id", "").strip() or None
    if not q:
        return jsonify({"error": "missing query"}), 400

    result = _fetch_song(q, upload_to_s3=True, video_id=video_id)
    if not result:
        return jsonify({"error": "not found"}), 404

    return jsonify({
        "title":     result["title"],
        "thumbnail": result["thumbnail"],
        "duration":  result["duration"],
        "channel":   result["channel"],
        "video_id":  result.get("video_id", ""),
        "audio_url": result["url"],
        "source":    result.get("source", "jiosaavn"),  # "s3" or "jiosaavn"
    })


@app.route("/api/stream")
def stream_audio():
    """
    Proxy fallback — only used if browser can't play audio_url directly.
    For S3 URLs: issues a 302 redirect (browser fetches from S3 directly).
    For JioSaavn URLs: proxies through Flask.
    """
    q = request.args.get("q", "").strip()
    video_id = request.args.get("video_id", "").strip() or None
    if not q:
        return jsonify({"error": "missing query"}), 400

    result = _fetch_song(q, upload_to_s3=True, video_id=video_id)
    if not result:
        return jsonify({"error": "not found"}), 404

    audio_url = result["url"]

    # S3 URLs → just redirect
    if result.get("source") == "s3":
        return redirect(audio_url, code=302)

    # JioSaavn URLs → proxy
    headers = dict(HEADERS)
    rng = request.headers.get("Range")
    if rng:
        headers["Range"] = rng
    try:
        jio_resp = req_lib.get(audio_url, headers=headers, stream=True, timeout=20)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    resp_headers = {
        "Content-Type":  jio_resp.headers.get("Content-Type", "audio/mp4"),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
    }
    for h in ("Content-Length", "Content-Range"):
        if h in jio_resp.headers:
            resp_headers[h] = jio_resp.headers[h]

    @stream_with_context
    def gen():
        for chunk in jio_resp.iter_content(chunk_size=65_536):
            if chunk:
                yield chunk

    return Response(gen(), status=jio_resp.status_code, headers=resp_headers)


@app.route("/api/prefetch")
def prefetch():
    q = request.args.get("q", "").strip()
    if q:
        executor.submit(_fetch_song, q, False)
    return jsonify({"status": "queued"})


@app.route("/api/cache-status")
def cache_status():
    with _lock:
        mem_entries = [
            {"key": k, "source": v.get("source", "?"), "title": v.get("title", "")}
            for k, v in _mem.items()
        ]
    return jsonify({
        "memory_cache":         len(mem_entries),
        "song_id_map":          len(_vid_map),
        "s3_uploads_active":    len(_uploading),
        "s3_bucket":            S3_BUCKET,
        "s3_region":            S3_REGION,
        "entries":              mem_entries,
    })


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    
    # Auto-start JioSaavn node server if running locally
    _ensure_jiosaavn_api()

    print("\n" + "=" * 55)
    print("  Velox Music  —  JioSaavn & S3 streaming")
    print(f"  Bucket : {S3_BUCKET}  ({S3_REGION})")
    print(f"  API    : {JIOSAAVN_API_URL}")
    print(f"  URL    : http://localhost:5000")
    print("=" * 55 + "\n")

    _setup_s3_cors()   # configure CORS on bucket so browser can play directly

    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
