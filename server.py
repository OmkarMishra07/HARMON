#!/usr/bin/env python3
"""
Velox Music Backend — S3-backed audio caching
==============================================
Cache hierarchy (fastest → slowest):
  1. In-memory dict   — 0 ms  (same session, hot paths)
  2. S3 presigned URL — ~80ms (persists forever, survives restarts)
  3. yt-dlp + YouTube — ~5s   (only on first-ever play of a song)

S3 upload strategy:
  - On cache miss: serve user immediately from YouTube URL
  - Simultaneously download full audio and upload to S3 in background thread
  - Next request for same song hits S3 → instant presigned URL returned
"""

import os, sys, io, json, time, threading, logging
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
load_dotenv()                          # reads .env file

import boto3
from botocore.exceptions import ClientError
import requests as req_lib
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context, redirect
from flask_cors import CORS
import yt_dlp


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

import re

def _sanitize_filename(name: str) -> str:
    # Remove characters that aren't alphanumeric, spaces, hyphens, or underscores
    s = re.sub(r"[^\w\s-]", "", name)
    # Replace spaces and multiple underscores/hyphens with a single underscore
    s = re.sub(r"[\s_]+", "_", s)
    return s[:60].strip("_")

def _clean_song_title(title: str) -> str:
    # 1. Remove common bracketed/parenthesized tags like (Official Video), [Lyrics], etc.
    tag_pattern = r"\s*[\(\[][^\]\)]*(?:official|lyrics?|video|audio|visualizer|clip|hd|hq|4k|remix|cover|edit|mv|ft\.|feat\.)[^\]\)]*[\)\]]"
    cleaned = re.sub(tag_pattern, "", title, flags=re.IGNORECASE)
    
    # 2. Also remove loose tags like "Official Video" at the end
    loose_pattern = r"\s*-\s*(?:official|lyrics?|video|audio|visualizer|clip)\b"
    cleaned = re.sub(loose_pattern, "", cleaned, flags=re.IGNORECASE)

    # 3. Clean up dangling delimiters like "Artist - Song - " or "Song | "
    cleaned = re.sub(r"\s*[|:\-–—\s]+$", "", cleaned)
    cleaned = re.sub(r"^\s*[|:\-–—\s]+", "", cleaned)
    
    # 4. Collapse multiple spaces
    cleaned = re.sub(r"\s+", " ", cleaned)
    
    return cleaned.strip() or title

def _audio_key(vid: str, title: str | None = None) -> str:
    if title:
        sanitized = _sanitize_filename(title)
        return f"audio/{sanitized}_{vid}.webm"
    return f"audio/{vid}.webm"

def _find_audio_key(vid: str, title: str | None = None) -> str:
    if title:
        key_with_title = _audio_key(vid, title)
        if _s3_exists(key_with_title):
            return key_with_title
    
    key_without_title = _audio_key(vid)
    if _s3_exists(key_without_title):
        return key_without_title
        
    return _audio_key(vid, title)

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
_uploading: set = set()   # video IDs currently being uploaded (dedup)

def _bg_upload(yt_url: str, vid: str, meta: dict):
    """
    Background thread: download audio from YouTube → upload to S3.
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
        r = req_lib.get(yt_url, headers=YT_HEADERS, stream=True, timeout=300)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "audio/webm")

        buf = io.BytesIO()
        for chunk in r.iter_content(chunk_size=131_072):   # 128 KB chunks
            buf.write(chunk)
        size_kb = buf.tell() // 1024
        buf.seek(0)

        # Upload with human readable filename using sanitized title and vid
        key = _audio_key(vid, title)
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
_vid_map: dict = {}   # query_key → video_id (never expires in-session, populated by suggest)
_lock = threading.Lock()

_vid_map_path = "vid_map.json"

def _load_vid_map():
    global _vid_map
    if os.path.exists(_vid_map_path):
        try:
            with open(_vid_map_path, "r", encoding="utf-8") as f:
                _vid_map = json.load(f)
            log.info(f"[Cache] Loaded {len(_vid_map)} video mappings from local file.")
        except Exception as e:
            log.warning(f"[Cache] Failed to load local video map: {e}")

def _save_vid_map():
    try:
        with open(_vid_map_path, "w", encoding="utf-8") as f:
            json.dump(_vid_map, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"[Cache] Failed to save local video map: {e}")

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

CACHE_TTL = 3500     # ~58 min — just under YouTube URL lifetime (~6 h)
                     # S3 presigned URLs renew every call so they never stale

# ── yt-dlp options ────────────────────────────────────────────────────────────
YDL_FULL = {
    "format": "bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio/best",
    "quiet": True, "no_warnings": True,
    "skip_download": True, "noplaylist": True,
}
YDL_FLAT = {
    "quiet": True, "no_warnings": True,
    "skip_download": True, "noplaylist": True,
    "extract_flat": True,
}
YT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.youtube.com/",
    "Origin": "https://www.youtube.com",
}


# ── Core: fast video-ID lookup ────────────────────────────────────────────────
def _get_vid_id(query: str) -> str | None:
    """
    Get YouTube video ID via fast flat extraction (~1-2s).
    Result cached in _vid_map so repeated calls are instant.
    """
    k = query.lower().strip()
    with _lock:
        if k in _vid_map:
            return _vid_map[k]
    try:
        with yt_dlp.YoutubeDL(YDL_FLAT) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        if info and info.get("entries"):
            vid = info["entries"][0].get("id")
            if vid:
                _update_vid_map(k, vid)
                return vid
    except Exception as e:
        log.error(f"[VID-ID ERR] {e}")
    return None


def _pick_best_url(video: dict) -> str | None:
    fmts = video.get("formats") or []
    audio_only = [
        f for f in fmts
        if f.get("acodec") != "none" and f.get("vcodec") in (None, "none", "")
    ]
    audio_only.sort(key=lambda f: f.get("abr") or 0, reverse=True)
    if audio_only:
        return audio_only[0]["url"]
    muxed = [f for f in fmts if f.get("acodec") != "none"]
    if muxed:
        return muxed[0]["url"]
    return video.get("url")


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
        # Step 2a: get video ID (fast, ~1-2s; or 0ms if already in _vid_map)
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

        # ── Layer 3: yt-dlp full fetch ────────────────────────────
        log.info(f"[YT  🔍] {key[:55]}")
        with yt_dlp.YoutubeDL(YDL_FULL) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None

        video = info["entries"][0]
        vid   = video.get("id", "") or vid or ""
        
        raw_title = video.get("title", query)
        clean_title = _clean_song_title(raw_title)
        
        _update_vid_map(key, vid)
        _update_vid_map(clean_title, vid)

        yt_url = _pick_best_url(video)
        if not yt_url:
            return None

        thumb = (
            video.get("thumbnail")
            or (f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg" if vid else "")
        )
        s3_meta = {
            "title":     clean_title,
            "thumbnail": thumb,
            "duration":  video.get("duration", 0),
            "channel":   video.get("channel") or video.get("uploader", ""),
            "video_id":  vid,
        }
        result = {
            **s3_meta,
            "url":     yt_url,
            "source":  "youtube",
            "expires": time.time() + CACHE_TTL,
        }
        with _lock:
            _mem[key] = result

        # ── Kick off background S3 upload ─────────────────────────
        if upload_to_s3 and vid and vid not in _uploading:
            log.info(f"[S3 ↑] queuing upload for {vid}")
            executor.submit(_bg_upload, yt_url, vid, s3_meta)

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
        with yt_dlp.YoutubeDL(YDL_FLAT) as ydl:
            info = ydl.extract_info(f"ytsearch6:{query}", download=False)
        if not info or "entries" not in info:
            return []
        out = []
        for e in info["entries"]:
            if not e:
                continue
            vid = e.get("id", "")
            raw_title = e.get("title", "")
            clean_title = _clean_song_title(raw_title)
            
            # Map both raw and cleaned titles to the video ID
            if vid:
                if raw_title:
                    _vid_map[raw_title.lower().strip()] = vid
                if clean_title:
                    _vid_map[clean_title.lower().strip()] = vid
            
            out.append({
                "title":    clean_title,
                "channel":  e.get("channel") or e.get("uploader", ""),
                "duration": e.get("duration", 0),
                "thumbnail": f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg" if vid else "",
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

    # Prefetch top 2 — if they're already in S3 (via _vid_map) this is instant
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
      - An S3 presigned URL  (source=s3)    → browser plays directly from S3, ~80ms
      - A YouTube CDN URL    (source=youtube)→ browser plays directly from YT, first play
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
        "source":    result.get("source", "youtube"),  # "s3" or "youtube"
    })


@app.route("/api/stream")
def stream_audio():
    """
    Proxy fallback — only used if browser can't play audio_url directly.
    For S3 URLs: issues a 302 redirect (browser fetches from S3 directly).
    For YouTube URLs: proxies through Flask (last resort).
    """
    q = request.args.get("q", "").strip()
    video_id = request.args.get("video_id", "").strip() or None
    if not q:
        return jsonify({"error": "missing query"}), 400

    result = _fetch_song(q, upload_to_s3=True, video_id=video_id)
    if not result:
        return jsonify({"error": "not found"}), 404

    audio_url = result["url"]

    # S3 URLs → just redirect (browser fetches directly, no proxy overhead)
    if result.get("source") == "s3":
        return redirect(audio_url, code=302)

    # YouTube URLs → proxy
    headers = dict(YT_HEADERS)
    rng = request.headers.get("Range")
    if rng:
        headers["Range"] = rng
    try:
        yt_resp = req_lib.get(audio_url, headers=headers, stream=True, timeout=20)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    resp_headers = {
        "Content-Type":  yt_resp.headers.get("Content-Type", "audio/webm"),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
    }
    for h in ("Content-Length", "Content-Range"):
        if h in yt_resp.headers:
            resp_headers[h] = yt_resp.headers[h]

    @stream_with_context
    def gen():
        for chunk in yt_resp.iter_content(chunk_size=65_536):
            if chunk:
                yield chunk

    return Response(gen(), status=yt_resp.status_code, headers=resp_headers)


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
        "video_id_map":         len(_vid_map),
        "s3_uploads_active":    len(_uploading),
        "s3_bucket":            S3_BUCKET,
        "s3_region":            S3_REGION,
        "entries":              mem_entries,
    })


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    print("\n" + "=" * 55)
    print("  Velox Music  —  S3-backed streaming")
    print(f"  Bucket : {S3_BUCKET}  ({S3_REGION})")
    print(f"  URL    : http://localhost:5000")
    print("=" * 55 + "\n")

    _setup_s3_cors()   # configure CORS on bucket so browser can play directly

    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
