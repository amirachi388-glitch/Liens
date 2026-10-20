#!/usr/bin/env python3
# Video Downloader Studio — single-file Flask app
# Install: pip install -U Flask yt-dlp
# Optional: install FFmpeg for audio conversion / format merging.
from __future__ import annotations

import os
import re
import json
import time
import uuid
import shutil
import sqlite3
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from flask import Flask, jsonify, request, render_template_string, send_from_directory, abort
import yt_dlp

APP_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = Path(os.environ.get("VIDEO_DOWNLOADER_DIR", APP_DIR / "downloads")).resolve()
DB_PATH = APP_DIR / "video_studio.sqlite3"
HOST = os.environ.get("VIDEO_STUDIO_HOST", "127.0.0.1")
PORT = int(os.environ.get("VIDEO_STUDIO_PORT", "5000"))
MAX_WORKERS = max(1, min(4, int(os.environ.get("VIDEO_STUDIO_WORKERS", "2"))))
MAX_ACTIVE_JOBS = 30
MAX_URL_LENGTH = 2048
ALLOWED_SCHEMES = {"http", "https"}
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="video-job")
jobs_lock = threading.RLock()
jobs: dict[str, dict] = {}
cancel_events: dict[str, threading.Event] = {}


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, url TEXT NOT NULL, title TEXT,
                status TEXT NOT NULL, progress REAL NOT NULL DEFAULT 0,
                speed TEXT, eta TEXT, filename TEXT, format TEXT,
                error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC)")


def now():
    return datetime.now().isoformat(timespec="seconds")


def valid_url(value):
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_URL_LENGTH:
        return False
    try:
        p = urlparse(value.strip())
        return p.scheme in ALLOWED_SCHEMES and bool(p.hostname) and not any(c.isspace() for c in p.netloc)
    except Exception:
        return False


def safe_error(exc):
    msg = str(exc).strip().replace("\n", " ")
    msg = re.sub(r"https?://\S+", "[رابط]", msg)
    return (msg[:260] or "حدث خطأ غير معروف")


def update_job(job_id, **changes):
    stamp = now()
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(changes)
            jobs[job_id]["updated_at"] = stamp
            snapshot = dict(jobs[job_id])
        else:
            return
    with db() as conn:
        conn.execute("""
            UPDATE tasks SET title=?, status=?, progress=?, speed=?, eta=?, filename=?,
                format=?, error=?, updated_at=? WHERE id=?
        """, (snapshot.get("title"), snapshot.get("status", "queued"),
              float(snapshot.get("progress", 0)), snapshot.get("speed"), snapshot.get("eta"),
              snapshot.get("filename"), snapshot.get("format"), snapshot.get("error"),
              stamp, job_id))


def public_job(j):
    return {k: j.get(k) for k in (
        "id", "url", "title", "status", "progress", "speed", "eta",
        "filename", "format", "error", "created_at", "updated_at"
    )}


def progress_hook(job_id):
    def hook(d):
        with jobs_lock:
            ev = cancel_events.get(job_id)
        if ev and ev.is_set():
            raise yt_dlp.utils.DownloadError("أُلغي التنزيل من قبل المستخدم")
        status = d.get("status")
        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            pct = min(99.0, (done / total * 100) if total else 0)
            speed = d.get("speed")
            eta = d.get("eta")
            update_job(job_id, status="downloading", progress=round(pct, 1),
                       speed=f"{speed / 1024 / 1024:.2f} MB/s" if speed else None,
                       eta=f"{int(eta)} ث" if eta is not None else None)
        elif status == "finished":
            update_job(job_id, status="processing", progress=99.0, speed=None, eta=None)
    return hook


def download_worker(job_id, url, fmt, audio_only):
    with jobs_lock:
        ev = cancel_events[job_id]
    try:
        update_job(job_id, status="fetching", progress=0, error=None)
        common = {
            "noplaylist": True,
            "outtmpl": str(DOWNLOAD_DIR / "%(title).180B_%(id)s.%(ext)s"),
            "restrictfilenames": True,
            "windowsfilenames": True,
            "retries": 5,
            "fragment_retries": 5,
            "socket_timeout": 25,
            "continuedl": True,
            "overwrites": False,
            "progress_hooks": [progress_hook(job_id)],
            "quiet": True,
            "no_warnings": True,
        }
        if audio_only:
            common.update({"format": "bestaudio/best",
                           "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3",
                                               "preferredquality": "192"}]})
        else:
            format_map = {
                "best": "bestvideo*+bestaudio/best",
                "1080": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                "720": "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                "480": "bestvideo[height<=480]+bestaudio/best[height<=480]/best",
                "360": "bestvideo[height<=360]+bestaudio/best[height<=360]/best",
            }
            common["format"] = format_map.get(fmt, format_map["best"])
            common["merge_output_format"] = "mp4"
        with yt_dlp.YoutubeDL(common) as ydl:
            info = ydl.extract_info(url, download=False)
            if ev.is_set():
                raise RuntimeError("أُلغي التنزيل من قبل المستخدم")
            title = info.get("title") or "فيديو بدون عنوان"
            update_job(job_id, title=title, status="downloading", format="MP3" if audio_only else fmt)
            result = ydl.extract_info(url, download=True)
            requested = result.get("requested_downloads") or []
            candidate_paths = []
            for item in requested:
                p = item.get("filepath") or item.get("_filename")
                if p:
                    candidate_paths.append(Path(p))
            for key in ("filepath", "_filename"):
                p = result.get(key)
                if p:
                    candidate_paths.append(Path(p))
            # Post-processors may change extension; use title/id as a conservative fallback.
            existing = next((p.resolve() for p in candidate_paths if p.exists() and p.is_file()), None)
        if ev.is_set():
            update_job(job_id, status="cancelled", error="تم إلغاء المهمة", speed=None, eta=None)
        else:
            update_job(job_id, status="completed", progress=100,
                       filename=existing.name if existing and existing.parent == DOWNLOAD_DIR else None,
                       speed=None, eta=None, error=None)
    except Exception as exc:
        message = safe_error(exc)
        with jobs_lock:
            cancelled = ev.is_set()
        update_job(job_id, status="cancelled" if cancelled else "error",
                   error="تم إلغاء المهمة" if cancelled else message, speed=None, eta=None)
    finally:
        with jobs_lock:
            cancel_events.pop(job_id, None)


@app.get("/")
def index():
    return render_template_string(PAGE, app_name="Video Studio")


@app.get("/api/tasks")
def api_tasks():
    with db() as conn:
        rows = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT 100").fetchall()
    return jsonify({"tasks": [dict(r) for r in rows]})


@app.post("/api/info")
def api_info():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not valid_url(url):
        return jsonify(error="أدخل رابط فيديو صحيحًا يبدأ بـ http أو https."), 400
    try:
        opts = {"quiet": True, "no_warnings": True, "noplaylist": True, "socket_timeout": 15, "skip_download": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return jsonify({
            "title": info.get("title") or "فيديو بدون عنوان",
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "views": info.get("view_count"),
            "description": (info.get("description") or "")[:240],
            "formats": sorted({f.get("height") for f in (info.get("formats") or []) if f.get("height")}, reverse=True)[:8]
        })
    except Exception as exc:
        return jsonify(error=safe_error(exc)), 422


@app.post("/api/download")
def api_download():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    fmt = str(data.get("quality") or "best")
    audio_only = bool(data.get("audio"))
    if not valid_url(url):
        return jsonify(error="الرابط غير صالح. تأكد من نسخه كاملًا."), 400
    if fmt not in {"best", "1080", "720", "480", "360"}:
        fmt = "best"
    if audio_only and not shutil.which("ffmpeg"):
        return jsonify(error="لتحويل الصوت إلى MP3 يجب تثبيت FFmpeg أولًا."), 400
    with jobs_lock:
        active = sum(1 for j in jobs.values() if j.get("status") in {"queued", "fetching", "downloading", "processing"})
        if active >= MAX_ACTIVE_JOBS:
            return jsonify(error="قائمة الانتظار ممتلئة. انتظر انتهاء بعض المهام."), 429
        job_id = uuid.uuid4().hex[:12]
        created = now()
        job = {"id": job_id, "url": url, "title": "جارٍ تجهيز المهمة…", "status": "queued",
               "progress": 0, "speed": None, "eta": None, "filename": None,
               "format": "MP3" if audio_only else fmt, "error": None,
               "created_at": created, "updated_at": created}
        jobs[job_id] = job
        cancel_events[job_id] = threading.Event()
    with db() as conn:
        conn.execute("""INSERT INTO tasks (id,url,title,status,progress,format,created_at,updated_at)
                        VALUES (?,?,?,?,?,?,?,?)""",
                     (job_id, url, job["title"], "queued", 0, job["format"], created, created))
    executor.submit(download_worker, job_id, url, fmt, audio_only)
    return jsonify(task=public_job(job)), 202


@app.post("/api/cancel/<job_id>")
def api_cancel(job_id):
    with jobs_lock:
        ev = cancel_events.get(job_id)
        job = jobs.get(job_id)
        if not job:
            with db() as conn:
                row = conn.execute("SELECT status FROM tasks WHERE id=?", (job_id,)).fetchone()
            if not row:
                return jsonify(error="المهمة غير موجودة."), 404
            if row["status"] in {"completed", "error", "cancelled"}:
                return jsonify(ok=True, status=row["status"])
        if ev:
            ev.set()
        if job and job.get("status") == "queued":
            job["status"] = "cancelled"
            job["error"] = "تم إلغاء المهمة"
    if job:
        update_job(job_id, status="cancelled", error="تم إلغاء المهمة", speed=None, eta=None)
    return jsonify(ok=True)


@app.get("/api/file/<path:filename>")
def api_file(filename):
    # Only serve files directly inside the configured downloads folder.
    candidate = (DOWNLOAD_DIR / filename).resolve()
    if candidate.parent != DOWNLOAD_DIR or not candidate.is_file():
        abort(404)
    return send_from_directory(DOWNLOAD_DIR, candidate.name, as_attachment=True)


@app.get("/health")
def health():
    return jsonify(ok=True, ffmpeg=bool(shutil.which("ffmpeg")), workers=MAX_WORKERS)


PAGE = r"""
<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0b1020">
<title>Video Studio — مركز التنزيل الذكي</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@400;450;500;550;600;650;700&display=swap');
:root{color-scheme:dark;--bg:#0d0e10;--surface:#151719;--surface-2:#1a1c1f;--line:#2b2e32;--text:#f1f2f3;--muted:#92979f;--accent:#c6f277;--accent-ink:#171b10;--red:#ff7777;--green:#c6f277;--shadow:0 18px 50px #0003}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;min-height:100vh;background:var(--bg);color:var(--text);font-family:'IBM Plex Sans Arabic',system-ui,sans-serif;font-size:13px;line-height:1.65;overflow-x:hidden}
body:before{content:"";position:fixed;inset:0;pointer-events:none;opacity:.12;background-image:radial-gradient(#fff 0.55px,transparent .55px);background-size:7px 7px;mask-image:linear-gradient(to bottom,#000,transparent 75%);z-index:-1}
.orb{position:fixed;width:300px;height:300px;border-radius:50%;filter:blur(110px);opacity:.06;pointer-events:none;z-index:-1;animation:ambient 26s cubic-bezier(.45,0,.55,1) infinite alternate}.orb.one{background:#c6f277;top:4%;right:-180px}.orb.two{background:#b5c7ff;bottom:0;left:-190px;animation-delay:-13s}
@keyframes ambient{0%{transform:translate3d(0,0,0) scale(.9)}50%{transform:translate3d(-35px,50px,0) scale(1.08)}100%{transform:translate3d(25px,-30px,0) scale(.96)}}
@keyframes enter{0%{opacity:0;transform:translateY(12px);filter:blur(4px)}100%{opacity:1;transform:translateY(0);filter:blur(0)}}
@keyframes reveal{0%{opacity:0;clip-path:inset(0 0 100% 0)}100%{opacity:1;clip-path:inset(0 0 0 0)}}
@keyframes sheen{0%{transform:translateX(140%);opacity:0}15%{opacity:.4}65%{opacity:.15}100%{transform:translateX(-140%);opacity:0}}
@keyframes ring{0%{box-shadow:0 0 0 0 #c6f27735}70%{box-shadow:0 0 0 7px #c6f27700}100%{box-shadow:0 0 0 0 #c6f27700}}
@keyframes rotate{to{transform:rotate(360deg)}}@keyframes barflow{to{background-position:200% center}}
@keyframes breathe{0%,100%{opacity:.65}50%{opacity:1}}
.wrap{width:min(1010px,calc(100% - 40px));margin:auto}.topbar{height:78px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line);animation:enter .6s both}.brand{display:flex;align-items:center;gap:11px;font-weight:650;font-size:14px;letter-spacing:.15px}.logo{width:34px;height:34px;display:grid;place-items:center;border:1px solid #c6f2775a;border-radius:10px;background:#c6f277;color:var(--accent-ink);font-size:13px}.brand small{display:block;color:var(--muted);font-size:10px;font-weight:400;letter-spacing:.3px;margin-top:1px}.top-actions{display:flex;gap:10px;align-items:center}.pill{border:1px solid var(--line);background:var(--surface);padding:6px 10px;border-radius:7px;font-size:10px;color:#c3c7cc;display:flex;gap:7px;align-items:center}.dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 0 3px #c6f27718}
.hero{padding:40px 0 27px;animation:enter .7s .05s both}.eyebrow{display:inline-flex;gap:8px;align-items:center;color:var(--accent);font-size:10px;font-weight:600;letter-spacing:.15px}.eyebrow:before{content:"";width:18px;height:1px;background:var(--accent)}.hero h1{font-size:clamp(27px,3.5vw,39px);line-height:1.4;letter-spacing:-.9px;margin:11px 0 5px;font-weight:600}.gradient{color:var(--muted)}.hero p{color:var(--muted);font-size:12px;line-height:1.9;max-width:550px;margin:0}
.layout{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(265px,.85fr);gap:14px;align-items:start}.panel{border:1px solid var(--line);background:var(--surface);border-radius:13px;overflow:hidden;box-shadow:var(--shadow);animation:enter .65s both;transition:border-color .35s,transform .35s}.panel:hover{border-color:#41464b}.panel-head{padding:18px 19px 0;display:flex;align-items:center;justify-content:space-between;gap:12px}.panel-title{font-size:13px;font-weight:600;letter-spacing:-.1px}.sub{color:var(--muted);font-size:10px;margin-top:3px}.step{font-size:9px;color:var(--muted);background:var(--surface-2);padding:5px 8px;border-radius:5px;border:1px solid var(--line);white-space:nowrap}.panel-body{padding:18px 19px}.label{display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:11px;font-weight:600;margin-bottom:8px}.hint{color:var(--muted);font-weight:400;font-size:9px}.urlbox{display:flex;align-items:center;gap:7px;border:1px solid #35393e;background:#101214;border-radius:9px;padding:5px;transition:border-color .25s,box-shadow .25s,background .25s}.urlbox:focus-within{border-color:#c6f2779a;box-shadow:0 0 0 3px #c6f27712;background:#111410}.url-icon{padding:0 7px;color:var(--accent);font-size:15px}.urlbox input{min-width:0;flex:1;border:0;outline:0;background:transparent;color:var(--text);font:inherit;font-size:11px;padding:10px 0;direction:ltr;text-align:left}.urlbox input::placeholder{color:#656a70}.iconbtn{border:1px solid var(--line);background:#202326;color:#c7cbd0;border-radius:7px;padding:9px;cursor:pointer;transition:background .2s,border-color .2s,transform .2s}.iconbtn:hover{background:#2a2e31;border-color:#555b60;transform:translateY(-1px)}button{font-family:inherit}.primary{position:relative;isolation:isolate;overflow:hidden;border:1px solid transparent;color:var(--accent-ink);background:var(--accent);border-radius:8px;padding:10px 13px;font-size:11px;font-weight:650;cursor:pointer;transition:transform .22s cubic-bezier(.2,.8,.2,1),box-shadow .22s,background .22s;white-space:nowrap}.primary:before{content:"";position:absolute;inset:-30% auto -30% -60%;width:35%;background:linear-gradient(90deg,transparent,#fff8,transparent);transform:skewX(-18deg);z-index:-1;opacity:0}.primary:hover:before{animation:sheen .8s ease}.primary:hover{transform:translateY(-1px);box-shadow:0 5px 18px #c6f27718;background:#d4ff91}.primary:active{transform:scale(.985)}.primary:disabled{opacity:.55;cursor:wait;transform:none;box-shadow:none}.full{width:100%;margin-top:15px;padding:12px}.spin{display:inline-block;width:12px;height:12px;border:1.5px solid #171b1044;border-top-color:var(--accent-ink);border-radius:50%;animation:rotate .7s linear infinite;vertical-align:middle;margin-left:7px}
.options{display:grid;grid-template-columns:1fr 1fr;gap:9px;margin-top:13px}.field{border:1px solid var(--line);background:#121416;border-radius:8px;padding:11px}.field label{display:block;color:var(--muted);font-size:10px;margin-bottom:6px}.field select{width:100%;background:transparent;color:var(--text);border:0;outline:0;font:inherit;font-size:11px;cursor:pointer}.field option{background:#17191b}.toggleline{display:flex;align-items:center;justify-content:space-between;gap:9px}.toggleline strong{display:block;font-size:10px;font-weight:500}.toggleline small{display:block;color:var(--muted);font-size:9px;margin-top:2px}.switch{appearance:none;width:32px;height:18px;background:#3a3e43;border-radius:99px;position:relative;cursor:pointer;transition:background .25s;flex-shrink:0}.switch:before{content:"";position:absolute;width:12px;height:12px;top:3px;right:3px;border-radius:50%;background:#e5e7eb;transition:transform .25s cubic-bezier(.2,.8,.2,1),background .25s}.switch:checked{background:var(--accent)}.switch:checked:before{transform:translateX(-14px);background:#1a1d15}
.preview{margin-top:12px;display:none;border:1px solid var(--line);border-radius:9px;overflow:hidden;background:#101214}.preview.show{display:flex;animation:enter .35s both}.thumb{width:105px;min-height:75px;object-fit:cover;background:#202326}.preview-info{padding:10px;min-width:0;display:flex;flex-direction:column;justify-content:center;gap:5px}.preview-title{font-size:10px;font-weight:600;line-height:1.7;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}.preview-meta{color:var(--muted);font-size:9px;display:flex;gap:8px;flex-wrap:wrap}
.notice{display:none;margin-top:11px;padding:9px 11px;border-radius:7px;font-size:10px;line-height:1.8}.notice.show{display:block;animation:enter .25s}.notice.error{background:#ff77770c;border:1px solid #ff77772c;color:#ff9a9a}.notice.success{background:#c6f2770c;border:1px solid #c6f2772c;color:var(--green)}
.side{display:grid;gap:14px}.statgrid{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:15px 17px 17px}.stat{padding:12px;border:1px solid var(--line);border-radius:8px;background:#111315;transition:transform .25s,border-color .25s}.stat:hover{transform:translateY(-2px);border-color:#4b5054}.stat .sicon{font-size:13px;color:var(--muted)}.stat strong{display:block;font-size:21px;line-height:1.3;margin-top:8px;font-weight:550;letter-spacing:-.5px}.stat small{display:block;color:var(--muted);font-size:9px;margin-top:4px}.queue-list{padding:12px 13px 14px;display:grid;gap:8px;max-height:410px;overflow:auto}.empty{text-align:center;padding:23px 12px;color:var(--muted);font-size:10px;line-height:2}.empty .emptyicon{font-size:22px;opacity:.5;margin-bottom:5px}.task{padding:11px;border:1px solid var(--line);border-radius:9px;background:#111315;animation:enter .35s both;transition:border-color .25s,background .25s}.task:hover{border-color:#454b50;background:#141719}.tasktop{display:flex;gap:9px;align-items:flex-start}.fileicon{width:30px;height:30px;flex-shrink:0;display:grid;place-items:center;border-radius:7px;background:#c6f27712;color:var(--accent);border:1px solid #c6f27716;font-size:12px}.tasktext{min-width:0;flex:1}.taskname{font-size:10px;line-height:1.7;font-weight:550;overflow-wrap:anywhere}.taskstatus{font-size:9px;color:var(--muted);margin-top:3px;overflow-wrap:anywhere}.taskstatus.done{color:var(--green)}.taskstatus.bad{color:var(--red)}.pct{font-size:9px;color:var(--accent);direction:ltr;font-variant-numeric:tabular-nums}.bar{height:3px;border-radius:99px;background:#2a2d30;margin-top:11px;overflow:hidden}.bar span{display:block;height:100%;border-radius:99px;background:var(--accent);transition:width .65s cubic-bezier(.2,.8,.2,1);position:relative}.bar span:after{content:"";position:absolute;inset:0;background:linear-gradient(90deg,transparent,#ffffffaa,transparent);background-size:200% 100%;animation:barflow 2.4s linear infinite}.taskbottom{display:flex;justify-content:space-between;align-items:center;margin-top:8px;color:var(--muted);font-size:9px;gap:8px}.task-actions{display:flex;gap:5px}.mini{border:1px solid var(--line);background:#1b1e20;color:#d9dcdf;padding:5px 8px;border-radius:6px;cursor:pointer;font:inherit;font-size:9px;text-decoration:none;transition:background .2s,border-color .2s,color .2s}.mini:hover{background:#292d30;border-color:#545a60;color:white}.features{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:14px 0 28px;animation:enter .7s .2s both}.feature{border:1px solid var(--line);background:#121416;border-radius:9px;padding:13px;display:flex;gap:10px;align-items:flex-start;transition:transform .25s,border-color .25s}.feature:hover{transform:translateY(-2px);border-color:#454b50}.feature-icon{width:29px;height:29px;display:grid;place-items:center;border-radius:7px;background:#24272a;color:var(--accent);flex-shrink:0;font-size:12px}.feature b{font-size:10px;font-weight:600}.feature p{color:var(--muted);font-size:9px;line-height:1.8;margin:4px 0 0}.footer{border-top:1px solid var(--line);padding:15px 0 22px;color:#777d84;font-size:9px;display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}.toast{position:fixed;bottom:20px;left:50%;transform:translate(-50%,12px);opacity:0;pointer-events:none;background:#202326;border:1px solid #3a3f43;padding:10px 14px;border-radius:8px;box-shadow:var(--shadow);font-size:10px;z-index:10;transition:opacity .25s,transform .25s}.toast.show{opacity:1;transform:translate(-50%,0)}.pulse{animation:ring 3.5s infinite}
@media(max-width:800px){.layout{grid-template-columns:1fr}.side{grid-template-columns:1fr 1fr}.queue-panel{grid-column:1/-1}.queue-list{max-height:370px}.hero{padding-top:30px}}@media(max-width:560px){.wrap{width:calc(100% - 24px)}.topbar{height:67px}.pill.hide-mobile{display:none}.hero h1{letter-spacing:-.5px}.panel{border-radius:10px}.panel-head{padding:15px 14px 0}.panel-body{padding:14px}.urlbox{flex-wrap:wrap}.urlbox input{width:calc(100% - 82px)}.urlbox .primary{width:100%;margin:0}.side{grid-template-columns:1fr}.queue-panel{grid-column:auto}.features{grid-template-columns:1fr}.feature{padding:11px}.options{gap:7px}.field{padding:9px}.thumb{width:90px}.brand small{font-size:8px}.hint{display:none}.footer{line-height:1.8}}
@media(prefers-reduced-motion:reduce){*,*:before,*:after{animation-duration:.01ms!important;animation-iteration-count:1!important;scroll-behavior:auto!important;transition-duration:.01ms!important}}
</style>
</head>
<body>
<div class="orb one"></div><div class="orb two"></div>
<div class="wrap">
<header class="topbar">
 <div class="brand"><div class="logo">▶</div><div>VIDEO STUDIO<small>مركز التنزيلات</small></div></div>
 <div class="top-actions"><div class="pill hide-mobile"><span>✦</span> واجهة ذكية وسريعة</div><div class="pill"><span class="dot"></span> <span id="serverState">النظام جاهز</span></div></div>
</header>
<section class="hero">
 <div class="eyebrow"><span class="spark">✧</span> تنزيل الفيديو <span>✧</span></div>
 <h1>تنزيلاتك، <span class="gradient">بوضوح وتحكّم.</span></h1>
 <p>الصق رابط الفيديو، حدّد الجودة، وتابع التقدّم من مكان واحد.</p>
</section>
<main class="layout">
<section class="panel">
 <div class="panel-head"><div><div class="panel-title">تنزيل جديد</div><div class="sub">أدخل رابط الفيديو</div></div><div class="step">تنزيل</div></div>
 <div class="panel-body">
  <div class="label"><span>رابط الفيديو</span><span class="hint">رابط عام</span></div>
  <div class="urlbox"><span class="url-icon">↗</span><input id="url" type="url" autocomplete="url" placeholder="https://example.com/watch?v=..." aria-label="رابط الفيديو"><button class="iconbtn" id="pasteBtn" title="لصق من الحافظة" aria-label="لصق">▣</button><button class="primary" id="previewBtn">تحليل الرابط <span>↗</span></button></div>
  <div id="preview" class="preview"><img class="thumb" id="thumb" alt="صورة الفيديو" src=""><div class="preview-info"><div class="preview-title" id="previewTitle"></div><div class="preview-meta" id="previewMeta"></div></div></div>
  <div class="options">
   <div class="field"><label for="quality">جودة الفيديو</label><select id="quality"><option value="best">أفضل جودة متاحة</option><option value="1080">Full HD · 1080p</option><option value="720">HD · 720p</option><option value="480">480p</option><option value="360">360p</option></select></div>
   <div class="field"><label>نوع الملف</label><div class="toggleline"><div><strong>استخراج الصوت MP3</strong><small>يتطلب FFmpeg مثبتًا</small></div><input class="switch" id="audio" type="checkbox" aria-label="استخراج الصوت MP3"></div></div>
  </div>
  <button class="primary full" id="downloadBtn">↓ &nbsp; إضافة إلى قائمة التنزيل</button>
  <div class="notice" id="notice" role="status"></div>
 </div>
</section>
<aside class="side">
 <section class="panel"><div class="panel-head"><div><div class="panel-title">ملخص</div><div class="sub">حالة التنزيلات</div></div><span class="step">LIVE</span></div>
  <div class="statgrid"><div class="stat"><span class="sicon">⇩</span><strong id="statDone">0</strong><small>مكتمل</small></div><div class="stat"><span class="sicon">◷</span><strong id="statActive">0</strong><small>قيد التنفيذ</small></div><div class="stat"><span class="sicon">☷</span><strong id="statTotal">0</strong><small>إجمالي المهام</small></div><div class="stat"><span class="sicon">✦</span><strong id="statRate">—</strong><small>حالة الخدمة</small></div></div>
 </section>
 <section class="panel"><div class="panel-head"><div><div class="panel-title">الميزات</div><div class="sub">الأدوات المتاحة</div></div></div><div class="panel-body" style="padding-top:17px"><div style="display:flex;gap:12px;align-items:center;margin-bottom:15px"><div class="feature-icon">⚡</div><div><b style="font-size:11px">تقدّم مباشر</b><div class="sub">تحديث تلقائي للحالة</div></div></div><div style="display:flex;gap:12px;align-items:center;margin-bottom:15px"><div class="feature-icon">◈</div><div><b style="font-size:11px">اختيار الجودة</b><div class="sub">عدة مستويات للجودة</div></div></div><div style="display:flex;gap:12px;align-items:center"><div class="feature-icon">⌁</div><div><b style="font-size:11px">إدارة المهام</b><div class="sub">عرض المهام أو إلغاؤها</div></div></div></div></section>
</aside>
<section class="panel queue-panel" style="grid-column:1/-1"><div class="panel-head"><div><div class="panel-title">التنزيلات</div><div class="sub">تحديث تلقائي لحالة المهام</div></div><button class="mini" id="refreshBtn">↻ تحديث القائمة</button></div><div class="queue-list" id="queue"><div class="empty"><div class="emptyicon">▤</div>لا توجد تنزيلات حتى الآن.<br>أضف رابطًا لبدء أول مهمة.</div></div></section>
</main>
<section class="features"><div class="feature"><div class="feature-icon">✧</div><div><b>حركة مدروسة</b><p>انتقالات سلسة مع دعم إعداد تقليل الحركة.</p></div></div><div class="feature"><div class="feature-icon">⛨</div><div><b>تشغيل محلي</b><p>الخدمة متاحة على جهازك افتراضيًا.</p></div></div><div class="feature"><div class="feature-icon">▧</div><div><b>حفظ السجل</b><p>تُحفظ بيانات المهام محليًا.</p></div></div></section>
<footer class="footer"><span>Video Studio · مدير التنزيلات</span><span>نزّل المحتوى الذي تملك حق حفظه أو إذنًا بتنزيله.</span></footer>
</div><div class="toast" id="toast"></div>
<script>
const $=id=>document.getElementById(id);
const state={tasks:[],preview:null,busy:false};
const labels={queued:'في قائمة الانتظار',fetching:'جارٍ تحليل الفيديو',downloading:'جارٍ التنزيل',processing:'جارٍ المعالجة',completed:'اكتمل التنزيل',error:'تعذر التنزيل',cancelled:'أُلغيت المهمة'};
const activeStatuses=['queued','fetching','downloading','processing'];
function escapeHtml(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function toast(msg){$('toast').textContent=msg;$('toast').classList.add('show');setTimeout(()=>$('toast').classList.remove('show'),2700)}
function notice(msg,type='error'){$('notice').textContent=msg;$('notice').className='notice show '+type}
function clearNotice(){$('notice').className='notice'}
function fmtDuration(s){if(!s)return '';s=Number(s);return `${Math.floor(s/60)}:${String(Math.floor(s%60)).padStart(2,'0')}`}
async function request(url,opts={}){
 const controller=new AbortController();
 const timeout=setTimeout(()=>controller.abort(),45000);
 try{
   const r=await fetch(url,{headers:{'Content-Type':'application/json'},...opts,signal:controller.signal,cache:'no-store'});
   const d=await r.json().catch(()=>({error:'استجابة غير مفهومة من الخادم'}));
   if(!r.ok)throw new Error(d.error||'حدث خطأ في الطلب');
   return d;
 }catch(e){
   if(e.name==='AbortError')throw new Error('استغرق فحص الرابط وقتًا طويلًا. تحقق من اتصال الإنترنت وحاول مجددًا.');
   if(e instanceof TypeError)throw new Error('تعذر الاتصال بالتطبيق. تأكد أن نافذة الخادم ما زالت تعمل.');
   throw e;
 }finally{clearTimeout(timeout)}
}
$('pasteBtn').onclick=async()=>{try{$('url').value=await navigator.clipboard.readText();$('url').focus();toast('تم لصق الرابط')}catch{toast('تعذر الوصول للحافظة؛ الصق الرابط يدويًا')}};
$('previewBtn').onclick=async()=>{
 const url=$('url').value.trim();
 if(!url)return notice('الصق رابط الفيديو أولًا.');
 const b=$('previewBtn'), preview=$('preview'), thumb=$('thumb');
 b.disabled=true;b.innerHTML='<span class="spin"></span> جارٍ التحليل';
 clearNotice();
 preview.classList.remove('show');
 $('previewTitle').textContent='';
 $('previewMeta').textContent='';
 thumb.removeAttribute('src');
 try{
   const d=await request('/api/info',{method:'POST',body:JSON.stringify({url})});
   state.preview=d;
   $('previewTitle').textContent=d.title||'فيديو بدون عنوان';
   $('previewMeta').textContent=[d.uploader,d.duration?fmtDuration(d.duration):'',d.views?Number(d.views).toLocaleString('ar'):null].filter(Boolean).join(' · ');
   if(d.thumbnail){
     thumb.style.display='block';
     thumb.alt=d.title||'صورة الفيديو';
     thumb.onload=()=>{preview.classList.add('show')};
     thumb.onerror=()=>{thumb.style.display='none';preview.classList.add('show')};
     thumb.src=d.thumbnail;
     // Don't wait forever for a third-party thumbnail before showing the returned metadata.
     preview.classList.add('show');
   }else{
     thumb.style.display='none';
     preview.classList.add('show');
   }
   notice('تم فحص الرابط بنجاح.','success');
   requestAnimationFrame(()=>preview.scrollIntoView({behavior:window.matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth',block:'nearest'}));
 }catch(e){
   state.preview=null;
   preview.classList.remove('show');
   notice(e.message||'تعذر فحص الرابط. حاول مرة أخرى.');
 }finally{
   b.disabled=false;b.innerHTML='تحليل الرابط <span>↗</span>';
 }
};
$('downloadBtn').onclick=async()=>{const url=$('url').value.trim();if(!url)return notice('أدخل رابط الفيديو أولًا.');const b=$('downloadBtn');b.disabled=true;b.innerHTML='<span class="spin"></span> جارٍ إضافة المهمة';clearNotice();try{const d=await request('/api/download',{method:'POST',body:JSON.stringify({url,quality:$('quality').value,audio:$('audio').checked})});notice('تمت إضافة المهمة إلى التنزيلات.','success');toast('أُضيفت مهمة جديدة بنجاح');await refreshTasks();$('url').value='';$('preview').classList.remove('show');state.preview=null}catch(e){notice(e.message)}finally{b.disabled=false;b.innerHTML='↓ &nbsp; إضافة إلى قائمة التنزيل'}};
$('url').addEventListener('keydown',e=>{if(e.key==='Enter')$('previewBtn').click()});
$('audio').addEventListener('change',()=>{$('quality').disabled=$('audio').checked;$('quality').style.opacity=$('audio').checked?'.45':'1'});
$('refreshBtn').onclick=()=>refreshTasks(true);
async function cancelTask(id){try{await request('/api/cancel/'+encodeURIComponent(id),{method:'POST',body:'{}'});toast('تم إرسال طلب إلغاء المهمة');await refreshTasks()}catch(e){toast(e.message)}}
function renderTasks(tasks){const q=$('queue');if(!tasks.length){q.innerHTML='<div class="empty"><div class="emptyicon">▤</div>لا توجد تنزيلات حتى الآن.<br>أضف رابطًا لبدء أول مهمة.</div>';return}
q.innerHTML=tasks.map(t=>{const status=t.status||'queued',pct=Math.max(0,Math.min(100,Number(t.progress)||0));const done=status==='completed',bad=['error','cancelled'].includes(status);const file=t.filename?`<a class="mini" href="/api/file/${encodeURIComponent(t.filename)}">تنزيل الملف ↙</a>`:'';const cancel=activeStatuses.includes(status)?`<button class="mini" data-cancel="${escapeHtml(t.id)}">إلغاء</button>`:'';return `<article class="task"><div class="tasktop"><div class="fileicon">${done?'✓':bad?'!':'▶'}</div><div class="tasktext"><div class="taskname">${escapeHtml(t.title||'مهمة تنزيل')}</div><div class="taskstatus ${done?'done':bad?'bad':''}">${escapeHtml(labels[status]||status)}${t.error?' · '+escapeHtml(t.error):''}</div></div><div class="pct">${Math.round(pct)}%</div></div><div class="bar"><span style="width:${pct}%;${done?'background:linear-gradient(90deg,#34d399,#6ee7b7)':''}"></span></div><div class="taskbottom"><span>${escapeHtml(t.speed|| (t.eta?'متبقي '+t.eta:'' )||new Date(t.created_at).toLocaleString('ar',{dateStyle:'short',timeStyle:'short'}))}</span><div class="task-actions">${file}${cancel}</div></div></article>`}).join('');
q.querySelectorAll('[data-cancel]').forEach(b=>b.onclick=()=>cancelTask(b.dataset.cancel))}
async function refreshTasks(showToast=false){try{const d=await request('/api/tasks');state.tasks=d.tasks||[];renderTasks(state.tasks);const done=state.tasks.filter(t=>t.status==='completed').length;const active=state.tasks.filter(t=>activeStatuses.includes(t.status)).length;$('statDone').textContent=done;$('statActive').textContent=active;$('statTotal').textContent=state.tasks.length;$('statRate').textContent='متصل';$('serverState').textContent='النظام جاهز';if(showToast)toast('تم تحديث القائمة')}catch(e){$('serverState').textContent='غير متصل';$('statRate').textContent='—'}}
refreshTasks();setInterval(()=>refreshTasks(),2200);
</script>
</body></html>
"""

init_db()

if __name__ == "__main__":
    print("\n" + "=" * 58)
    print("  Video Studio — مركز تنزيل الفيديو")
    print(f"  افتح المتصفح على: http://{HOST}:{PORT}")
    print(f"  مجلد التنزيلات: {DOWNLOAD_DIR}")
    print(f"  عدد العمال: {MAX_WORKERS}")
    print("  أوقف التطبيق باستخدام Ctrl+C")
    print("=" * 58 + "\n")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
