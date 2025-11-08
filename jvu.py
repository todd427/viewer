#!/usr/bin/env python3
"""
JVU — JSON/JSONL Viewer + Directory Browser

Features:
- Full directory traversal (default root = "/")
- Browse JSON / JSONL files with markdown rendering
- Breadcrumb navigation (/api/breadcrumbs)
- Back, Up, Home navigation with /browse
- Host info display
- Robust /api/read that normalizes absolute paths
"""

from __future__ import annotations
import os, json, gzip, fnmatch, time, uuid, logging, subprocess
from pathlib import Path
from typing import List, Dict, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------- Optional imports ----------
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*_, **__): return False

try:
    import yaml
except Exception:
    yaml = None

load_dotenv()

# ---------- Config Loading ----------
DEFAULT_SKIP_DIRS = [
    "/proc", "/sys", "/dev", "/run", "/snap", "/tmp",
    "/lost+found", "/var/lib/docker", "/var/lib/containers",
]

def _load_yaml_config() -> dict:
    cfg_path = os.getenv("VIEWER_CONFIG")
    if not cfg_path:
        candidate = Path(__file__).with_name("config.yaml")
        cfg_path = str(candidate) if candidate.exists() else None
    if not cfg_path:
        return {}
    try:
        if yaml is None:
            return {}
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

CFG = _load_yaml_config()

BASE_DIR = Path(os.getenv("VIEWER_BASE_DIR", CFG.get("base_dir", Path(__file__).parent))).resolve()
TEMPLATES_DIR = Path(os.getenv("VIEWER_TEMPLATES_DIR", CFG.get("templates_dir", BASE_DIR / "templates"))).resolve()
STATIC_DIR = Path(os.getenv("VIEWER_STATIC_DIR", CFG.get("static_dir", BASE_DIR / "static"))).resolve()

# Default to "/" so we see everything unless overridden
_default_roots = CFG.get("search_roots", ["/"])
search_roots_raw = os.getenv("VIEWER_SEARCH_ROOTS") or os.pathsep.join(_default_roots)
SEARCH_ROOTS: List[Path] = [Path(p).expanduser().resolve() for p in search_roots_raw.split(os.pathsep) if p.strip()]

skip_dirs_raw = os.getenv("VIEWER_SKIP_DIRS")
if skip_dirs_raw is None:
    skip_dirs_raw = os.pathsep.join(CFG.get("skip_dirs", DEFAULT_SKIP_DIRS))
SKIP_DIRS = {str(Path(s).resolve()) for s in skip_dirs_raw.split(os.pathsep) if s.strip()}

PRUNE_GLOBS = [g for g in os.getenv("VIEWER_PRUNE_GLOBS", "").split(os.pathsep) if g.strip()]
RELOAD_DIRS = [p for p in (os.getenv("VIEWER_RELOAD_DIRS") or os.pathsep.join([str(BASE_DIR), str(TEMPLATES_DIR)])).split(os.pathsep) if p.strip()]
HOST = os.getenv("VIEWER_HOST", CFG.get("host", "127.0.0.1"))
PORT = int(os.getenv("VIEWER_PORT", CFG.get("port", 8002)))
RELOAD = (os.getenv("VIEWER_RELOAD", str(CFG.get("reload", "true"))).lower() == "true")
MAX_FILES = int(os.getenv("VIEWER_MAX_FILES", str(CFG.get("max_files", 5000))))
LOG_LEVEL = os.getenv("VIEWER_LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("jvu")

# ---------- App ----------
app = FastAPI(title="JVU — JSON/JSONL Viewer")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    print(f"[jvu] Warning: static dir missing: {STATIC_DIR}")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR)) if TEMPLATES_DIR.exists() else None
if not templates:
    print(f"[jvu] Warning: templates dir missing: {TEMPLATES_DIR}")

# ---------- Host Info ----------
def _host_info() -> Dict[str, str]:
    hostname = os.uname().nodename if hasattr(os, "uname") else os.getenv("HOSTNAME", "")
    try:
        out = subprocess.check_output(["uname", "-a"], text=True).strip()
    except Exception:
        out = ""
    return {"hostname": hostname, "uname": out}

# ---------- Middleware ----------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    rid = uuid.uuid4().hex[:8]
    t0 = time.time()
    logger.info("[req %s] %s %s", rid, request.method, request.url.path)
    try:
        resp = await call_next(request)
    except Exception:
        logger.exception("[req %s] unhandled", rid)
        raise
    dt = (time.time() - t0) * 1000.0
    logger.info("[res %s] %s %s -> %s in %.1fms", rid, request.method, request.url.path, resp.status_code, dt)
    return resp

# ---------- Helpers ----------
def _norm_exts(exts: List[str]) -> List[str]:
    return [(e if e.startswith('.') else f'.{e}').lower() for e in exts]

def _prune_dir(path: Path) -> bool:
    try:
        rp = str(path.resolve())
    except Exception:
        rp = str(path)
    if rp in SKIP_DIRS:
        return True
    for pat in PRUNE_GLOBS:
        if fnmatch.fnmatch(rp, pat):
            return True
    return False

def iter_tree(path: Path, exts: List[str]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    exts = _norm_exts(exts)
    try:
        entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        return []
    for p in entries:
        if _prune_dir(p):
            continue
        if p.is_dir():
            out.append({"type": "dir", "name": p.name, "path": str(p.resolve())})
        else:
            if exts and p.suffix.lower() not in exts:
                continue
            out.append({"type": "file", "name": p.name, "path": str(p.resolve())})
    return out

def _safe_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        return [{"error": str(e)}]

def _safe_jsonl(path: Path, limit: int = 5000):
    out = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    out.append({"_raw": line})
                if i >= limit:
                    break
    except Exception as e:
        out.append({"error": str(e)})
    return out

def _find_owning_root(path: Path) -> Optional[Path]:
    rp = path.resolve()
    for root in SEARCH_ROOTS:
        try:
            rp.relative_to(root.resolve())
            return root
        except Exception:
            continue
    return None

# ---------- API Routes ----------
@app.get("/api/host")
def api_host():
    return _host_info()

@app.get("/api/health")
def api_health():
    return {
        "ok": True,
        "host": _host_info(),
        "roots": [str(p) for p in SEARCH_ROOTS],
        "skip_dirs": sorted(SKIP_DIRS),
        "max_files": MAX_FILES,
    }

@app.get("/api/tree")
def api_tree(
    path: Optional[str] = Query(None),
    ext: Optional[str] = Query("json,jsonl"),
):
    exts = [e.strip() for e in (ext.split(",") if ext else []) if e.strip()]
    if path:
        p = Path(path)
        if not p.exists() or not p.is_dir():
            raise HTTPException(404, f"Not a directory: {path}")
        nodes = iter_tree(p, exts)
        return {"path": str(p.resolve()), "nodes": nodes}
    roots = [str(r) for r in SEARCH_ROOTS if r.exists()]
    return {"path": None, "roots": roots}

@app.get("/api/files")
def api_files(
    ext: Optional[str] = Query("json,jsonl"),
    max_files: int = Query(None),
):
    exts = [e.strip() for e in (ext.split(",") if ext else []) if e.strip()]
    limit = max_files or MAX_FILES
    found = []
    for root in SEARCH_ROOTS:
        for r, dirs, files in os.walk(root, topdown=True):
            dirs[:] = [d for d in dirs if not _prune_dir(Path(r, d))]
            for f in files:
                p = Path(r, f)
                if exts and p.suffix.lower() not in _norm_exts(exts):
                    continue
                found.append(str(p))
                if len(found) >= limit:
                    return {"count": len(found), "files": found}
    return {"count": len(found), "files": found}

@app.get("/api/parent")
def api_parent(path: str = Query(...)):
    p = Path(path).resolve()
    if not p.exists():
        raise HTTPException(404, f"Not found: {path}")
    owner = _find_owning_root(p)
    parent = p.parent if p.parent != p else None
    if owner and parent:
        try:
            parent.resolve().relative_to(owner.resolve())
        except Exception:
            parent = None
    return {"path": str(p), "parent": str(parent) if parent else None, "root": str(owner) if owner else None}

@app.get("/api/breadcrumbs")
def api_breadcrumbs(path: str):
    p = Path(path)
    if not p.is_absolute():
        p = Path("/") / path.lstrip("/")
    rp = p.resolve()
    owner = _find_owning_root(rp)
    crumbs = []
    if owner is None:
        parts = rp.parts
        accum = Path(parts[0])
        for part in parts[1:]:
            accum = accum / part
            crumbs.append({"label": part, "path": str(accum), "type": "dir"})
        if rp.exists() and rp.is_file():
            crumbs[-1]["type"] = "file"
        return {"root": None, "crumbs": crumbs}

    rel = rp.relative_to(owner.resolve())
    accum = owner.resolve()
    crumbs.append({"label": str(owner), "path": str(owner.resolve()), "type": "root"})
    for seg in rel.parts:
        accum = accum / seg
        crumbs.append({
            "label": seg,
            "path": str(accum),
            "type": "file" if accum.exists() and accum.is_file() else "dir"
        })
    return {"root": str(owner.resolve()), "crumbs": crumbs}

@app.get("/api/read")
def api_read(name: Optional[str] = Query(None), path: Optional[str] = Query(None)):
    """
    Read JSON/JSONL file. Accepts absolute or relative paths; relative paths are
    normalized to absolute (prefixing '/' on POSIX).
    """
    raw = name if name else path
    if not raw:
        raise HTTPException(400, "Missing name/path")

    # Normalize to absolute, but keep Windows drive letters intact if present
    if not os.path.isabs(raw):
        raw = "/" + raw.lstrip("/")

    p = Path(raw).resolve()
    if not p.exists():
        raise HTTPException(404, f"File not found: {p}")

    ext = p.suffix.lower()
    if ext == ".jsonl":
        data = _safe_jsonl(p)
    elif ext == ".json":
        data = _safe_json(p)
    else:
        return [{"error": f"Unsupported extension: {p.suffix}"}]
    return data

# ---------- Page Routes ----------
@app.get("/")
async def index(request: Request):
    if not templates:
        raise HTTPException(500, "Templates dir missing")
    payload = {"request": request, "roots": [str(p) for p in SEARCH_ROOTS], "host": _host_info()}
    return templates.TemplateResponse("index.html", payload)

@app.get("/browse")
async def browse(request: Request):
    if not templates:
        raise HTTPException(500, "Templates dir missing")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "roots": [str(p) for p in SEARCH_ROOTS],
        "host": _host_info(),
    })

@app.get("/view/{path:path}")
async def view_file_page(request: Request, path: str):
    if not templates:
        raise HTTPException(500, "Templates dir missing")
    return templates.TemplateResponse("file.html", {
        "request": request,
        "name": path,
        "host": _host_info(),
    })

# ---------- Dev Entry ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "jvu:app",
        host=HOST,
        port=PORT,
        reload=RELOAD,
        reload_dirs=RELOAD_DIRS,
    )

