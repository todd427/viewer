#!/usr/bin/env python3
"""
JVU — JSON/JSONL Viewer (server only; templates/static are separate)

Additions in this revision:
- Host/computer info: /api/host, plus included in /api/health and template payloads.
- Parent helper: /api/parent?path=... returns parent path and the root it belongs to.
- No hard-coded paths; roots/env/config behavior unchanged.
"""
from __future__ import annotations

import os, json, gzip, fnmatch, time, uuid, logging, subprocess, shlex
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------- Optional helpers (.env / YAML) ----------
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    def load_dotenv(*_, **__): return False

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

load_dotenv()

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

# ---------- Resolve config: env → YAML → defaults ----------
BASE_DIR = Path(os.getenv("VIEWER_BASE_DIR", CFG.get("base_dir", Path(__file__).parent))).resolve()
TEMPLATES_DIR = Path(os.getenv("VIEWER_TEMPLATES_DIR", CFG.get("templates_dir", BASE_DIR / "templates"))).resolve()
STATIC_DIR = Path(os.getenv("VIEWER_STATIC_DIR", CFG.get("static_dir", BASE_DIR / "static"))).resolve()

search_roots_raw = os.getenv("VIEWER_SEARCH_ROOTS") or os.pathsep.join(CFG.get("search_roots", [str(BASE_DIR)]))
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

HIDE_EMPTY_DIRS = os.getenv("VIEWER_HIDE_EMPTY_DIRS", "true").lower() == "true"
DIR_PROBE_DEPTH = int(os.getenv("VIEWER_DIR_PROBE_DEPTH", str(CFG.get("dir_probe_depth", 2))))
DIR_PROBE_LIMIT = int(os.getenv("VIEWER_DIR_PROBE_LIMIT", str(CFG.get("dir_probe_limit", 500))))

LOG_LEVEL = os.getenv("VIEWER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("jvu")

# ---------- App ----------
app = FastAPI(title="JVU — JSON/JSONL Viewer")

# Static/templates (do not crash if missing)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    print(f"[jvu] Warning: static dir missing: {STATIC_DIR}")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR)) if TEMPLATES_DIR.exists() else None
if not templates:
    print(f"[jvu] Warning: templates dir missing: {TEMPLATES_DIR}")

# ---------- Host info ----------
def _host_info() -> Dict[str, str]:
    hostname = os.uname().nodename if hasattr(os, "uname") else os.getenv("HOSTNAME", "")
    try:
        out = subprocess.check_output(["uname", "-a"], text=True).strip()
    except Exception:
        out = ""
    return {"hostname": hostname, "uname": out}

# ---------- Middleware: request/response timing ----------
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

def _dir_has_match(root: Path, exts: List[str], max_depth: int, budget: int) -> bool:
    if max_depth < 0 or budget <= 0:
        return False
    try:
        with os.scandir(root) as it:
            for entry in it:
                budget -= 1
                if budget <= 0:
                    return False
                p = Path(entry.path)
                if _prune_dir(p):
                    continue
                if entry.is_file():
                    if not exts or p.suffix.lower() in exts:
                        return True
                elif entry.is_dir(follow_symlinks=False):
                    if _dir_has_match(p, exts, max_depth - 1, budget):
                        return True
    except PermissionError:
        return False
    return False

def iter_files(roots: Iterable[Path], exts: List[str], max_files: int) -> List[str]:
    found: List[str] = []
    exts = _norm_exts(exts)
    for root in roots:
        if _prune_dir(root):
            continue
        for r, dirs, files in os.walk(root, topdown=True):
            dirs[:] = [d for d in dirs if not _prune_dir(Path(r, d))]
            for name in files:
                p = Path(r, name)
                if exts and p.suffix.lower() not in exts:
                    continue
                found.append(str(p))
                if len(found) >= max_files:
                    return found
    return found

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
            if HIDE_EMPTY_DIRS and not _dir_has_match(p, exts, DIR_PROBE_DEPTH, DIR_PROBE_LIMIT):
                continue
            out.append({"type": "dir", "name": p.name, "path": str(p.resolve())})
        else:
            if exts and p.suffix.lower() not in exts:
                continue
            out.append({"type": "file", "name": p.name, "path": str(p.resolve())})
    return out

def read_text_maybe_gz(p: Path, limit_bytes: int = 2_000_000) -> str:
    with open(p, "rb") as f:
        head = f.read(2); f.seek(0)
        data = gzip.open(f, "rb").read(limit_bytes) if head == b"\x1f\x8b" else f.read(limit_bytes)
    try:
        return data.decode("utf-8")
    except Exception:
        return data.decode("utf-8", errors="ignore")

def _normalize_query_path(raw: str) -> Path:
    s = (raw or "").replace("\\", "/")
    if not s:
        return Path("/")
    p = Path(s)
    if not p.is_absolute():
        p = Path("/") / s.lstrip("/")
    return p

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
    """Return which SEARCH_ROOTS entry is an ancestor of 'path' (first match), else None."""
    rp = path.resolve()
    for root in SEARCH_ROOTS:
        try:
            rp.relative_to(root.resolve())
            return root
        except Exception:
            continue
    return None

# ---------- Breadcrumbs ----------
def _breadcrumbs_for(path: Path) -> dict:
    rp = path.resolve()
    owner = _find_owning_root(rp)
    if owner is None:
        # not under any configured root; just show absolute
        parts = rp.parts
        crumbs = []
        accum = Path(parts[0])
        for part in parts[1:]:
            accum = accum / part
            crumbs.append({"label": part, "path": str(accum), "type": "dir"})
        # mark last as file/dir by FS query
        if rp.exists() and rp.is_file():
            crumbs[-1]["type"] = "file"
        return {"root": None, "crumbs": crumbs}

    # Build from the owning root downward
    rel = rp.relative_to(owner.resolve())
    crumbs = []
    accum = owner.resolve()
    # Root entry
    crumbs.append({"label": str(owner), "path": str(owner.resolve()), "type": "root"})
    # Each segment under the root
    for seg in rel.parts:
        accum = accum / seg
        crumbs.append({
            "label": seg,
            "path": str(accum),
            "type": ("file" if accum.exists() and accum.is_file() else "dir")
        })
    return {"root": str(owner.resolve()), "crumbs": crumbs}

@app.get("/api/breadcrumbs")
def api_breadcrumbs(path: str):
    """
    Return breadcrumb list for an absolute file or directory path.
    Response: { root: <root-or-null>, crumbs: [{label, path, type}] }
    """
    p = Path(path)
    if not p.is_absolute():
        p = Path("/") / path.lstrip("/")
    return _breadcrumbs_for(p)


# ---------- API Routes ----------
@app.get("/api/host")
def api_host():
    return _host_info()

@app.get("/api/health")
def health():
    h = _host_info()
    return {
        "ok": True,
        "host": h,
        "base_dir": str(BASE_DIR),
        "templates": str(TEMPLATES_DIR),
        "static": str(STATIC_DIR),
        "search_roots": [str(p) for p in SEARCH_ROOTS],
        "skip_dirs": sorted(SKIP_DIRS),
        "prune_globs": PRUNE_GLOBS,
        "hide_empty_dirs": HIDE_EMPTY_DIRS,
        "dir_probe_depth": DIR_PROBE_DEPTH,
        "dir_probe_limit": DIR_PROBE_LIMIT,
        "max_files": MAX_FILES,
        "log_level": LOG_LEVEL,
    }

@app.get("/api/files")
def api_files(
    ext: Optional[str] = Query("json,jsonl", description="Comma-separated extensions"),
    max_files: int = Query(None, description="Cap results; defaults to VIEWER_MAX_FILES"),
):
    exts = [e.strip() for e in (ext.split(",") if ext else []) if e.strip()]
    limit = max_files or MAX_FILES
    logger.debug("/api/files roots=%s exts=%s limit=%s", [str(p) for p in SEARCH_ROOTS], exts, limit)
    files = iter_files(SEARCH_ROOTS, exts, limit)
    logger.info("/api/files -> %d files", len(files))
    return {"count": len(files), "files": files}

@app.get("/api/tree")
def api_tree(
    path: Optional[str] = Query(None, description="Directory path to list"),
    ext: Optional[str] = Query("json,jsonl", description="Comma-separated extensions"),
):
    exts = [e.strip() for e in (ext.split(",") if ext else []) if e.strip()]
    if path:
        p = Path(path)
        if not p.exists() or not p.is_dir():
            logger.warning("/api/tree path not dir or missing: %s", path)
            raise HTTPException(404, f"Not a directory: {path}")
        logger.debug("/api/tree list path=%s exts=%s", p, exts)
        nodes = iter_tree(p, exts)
        logger.info("/api/tree path=%s -> %d nodes", p, len(nodes))
        return {"path": str(p.resolve()), "nodes": nodes}
    roots = [str(r) for r in SEARCH_ROOTS if r.exists()]
    logger.info("/api/tree roots -> %d roots", len(roots))
    return {"path": None, "roots": roots}

@app.get("/api/parent")
def api_parent(path: str = Query(..., description="Absolute file or directory path")):
    """Return parent path for a given path, plus the owning root."""
    p = Path(path).resolve()
    if not p.exists():
        raise HTTPException(404, f"Not found: {path}")
    owner = _find_owning_root(p)
    if p.is_file():
        parent = p.parent
    else:
        parent = p.parent
    # Stop at owner root: if parent would escape, we signal None
    if owner is not None:
        try:
            parent.resolve().relative_to(owner.resolve())
        except Exception:
            parent = None
    return {
        "path": str(p),
        "parent": (str(parent) if parent else None),
        "root": (str(owner) if owner else None),
    }

@app.get("/api/head")
def api_head(path: str = Query(..., description="Absolute path to file"), bytes: int = 10000):
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise HTTPException(404, f"Not found: {path}")
    text = read_text_maybe_gz(p, limit_bytes=bytes)
    return PlainTextResponse(text)

# ---------- Back-compat: /api/read (accepts name= or path=) ----------
@app.get("/api/read")
def api_read(name: Optional[str] = Query(None), path: Optional[str] = Query(None)):
    raw = name if name is not None else path
    if not raw:
        raise HTTPException(400, "Missing 'name' or 'path' query parameter.")
    p = _normalize_query_path(raw)
    logger.info("/api/read %s", p)
    if not p.exists():
        return [{"error": f"File not found: {p}"}]
    if p.is_dir():
        try:
            items = [child.name for child in p.iterdir()]
        except Exception as e:
            items = [f"<error: {e}>"]
        return [{"directory": str(p), "items": items}]
    if p.suffix.lower() == ".jsonl":
        data = _safe_jsonl(p)
    elif p.suffix.lower() == ".json":
        data = _safe_json(p)
    else:
        return [{"error": f"Unsupported extension: {p.suffix}"}]
    summary = "No records found."
    if data:
        first = data[0]
        if isinstance(first, dict):
            keys = ", ".join(first.keys())
            roles = set()
            if "messages" in first and isinstance(first["messages"], list):
                for m in first["messages"]:
                    r = (m or {}).get("role")
                    if r: roles.add(r)
            summary = f"Top-level keys: {keys}"
            if roles: summary += f" | Message roles: {', '.join(sorted(roles))}"
        else:
            summary = f"Sample type: {type(first).__name__}"
    return [{"_schema_summary": summary}, *data]

@app.get("/api/file")
def api_file(path: str = Query(...)):
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise HTTPException(404, f"Not found: {path}")
    return FileResponse(str(p))

# ---------- Page routes (use split templates) ----------
@app.get("/")
async def index(request: Request):
    if not templates:
        raise HTTPException(500, f"Templates directory missing: {TEMPLATES_DIR}")
    payload = {
        "request": request,
        "roots": [str(p) for p in SEARCH_ROOTS],
        "host": _host_info(),  # <— show in header in your template
    }
    logger.debug("render / with roots=%s", payload["roots"])
    return templates.TemplateResponse("index.html", payload)

@app.get("/view/{path:path}")
async def view_file_page(request: Request, path: str):
    if not templates:
        raise HTTPException(500, f"Templates directory missing: {TEMPLATES_DIR}")
    return templates.TemplateResponse("file.html", {
        "request": request,
        "name": path,
        "host": _host_info(),  # <— include here too
    })

# ---------- Dev entry ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "jvu:app",
        host=HOST,
        port=PORT,
        reload=RELOAD,
        reload_dirs=RELOAD_DIRS,
    )

