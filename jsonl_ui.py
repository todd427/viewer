#!/usr/bin/env python3
"""
jsonl_ui.py – JSONL Viewer/Editor with safe full-directory traversal.

Features:
  • Full recursive search from `/`, with safe skips for system dirs
  • JSONL file listing, viewing, and schema inference
  • Optional schema validation
  • Jinja2 templates for index + file pages
"""

import os
import json
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
from jsonschema import validate as js_validate, ValidationError

# --- Configuration ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Directories to start scanning from:
ROOT_DIRS = ["/"]  # change to ['/home', '/mnt', '/data'] for speed

# Directories to skip completely:
SKIP_DIRS = {
    "/proc", "/sys", "/dev", "/run", "/snap",
    "/tmp", "/.Trash", "/lost+found", "/var/lib/docker"
}

# -----------------------------------------------------------------------------

app = FastAPI(title="JSONL Viewer/Editor")
env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

# --- Utility functions --------------------------------------------------------


def list_jsonl_files() -> list[Path]:
    """Walk entire filesystem from ROOT_DIRS safely, skipping unreadable paths."""
    files = []
    for root in ROOT_DIRS:
        for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            # prune directories we shouldn't touch
            dirnames[:] = [
                d for d in dirnames
                if not any((Path(dirpath) / d).as_posix().startswith(sd) for sd in SKIP_DIRS)
            ]
            for f in filenames:
                if f.endswith(".jsonl"):
                    full = Path(dirpath) / f
                    try:
                        if full.is_file():
                            files.append(full)
                    except (PermissionError, OSError):
                        continue
    return sorted(files)


def read_jsonl(path: Path) -> list:
    """Read JSONL file into a list of JSON objects."""
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    rows.append({"_error": str(e), "_raw": line})
    except (FileNotFoundError, PermissionError) as e:
        rows.append({"_error": str(e)})
    return rows


def infer_schema(rows):
    """Infer a simple JSON Schema from first record."""
    if not rows:
        return {"type": "object"}

    sample = rows[0]
    if isinstance(sample, dict):
        if "messages" in sample and isinstance(sample["messages"], list):
            return {
                "type": "object",
                "properties": {
                    "messages": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "role": {"type": "string"},
                                "content": {"type": "string"},
                            },
                            "required": ["role", "content"],
                        },
                    }
                },
                "required": ["messages"],
            }
        elif {"input", "output"} <= set(sample.keys()):
            return {
                "type": "object",
                "properties": {
                    "input": {"type": "string"},
                    "output": {"type": "string"},
                    "meta": {"type": "object"},
                },
                "required": ["input", "output"],
            }
        else:
            props = {}
            for k, v in sample.items():
                t = type(v).__name__
                if t == "dict":
                    props[k] = {"type": "object"}
                elif t == "list":
                    props[k] = {"type": "array"}
                elif t in ("str", "string"):
                    props[k] = {"type": "string"}
                elif t in ("int", "float"):
                    props[k] = {"type": "number"}
                elif t == "bool":
                    props[k] = {"type": "boolean"}
                else:
                    props[k] = {"type": "string"}
            return {"type": "object", "properties": props}
    elif isinstance(sample, list):
        return {"type": "array"}
    else:
        return {"type": type(sample).__name__}


# --- Routes -------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    files = [str(p) for p in list_jsonl_files()]
    template = env.get_template("index.html")
    return template.render(request=request, files=files)


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    fpath = Path(path)
    if not fpath.exists():
        return HTMLResponse(f"<h1>File not found:</h1><pre>{path}</pre>", status_code=404)
    template = env.get_template("file.html")
    return template.render(request=request, name=path, rows_preview=pretty)


@app.get("/api/files")
def api_files():
    """Return list of all JSONL files with absolute paths (always starting with /)."""
    files = []
    for p in list_jsonl_files():
        # ensure leading slash for browser consistency
        full = p.as_posix()
        if not full.startswith("/"):
            full = "/" + full
        files.append(full)
    return JSONResponse(files)


@app.get("/api/read")
def api_read(name: str):
    """Return contents of a JSONL file as JSON array."""
    # Guarantee we have an absolute path
    if not name.startswith("/"):
        name = "/" + name
    path = Path(name)
    rows = read_jsonl(path)
    return JSONResponse(rows)

@app.get("/api/infer_schema")
def api_infer_schema(name: str):
    """Infer schema for a given JSONL file."""
    path = Path(name)
    rows = read_jsonl(path)
    schema = infer_schema(rows)
    return JSONResponse(schema)


@app.post("/api/validate")
def api_validate(name: str):
    """Validate JSONL file against its inferred schema."""
    path = Path(name)
    rows = read_jsonl(path)
    schema = infer_schema(rows)
    errors = []
    for i, row in enumerate(rows):
        try:
            js_validate(instance=row, schema=schema)
        except ValidationError as e:
            errors.append({"index": i, "error": e.message})
    return JSONResponse({"ok": not errors, "errors": errors, "schema": schema})


@app.get("/download/{path:path}")
def download(path: str):
    """Serve a file download."""
    fpath = Path(path)
    if not fpath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(fpath)


# --- Static mounts ------------------------------------------------------------

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Entrypoint ---------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("jsonl_ui:app", host="127.0.0.1", port=8501, reload=True)
