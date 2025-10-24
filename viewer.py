#!/usr/bin/env python3
"""
Restored JSONL Viewer/Editor — full traversal, schema inference, working templates.
"""

import os
import json
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
from jsonschema import validate as js_validate, ValidationError

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

ROOT_DIRS = ["/"]
SKIP_DIRS = {
    "/proc", "/sys", "/dev", "/run", "/snap", "/tmp",
    "/.Trash", "/lost+found", "/var/lib/docker"
}

app = FastAPI(title="JSONL Viewer/Editor")
env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
env.cache = {}



# --- UTILITIES ----------------------------------------------------------------
def list_jsonl_files() -> list[Path]:
    files = []
    for root in ROOT_DIRS:
        for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            dirnames[:] = [
                d for d in dirnames
                if not any((Path(dirpath) / d).as_posix().startswith(sd) for sd in SKIP_DIRS)
            ]
            for f in filenames:
                if f.endswith(".jsonl"):
                    p = Path(dirpath) / f
                    try:
                        if p.is_file():
                            files.append(p)
                    except (PermissionError, OSError):
                        continue
    return sorted(files)


def read_jsonl(path: Path) -> list:
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        rows.append({"_error": str(e), "_raw": line})
    except (FileNotFoundError, PermissionError) as e:
        rows.append({"_error": str(e)})
    return rows


def infer_schema(rows):
    if not rows:
        return {"type": "object"}
    sample = rows[0]
    if isinstance(sample, dict):
        props = {k: {"type": type(v).__name__} for k, v in sample.items()}
        return {"type": "object", "properties": props}
    elif isinstance(sample, list):
        return {"type": "array"}
    return {"type": type(sample).__name__}


# --- ROUTES -------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    files = [str(p) for p in list_jsonl_files()]
    template = env.get_template("index.html")
    return template.render(request=request, files=files)


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    """Serve the grid view template directly."""
    # Normalize to absolute path
    if not path.startswith("/"):
        path = "/" + path

    fpath = Path(path)

    if not fpath.exists():
        return HTMLResponse(f"<h1>File not found:</h1><pre>{path}</pre>", status_code=404)
    template = env.get_template("file.html")

    return template.render(request=request, name=str(fpath))


@app.get("/api/files")
def api_files():
    files = []
    for p in list_jsonl_files():
        full = p.as_posix()
        if not full.startswith("/"):
            full = "/" + full
        files.append(full)
    return JSONResponse(files)


@app.get("/api/read")
def api_read(name: str):
    if not name.startswith("/"):
        name = "/" + name
    path = Path(name)
    rows = read_jsonl(path)
    return JSONResponse(rows)


@app.get("/api/infer_schema")
def api_infer_schema(name: str):
    if not name.startswith("/"):
        name = "/" + name
    rows = read_jsonl(Path(name))
    schema = infer_schema(rows)
    return JSONResponse(schema)


@app.post("/api/save")
async def api_save(request: Request):
    """Write JSONL back to disk."""
    body = await request.json()
    path = Path(body.get("name"))
    data = body.get("data")
    if not path.exists():
        return JSONResponse({"error": f"File not found: {path}"}, status_code=404)
    try:
        with open(path, "w", encoding="utf-8") as f:
            for row in data:
                json.dump(row, f, ensure_ascii=False)
                f.write("\n")
        return JSONResponse({"ok": True, "records": len(data)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/validate")
def api_validate(name: str):
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
    fpath = Path(path)
    if not fpath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(fpath)


# --- STATIC -------------------------------------------------------------------
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- ENTRYPOINT ---------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("viewer:app", host="127.0.0.1", port=8002, reload=True)

