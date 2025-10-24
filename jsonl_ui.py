#!/usr/bin/env python3
"""
Enhanced JSONL UI — browse, view, and infer schema from JSONL datasets.

Upgrades:
- Full recursive directory traversal (rglob)
- Automatic JSON schema inference based on sample structure
- API endpoint: /api/infer_schema?name=<relative_path>
"""

import os
import json
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="JSONL Viewer/Editor")
env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

# --- Utility functions -------------------------------------------------------

from jsonschema import validate as js_validate, ValidationError
import fastjsonschema
import requests

@app.post("/api/validate")
def api_validate(name: str):
    """Validate JSONL file against inferred schema."""
    path = DATA_DIR / name
    rows = read_jsonl(path)
    schema = infer_schema(rows)
    errors = []
    for i, row in enumerate(rows):
        try:
            js_validate(instance=row, schema=schema)
        except ValidationError as e:
            errors.append({"index": i, "error": e.message})
    return JSONResponse({"ok": not errors, "errors": errors, "schema": schema})


@app.post("/api/repair")
def api_repair(name: str):
    """Attempt to fix invalid JSON rows using LLM if configured."""
    base = os.getenv("OPENAI_API_BASE")
    key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    if not (base and key):
        return JSONResponse({"error": "LLM not configured"}, status_code=503)

    path = DATA_DIR / name
    text = path.read_text(encoding="utf-8")
    prompt = f"Fix this JSONL so that every line is valid JSON:\n{text[:6000]}"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    resp = requests.post(f"{base}/chat/completions", headers=headers, json={
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": "You output only valid JSONL text"},
            {"role": "user", "content": prompt}
        ]
    })
    fixed = resp.json()["choices"][0]["message"]["content"]
    fixed_path = path.with_suffix(".fixed.jsonl")
    fixed_path.write_text(fixed, encoding="utf-8")
    return JSONResponse({"ok": True, "fixed_file": str(fixed_path.relative_to(DATA_DIR))})

def list_jsonl_files() -> list[Path]:
    """Return list of all JSONL files recursively under DATA_DIR."""
    if not DATA_DIR.exists():
        return []
    files = []
    for p in DATA_DIR.rglob("*.jsonl"):
        if not str(p).endswith(".jsonl.bak"):
            files.append(p)
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
    except FileNotFoundError:
        pass
    return rows


def infer_schema(rows):
    """Return a JSON Schema guess based on keys/structure of sample rows."""
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


# --- Routes ------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    files = [str(p.relative_to(DATA_DIR)) for p in list_jsonl_files()]
    template = env.get_template("index.html")
    return template.render(request=request, files=files)


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    fpath = DATA_DIR / path
    if not fpath.exists():
        return HTMLResponse(f"<h1>File not found:</h1><pre>{path}</pre>", status_code=404)
    rows = read_jsonl(fpath)
    pretty = json.dumps(rows, indent=2, ensure_ascii=False)
    template = env.get_template("file.html")
    return template.render(request=request, name=path, rows_preview=pretty[:4000])


@app.get("/api/files")
def api_files():
    """Return JSON list of available JSONL files (recursive)."""
    files = [str(p.relative_to(DATA_DIR)) for p in list_jsonl_files()]
    return JSONResponse(files)


@app.get("/api/read")
def api_read(name: str):
    """Return contents of a JSONL file as JSON array."""
    path = DATA_DIR / name
    rows = read_jsonl(path)
    return JSONResponse(rows)


@app.get("/api/infer_schema")
def api_infer_schema(name: str):
    """Infer schema for a given JSONL file."""
    path = DATA_DIR / name
    rows = read_jsonl(path)
    schema = infer_schema(rows)
    return JSONResponse(schema)


@app.get("/download/{path:path}")
def download(path: str):
    """Serve file download."""
    fpath = DATA_DIR / path
    if not fpath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(fpath)


# --- Static mounts -----------------------------------------------------------

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- Run via `uvicorn jsonl_ui:app --reload` --------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("jsonl_ui:app", host="127.0.0.1", port=8501, reload=True)

