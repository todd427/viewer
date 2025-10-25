from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import json
import os

ROOT_PATH = Path("/")  # start from root
PROJECT_ROOT = Path("/home/Projects/toddric")
TEMPLATES_DIR = PROJECT_ROOT / "viewer" / "templates"

app = FastAPI()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(PROJECT_ROOT / "viewer" / "static")), name="static")

SKIP_DIRS = {
    "/proc", "/sys", "/dev", "/run", "/snap", "/tmp",
    "/lost+found", "/var/lib/docker", "/var/lib/containers"
}


def safe_jsonl_load(path: Path, limit: int = 500):
    records = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    records.append({"raw": line})
                if i >= limit:
                    break
    except Exception as e:
        records.append({"error": str(e)})
    return records


def safe_json_load(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        return [{"error": str(e)}]


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/files")
async def api_files():
    exts = {".json", ".jsonl"}
    files = []

    for root, dirs, filenames in os.walk(ROOT_PATH):
        dirs[:] = [d for d in dirs if os.path.join(root, d) not in SKIP_DIRS]
        try:
            for name in filenames:
                if Path(name).suffix.lower() in exts:
                    full = Path(root) / name
                    files.append(str(full))
        except (PermissionError, OSError):
            continue

    return {"files": files}


@app.get("/api/read")
async def api_read(name: str):
    fpath = Path("/") / name.lstrip("/")
    if not fpath.exists():
        return JSONResponse({"error": f"File not found: {fpath}"}, status_code=404)
    if fpath.is_dir():
        return {"directory": str(fpath), "items": [str(p.name) for p in fpath.iterdir()]}

    # Read based on extension
    if fpath.suffix == ".jsonl":
        data = safe_jsonl_load(fpath)
    elif fpath.suffix == ".json":
        data = safe_json_load(fpath)
    else:
        return JSONResponse({"error": f"Unsupported file type: {fpath.suffix}"}, status_code=400)

    # Return only the record list so file.html gets what it expects
    return JSONResponse(data)


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    fpath = Path("/") / path
    if not fpath.exists():
        return HTMLResponse(
            f"<h2 style='color:red'>File not found:</h2><pre>{fpath}</pre>",
            status_code=404,
        )
    return templates.TemplateResponse("file.html", {"request": request, "name": str(fpath)})


if __name__ == "__main__":
    import uvicorn
    print(">> JSONL Viewer running from /")
    uvicorn.run(app, host="127.0.0.1", port=8002)
