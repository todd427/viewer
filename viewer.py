from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path
import json
import os

# -------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------
BASE_DIR = Path("/home/todd/viewer")
TEMPLATES_DIR = BASE_DIR / "templates"

app = FastAPI()
from fastapi.staticfiles import StaticFiles
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Directories we don’t want to traverse from /
SKIP_DIRS = {
    "/proc", "/sys", "/dev", "/run", "/snap", "/tmp",
    "/lost+found", "/var/lib/docker", "/var/lib/containers"
}

# -------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------
def safe_jsonl_load(path: Path, limit: int = 5000):
    """Load JSONL safely with fallback for broken lines."""
    out = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    out.append({"raw": line})
                if i >= limit:
                    break
    except Exception as e:
        out.append({"error": str(e)})
    return out


def safe_json_load(path: Path):
    """Load plain JSON safely."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        return [{"error": str(e)}]


def summarize_schema(records):
    """Generate a readable schema summary."""
    if not records:
        return "No records found."

    first = records[0]
    if isinstance(first, dict):
        keys = list(first.keys())
        roles = set()
        if "messages" in first and isinstance(first["messages"], list):
            for m in first["messages"]:
                role = m.get("role")
                if role:
                    roles.add(role)
        text = f"Top-level keys: {', '.join(keys)}"
        if roles:
            text += f" | Message roles: {', '.join(sorted(roles))}"
        return text
    return f"Sample type: {type(first).__name__}"


# -------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    """Render the file.html template for a given path."""
    print(f">> /view/{path}")
    return templates.TemplateResponse("file.html", {"request": request, "name": f"/{path}"})


@app.get("/api/files")
async def api_files():
    """Recursively list all JSON and JSONL files starting at root, skipping system dirs."""
    files = []
    for root, dirs, names in os.walk("/", topdown=True):
        dirs[:] = [d for d in dirs if os.path.join(root, d) not in SKIP_DIRS]
        for n in names:
            if n.endswith(".jsonl") or n.endswith(".json"):
                files.append(os.path.join(root, n))
    return {"files": files}


@app.get("/api/read")
async def api_read(name: str):
    """Return file contents as a JSON array (no hang, guaranteed)."""
    path = Path("/") / name.lstrip("/")
    print(f">> /api/read received: {path}")

    if not path.exists():
        return [{"error": f"File not found: {path}"}]

    if path.is_dir():
        return [{"directory": str(path), "items": [p.name for p in path.iterdir()]}]

    # Load the file
    if path.suffix == ".jsonl":
        data = safe_jsonl_load(path)
    elif path.suffix == ".json":
        data = safe_json_load(path)
    else:
        data = [{"error": f"Unsupported extension: {path.suffix}"}]

    summary = summarize_schema(data)
    return [{"_schema_summary": summary}, *data]

# -------------------------------------------------------------
# MAIN ENTRY (for development)
# -------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    print("Viewer running on http://127.0.0.1:8002")
    uvicorn.run(
        "viewer:app",
        host="127.0.0.1",
        port=8002,
        reload=True,
        reload_dirs=[
            "/home/Projects/toddric/viewer",
            "/home/Projects/toddric/viewer/templates"
        ]
    )

