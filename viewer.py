from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import json

app = FastAPI()
BASE_DIR = Path.home()
templates = Jinja2Templates(directory="templates")

# Mount static directory if you decide to add custom CSS or JS
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home route showing all files."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/files")
async def list_files():
    """List JSON/JSONL files recursively under ~/Projects."""
    project_root = BASE_DIR / "Projects"
    results = []
    for path in project_root.rglob("*.json*"):
        try:
            rel = path.relative_to(BASE_DIR)
        except ValueError:
            rel = path
        results.append(str(rel))
    return JSONResponse(results)


def load_jsonl(path: Path, limit: int = 100):
    """Load a JSONL file safely (absolute path enforced)."""
    if not path.is_absolute():
        path = Path("/") / path  # ensure leading slash
    lines = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if line.strip():
                    try:
                        lines.append(json.loads(line))
                    except json.JSONDecodeError:
                        lines.append({"raw": line.strip()})
                if i >= limit:
                    break
    except FileNotFoundError:
        return None
    return lines


@app.get("/view/{path:path}", response_class=HTMLResponse)
async def view_file(request: Request, path: str):
    """Render the file viewer page for the selected JSONL."""
    # Guarantee leading slash for full absolute path
    if not path.startswith("/"):
        path = "/" + path
    fpath = Path(path)
    print(f">> view_file: {fpath}")

    lines = load_jsonl(fpath)
    if not lines:
        return HTMLResponse(
            f"<h2 style='color:red'>File not found or empty:</h2><pre>{path}</pre>",
            status_code=404,
        )
    return templates.TemplateResponse(
        "file.html",
        {"request": request, "path": str(fpath), "lines": lines, "total": len(lines)},
    )


@app.get("/api/read")
async def read_file(name: str):
    """Return JSONL contents as JSON."""
    fpath = BASE_DIR / name
    print(f">> /api/read {fpath}")
    lines = load_jsonl(fpath)
    if lines is None:
        return JSONResponse({"error": "File not found"}, status_code=404)
    return JSONResponse(lines)


def infer_schema_name(schema_keys):
    """Heuristic schema type detection based on field names."""
    keys = set(k.lower() for k in schema_keys)
    if {"prompt", "completion"} <= keys:
        return "OpenAI-SFT Schema"
    if {"prompt", "response"} <= keys:
        return "Alpaca-Style Schema"
    if {"chosen", "rejected"} <= keys:
        return "Preference (DPO) Schema"
    if {"system", "messages"} <= keys:
        return "ChatML / Conversation Schema"
    if {"input", "output"} <= keys:
        return "Generic IO Schema"
    return "Unknown / Custom Schema"


@app.get("/api/schema")
async def schema_info(name: str):
    """Scan a JSONL/JSON file to infer schema and type coverage."""
    # Ensure full absolute path
    if not name.startswith("/"):
        name = "/" + name
    fpath = Path(name)
    print(f">> /api/schema {fpath}")

    try:
        with open(fpath, "r", encoding="utf-8") as f:
            lines = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    lines.append(json.loads(line))
                except json.JSONDecodeError:
                    # Try to parse as a full JSON array (not JSONL)
                    f.seek(0)
                    try:
                        data = json.load(f)
                        if isinstance(data, list):
                            lines = data
                        else:
                            lines = [data]
                    except Exception as e:
                        return JSONResponse(
                            {"error": f"JSON parse error: {e}"}, status_code=400
                        )
                    break
    except FileNotFoundError:
        return JSONResponse({"error": f"File not found: {fpath}"}, status_code=404)

    total = len(lines)
    if not total:
        return JSONResponse({"error": f"Empty file: {fpath}"})

    key_stats = {}
    for obj in lines:
        if not isinstance(obj, dict):
            continue
        for k, v in obj.items():
            t = type(v).__name__
            if k not in key_stats:
                key_stats[k] = {"types": {}, "count": 0}
            key_stats[k]["count"] += 1
            key_stats[k]["types"][t] = key_stats[k]["types"].get(t, 0) + 1

    schema = [
        {
            "key": k,
            "types": info["types"],
            "coverage": round((info["count"] / total) * 100, 1),
        }
        for k, info in key_stats.items()
    ]

    schema_name = infer_schema_name([s["key"] for s in schema])
    return {
        "records_scanned": total,
        "schema_name": schema_name,
        "schema": schema,
    }


# Run with: uvicorn viewer:app --port 8002 --reload
if __name__ == "__main__":
    import uvicorn

    print(">> Starting JSONL Viewer on http://127.0.0.1:8002")
    uvicorn.run(app, host="127.0.0.1", port=8002)
