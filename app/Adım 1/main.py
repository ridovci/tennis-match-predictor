# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import asyncio
from pathlib import Path
from app.collector import fetch_live_events_via_page
import sys

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="Tennis Live Dashboard - MVP")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

LIVE_CACHE = {"data": {"events": []}, "ts": 0, "ttl": 10}

async def _get_live_events_cached(ttl: int = 10):
    now = asyncio.get_event_loop().time()
    if now - LIVE_CACHE["ts"] < ttl and LIVE_CACHE["data"]:
        return LIVE_CACHE["data"]
    try:
        data = await fetch_live_events_via_page(timeout_sec=20, headless=True)
    except Exception as e:
        print("fetch_live_events hata:", e)
        return LIVE_CACHE["data"]
    LIVE_CACHE["data"] = data
    LIVE_CACHE["ts"] = now
    return data

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/live-matches")
async def api_live_matches():
    try:
        data = await _get_live_events_cached()
    except Exception:
        return JSONResponse(content={"events": []})
    return JSONResponse(content=data)
