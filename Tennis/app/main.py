# app/main.py

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import asyncio
from pathlib import Path
import sys
import requests

try:
    from app.collector import (
        fetch_live_events_via_page, fetch_all_event_details, fetch_player_profile,
        fetch_player_matches, fetch_rankings_via_page
    )
except ImportError:
    from collector import (
        fetch_live_events_via_page, fetch_all_event_details, fetch_player_profile,
        fetch_player_matches, fetch_rankings_via_page
    )

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI(title="Tennis Live Dashboard - MVP")

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

LIVE_CACHE = {"data": {"events": []}, "ts": 0}

async def _get_live_events_cached(ttl: int = 15):
    now = asyncio.get_event_loop().time()
    if (now - LIVE_CACHE["ts"]) < ttl and LIVE_CACHE["data"].get("events"):
        return LIVE_CACHE["data"]
    try:
        data = await fetch_live_events_via_page(timeout_sec=25, headless=True)
    except Exception as e:
        print("fetch_live_events hata:", e)
        return LIVE_CACHE["data"]
    if data and data.get("events"):
        LIVE_CACHE["data"] = data
        LIVE_CACHE["ts"] = now
    return data

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/live-matches")
async def api_live_matches():
    data = await _get_live_events_cached()
    if not data or not data.get("events"):
        return JSONResponse(content={"events": []}, status_code=503)
    return JSONResponse(content=data)

@app.get("/api/match-details/{event_id}")
async def api_match_details(event_id: int):
    """Maç detaylarını (oranlar dahil) tek bir tarayıcı oturumunda verimli bir şekilde çeker."""
    try:
        endpoint_map = {
            "statistics": "statistics",
            "pointByPoint": "point-by-point",
            "tennisPower": "tennis-power",
            "h2h": "h2h",
            "teamStreaks": "team-streaks",
            "votes": "votes",
            "oddsAll": "odds/1/all",
            "winningOdds": "provider/1/winning-odds"
        }
        
        # Collector'daki yeni, optimize edilmiş fonksiyonu çağır
        results = await fetch_all_event_details(event_id, list(endpoint_map.values()))
        
        # Gelen sonuçları anahtarlarla eşleştir
        data = dict(zip(endpoint_map.keys(), results))

        return JSONResponse(content=data)

    except Exception as e:
        print(f"api_match_details (event_id: {event_id}) genel hata:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)

# Diğer endpoint'lerinizde değişiklik yapmanıza gerek yok
@app.get("/api/player/{team_id}")
async def api_player_profile(team_id: int):
    try:
        data = await fetch_player_profile(team_id)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"player_profile (team_id: {team_id}) hata:", e)
        return JSONResponse(content={"error": "Oyuncu profili alınamadı."}, status_code=500)

@app.get("/api/player/{team_id}/matches")
async def api_player_matches(team_id: int, page: int = 0):
    try:
        data = await fetch_player_matches(team_id, page)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"player_matches (team_id: {team_id}) hata:", e)
        return JSONResponse(content={"error": "Oyuncu maçları alınamadı."}, status_code=500)
    
@app.get("/api/player/{team_id}/rankings")
async def get_player_rankings(team_id: int):
    try:
        data = await fetch_rankings_via_page(team_id)
        if "error" in data:
            return JSONResponse(content=data, status_code=404)
        return JSONResponse(content=data)
    except Exception as e:
        print(f"get_player_rankings (team_id: {team_id}) hata:", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/player/{team_id}/active-tournament-stats")
def get_active_tournament_stats(team_id: int):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url_last = f"https://www.sofascore.com/api/v1/team/{team_id}/events/last/0"
        resp_last = requests.get(url_last, headers=headers, timeout=10)
        resp_last.raise_for_status()
        last_data = resp_last.json()
        events = last_data.get("events", [])
        if not events:
            return JSONResponse(content={"error": "Son maç bulunamadı"}, status_code=404)
        active_event = next((ev for ev in events if ev.get("tournament") and ev["tournament"].get("season")), events[0])
        tournament = active_event.get("tournament", {})
        unique_tournament = tournament.get("uniqueTournament", {})
        season = tournament.get("season", {})
        tournament_id = unique_tournament.get("id")
        season_id = season.get("id")
        if not tournament_id or not season_id:
            return JSONResponse(content={"error": "Turnuva veya sezon bilgisi bulunamadı"}, status_code=404)
        url_stats = (f"https://www.sofascore.com/api/v1/team/{team_id}/unique-tournament/"
                     f"{tournament_id}/season/{season_id}/statistics/overall")
        resp_stats = requests.get(url_stats, headers=headers, timeout=10)
        resp_stats.raise_for_status()
        stats_data = resp_stats.json()
        stats_data["tournamentName"] = unique_tournament.get("name", "Bilinmeyen Turnuva")
        stats_data["seasonName"] = season.get("name", "")
        stats_data["tournamentId"] = tournament_id
        stats_data["seasonId"] = season_id
        return JSONResponse(content=stats_data)
    except requests.exceptions.RequestException as e:
        print(f"active_tournament_stats (requests) hata: {e}")
        return JSONResponse(content={"error": "İstatistik sunucusuna ulaşılamadı."}, status_code=503)
    except Exception as e:
        print(f"active_tournament_stats (genel) hata: {e}")
        return JSONResponse(content={"error": "Beklenmedik bir hata oluştu."}, status_code=500)