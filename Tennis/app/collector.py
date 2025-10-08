# app/collector.py
import asyncio
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from typing import Dict, Any, List, Optional
import logging
import json
import re

logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)


class PlaywrightManager:
    def __init__(self, headless: bool = True):
        self._headless = headless
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._lock = asyncio.Lock()

    async def start(self):
        async with self._lock:
            if self._browser is not None:
                return
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=self._headless,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            self._context = await self._browser.new_context()

    async def new_page(self) -> Page:
        if self._browser is None:
            await self.start()
        page = await self._context.new_page()
        page.set_default_navigation_timeout(30_000)
        page.set_default_timeout(30_000)
        return page

    async def close(self):
        async with self._lock:
            try:
                if self._context:
                    await self._context.close()
                if self._browser:
                    await self._browser.close()
                if self._playwright:
                    await self._playwright.stop()
            finally:
                self._browser = None
                self._context = None
                self._playwright = None


PW_MANAGER: Optional[PlaywrightManager] = None


async def ensure_manager(headless: bool = True) -> PlaywrightManager:
    global PW_MANAGER
    if PW_MANAGER is None:
        PW_MANAGER = PlaywrightManager(headless=headless)
        await PW_MANAGER.start()
    return PW_MANAGER


async def safe_close_page(page: Optional[Page]):
    if page is None:
        return
    try:
        await page.close()
    except Exception:
        pass


async def fetch_live_events_via_page(timeout_sec: int = 20, headless: bool = True) -> Dict[str, Any]:
    api_url = "https://api.sofascore.com/api/v1/sport/tennis/events/live"
    captured = None
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec * 1000)
        captured = await page.evaluate(
            f"""() => fetch("{api_url}")
                .then(r => r.json())
                .catch(() => null)"""
        )
    except Exception as e:
        logger.warning("fetch_live_events_via_page hata: %s", e)
    finally:
        await safe_close_page(page)
    return captured or {"events": []}


async def fetch_all_event_details(event_id: int, endpoints: List[str], timeout_sec: int = 25, headless: bool = True) -> List[Dict[str, Any]]:
    """
    Verilen endpoint listesi için TÜM verileri TEK bir Playwright oturumunda çeker.
    Bu, her endpoint için ayrı ayrı tarayıcı başlatma yükünü ortadan kaldırır.
    """
    base_url = f"https://api.sofascore.com/api/v1/event/{event_id}"
    all_results = []

    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec * 1000)

        for endpoint in endpoints:
            api_url = f"{base_url}/{endpoint}"
            try:
                captured = await page.evaluate(
                    f"""() => fetch("{api_url}")
                        .then(r => r.json())
                        .catch(() => null)"""
                )
                all_results.append(captured or {})
            except Exception as e:
                logger.warning(f"Endpoint '{endpoint}' için evaluate hatası: {e}")
                all_results.append({"error": f"Endpoint fetch failed for {endpoint}"})

    except Exception as e:
        logger.error(f"fetch_all_event_details genel hata (event_id: {event_id}): {e}")
        return [{} for _ in endpoints]
    finally:
        await safe_close_page(page)

    return all_results


async def fetch_player_profile(team_id: int, headless: bool = True):
    url = f"https://www.sofascore.com/api/v1/team/{team_id}"
    text = "{}"
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto(url)
        text = await page.inner_text("pre, body")
    except Exception as e:
        logger.warning("fetch_player_profile hata: %s", e)
    finally:
        await safe_close_page(page)
    return json.loads(text)


async def fetch_player_matches(team_id: int, page: int = 0, headless: bool = True):
    url = f"https://www.sofascore.com/api/v1/team/{team_id}/events/last/{page}"
    text = "{}"
    page_handle: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page_handle = await mgr.new_page()
        await page_handle.goto(url, wait_until="domcontentloaded", timeout=20000)
        text = await page_handle.inner_text("pre, body")
    except Exception as e:
        logger.warning("fetch_player_matches hata: %s", e)
    finally:
        await safe_close_page(page_handle)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.warning(f"JSON parse hatası: {url}")
        return {"events": []}
    all_events = data.get("events", [])
    finished_events = sorted(
        [e for e in all_events if e.get("status", {}).get("type") == "finished" and e.get("winnerCode") in [1, 2]],
        key=lambda e: e.get("startTimestamp", 0),
        reverse=True
    )
    data["events"] = finished_events[:10]
    return data


async def fetch_rankings_via_page(team_id: int, timeout_sec: int = 20, headless: bool = True):
    url = f"https://www.sofascore.com/api/v1/team/{team_id}/rankings"
    data = {"error": "Veri alınamadı."}
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto(url, timeout=timeout_sec * 1000)
        html = await page.content()
        match = re.search(r"<pre.*?>(.*?)</pre>", html, re.S)
        if match:
            data = json.loads(match.group(1))
        else:
            data = {"error": "JSON verisi bulunamadı."}
    except Exception as e:
        logger.warning("fetch_rankings_via_page hata: %s", e)
        data = {"error": str(e)}
    finally:
        await safe_close_page(page)
    return data

async def fetch_scheduled_events_for_dates(dates: List[str], timeout_sec: int = 20, headless: bool = True) -> Dict[str, Any]:
    """Verilen ISO tarih listesi (YYYY-MM-DD) için planlanan tenis maçlarını döndürür.

    Sofascore endpoint: /api/v1/sport/tennis/scheduled-events/{date}
    """
    all_events: List[Dict[str, Any]] = []
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec * 1000)
        for d in dates:
            api_url = f"https://www.sofascore.com/api/v1/sport/tennis/scheduled-events/{d}"
            try:
                captured = await page.evaluate(
                    f"""() => fetch("{api_url}")
                        .then(r => r.json())
                        .catch(() => null)"""
                )
                events = (captured or {}).get("events", [])
                if isinstance(events, list):
                    all_events.extend(events)
            except Exception as e:
                logger.warning("scheduled-events evaluate hata (%s): %s", d, e)
    except Exception as e:
        logger.warning("fetch_scheduled_events_for_dates genel hata: %s", e)
    finally:
        await safe_close_page(page)
    # Aynı event id'leri tekilleştir
    unique = {e.get("id"): e for e in all_events if e and e.get("id")}
    return {"events": list(unique.values())}

async def fetch_year_statistics(team_id: int, year: int, headless: bool = True) -> Dict[str, Any]:
    """Bir oyuncunun belirli bir yıldaki istatistiklerini çeker."""
    url = f"https://www.sofascore.com/api/v1/team/{team_id}/year-statistics/{year}"
    data = {"statistics": []}
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto(url, timeout=20 * 1000)
        content = await page.inner_text("pre, body")
        data = json.loads(content)
    except Exception as e:
        logger.warning(f"fetch_year_statistics (team_id: {team_id}, year: {year}) hata: {e}")
    finally:
        await safe_close_page(page)
    return data


async def fetch_bulk_odds_for_date(date_str: str, timeout_sec: int = 25, headless: bool = True) -> Dict[str, Any]:
    """
    Belirli bir tarih için (YYYY-MM-DD) toplu oranları döndürür.
    API'dan gelen {id: odds_data} formatındaki objeyi, [{id:..., ...}, {...}] formatındaki listeye çevirir.
    """
    api_url = f"https://www.sofascore.com/api/v1/sport/tennis/odds/1/{date_str}"
    page: Optional[Page] = None
    try:
        mgr = await ensure_manager(headless=headless)
        page = await mgr.new_page()
        await page.goto(api_url, wait_until="domcontentloaded", timeout=timeout_sec * 1000)
        content = await page.inner_text("pre, body")

        parsed_json = json.loads(content)
        if isinstance(parsed_json, dict) and "odds" in parsed_json and isinstance(parsed_json["odds"], dict):
            odds_dict = parsed_json["odds"]

            odds_list: List[Dict[str, Any]] = []
            for event_id, odds_data in odds_dict.items():
                odds_data["id"] = int(event_id)
                odds_list.append(odds_data)

            logger.info(f"{date_str} için {len(odds_list)} adet oran verisi başarıyla formatlandı.")
            return {"odds": odds_list}
        else:
            logger.warning(f"Odds verisi beklenen {{\"odds\": {{...}} }} formatında değil: {api_url}")
            return {"odds": []}

    except Exception as e:
        logger.error(f"fetch_bulk_odds_for_date KRİTİK HATA: {e}")
        return {"odds": []}
    finally:
        await safe_close_page(page)
        
