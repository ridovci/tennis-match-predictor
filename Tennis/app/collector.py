# app/collector.py
import asyncio
from playwright.async_api import async_playwright
from typing import Dict, Any, List
import logging
import json
import re

logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)


async def fetch_live_events_via_page(timeout_sec: int = 20, headless: bool = True) -> Dict[str, Any]:
    api_url = "https://api.sofascore.com/api/v1/sport/tennis/events/live"
    captured = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec * 1000)
            captured = await page.evaluate(
                f"""() => fetch("{api_url}")
                    .then(r => r.json())
                    .catch(() => null)"""
            )
            await browser.close()
    except Exception as e:
        logger.warning("fetch_live_events_via_page hata: %s", e)
    return captured or {"events": []}


async def fetch_all_event_details(event_id: int, endpoints: List[str], timeout_sec: int = 25, headless: bool = True) -> List[Dict[str, Any]]:
    """
    Verilen endpoint listesi için TÜM verileri TEK bir Playwright oturumunda çeker.
    Bu, her endpoint için ayrı ayrı tarayıcı başlatma yükünü ortadan kaldırır.
    """
    base_url = f"https://api.sofascore.com/api/v1/event/{event_id}"
    all_results = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            # Oturum cookielerini ve state'i ayarlamak için ana sayfaya bir kez git
            await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec * 1000)

            # Tüm endpoint'leri aynı sayfa üzerinden paralel olarak sorgula
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
            
            await browser.close()

    except Exception as e:
        logger.error(f"fetch_all_event_details genel hata (event_id: {event_id}): {e}")
        # Hata durumunda, her endpoint için boş bir sonuç döndür
        return [{} for _ in endpoints]

    return all_results


async def fetch_player_profile(team_id: int, headless: bool = True):
    url = f"https://www.sofascore.com/api/v1/team/{team_id}"
    text = "{}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            await page.goto(url)
            text = await page.inner_text("pre, body")
            await browser.close()
    except Exception as e:
        logger.warning("fetch_player_profile hata: %s", e)
    return json.loads(text)


async def fetch_player_matches(team_id: int, page: int = 0, headless: bool = True):
    url = f"https://www.sofascore.com/api/v1/team/{team_id}/events/last/{page}"
    text = "{}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page_ = await browser.new_page()
            await page_.goto(url, wait_until="domcontentloaded", timeout=20000)
            text = await page_.inner_text("pre, body")
            await browser.close()
    except Exception as e:
        logger.warning("fetch_player_matches hata: %s", e)
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
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            page = await browser.new_page()
            await page.goto(url, timeout=timeout_sec * 1000)
            html = await page.content()
            await browser.close()
            match = re.search(r"<pre.*?>(.*?)</pre>", html, re.S)
            if match:
                data = json.loads(match.group(1))
            else:
                data = {"error": "JSON verisi bulunamadı."}
    except Exception as e:
        logger.warning("fetch_rankings_via_page hata: %s", e)
        data = {"error": str(e)}
    return data