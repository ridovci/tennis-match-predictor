# app/collector.py
import asyncio
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from typing import Dict, Any, Optional
import logging

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
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            self._context = await self._browser.new_context()

    async def new_page(self) -> Page:
        if self._browser is None:
            await self.start()
        page = await self._context.new_page()
        page.set_default_navigation_timeout(30000)
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

async def ensure_manager(headless: bool = True):
    global PW_MANAGER
    if PW_MANAGER is None:
        PW_MANAGER = PlaywrightManager(headless=headless)
        await PW_MANAGER.start()
    return PW_MANAGER


async def fetch_live_events_via_page(timeout_sec: int = 15, headless: bool = True) -> Dict[str, Any]:
    api_url = "https://api.sofascore.com/api/v1/sport/tennis/events/live"
    mgr = await ensure_manager(headless=headless)
    page = await mgr.new_page()

    captured = None
    try:
        await page.goto("https://www.sofascore.com/tr/tenis", wait_until="domcontentloaded", timeout=timeout_sec*1000)

        captured = await page.evaluate(
            f"""() => fetch("{api_url}")
                .then(r => r.json())
                .catch(() => null)"""
        )

    except Exception as e:
        logger.warning("fetch_live_events_via_page hata: %s", e)
    finally:
        try:
            await page.close()
        except Exception:
            pass

    if not captured:
        return {"events": []}
    return captured
