# test_fetch.py
import asyncio
from collector import fetch_live_events_via_page

async def main():
    data = await fetch_live_events_via_page(timeout_sec=25, headless=False)
    print("Event count:", len(data.get("events", [])))
    for e in data.get("events", [])[:3]:
        print(e.get("tournament", {}).get("name"), "-", e.get("homeTeam", {}).get("name"), "vs", e.get("awayTeam", {}).get("name"))

asyncio.run(main())
