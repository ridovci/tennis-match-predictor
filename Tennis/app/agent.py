import asyncio
from datetime import datetime, timedelta
from typing import List

from app.tgs_calculator import get_match_prediction
from app.collector import fetch_scheduled_events_for_dates
from app.pred_store import read_predictions, write_predictions


def _time_offsets_minutes() -> List[int]:
    return [120, 60, 30, 10, 5]


async def _list_upcoming_event_ids_for_today() -> List[int]:
    today = datetime.now().date()
    dates = [today.strftime("%Y-%m-%d")]
    data = await fetch_scheduled_events_for_dates(dates)
    events = data.get("events", []) if data else []
    now_ts = int(datetime.now().timestamp())
    upcoming = [e for e in events if (e.get("status", {}).get("type") in ("notstarted", "scheduled")) and (e.get("startTimestamp") or 0) > now_ts]
    return [e.get("id") for e in upcoming if e.get("id")]


def _should_run_now(event_ts: int, now: datetime, offset_min: int) -> bool:
    target = datetime.fromtimestamp(event_ts) - timedelta(minutes=offset_min)
    # within 30 seconds window
    delta = abs((now - target).total_seconds())
    return delta <= 30


async def _compute_and_store(event_id: int):
    pred = await get_match_prediction(event_id)
    if "error" in pred:
        return False
    date_str = datetime.now().strftime("%Y-%m-%d")
    all_preds = read_predictions(date_str)
    all_preds[str(event_id)] = pred
    write_predictions(date_str, all_preds)
    return True


async def run_agent_loop(poll_seconds: int = 30, parallelism: int = 4):
    sem = asyncio.Semaphore(parallelism)
    offsets = _time_offsets_minutes()
    while True:
        try:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            data = await fetch_scheduled_events_for_dates([today])
            events = data.get("events", []) if data else []
            tasks = []
            for e in events:
                eid = e.get("id")
                ts = e.get("startTimestamp") or 0
                if not eid or not ts:
                    continue
                # Decide if any offset should trigger now
                if any(_should_run_now(ts, now, off) for off in offsets):
                    async def _task(eid=eid):
                        async with sem:
                            await _compute_and_store(eid)
                    tasks.append(asyncio.create_task(_task()))
            if tasks:
                await asyncio.gather(*tasks)
        except Exception as e:
            # Log and continue
            print("agent loop error:", e)
        await asyncio.sleep(poll_seconds)


