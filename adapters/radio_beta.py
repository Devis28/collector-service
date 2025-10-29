import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://beta-server.fly.dev/song"  # nahraď reálnym endpointom
LISTENERS_WS = "wss://beta-server.fly.dev/ws/listeners"  # nahraď reálnym endpointom

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        title = data.get("title")
        artist = data.get("artist")
        raw_valid = title is not None and artist is not None
        session_id = str(uuid.uuid4())
        return {
            "data": data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception:
        return {
            "data": {},
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def get_current_listeners(session_id=None):
    uri = LISTENERS_WS
    listeners_data = None
    async with websockets.connect(uri) as websocket:
        try:
            latest = None
            # 18 pokusov * 2.5s = max 45 sekúnd čakanie
            for _ in range(18):
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=2.5)
                except asyncio.TimeoutError:
                    continue
                data = json.loads(raw)
                if "listeners" in data:
                    latest = data["listeners"]
                    break  # zachytí prvé listeners, ukončí cyklus
            if latest is not None:
                listeners_data = {
                    "listeners": latest,
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": True,
                    "song_session_id": session_id
                }
            else:
                listeners_data = {
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": False,
                    "song_session_id": session_id
                }
        except Exception:
            listeners_data = {
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": False,
                "song_session_id": session_id
            }
    return listeners_data
