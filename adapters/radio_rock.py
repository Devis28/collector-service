import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS = "wss://rock-server.fly.dev/ws/push/listenership"

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()
        raw_valid = (
            "last_update" in data and
            "song" in data and
            all(k in data["song"] for k in [
                "musicAuthor", "musicCover", "musicTitle", "radio", "startTime"
            ])
        )
        session_id = str(uuid.uuid4())
        return {
            **data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": raw_valid,
            "song_session_id": session_id
        }
    except Exception:
        return {
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def get_current_listeners(session_id=None):
    uri = LISTENERS_WS
    listeners_data = None
    async with websockets.connect(uri) as websocket:
        try:
            listeners_msg = None
            for _ in range(10):  # 10 pokusov po 3 sekundy = max 30 sekúnd čakanie
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=3)
                except asyncio.TimeoutError:
                    continue
                data = json.loads(raw)
                if data.get("type") == "listeners_update":
                    listeners_msg = data
                    break
            if listeners_msg:
                listeners = listeners_msg.get("listeners")
                last_update = listeners_msg.get("last_update")
                raw_valid = listeners is not None and last_update is not None
                listeners_data = {
                    "listeners": listeners,
                    "last_update": last_update,
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": raw_valid,
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
