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
    except Exception as e:
        return {
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }

async def get_current_listeners(session_id=None):
    listeners_data = None
    uri = LISTENERS_WS
    async with websockets.connect(uri) as websocket:
        while True:
            try:
                msg = await asyncio.wait_for(websocket.recv(), timeout=10)
                data = json.loads(msg)
                if data.get("type") == "listeners_update":
                    raw_valid = "listeners" in data and "last_update" in data
                    listeners_data = {
                        "listeners": data.get("listeners"),
                        "last_update": data.get("last_update"),
                        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                        "raw_valid": raw_valid,
                        "song_session_id": session_id
                    }
                    break  # exit after first listeners_update
            except Exception:
                listeners_data = {
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": False,
                    "song_session_id": session_id
                }
                break
    return listeners_data
