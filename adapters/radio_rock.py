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
    uri = "wss://rock-server.fly.dev/ws/push/listenership"
    listeners_data = None
    last_data = None
    async with websockets.connect(uri) as websocket:
        try:
            # Slučka - 12 pokusov po 2.5 sekundy (spolu max 30 sekúnd na listeners)
            for _ in range(12):
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=2.5)
                except asyncio.TimeoutError:
                    continue
                data = json.loads(raw)
                last_data = data
                if data.get("type") == "listeners_update":
                    listeners = data.get("listeners")
                    last_update = data.get("last_update")
                    raw_valid = listeners is not None and last_update is not None
                    listeners_data = {
                        "listeners": listeners,
                        "last_update": last_update,
                        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                        "raw_valid": raw_valid,
                        "song_session_id": session_id
                    }
                    break
            # Je listeners správa?
            if not listeners_data:
                listeners_data = {
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": False,
                    "song_session_id": session_id,
                    "debug_ws_last_message": last_data  # na debug, môžeš mazať v ostrej prevádzke
                }
        except Exception:
            listeners_data = {
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": False,
                "song_session_id": session_id
            }
    return listeners_data
