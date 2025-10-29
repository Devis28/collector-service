import requests
import websockets
import threading
import asyncio
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"

# Shared thread-safe storage pre poslednú listeners hodnotu
latest_beta_listeners = {"value": None, "recorded_at": None}
listeners_lock = threading.Lock()

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

def start_beta_listeners_ws():
    async def ws_loop():
        uri = LISTENERS_WS
        while True:
            try:
                async with websockets.connect(uri) as websocket:
                    while True:
                        msg = await websocket.recv()
                        try:
                            data = json.loads(msg)
                            if "listeners" in data:
                                with listeners_lock:
                                    latest_beta_listeners["value"] = data["listeners"]
                                    latest_beta_listeners["recorded_at"] = datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")
                        except Exception:
                            continue
            except Exception:
                # WS výpadok/limit, počkaj pár sekúnd a reconnectni
                time.sleep(5)

    threading.Thread(target=lambda: asyncio.run(ws_loop()), daemon=True).start()

def get_current_listeners(session_id=None):
    # Číta len z cache, už nie je coroutine!
    with listeners_lock:
        value = latest_beta_listeners["value"]
        recorded_at = latest_beta_listeners["recorded_at"]
    return {
        "listeners": value,
        "recorded_at": recorded_at if recorded_at else datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": value is not None,
        "song_session_id": session_id
    }
