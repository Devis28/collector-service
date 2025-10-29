import os
import threading
import asyncio
import websockets
import json
import time
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

BETA_HTTP_SONG_URL = os.getenv(
    "BETA_HTTP_SONG_URL",
    "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing",
)

BETA_WS_LISTENERS = os.getenv(
    "BETA_WS_LISTENERS",
    "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners",
)

latest_beta_listeners = {"value": None, "timestamp": None}
listeners_lock = threading.Lock()

def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")

def get_current_song():
    try:
        req = urllib.request.Request(BETA_HTTP_SONG_URL, headers={"User-Agent": "radio-collector"})
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
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
        uri = BETA_WS_LISTENERS
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
                                    latest_beta_listeners["timestamp"] = data.get("timestamp") or datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S")
                        except Exception:
                            continue
            except Exception:
                time.sleep(5) # reconnect

    threading.Thread(target=lambda: asyncio.run(ws_loop()), daemon=True).start()

def get_current_listeners(session_id=None):
    # Vracia poslednú známu hodnotu (alebo None ak žiadna neprišla)
    with listeners_lock:
        value = latest_beta_listeners["value"]
        timestamp = latest_beta_listeners["timestamp"]
    return {
        "listeners": value,
        "recorded_at": timestamp if timestamp else datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": value is not None,
        "song_session_id": session_id
    }
