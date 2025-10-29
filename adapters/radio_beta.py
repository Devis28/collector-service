import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid
import time as time_module

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"

# Globálna premenná pre posledné dáta o poslucháčoch
last_successful_listeners = None
last_listeners_update = 0
LISTENERS_CACHE_TIME = 300  # 5 minút


def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")


def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()

        if data.get("is_playing") == False:
            transformed_data = {
                "song": {
                    "musicAuthor": None,
                    "musicTitle": None,
                    "radio": data.get("radio", "Beta"),
                    "startTime": None
                },
                "last_update": data.get("timestamp"),
                "raw_valid": True
            }
        else:
            transformed_data = {
                "song": {
                    "musicAuthor": data.get("interpreters"),
                    "musicTitle": data.get("title"),
                    "radio": data.get("radio", "Beta"),
                    "startTime": data.get("start_time")
                },
                "last_update": data.get("timestamp"),
                "raw_valid": True
            }

        session_id = str(uuid.uuid4())
        return {
            **transformed_data,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "song_session_id": session_id
        }
    except Exception as e:
        log_radio_event("BETA", f"Chyba pri získavaní skladby: {e}")
        return {
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": str(uuid.uuid4())
        }


async def try_get_listeners_once():
    """Pokúsi sa získať dáta o poslucháčoch raz s veľkým timeoutom"""
    global last_successful_listeners, last_listeners_update

    try:
        async with websockets.connect(LISTENERS_WS, timeout=30) as websocket:
            # Čakáme na jednu správu s veľkým timeoutom
            raw = await asyncio.wait_for(websocket.recv(), timeout=60.0)
            data = json.loads(raw)

            if "listeners" in data:
                last_successful_listeners = data["listeners"]
                last_listeners_update = time_module.time()
                return last_successful_listeners
    except Exception as e:
        log_radio_event("BETA", f"Chyba pri jednorázovom pokuse o listeners: {e}")

    return None


async def get_current_listeners(session_id=None):
    """Získa aktuálne dáta o poslucháčoch s veľkým cache"""
    global last_successful_listeners, last_listeners_update

    current_time = time_module.time()

    # Ak máme fresh dáta v cache, vrátime ich
    if (last_successful_listeners is not None and
            current_time - last_listeners_update < LISTENERS_CACHE_TIME):
        return {
            "listeners": last_successful_listeners,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": True,
            "song_session_id": session_id
        }

    # Inak sa pokúsime získať nové dáta (max raz za 5 minút)
    if current_time - last_listeners_update >= LISTENERS_CACHE_TIME:
        listeners = await try_get_listeners_once()
        if listeners is not None:
            return {
                "listeners": listeners,
                "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                "raw_valid": True,
                "song_session_id": session_id
            }

    # Ak nemáme žiadne dáta
    return {
        "listeners": None,
        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
        "raw_valid": False,
        "song_session_id": session_id
    }


def start_beta_listeners_ws():
    """Pre Beta už nepoužívame neustále WebSocket spojenie"""
    log_radio_event("BETA", "Používame cache pre dáta o poslucháčoch s obmedzeným prístupom")