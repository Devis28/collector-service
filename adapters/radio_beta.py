import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"


def log_radio_event(radio_name, text, session_id=None):
    now = datetime.now(ZoneInfo("Europe/Bratislava"))
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    session_part = f" [{session_id}]" if session_id else ""
    print(f"[{timestamp}] [{radio_name}]{session_part} {text}")


def get_current_song():
    try:
        r = requests.get(SONG_API)
        data = r.json()

        # Transformácia dát do formátu kompatibilného s app.py
        if data.get("is_playing") == False:
            # Rádio hrá reklamy alebo je ticho
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
            # Rádio hrá skladbu
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


async def get_current_listeners(session_id=None):
    uri = LISTENERS_WS
    try:
        async with websockets.connect(uri) as websocket:
            # Čakáme na správu s timeoutom 20 sekúnd
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=20.0)
                data = json.loads(raw)

                if "listeners" in data:
                    return {
                        "listeners": data["listeners"],
                        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                        "raw_valid": True,
                        "song_session_id": session_id
                    }
                else:
                    return {
                        "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                        "raw_valid": False,
                        "song_session_id": session_id
                    }

            except asyncio.TimeoutError:
                log_radio_event("BETA", "Timeout pri čakaní na listeners dáta", session_id)
                return {
                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                    "raw_valid": False,
                    "song_session_id": session_id
                }

    except Exception as e:
        log_radio_event("BETA", f"Chyba pri získavaní listeners: {e}", session_id)
        return {
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }


# Funkcia pre spustenie WebSocket connection (pre kompatibilitu s app.py)
def start_beta_listeners_ws():
    # Pre Beta nie je potrebné udržiavať separátne WebSocket spojenie
    # Každé volanie get_current_listeners vytvorí nové spojenie
    pass