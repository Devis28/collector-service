import requests
import websockets
import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_API = "https://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/now-playing"
LISTENERS_WS = "wss://radio-beta-generator-stable-czarcpe4f0bee5h7.polandcentral-01.azurewebsites.net/listeners"

# Globálne premenné pre WebSocket spojenie
websocket_connection = None
last_listeners_data = None
listeners_lock = asyncio.Lock()


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


async def maintain_websocket_connection():
    """Udržiava WebSocket spojenie a prijíma správy"""
    global websocket_connection, last_listeners_data

    while True:
        try:
            async with websockets.connect(LISTENERS_WS) as ws:
                websocket_connection = ws
                log_radio_event("BETA", "WebSocket spojenie pre poslucháčov bolo úspešne nadviazané")

                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(raw)

                        if "listeners" in data:
                            async with listeners_lock:
                                last_listeners_data = {
                                    "listeners": data["listeners"],
                                    "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime(
                                        "%d.%m.%Y %H:%M:%S"),
                                    "raw_valid": True
                                }
                                log_radio_event("BETA", f"Prijaté nové dáta o poslucháčoch: {data['listeners']}")

                    except asyncio.TimeoutError:
                        # Timeout je normálny, pokračujeme v čakaní na ďalšiu správu
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        log_radio_event("BETA", "WebSocket spojenie bolo uzavreté, pokúšam sa znova...")
                        break

        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg:
                log_radio_event("BETA", "HTTP 429 - Príliš veľa požiadaviek, čakám 60 sekúnd pred ďalším pokusom")
                await asyncio.sleep(60)
            else:
                log_radio_event("BETA", f"Chyba pri pripájaní WebSocket: {e}, pokúšam sa znova o 30 sekúnd")
                await asyncio.sleep(30)


async def get_current_listeners(session_id=None):
    """Získa aktuálne dáta o poslucháčoch z globálneho spojenia"""
    global last_listeners_data

    # Ak máme uložené dáta, vrátime ich
    if last_listeners_data:
        return {
            **last_listeners_data,
            "song_session_id": session_id
        }
    else:
        # Ak ešte nemáme žiadne dáta
        return {
            "listeners": None,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }


def start_beta_listeners_ws():
    """Spustí udržiavanie WebSocket spojenia v pozadí"""

    def run_websocket():
        asyncio.run(maintain_websocket_connection())

    import threading
    thread = threading.Thread(target=run_websocket, daemon=True)
    thread.start()
    log_radio_event("BETA", "WebSocket worker bol spustený")