import requests
import asyncio
import websockets
import datetime
import uuid
import json
import zoneinfo
import time

SONG_API = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS = "wss://radio-melody-api.fly.dev/ws/listeners"
TZI = zoneinfo.ZoneInfo("Europe/Bratislava")

def now_bratislava():
    return datetime.datetime.now(TZI).strftime("%d.%m.%Y %H:%M:%S")

seen_song_titles = set()

def fetch_song():
    response = requests.get(SONG_API)
    data = response.json()
    required = ["station", "title", "artist", "date", "time", "last_update"]
    raw_valid = all(data.get(col) for col in required)
    data["recorded_at"] = datetime.datetime.now().isoformat()
    data["raw_valid"] = raw_valid
    data["song_session_id"] = str(uuid.uuid4()) if raw_valid else None

    # Výpis a evidencia len ak nový song (podľa title + artist)
    song_key = f"{data.get('artist', '?')}–{data.get('title', '?')}"
    if raw_valid and song_key not in seen_song_titles:
        seen_song_titles.add(song_key)
        print(f"[{now_bratislava()}] [MELODY] Zaznamenaná skladba: {data.get('artist', '?')} – {data.get('title', '?')} | Session ID: {data['song_session_id']}")
        return data
    return None

async def collect_listeners(song_session_id, interval=30):
    print(f"[{now_bratislava()}] [MELODY] Čakám na údaje o poslucháčoch (listeners)...")
    start_time = time.time()
    listener_record = None
    async with websockets.connect(LISTENERS_WS) as ws:
        while True:
            elapsed = time.time() - start_time
            if elapsed >= interval:
                break
            try:
                timeout = interval - elapsed if interval - elapsed > 0 else 1
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
                info = json.loads(message)
                required = ["listeners", "last_update"]
                raw_valid = all(col in info for col in required)
                info["recorded_at"] = datetime.datetime.now().isoformat()
                info["raw_valid"] = raw_valid
                info["song_session_id"] = song_session_id
                if not listener_record:
                    listener_record = info
                    print(f"[{now_bratislava()}] [MELODY] Zaznamenaný listeners: {info.get('listeners', '?')} | Session ID: {song_session_id}")
            except asyncio.TimeoutError:
                break
    return [listener_record] if listener_record else []

