import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://funradio-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://funradio-server.fly.dev/ws/push/listenership"
LISTENERS_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
            print(f"{now_log()}[FUNRADIO] RAW NOW-PLAYING DATA: {json.dumps(data, ensure_ascii=False)}", flush=True)
            return data
    except Exception as e:
        print(f"{now_log()}[FUNRADIO] Error fetching song: {e}", flush=True)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        print(f"{now_log()}[FUNRADIO] RAW LISTENERS DATA: {json.dumps(listeners_data, ensure_ascii=False)}", flush=True)
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[FUNRADIO] Error fetching listeners: {e}", flush=True)
    return None

def extract_song_signature(song_data):
    if 'song' in song_data and isinstance(song_data['song'], dict):
        ref = song_data['song']
        title = ref.get('musicTitle') or ref.get('title') or ''
        author = ref.get('musicAuthor') or ref.get('author') or ''
        start_time = ref.get('startTime') or ref.get('start') or ''
        if author and title and start_time:
            return f"{author}|{title}|{start_time}"
    return ""

def main_loop():
    last_signature = ""
    current_song_session_id = None

    while True:
        song_data = fetch_current_song()
        if not song_data:
            time.sleep(5)
            continue

        song_signature = extract_song_signature(song_data)
        if song_signature != last_signature:
            current_song_session_id = str(uuid.uuid4())
            song_data['song_session_id'] = current_song_session_id
            print(f"{now_log()}[FUNRADIO] New song, signature: {song_signature}, session_id: {current_song_session_id}", flush=True)
            # Tu prípadne uložiť song_data

        last_signature = song_signature

        # Režim zberu listeners opakovane, kým sa song nezmení
        while True:
            listeners_data = fetch_listeners_once()
            if listeners_data:
                listeners_data['song_session_id'] = current_song_session_id
                print(f"{now_log()}[FUNRADIO] Listeners: {listeners_data.get('listeners')} for song_session_id: {current_song_session_id}", flush=True)
                # Tu prípadne uložiť listeners_data

            time.sleep(LISTENERS_INTERVAL)

            new_song_data = fetch_current_song()
            if not new_song_data or extract_song_signature(new_song_data) != song_signature:
                break

if __name__ == "__main__":
    main_loop()
