import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://radio-melody-api.fly.dev/song"
LISTENERS_WS_URL = "wss://radio-melody-api.fly.dev/ws/listeners"
LISTENERS_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
            # song_session_id bude pridaný až v hlavnej slučke
            print(f"{now_log()}[MELODY] NOW-PLAYING RAW: {json.dumps(data, ensure_ascii=False)}", flush=True)
            return data
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching song: {e}", flush=True)
    return None

def extract_song_signature(song_data):
    author = song_data.get('artist') or song_data.get('musicAuthor') or song_data.get('author') or song_data.get('interprets') or ''
    title = song_data.get('title') or song_data.get('musicTitle') or song_data.get('song') or ''
    song_time = song_data.get('time', '')
    song_date = song_data.get('date', '')
    if author and title and song_time and song_date:
        return f"{author}|{title}|{song_date} {song_time}"
    return ""

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        print(f"{now_log()}[MELODY] LISTENERS RAW: {json.dumps(listeners_data, ensure_ascii=False)}", flush=True)
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[MELODY] Error fetching listeners: {e}", flush=True)
    return None

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
            print(f"{now_log()}[MELODY] New song, signature: {song_signature}, session_id: {current_song_session_id}", flush=True)
            # Tu prípadne uložiť song_data

        last_signature = song_signature

        # Režim zberu listeners opakovane, kým sa song nezmení
        while True:
            listeners_data = fetch_listeners_once()
            if listeners_data:
                listeners_data['song_session_id'] = current_song_session_id
                print(f"{now_log()}[MELODY] Listeners: {listeners_data.get('listeners')} for song_session_id: {current_song_session_id}", flush=True)
                # Tu prípadne uložiť listeners_data

            time.sleep(LISTENERS_INTERVAL)

            new_song_data = fetch_current_song()
            if not new_song_data or extract_song_signature(new_song_data) != song_signature:
                break

if __name__ == "__main__":
    main_loop()
