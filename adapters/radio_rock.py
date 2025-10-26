import requests
import websocket
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid

SONG_URL = "https://rock-server.fly.dev/pull/playing"
LISTENERS_WS_URL = "wss://rock-server.fly.dev/ws/push/listenership"
LISTENERS_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def fetch_current_song():
    try:
        response = requests.get(SONG_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
            # Nie tu! song_session_id dostane až po detekcii zmeny songu v hlavnej slučke
            data['raw_valid'] = (
                isinstance(data.get('song'), dict) and
                'musicTitle' in data['song'] and
                'musicAuthor' in data['song'] and
                'startTime' in data['song']
            )
            return data
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching song: {e}", flush=True)
    return None

def fetch_listeners_once():
    try:
        ws = websocket.create_connection(LISTENERS_WS_URL, timeout=20)
        data = ws.recv()
        ws.close()
        listeners_data = json.loads(data)
        listeners_data['recorded_at'] = datetime.now(ZoneInfo("Europe/Bratislava")).isoformat()
        listeners_data['raw_valid'] = ('listeners' in listeners_data and isinstance(listeners_data['listeners'], int))
        return listeners_data
    except Exception as e:
        print(f"{now_log()}[ROCK] Error fetching listeners: {e}", flush=True)
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
        # Nový song: nový song_session_id
        if song_signature != last_signature:
            current_song_session_id = str(uuid.uuid4())
            song_data['song_session_id'] = current_song_session_id
            print(f"{now_log()}[ROCK] New song, signature: {song_signature}, session_id: {current_song_session_id}", flush=True)
            # Sem môžeš pridať zápis song_data do súboru/Cloudflare...

        last_signature = song_signature

        # Nech žijeme v slučke, pokiaľ song_signature nezmení
        while True:
            listeners_data = fetch_listeners_once()
            if listeners_data:
                listeners_data['song_session_id'] = current_song_session_id
                print(f"{now_log()}[ROCK] Listeners: {listeners_data['listeners']} for song_session_id: {current_song_session_id}", flush=True)
                # Sem môžeš pridať zápis listeners_data do súboru/Cloudflare...

            time.sleep(LISTENERS_INTERVAL)

            # Skontroluj či song stále platí, ak nie, break a ideme na ďalšie kolo hlavnej slučky
            new_song_data = fetch_current_song()
            if not new_song_data or extract_song_signature(new_song_data) != song_signature:
                break

if __name__ == "__main__":
    main_loop()
