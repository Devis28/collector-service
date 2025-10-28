import time
import json
import asyncio
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters.radio_melody import get_current_song, get_current_listeners, log_radio_event
from writer import upload_file

INTERVAL = 30
BATCH_TIME = 600
RADIO_NAME = "melody"

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def flatten_song(song_obj):
    # Preflatovaná štruktúra ako požaduješ
    result = dict(song_obj["data"])
    result["recorded_at"] = song_obj.get("recorded_at")
    result["raw_valid"] = song_obj.get("raw_valid")
    result["song_session_id"] = song_obj.get("song_session_id")
    return result

def flatten_listener(listener_obj):
    result = dict(listener_obj.get("data", {}))
    result["recorded_at"] = listener_obj.get("recorded_at")
    result["raw_valid"] = listener_obj.get("raw_valid")
    result["song_session_id"] = listener_obj.get("song_session_id")
    return result

def main():
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_artist = None
    session_id = None

    while True:
        current_song = get_current_song()
        title = current_song["data"].get("title")
        artist = current_song["data"].get("artist")

        # Pri zmene songu vygeneruj nové session_id, zapíš song len raz
        if title != previous_title or artist != previous_artist:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_artist = artist
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title}", session_id)
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_current_listeners())
        listeners_data["song_session_id"] = session_id
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {listeners_data.get('data',{}).get('listeners', '?')}", session_id)
        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")

            flat_song_batch = [flatten_song(s) for s in song_data_batch]
            flat_listeners_batch = [flatten_listener(l) for l in listeners_data_batch]

            song_path_local = f"{timestamp}_song.json"
            listeners_path_local = f"{timestamp}_listeners.json"
            song_path_r2 = f"bronze/{RADIO_NAME}/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/{RADIO_NAME}/listeners/{date_str}/{timestamp}.json"

            save_json(flat_song_batch, song_path_local)
            save_json(flat_listeners_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            song_data_batch = []
            listeners_data_batch = []
            last_batch_time = time.time()

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
