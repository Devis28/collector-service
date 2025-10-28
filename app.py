import os
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio
from adapters.radio_melody import get_current_song, get_listeners
from writer import upload_file

INTERVAL = 30     # sekund pre polling songu/listeners
BATCH_TIME = 600  # 10 minút (v sekundách)
RADIO_NAME = "melody"

def is_song_changed(song_a, song_b):
    if not song_a or not song_b:
        return True
    keys = ["title", "artist", "date", "time"]
    return any(song_a["data"].get(k) != song_b["data"].get(k) for k in keys)

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []

    previous_song_record = None

    while True:
        current_song_record = get_current_song()
        current_time = datetime.now(ZoneInfo("Europe/Bratislava"))

        # Kontrola zmeny skladby
        if is_song_changed(current_song_record, previous_song_record):
            session_id = current_song_record["song_session_id"]
            previous_song_record = current_song_record

            # na 10 minút zbieraj listeners každých 30s
            listeners = asyncio.run(get_listeners(session_id, interval=INTERVAL, duration=BATCH_TIME))
            song_data_batch.append(current_song_record)
            listeners_data_batch.extend(listeners)  # listeners je zoznam záznamov

        # Po uplynutí 10 minút upload batch na Cloudflare podľa timestampu
        if time.time() - last_batch_time >= BATCH_TIME:
            date_str = current_time.strftime("%d-%m-%Y")
            timestamp = current_time.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_song.json"
            listeners_path_local = f"{timestamp}_listeners.json"
            song_path_r2 = f"bronze/{RADIO_NAME}/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/{RADIO_NAME}/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            song_data_batch = []
            listeners_data_batch = []
            last_batch_time = time.time()

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
