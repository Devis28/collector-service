import os
import time
import json
import asyncio
import datetime
from adapters.radio_melody import get_current_song, get_listeners
from writer import upload_file

INTERVAL = 30  # sekund
UPLOAD_PERIOD = 10*60  # každých 10 minút
OUTPUT_DIR = "bronze/melody"
DATE_FMT = "%d-%m-%Y"

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    session_data = []
    last_upload = time.time()
    prev_song = None
    session_id = None

    while True:
        song_obj = get_current_song()
        song_now = song_obj["data"]

        # Zmena pesničky, nový session_id
        if prev_song != (song_now["title"], song_now["artist"], song_now["date"], song_now["time"]):
            session_id = song_obj["song_session_id"]
            prev_song = (song_now["title"], song_now["artist"], song_now["date"], song_now["time"])
            # Získavame listeners pre tento session_id
            loop_data = asyncio.run(get_listeners(session_id, interval=INTERVAL, duration=UPLOAD_PERIOD))

            # Priprav a ulož súbory
            date_part = datetime.now().strftime(DATE_FMT)
            song_dir = f"{OUTPUT_DIR}/song/{date_part}"
            listeners_dir = f"{OUTPUT_DIR}/listeners/{date_part}"
            os.makedirs(song_dir, exist_ok=True)
            os.makedirs(listeners_dir, exist_ok=True)

            # song záznam
            song_path = f"{song_dir}/dátový záznam.json"
            save_json([song_obj], song_path)
            upload_file(song_path, song_path)

            # listeners záznam
            listeners_path = f"{listeners_dir}/dátový záznam.json"
            save_json(loop_data, listeners_path)
            upload_file(listeners_path, listeners_path)

        time.sleep(INTERVAL)
