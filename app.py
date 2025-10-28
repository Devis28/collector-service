import asyncio
import time
import json
import os
from adapters import radio_melody
from writer import upload_to_r2
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("R2_BUCKET")
LOCAL_PATH = "/tmp/radiomelody/"

os.makedirs(LOCAL_PATH, exist_ok=True)

def save_locally(identifier, suffix, data):
    with open(f"{LOCAL_PATH}{identifier}_{suffix}.json", "a") as f:
        f.write(json.dumps(data) + "\n")

def main():
    last_title = None
    file_buffer = []
    upload_interval = 10 * 60  # 10 min v sekundách
    last_upload = time.time()

    while True:
        song_data = radio_melody.fetch_song()
        if song_data is None:
            time.sleep(5)
            continue

        # Nová skladba začína nový session, listener records buffer
        if last_title != song_data["title"]:
            last_title = song_data["title"]
            song_session_id = song_data["song_session_id"]
            save_locally(song_session_id, "song", song_data)
            listeners_records = []

            # Zaznamenávaj listeners každých 30 sekúnd, kým sa nezmení song
            while True:
                current_song = radio_melody.fetch_song()
                if current_song is not None and current_song["title"] != last_title:
                    break
                listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=30))
                listeners_records.extend(listeners)
                for l in listeners:
                    save_locally(song_session_id, "listeners", l)
                time.sleep(1)

            file_buffer.append((song_session_id, song_data, listeners_records))

        now = time.time()
        if now - last_upload >= upload_interval and file_buffer:
            for sid, song, listeners in file_buffer:
                upload_to_r2(BUCKET, f"songs/{sid}_song.json", json.dumps(song))
                upload_to_r2(BUCKET, f"listeners/{sid}_listeners.json", json.dumps(listeners))
            file_buffer = []
            last_upload = now

if __name__ == "__main__":
    main()
