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
    with open(f"{LOCAL_PATH}{identifier}_{suffix}.json", "w") as f:
        f.write(json.dumps(data))

def main():
    last_title = None
    file_buffer = []
    upload_interval = 10 * 60  # 10 min v sekundách
    last_upload = time.time()

    while True:
        song_data = radio_melody.fetch_song()
        if song_data is None:
            time.sleep(30)
            continue  # Preskoč cyklus, počkaj na ďalší song
        if last_title != song_data["title"]:
            last_title = song_data["title"]
            song_session_id = song_data["song_session_id"]
            save_locally(song_session_id, "song", song_data)
            listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=30))
            save_locally(song_session_id, "listeners", listeners)
            file_buffer.append((song_session_id, song_data, listeners))

        now = time.time()
        if now - last_upload >= upload_interval and file_buffer:
            for sid, song, listeners in file_buffer:
                upload_to_r2(BUCKET, f"songs/{sid}_song.json", json.dumps(song))
                upload_to_r2(BUCKET, f"listeners/{sid}_listeners.json", json.dumps(listeners))
            file_buffer = []
            last_upload = now

        time.sleep(30)

if __name__ == "__main__":
    main()
