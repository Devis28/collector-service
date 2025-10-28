import asyncio
import time
from datetime import datetime
from writer import upload_bronze_station
from adapters.radio_melody import fetch_song, collect_listeners
from dotenv import load_dotenv
import os

load_dotenv()
BUCKET = os.getenv("R2_BUCKET")
STATION = "melody"

def main():
    last_song_id = None

    while True:
        song_data = fetch_song()
        if song_data is None:
            print("[WARNING] fetch_song() vrátil None, čakám 5s...")
            time.sleep(5)
            continue

        actual_song_id = song_data.get("song_session_id") or song_data.get("title")
        if actual_song_id != last_song_id:
            last_song_id = actual_song_id
            song_dt = datetime.fromisoformat(song_data["recorded_at"])
            upload_bronze_station(
                BUCKET, "song", STATION, timestamp=song_dt, json_data=song_data
            )
            # Prvý listeners batch
            listeners = collect_listeners(actual_song_id, interval=1)
            for l in listeners or []:
                listen_dt = datetime.fromisoformat(l["recorded_at"])
                upload_bronze_station(
                    BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                )
            # Interval cyklus listeners, kým sa nezmení ID
            while True:
                listeners = collect_listeners(actual_song_id, interval=30)
                for l in listeners or []:
                    listen_dt = datetime.fromisoformat(l["recorded_at"])
                    upload_bronze_station(
                        BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                    )
                current_song = fetch_song()
                current_id = None
                if current_song is not None:
                    current_id = current_song.get("song_session_id") or current_song.get("title")
                if (current_id is not None) and (current_id != last_song_id):
                    break

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('[FATAL ERROR]:', e)
        time.sleep(10)
