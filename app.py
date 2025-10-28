import asyncio
import time
from datetime import datetime
from adapters import radio_melody
from writer import upload_bronze_station
from dotenv import load_dotenv
import os

load_dotenv()
BUCKET = os.getenv("R2_BUCKET")
STATION = "melody"

def main():
    last_title = None

    while True:
        song_data = radio_melody.fetch_song()
        if song_data is None:
            print("[WARNING] fetch_song() vrátil None, čakám 5s...")
            time.sleep(5)
            continue

        if last_title != song_data["title"]:
            last_title = song_data["title"]
            song_session_id = song_data.get("song_session_id")
            song_dt = datetime.fromisoformat(song_data["recorded_at"])
            upload_bronze_station(
                BUCKET, "song", STATION, timestamp=song_dt, json_data=song_data
            )
            # Prvý batch po songu
            listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=0.5))
            for l in listeners or []:
                listen_dt = datetime.fromisoformat(l["recorded_at"])
                upload_bronze_station(
                    BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                )

            # Hlavná listeners slučka cez interval
            while True:
                listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=30))
                for l in listeners or []:
                    listen_dt = datetime.fromisoformat(l["recorded_at"])
                    upload_bronze_station(
                        BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                    )
                # Čo robí tvoj expres: pri None to vôbec neláme cyklus!
                current_song = radio_melody.fetch_song()
                if current_song is not None and current_song["title"] != last_title:
                    break
                # Ak je current_song None, pokojne pokračujeme - o ďalších 30s znova roztočíme fetch

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f'[FATAL ERROR]: {e}')
        time.sleep(10)
