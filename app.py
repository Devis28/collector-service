import asyncio
import time
from datetime import datetime
from adapters import radio_melody  # uprav podľa reálneho názvu
from writer import upload_bronze_station
from dotenv import load_dotenv
import os

load_dotenv()
BUCKET = os.getenv("R2_BUCKET")
STATION = "melody"

def main():
    last_song_id = None

    while True:
        song_data = radio_melody.fetch_song()
        if song_data is None:
            print("[WARNING] fetch_song() vrátil None, čakám 5s...")
            time.sleep(5)
            continue

        actual_song_id = (song_data.get("song_session_id") or song_data.get("title"))
        # ak sú v API session_id unikátne, je lepšie porovnávať podľa nich!
        if actual_song_id != last_song_id:
            last_song_id = actual_song_id
            song_dt = datetime.fromisoformat(song_data["recorded_at"])
            upload_bronze_station(
                BUCKET,
                data_type="song",
                station=STATION,
                timestamp=song_dt,
                json_data=song_data
            )

            # listeners ihneď po songu
            listeners = asyncio.run(radio_melody.collect_listeners(actual_song_id, interval=0.5))
            for l in listeners or []:
                listen_dt = datetime.fromisoformat(l["recorded_at"])
                upload_bronze_station(
                    BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                )

            # Intervalový cyklus pre listeners trvalý na song_id/title!
            while True:
                listeners = asyncio.run(radio_melody.collect_listeners(actual_song_id, interval=30))
                for l in listeners or []:
                    listen_dt = datetime.fromisoformat(l["recorded_at"])
                    upload_bronze_station(
                        BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                    )
                # fetch_song a kontrola len pri úspešnom fetche!
                current_song = radio_melody.fetch_song()
                current_id = None
                if current_song is not None:
                    current_id = (current_song.get("song_session_id") or current_song.get("title"))
                # cyklus skončí iba keď song/session ID je nové!
                if (current_id is not None) and (current_id != last_song_id):
                    break
        # celá while-True nikdy nekončí okrem výnimky:
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f'[FATAL ERROR]: {e}')
        time.sleep(10)
