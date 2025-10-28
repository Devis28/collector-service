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
            time.sleep(5)
            continue

        if last_title != song_data["title"]:
            last_title = song_data["title"]
            song_session_id = song_data["song_session_id"]
            song_dt = datetime.fromisoformat(song_data["recorded_at"])
            upload_bronze_station(
                BUCKET,
                data_type="song",
                station=STATION,
                timestamp=song_dt,
                json_data=song_data
            )

            # Hneď po novej skladbe fetchni listeners
            listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=0.5))
            for l in listeners:
                listen_dt = datetime.fromisoformat(l["recorded_at"])
                upload_bronze_station(
                    BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                )

            # Začína robustný cyklus na listeners s watchdogom
            stuck_counter = 0
            MAX_STUCK = 6  # 6*30s = 3 min
            while True:
                listeners = asyncio.run(radio_melody.collect_listeners(song_session_id, interval=30))
                for l in listeners:
                    listen_dt = datetime.fromisoformat(l["recorded_at"])
                    upload_bronze_station(
                        BUCKET, "listeners", STATION, timestamp=listen_dt, json_data=l
                    )

                current_song = radio_melody.fetch_song()
                if current_song and current_song["title"] != last_title:
                    break

                if not listeners:
                    stuck_counter += 1
                else:
                    stuck_counter = 0

                if stuck_counter > MAX_STUCK:
                    print(f"[WARNING] Song '{last_title}' trvá nezvyčajne dlho bez listeners, preskakujem...")
                    break

if __name__ == "__main__":
    main()
