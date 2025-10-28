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

        # Nová skladba štartuje cyklus na listeners
        if last_title != song_data["title"]:
            last_title = song_data["title"]
            song_session_id = song_data["song_session_id"]
            song_dt = datetime.fromisoformat(song_data["recorded_at"])
            upload_bronze_station(
                BUCKET, "song", STATION,
                timestamp=song_dt,
                json_data=song_data
            )
            # Monitoring listeners: vždy raz za 30 sekúnd až do zmeny skladby
            while True:
                try:
                    listeners = asyncio.run(
                        radio_melody.collect_listeners(song_session_id, interval=30)
                    )
                    for l in listeners:
                        listen_dt = datetime.fromisoformat(l["recorded_at"])
                        upload_bronze_station(
                            BUCKET, "listeners", STATION,
                            timestamp=listen_dt,
                            json_data=l
                        )
                    # Počkaj 30 sekúnd (je už implicitné v collect_listeners)
                    current_song = radio_melody.fetch_song()
                    if current_song is not None and current_song["title"] != last_title:
                        break
                except Exception as e:
                    print(f"[ERROR] Listener monitoring failed: {e}")
                    time.sleep(10)  # krátky retry pri výpadku spojenia
                    continue

if __name__ == "__main__":
    main()
