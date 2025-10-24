import time
import json
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 3600  # 1 hod (v sekundách)

def main():
    records = []
    listeners_records = []
    last_song_id = None
    t0 = time.time()

    def raw_song_id(song_data):
        # Najrobustnejšia detekcia zmeny: porovnaj celú RAW odpoveď
        return json.dumps(song_data, sort_keys=True)

    while True:
        song_data = radio_rock.fetch_current_song()
        if song_data:
            song_id = raw_song_id(song_data)
            if last_song_id != song_id:
                records.append(song_data)
                last_song_id = song_id
                listeners = radio_rock.fetch_listeners_once()
                if listeners:
                    listeners_records.append(listeners)
        if (time.time() - t0) >= SEND_INTERVAL:
            if records:
                save_data_to_r2(records, SONG_PREFIX)
                records.clear()
            if listeners_records:
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records.clear()
            t0 = time.time()
        time.sleep(20)

if __name__ == "__main__":
    main()
