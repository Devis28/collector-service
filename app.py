import time
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 3600  # 1 hodina v sekundách

def main():
    records = []
    listeners_records = []
    last_song_data = None
    last_song_id = None
    t0 = time.time()
    while True:
        song_data = radio_rock.fetch_current_song()
        if song_data:
            # Prvý cyklus alebo song sa zmenil = nový záznam
            song_id = json.dumps(song_data["song"], sort_keys=True)
            if last_song_id != song_id:
                records.append(song_data)
                last_song_id = song_id
                last_song_data = song_data
                # Hneď po songu priamo listener
                listeners = radio_rock.fetch_listeners_once()
                if listeners:
                    listeners_records.append(listeners)
        # Odošli každú hodinu
        if (time.time() - t0) >= SEND_INTERVAL:
            if records:
                save_data_to_r2(records, SONG_PREFIX)
                records.clear()
            if listeners_records:
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records.clear()
            t0 = time.time()
        time.sleep(20)  # kontroluje každých 20 sekúd

if __name__ == "__main__":
    main()
