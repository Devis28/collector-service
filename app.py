import time
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 10 * 60  # 10 minút v sekundách

def main():
    records = []
    listeners_records = []
    current_song_id = None
    t0 = time.time()
    while True:
        # 1. Spýtaj song
        song = radio_rock.fetch_current_song()
        if song and (current_song_id is None or song != current_song_id):
            records.append(song)
            current_song_id = song  # Prípadne song['musicTitle'], podľa dát DOC
            # 2. Hneď po songu získaj listeners
            listeners = radio_rock.fetch_listeners_once()
            if listeners:
                listeners_records.append(listeners)

        # Po 10 minútach odošli batch:
        if (time.time() - t0) >= SEND_INTERVAL:
            if records:
                save_data_to_r2(records, SONG_PREFIX)
                records.clear()
            if listeners_records:
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records.clear()
            t0 = time.time()

        # Slučka kontroluj napr. každých 30 sekúnd
        time.sleep(30)

if __name__ == "__main__":
    main()
