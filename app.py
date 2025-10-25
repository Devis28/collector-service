import time
from datetime import datetime
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600
SONG_CHECK_INTERVAL = 30

def main():
    records = []
    listeners_records = []
    last_song_signature = None
    t0 = time.time()

    print(f"[APP] Starting collector service at {datetime.now()}")

    while True:
        song_data, song_signature = radio_rock.process_and_log_song(last_song_signature)
        if song_data:
            last_song_signature = song_signature
            records.append(song_data)

            listeners_data = radio_rock.process_and_log_listeners(song_signature=song_signature)
            if listeners_data:
                listeners_records.append(listeners_data)

        # Upload každých 10 minút
        if time.time() - t0 >= SEND_INTERVAL:
            if records:
                print(f"[WRITER] Saving {len(records)} song records for RADIO ROCK to {SONG_PREFIX}")
                save_data_to_r2(records, SONG_PREFIX)
                records = []
            if listeners_records:
                print(f"[WRITER] Saving {len(listeners_records)} listeners records for RADIO ROCK to {LISTENERS_PREFIX}")
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records = []
            t0 = time.time()

        time.sleep(SONG_CHECK_INTERVAL)

if __name__ == "__main__":
    main()
