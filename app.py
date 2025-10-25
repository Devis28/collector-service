import time
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600
SONG_CHECK_INTERVAL = 30

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def main():
    records = []
    listeners_records = []
    last_song_signature = None
    last_song_session_id = None
    t0 = time.time()

    print(f"{now_log()}[APP] Starting collector service at {datetime.now(ZoneInfo('Europe/Bratislava'))}")

    while True:
        song_data, song_signature = radio_rock.process_and_log_song(last_song_signature)
        if song_data:
            last_song_signature = song_signature
            last_song_session_id = song_data.get('song_session_id')
            records.append(song_data)

            listeners_data = radio_rock.process_and_log_listeners(song_signature=song_signature)
            if listeners_data:
                # Pridané: session_id do každého listeners záznamu
                listeners_data['song_session_id'] = last_song_session_id
                listeners_records.append(listeners_data)

        # Upload každých 10 minút
        if time.time() - t0 >= SEND_INTERVAL:
            if records:
                print(f"{now_log()}[WRITER] Saving {len(records)} song records for ROCK to {SONG_PREFIX}")
                save_data_to_r2(records, SONG_PREFIX)
                records = []
            if listeners_records:
                print(f"{now_log()}[WRITER] Saving {len(listeners_records)} listeners records for ROCK to {LISTENERS_PREFIX}")
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records = []
            t0 = time.time()

        time.sleep(SONG_CHECK_INTERVAL)

if __name__ == "__main__":
    main()
