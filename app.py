import time
import json
from datetime import datetime
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600
SONG_CHECK_INTERVAL = 30
LISTENERS_DELAY = 20
LISTENERS_RETRY_ATTEMPTS = 3
LISTENERS_RETRY_DELAY = 10

def extract_song_signature(song_data):
    # Unikátny podpis skladby (author+title+startTime)
    if 'song' in song_data and isinstance(song_data['song'], dict):
        ref = song_data['song']
        title = ref.get('musicTitle', '')
        author = ref.get('musicAuthor', '')
        start_time = ref.get('startTime', '')
        return f"{author}|{title}|{start_time}"
    return ""

def main():
    records = []
    listeners_records = []
    last_song_signature = None
    t0 = time.time()

    print(f"[APP] Starting collector service at {datetime.now()}")
    print(f"[APP] Upload interval: {SEND_INTERVAL}s ({SEND_INTERVAL // 60} min)")
    print(f"[APP] Song check interval: {SONG_CHECK_INTERVAL}s")
    print(f"[APP] Listeners delay: {LISTENERS_DELAY}s, retries: {LISTENERS_RETRY_ATTEMPTS}")

    while True:
        song_data = radio_rock.fetch_current_song(print_log=False)
        if song_data:
            song_signature = extract_song_signature(song_data)
            song_session_id = song_data.get('song_session_id')

            if song_signature != last_song_signature:
                last_song_signature = song_signature

                # Výpis o songu len pri novej skladbe!
                artist = song_data.get('song', {}).get('musicAuthor', 'Unknown')
                title = song_data.get('song', {}).get('musicTitle', 'Unknown')
                print(f"[ROCK] Song recorded: {artist} - {title}")

                records.append(song_data)
                print(f"[APP] Waiting {LISTENERS_DELAY}s before fetching listeners...")
                time.sleep(LISTENERS_DELAY)

                for attempt in range(LISTENERS_RETRY_ATTEMPTS):
                    # Pred kažým pokusom o listeners najskôr over skladbu
                    current_song_check = radio_rock.fetch_current_song(print_log=False)
                    current_signature = extract_song_signature(current_song_check) if current_song_check else None

                    if current_signature == song_signature:
                        listeners_data = radio_rock.fetch_listeners_once()
                        if listeners_data:
                            listeners_data['song_session_id'] = song_session_id
                            listeners_records.append(listeners_data)
                            break
                    else:
                        print("[APP] Song changed during listeners retry, not recording listeners.")
                        break

                    if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
                        print(f"[APP] Listeners retry {attempt + 1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...")
                        time.sleep(LISTENERS_RETRY_DELAY)

        # Upload každých 10 minút
        if time.time() - t0 >= SEND_INTERVAL:
            if records:
                print(f"[APP] Uploading {len(records)} song records to R2...")
                save_data_to_r2(records, SONG_PREFIX)
                records = []
            if listeners_records:
                print(f"[APP] Uploading {len(listeners_records)} listeners records to R2...")
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                listeners_records = []
            t0 = time.time()

        time.sleep(SONG_CHECK_INTERVAL)

if __name__ == "__main__":
    main()
