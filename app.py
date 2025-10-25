import time
import json
from datetime import datetime
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600  # 10 minút v sekundách
SONG_CHECK_INTERVAL = 30  # Kontrola skladby každých 30 sekúnd
LISTENERS_DELAY = 20  # Čakanie 20 sekúnd pred získaním listeners
LISTENERS_RETRY_ATTEMPTS = 3  # Počet pokusov pre listeners
LISTENERS_RETRY_DELAY = 10  # Čakanie medzi pokusmi (sekúnd)


def extract_song_id(song_data):
    """Helper funkcia na extrahovanie song_id z nested štruktúry."""
    # Skús získať song_id z rôznych možných miest
    if 'song' in song_data and isinstance(song_data['song'], dict):
        # Ak je song nested objekt, hľadaj ID v ňom
        return song_data['song'].get('id') or song_data['song'].get('song_id')
    # Fallback: hľadaj na top-level
    return song_data.get('song_id') or song_data.get('id')


def main():
    records = []
    listeners_records = []
    last_song_id = None
    t0 = time.time()

    print(f"[APP] Starting collector service at {datetime.now()}")
    print(f"[APP] Upload interval: {SEND_INTERVAL}s ({SEND_INTERVAL // 60} min)")
    print(f"[APP] Song check interval: {SONG_CHECK_INTERVAL}s")
    print(f"[APP] Listeners delay: {LISTENERS_DELAY}s, retries: {LISTENERS_RETRY_ATTEMPTS}")

    while True:
        # Získaj aktuálnu skladbu
        song_data = radio_rock.fetch_current_song()

        if song_data:
            # Extrahuj song_id (môže byť na rôznych miestach v JSON)
            current_song_id = extract_song_id(song_data)
            song_session_id = song_data.get('song_session_id')

            # Kontrola zmeny skladby
            if current_song_id != last_song_id:
                last_song_id = current_song_id
                records.append(song_data)

                # Čakaj 20 sekúnd pred získaním listeners
                print(f"[APP] Waiting {LISTENERS_DELAY}s before fetching listeners...")
                time.sleep(LISTENERS_DELAY)

                # Retry logika pre listeners (max 3 pokusy)
                listeners_data = None
                for attempt in range(LISTENERS_RETRY_ATTEMPTS):
                    listeners_data = radio_rock.fetch_listeners_once()
                    if listeners_data:
                        # Pridaj song_session_id pre spätnú väzbu v silver
                        listeners_data['song_session_id'] = song_session_id
                        listeners_records.append(listeners_data)
                        break  # Úspech, pokračuj
                    else:
                        if attempt < LISTENERS_RETRY_ATTEMPTS - 1:
                            print(
                                f"[APP] Listeners retry {attempt + 1}/{LISTENERS_RETRY_ATTEMPTS}, waiting {LISTENERS_RETRY_DELAY}s...")
                            time.sleep(LISTENERS_RETRY_DELAY)

                # Ak všetky pokusy zlyhali
                if not listeners_data:
                    print(
                        f"[APP] ERROR: Failed to fetch listeners for song {song_session_id} after {LISTENERS_RETRY_ATTEMPTS} attempts")

        # Odošli dáta každých 10 minút
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

        # Čakaj 30 sekúnd pred ďalšou kontrolou skladby
        time.sleep(SONG_CHECK_INTERVAL)

if __name__ == "__main__":
    main()
