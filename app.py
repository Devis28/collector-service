import time
import json
from datetime import datetime
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600  # 10 minút v sekundách
SONG_CHECK_INTERVAL = 60  # Kontrola skladby každú minútu
LISTENERS_DELAY = 20  # Čakanie 20 sekúnd pred získaním listeners


def main():
    records = []
    listeners_records = []
    last_song_id = None
    t0 = time.time()

    print(f"[APP] Starting collector service at {datetime.now()}")
    print(f"[APP] Upload interval: {SEND_INTERVAL} seconds ({SEND_INTERVAL // 60} minutes)")
    print(f"[APP] Song check interval: {SONG_CHECK_INTERVAL} seconds")
    print(f"[APP] Listeners delay after new song: {LISTENERS_DELAY} seconds")

    def song_id_from_data(song_data):
        # Porovnávaj len obsah 'song', nie 'last_update'
        song_info = song_data.get('song', {})
        return json.dumps(song_info, sort_keys=True)

    while True:
        song_data = radio_rock.fetch_current_song()

        if song_data:
            song_id = song_id_from_data(song_data)

            # Ak sa skladba zmenila (porovnanie len song objektu)
            if last_song_id != song_id:
                # Zaznamenaj novú skladbu
                records.append(song_data)
                last_song_id = song_id

                song_info = song_data.get('song', {})
                title = song_info.get('musicTitle', 'Unknown')
                author = song_info.get('musicAuthor', 'Unknown')
                print(f"[APP] New song detected and recorded: {author} - {title}")

                # Čakaj 20 sekúnd pred získaním listeners
                print(f"[APP] Waiting {LISTENERS_DELAY} seconds before fetching listeners...")
                time.sleep(LISTENERS_DELAY)

                # Teraz získaj listeners pre túto skladbu
                listeners = radio_rock.fetch_listeners_once()
                if listeners:
                    listeners_records.append(listeners)
                    count = listeners.get('listenership', 'Unknown')
                    print(f"[APP] Listeners recorded: {count}")

        # Odoslanie batchu každých 10 minút
        if (time.time() - t0) >= SEND_INTERVAL:
            print(f"[APP] Upload interval reached at {datetime.now()}")
            if records:
                save_data_to_r2(records, SONG_PREFIX)
                print(f"[APP] ✓ Saved {len(records)} songs to R2")
                records.clear()
            if listeners_records:
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                print(f"[APP] ✓ Saved {len(listeners_records)} listeners to R2")
                listeners_records.clear()
            t0 = time.time()

        # Čakaj 1 minútu pred ďalšou kontrolou skladby
        time.sleep(SONG_CHECK_INTERVAL)


if __name__ == "__main__":
    main()
