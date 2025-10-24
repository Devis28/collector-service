import time
import json
from adapters import radio_rock
from writer import save_data_to_r2

SONG_PREFIX = "bronze/rock/song"
LISTENERS_PREFIX = "bronze/rock/listeners"
SEND_INTERVAL = 600  # 10 minút v sekundách


def main():
    records = []
    listeners_records = []
    last_song_id = None
    t0 = time.time()

    def raw_song_id(song_data):
        # Porovnáva celú RAW dátovú štruktúru
        return json.dumps(song_data, sort_keys=True)

    while True:
        song_data = radio_rock.fetch_current_song()

        if song_data:
            song_id = raw_song_id(song_data)

            # Ak sa skladba zmenila
            if last_song_id != song_id:
                # Zaznamenaj novú skladbu
                records.append(song_data)
                last_song_id = song_id
                print(f"New song detected: {song_data.get('song', {}).get('musicTitle', 'Unknown')}")

                # Hneď získaj listeners pre túto skladbu
                listeners = radio_rock.fetch_listeners_once()
                if listeners:
                    listeners_records.append(listeners)
                    print(f"Listeners captured: {listeners}")

        # Odoslanie batchu každých 10 minút
        if (time.time() - t0) >= SEND_INTERVAL:
            if records:
                save_data_to_r2(records, SONG_PREFIX)
                print(f"Saved {len(records)} songs to R2")
                records.clear()
            if listeners_records:
                save_data_to_r2(listeners_records, LISTENERS_PREFIX)
                print(f"Saved {len(listeners_records)} listeners to R2")
                listeners_records.clear()
            t0 = time.time()

        # Čakaj 20 sekúnd pred ďalšou kontrolou
        time.sleep(20)


if __name__ == "__main__":
    main()
