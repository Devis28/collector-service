import time
import json
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters.radio_melody import get_current_song, get_current_listeners, log_radio_event
from writer import upload_file

INTERVAL = 30
BATCH_TIME = 600
RADIO_NAME = "melody"

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []

    previous_title = None
    previous_artist = None
    session_id = None

    while True:
        current_song = get_current_song()
        title = current_song["data"].get("title")
        artist = current_song["data"].get("artist")

        # Song sa zapíše IBA ak je iný title alebo artist
        if title != previous_title or artist != previous_artist:
            session_id = str(uuid.uuid4())  # VYGENERUJ IBA PRI ZMENE
            previous_title = title
            previous_artist = artist
            # DOPLŇ song_session_id do objektu
            current_song["song_session_id"] = session_id
            log_radio_event(RADIO_NAME, f"Zachytená skladba: {title}", session_id)
            song_data_batch.append(current_song)

        # Listeners zapisuj vždy k aktuálnemu session_id, nevolaj get_current_song znova!
        listeners_data = asyncio.run(get_current_listeners())
        listeners_data["song_session_id"] = session_id
        log_radio_event(RADIO_NAME, f"Zachytení poslucháči: {listeners_data.get('data',{}).get('listeners', '?')}", session_id)
        listeners_data_batch.append(listeners_data)

        # Upload každých 10 minút
        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_song.json"
            listeners_path_local = f"{timestamp}_listeners.json"
            song_path_r2 = f"bronze/{RADIO_NAME}/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/{RADIO_NAME}/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            song_data_batch = []
            listeners_data_batch = []
            last_batch_time = time.time()

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
