import time
import json
import asyncio
import uuid
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters.radio_melody import (
    get_current_song as get_song_melody,
    get_current_listeners as get_listeners_melody,
    log_radio_event as log_melody_event,
)
from adapters.radio_rock import (
    get_current_song as get_song_rock,
    get_current_listeners as get_listeners_rock,
    log_radio_event as log_rock_event,
)
from adapters.radio_beta import (
    get_current_song as get_song_beta,
    get_current_listeners as get_listeners_beta,
    log_radio_event as log_beta_event,
    start_beta_listeners_ws
)
from writer import upload_file

INTERVAL = 30
BATCH_TIME = 600


def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def melody_worker():
    RADIO_NAME = "MELODY"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_artist = None
    session_id = None

    while True:
        current_song = get_song_melody()
        title = current_song["data"].get("title")
        artist = current_song["data"].get("artist")

        if title != previous_title or artist != previous_artist:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_artist = artist
            current_song["song_session_id"] = session_id
            log_melody_event(RADIO_NAME, f"Zachytená skladba: {title}", session_id)
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_listeners_melody())
        listeners_data["song_session_id"] = session_id
        log_melody_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {listeners_data.get('data', {}).get('listeners', '?')}",
            session_id
        )
        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_melody_song.json"
            listeners_path_local = f"{timestamp}_melody_listeners.json"
            song_path_r2 = f"bronze/MELODY/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/MELODY/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_melody_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_melody_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)


def rock_worker():
    RADIO_NAME = "ROCK"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_author = None
    session_id = None

    while True:
        current_song = get_song_rock()
        song = current_song.get("song", {})
        title = song.get("musicTitle")
        author = song.get("musicAuthor")

        if title != previous_title or author != previous_author:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_author = author
            current_song["song_session_id"] = session_id
            log_rock_event(RADIO_NAME, f"Zachytená skladba: {title} / {author}", session_id)
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_listeners_rock(session_id))
        log_rock_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {listeners_data.get('listeners', '?')}",
            session_id
        )
        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_rock_song.json"
            listeners_path_local = f"{timestamp}_rock_listeners.json"
            song_path_r2 = f"bronze/ROCK/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/ROCK/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_rock_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_rock_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)


def beta_worker():
    RADIO_NAME = "BETA"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_author = None
    session_id = None

    while True:
        current_song = get_song_beta()
        song = current_song.get("song", {})
        title = song.get("musicTitle")
        author = song.get("musicAuthor")

        if title != previous_title or author != previous_author:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_author = author
            current_song["song_session_id"] = session_id

            if title and author:
                log_beta_event(RADIO_NAME, f"Zachytená skladba: {title} / {author}", session_id)
            else:
                log_beta_event(RADIO_NAME, "Rádio hrá reklamy alebo je ticho", session_id)

            song_data_batch.append(current_song)

        # Získanie poslucháčov - s cache a obmedzeným prístupom
        listeners_data = asyncio.run(get_listeners_beta(session_id))

        # Logovanie podľa dostupnosti dát
        if listeners_data.get('raw_valid') and listeners_data.get('listeners') is not None:
            log_beta_event(
                RADIO_NAME,
                f"Zachytení poslucháči: {listeners_data.get('listeners')}",
                session_id
            )
        else:
            log_beta_event(
                RADIO_NAME,
                "Dáta o poslucháčoch nie sú dostupné (obmedzenie servera)",
                session_id
            )

        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_beta_song.json"
            listeners_path_local = f"{timestamp}_beta_listeners.json"
            song_path_r2 = f"bronze/BETA/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/BETA/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_beta_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_beta_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)

def main():
    start_beta_listeners_ws()
    threading.Thread(target=melody_worker, daemon=True).start()
    threading.Thread(target=rock_worker, daemon=True).start()
    threading.Thread(target=beta_worker, daemon=True).start()
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()