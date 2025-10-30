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
from adapters.radio_funradio import (
    get_current_song as get_song_funradio,
    get_current_listeners as get_listeners_funradio,
    log_radio_event as log_funradio_event,
)
from adapters.radio_vlna import (
    get_current_song as get_song_vlna,
    get_current_listeners as get_listeners_vlna,
    log_radio_event as log_vlna_event
)
from adapters.radio_beta import (
    get_current_song as get_song_beta,
    get_current_listeners as get_listeners_beta,
    log_radio_event as log_beta_event
)
from adapters.radio_expres import (
    get_current_song as get_song_expres,
    get_current_listeners as get_listeners_expres,
    log_radio_event as log_expres_event,
)
from writer import upload_file
from adapters.radio_melody import flatten_song as flatten_melody_song, flatten_listener as flatten_melody_listener

INTERVAL = 40
BATCH_TIME = 7200

def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def melody_worker():
    RADIO_NAME = "MELODY"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_key = None
    session_id = None

    while True:
        current_song = get_song_melody()
        # hra ideme podla raw
        raw = current_song.get("raw", {})
        title = raw.get("title")
        artist = raw.get("artist")
        key = (title, artist)

        if previous_key != key:
            session_id = str(uuid.uuid4())
            previous_key = key
            current_song["song_session_id"] = session_id
            log_melody_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(flatten_melody_song(current_song))

        listeners_data = asyncio.run(get_listeners_melody(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        log_melody_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {raw_list.get('listeners', '?')}",
            session_id
        )
        listeners_data_batch.append(flatten_melody_listener(listeners_data))

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
            log_rock_event(RADIO_NAME, f"Zachytená skladba: {title} | {author}", session_id)
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

def funradio_worker():
    RADIO_NAME = "FUNRADIO"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_title = None
    previous_author = None
    session_id = None

    while True:
        current_song = get_song_funradio()
        song = current_song.get("song", {})
        title = song.get("musicTitle")
        author = song.get("musicAuthor")

        if title != previous_title or author != previous_author:
            session_id = str(uuid.uuid4())
            previous_title = title
            previous_author = author
            current_song["song_session_id"] = session_id
            log_funradio_event(RADIO_NAME, f"Zachytená skladba: {title} | {author}", session_id)
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_listeners_funradio(session_id))
        log_funradio_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {listeners_data.get('listeners', '?')}",
            session_id
        )
        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_funradio_song.json"
            listeners_path_local = f"{timestamp}_funradio_listeners.json"
            song_path_r2 = f"bronze/FUNRADIO/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/FUNRADIO/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_funradio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_funradio_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)

def vlna_worker():
    RADIO_NAME = "VLNA"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    last_song = None
    session_id = None

    while True:
        current_song = get_song_vlna()
        raw = current_song.get("raw", {})
        title = raw.get("song")
        artist = raw.get("artist")

        if last_song != (title, artist):
            session_id = str(uuid.uuid4())
            last_song = (title, artist)
            current_song["song_session_id"] = session_id
            log_vlna_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_listeners_vlna(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        log_vlna_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {raw_list.get('listeners', '?')}",
            session_id
        )
        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_vlna_song.json"
            listeners_path_local = f"{timestamp}_vlna_listeners.json"
            song_path_r2 = f"bronze/VLNA/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/VLNA/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_vlna_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_vlna_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)

def beta_worker():
    RADIO_NAME = "BETA"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    last_song_data = None
    session_id = None

    while True:
        current_song = get_song_beta()
        raw = current_song.get("raw", {})
        # Ak hrá, extrahuj z raw záznamu identifikátory songu pre zmenu sklady
        if raw.get("is_playing", True) is False:
            # Idle stav - vždy sa zaznamenáva, každý nový idle je nová session
            song_key = (raw.get("radio"), raw.get("is_playing"), raw.get("timestamp"))
        else:
            song_key = (
                raw.get("radio"),
                raw.get("interpreters"),
                raw.get("title"),
                raw.get("start_time"),
            )

        if last_song_data != song_key:
            session_id = str(uuid.uuid4())
            last_song_data = song_key
            current_song["song_session_id"] = session_id
            log_beta_event(
                RADIO_NAME,
                f"Zachytená skladba: {raw.get('title', 'interpreters')}" if raw.get("is_playing", True) else "Nothing is playing",
                session_id,
            )
            song_data_batch.append(current_song)

        listeners_data = asyncio.run(get_listeners_beta(session_id))
        listeners_data["song_session_id"] = session_id
        raw_list = listeners_data.get("raw", {})
        log_beta_event(
            RADIO_NAME,
            f"Zachytení poslucháči: {raw_list.get('listeners', '?')}",
            session_id,
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

def expres_worker():
    RADIO_NAME = "EXPRES"
    last_batch_time = time.time()
    song_data_batch = []
    listeners_data_batch = []
    previous_song = None
    session_id = None

    while True:
        current_song = get_song_expres()
        song = current_song["data"]
        title = song.get("song")
        artist = ", ".join(song.get("artists", []))

        # Použijeme kombináciu titulu a umelca na detekciu zmeny
        current_song_identifier = f"{title}_{artist}"

        if title and (current_song_identifier != previous_song):
            session_id = current_song.get("song_session_id", str(uuid.uuid4()))
            previous_song = current_song_identifier
            current_song["song_session_id"] = session_id
            log_expres_event(RADIO_NAME, f"Zachytená skladba: {title} | {artist}", session_id)
            song_data_batch.append(current_song)

        # Získanie poslucháčov - SYNCHRONNE, nie asynchrónne!
        listeners_data = get_listeners_expres(session_id)

        # Logovanie podľa dostupnosti dát
        if listeners_data.get('raw_valid') and listeners_data.get('listeners') is not None:
            log_expres_event(
                RADIO_NAME,
                f"Zachytení poslucháči: {listeners_data.get('listeners')}",
                session_id
            )
        else:
            log_expres_event(
                RADIO_NAME,
                "Nepodarilo sa získať dáta o poslucháčoch",
                session_id
            )

        listeners_data_batch.append(listeners_data)

        if time.time() - last_batch_time >= BATCH_TIME:
            now = datetime.now(ZoneInfo("Europe/Bratislava"))
            date_str = now.strftime("%d-%m-%Y")
            timestamp = now.strftime("%d-%m-%YT%H-%M-%S")
            song_path_local = f"{timestamp}_expres_song.json"
            listeners_path_local = f"{timestamp}_expres_listeners.json"
            song_path_r2 = f"bronze/EXPRES/song/{date_str}/{timestamp}.json"
            listeners_path_r2 = f"bronze/EXPRES/listeners/{date_str}/{timestamp}.json"

            save_json(song_data_batch, song_path_local)
            save_json(listeners_data_batch, listeners_path_local)

            upload_file(song_path_local, song_path_r2)
            upload_file(listeners_path_local, listeners_path_r2)

            log_expres_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {song_path_r2}", session_id)
            log_expres_event(RADIO_NAME, f"Dáta nahrané do Cloudflare: {listeners_path_r2}", session_id)

            song_data_batch.clear()
            listeners_data_batch.clear()
            last_batch_time = time.time()

        time.sleep(INTERVAL)


def main():
    from adapters.radio_expres import start_expres_webhook

    # Spusti Flask server pre Expres webhook
    start_expres_webhook()

    threading.Thread(target=melody_worker, daemon=True).start()
    threading.Thread(target=rock_worker, daemon=True).start()
    threading.Thread(target=funradio_worker, daemon=True).start()
    threading.Thread(target=vlna_worker, daemon=True).start()
    threading.Thread(target=beta_worker, daemon=True).start()
    threading.Thread(target=expres_worker, daemon=True).start()

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()