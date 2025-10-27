import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters import radio_rock, radio_beta, radio_funradio, radio_melody, radio_expres
from writer import save_data_to_r2

SEND_INTERVAL = 600      # interval pre upload
SONG_CHECK_INTERVAL = 30  # presný interval listeners v sekundách

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

def run_radio(cfg):
    state = {
        "last_song_signature": None,
        "last_song_session_id": None,
        "records": [],
        "listeners_records": []
    }
    t0 = time.time()
    radio = cfg["module"]
    print(f"{now_log()}[THREAD] Starting {cfg['label']}", flush=True)
    while True:
        # Zisti novú skladbu (cyklus cez skladby)
        song_data, song_signature = radio.process_and_log_song(state["last_song_signature"])
        if song_data:
            state["last_song_signature"] = song_signature
            state["last_song_session_id"] = song_data.get('song_session_id')
            state["records"].append(song_data)
            print(f"{now_log()}[{cfg['label']}] New song: {song_signature}, session_id: {state['last_song_session_id']}", flush=True)

        if state["last_song_session_id"]:
            # listeners cyklus
            anchor_time = time.time()
            first = True
            while True:
                now = time.time()
                if first or now - anchor_time >= SONG_CHECK_INTERVAL:
                    listeners_data = radio.process_and_log_listeners(song_signature=state["last_song_signature"])
                    if listeners_data:
                        listeners_data["song_session_id"] = state["last_song_session_id"]
                        state["listeners_records"].append(listeners_data)
                        print(f"{now_log()}[{cfg['label']}] Listeners: {listeners_data.get('listeners')} for session_id: {state['last_song_session_id']}", flush=True)
                    # reset anchor and continue with interval
                    anchor_time = now
                    first = False

                # Kontrola novej skladby každú 1s
                time.sleep(1)
                next_song_data, next_signature = radio.process_and_log_song(state["last_song_signature"])
                if next_song_data or next_signature != state['last_song_signature']:
                    break

        # Uloženie batchu pri SEND_INTERVAL
        if time.time() - t0 >= SEND_INTERVAL:
            if state["records"]:
                print(f"{now_log()}[WRITER] Saving {len(state['records'])} song records for {cfg['label']} to {cfg['song_prefix']}", flush=True)
                save_data_to_r2(state["records"], cfg["song_prefix"])
                state["records"] = []
            if state["listeners_records"]:
                print(f"{now_log()}[WRITER] Saving {len(state['listeners_records'])} listeners records for {cfg['label']} to {cfg['listeners_prefix']}", flush=True)
                save_data_to_r2(state["listeners_records"], cfg["listeners_prefix"])
                state["listeners_records"] = []
            t0 = time.time()

def main():
    configs = [
        {
            "module": radio_rock,
            "song_prefix": "bronze/rock/song",
            "listeners_prefix": "bronze/rock/listeners",
            "label": "ROCK"
        },
        {
            "module": radio_beta,
            "song_prefix": "bronze/beta/song",
            "listeners_prefix": "bronze/beta/listeners",
            "label": "BETA"
        },
        {
            "module": radio_funradio,
            "song_prefix": "bronze/funradio/song",
            "listeners_prefix": "bronze/funradio/listeners",
            "label": "FUNRADIO"
        },
        {
            "module": radio_melody,
            "song_prefix": "bronze/melody/song",
            "listeners_prefix": "bronze/melody/listeners",
            "label": "MELODY"
        },
        {
            "module": radio_expres,
            "song_prefix": "bronze/expres/song",
            "listeners_prefix": "bronze/expres/listeners",
            "label": "EXPRES"
        }    ]
    threads = []
    for cfg in configs:
        t = threading.Thread(target=run_radio, args=(cfg,), daemon=True)
        t.start()
        threads.append(t)
    print(f"{now_log()}[APP] All radio threads started...", flush=True)
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
