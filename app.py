import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters import radio_rock, radio_beta, radio_funradio, radio_melody, radio_expres
from writer import save_data_to_r2

SEND_INTERVAL = 7200      # interval pre upload (v sekundách)
SONG_CHECK_INTERVAL = 30  # defaultný interval listeners ak nie je špecifikovaný

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
    interval = cfg.get("interval", SONG_CHECK_INTERVAL)

    print(f"{now_log()}[THREAD] Starting {cfg['label']}", flush=True)
    while True:
        try:
            # Nová skladba
            song_data, song_signature = radio.process_and_log_song(state["last_song_signature"])
            if song_data:
                state["last_song_signature"] = song_signature
                state["last_song_session_id"] = song_data.get('song_session_id')
                state["records"].append(song_data)
                print(f"{now_log()}[{cfg['label']}] New song: {song_signature}, session_id: {state['last_song_session_id']}", flush=True)

            # Listeners cyklus (pre všetky rádiá)
            if state["last_song_session_id"]:
                listeners_data = radio.process_and_log_listeners(song_signature=state["last_song_signature"])
                if listeners_data:
                    listeners_data["song_session_id"] = state["last_song_session_id"]
                    state["listeners_records"].append(listeners_data)
                    count = listeners_data.get('listeners', 'Unknown')
                    print(f"{now_log()}[{cfg['label']}] Listeners: {count}", flush=True)

            # Upload batchu po SEND_INTERVAL
            if time.time() - t0 >= SEND_INTERVAL:
                if state["records"]:
                    print(f"{now_log()}[WRITER] Saving {len(state['records'])} songs for {cfg['label']}", flush=True)
                    save_data_to_r2(state["records"], cfg["song_prefix"])
                    state["records"] = []
                if state["listeners_records"]:
                    print(f"{now_log()}[WRITER] Saving {len(state['listeners_records'])} listeners for {cfg['label']}", flush=True)
                    save_data_to_r2(state["listeners_records"], cfg["listeners_prefix"])
                    state["listeners_records"] = []
                t0 = time.time()

            time.sleep(interval)

        except Exception as e:
            print(f"{now_log()}[{cfg['label']}] ERROR: {e}", flush=True)
            time.sleep(10)

def main():
    print(f"{now_log()}[APP] Starting Expres Flask webhook server...", flush=True)
    radio_expres.start_background_flask()
    time.sleep(3)

    configs = [
        {
            "module": radio_rock,
            "song_prefix": "bronze/rock/song",
            "listeners_prefix": "bronze/rock/listeners",
            "label": "ROCK",
            "interval": 30,
        },
        {
            "module": radio_beta,
            "song_prefix": "bronze/beta/song",
            "listeners_prefix": "bronze/beta/listeners",
            "label": "BETA",
            "interval": 60,  # 3 minúty (zvýšené pre BETA)
        },
        {
            "module": radio_funradio,
            "song_prefix": "bronze/funradio/song",
            "listeners_prefix": "bronze/funradio/listeners",
            "label": "FUNRADIO",
            "interval": 30,
        },
        {
            "module": radio_melody,
            "song_prefix": "bronze/melody/song",
            "listeners_prefix": "bronze/melody/listeners",
            "label": "MELODY",
            "interval": 30,
        },
        {
            "module": radio_expres,
            "song_prefix": "bronze/expres/song",
            "listeners_prefix": "bronze/expres/listeners",
            "label": "EXPRES",
            "interval": 30,
        }
    ]

    threads = []
    for cfg in configs:
        t = threading.Thread(target=run_radio, args=(cfg,), daemon=True)
        t.start()
        threads.append(t)

    print(f"{now_log()}[APP] All radio threads started. Expres webhook: http://68.183.213.156:8000/expres_webhook", flush=True)
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
