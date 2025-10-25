import time
from datetime import datetime
from zoneinfo import ZoneInfo
from adapters import radio_rock, radio_beta, radio_funradio
from writer import save_data_to_r2

SEND_INTERVAL = 600         # interval pre upload (10 minút)
SONG_CHECK_INTERVAL = 30    # interval pre kontrolu skladby (30 sekúnd)

def now_log():
    return datetime.now(ZoneInfo("Europe/Bratislava")).strftime("[%Y-%m-%d %H:%M:%S]")

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
        }
    ]
    state = {}
    t0 = time.time()
    for cfg in configs:
        state[cfg['label']] = {
            "last_song_signature": None,
            "last_song_session_id": None,
            "records": [],
            "listeners_records": []
        }
    print(f"{now_log()}[APP] Starting collector service at {datetime.now(ZoneInfo('Europe/Bratislava'))}", flush=True)
    while True:
        for cfg in configs:
            radio = cfg["module"]
            s = state[cfg["label"]]
            song_data, song_signature = radio.process_and_log_song(s["last_song_signature"])
            if song_data:
                s["last_song_signature"] = song_signature
                s["last_song_session_id"] = song_data.get('song_session_id')
                s["records"].append(song_data)

                listeners_data = radio.process_and_log_listeners(song_signature=song_signature)
                if listeners_data:
                    listeners_data["song_session_id"] = s["last_song_session_id"]
                    s["listeners_records"].append(listeners_data)

        if time.time() - t0 >= SEND_INTERVAL:
            for cfg in configs:
                s = state[cfg["label"]]
                if s["records"]:
                    print(f"{now_log()}[WRITER] Saving {len(s['records'])} song records for {cfg['label']} to {cfg['song_prefix']}", flush=True)
                    save_data_to_r2(s["records"], cfg["song_prefix"])
                    s["records"] = []
                if s["listeners_records"]:
                    print(f"{now_log()}[WRITER] Saving {len(s['listeners_records'])} listeners records for {cfg['label']} to {cfg['listeners_prefix']}", flush=True)
                    save_data_to_r2(s["listeners_records"], cfg["listeners_prefix"])
                    s["listeners_records"] = []
            t0 = time.time()
        time.sleep(SONG_CHECK_INTERVAL)

if __name__ == "__main__":
    main()
