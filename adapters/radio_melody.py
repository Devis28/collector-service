# adapters/radio_melody.py
import os, json, asyncio, urllib.request
from typing import Dict, Any

# HTTP: aktuálny song (RAW, ploché polia podľa dokumentácie)
MELODY_HTTP_SONG_URL = os.getenv(
    "MELODY_HTTP_SONG_URL",
    "https://radio-melody-api.fly.dev/song",
)

# WS: priebežný listeners (RAW) {"last_update":"DD.MM.YYYY HH:MM:SS","listeners":N}
MELODY_WS_LISTENERS = os.getenv(
    "MELODY_WS_LISTENERS",
    "wss://radio-melody-api.fly.dev/ws/listeners",
)

# Ako často pollovať song HTTP (sekundy)
MELODY_POLL_SECS = float(os.getenv("MELODY_POLL_SECS", "60"))

# ---- mapre (RAW) -------------------------------------------------------------

def _map_song_http(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Vraciame *presne* to, čo prišlo z /song (station/title/artist/date/time/last_update).
    Nič nepridávame ani nepremenovávame.
    """
    return dict(payload or {})

def _map_listeners_ws(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Vraciame *presne* to, čo posiela WS /ws/listeners:
      {"last_update":"DD.MM.YYYY HH:MM:SS","listeners": N}
    """
    return dict(d or {})

# ---- jednoduchý HTTP fetch ---------------------------------------------------

def _fetch_json(url: str, timeout: float = 10.0) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "radio-collector"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))

# ---- public register ---------------------------------------------------------

def register(add_ws_consumer, add_hook):
    """
    - HTTP poller pre song -> bronze/melody/song (RAW)
    - WebSocket listeners -> bronze/melody/listeners (RAW)
    - App páruje: 1× listeners k poslednému novému songu.
    """
    # 1) WS listeners
    add_ws_consumer(
        station="melody",
        url=MELODY_WS_LISTENERS,
        kind="listeners",
        map_fn=_map_listeners_ws,
    )

    # 2) HTTP song poller cez emit z add_hook (aby prešlo jednotnou ingest logikou a párovaním)
    emit_song = add_hook(
        station="melody",
        path="/hook/melody/song",   # interne dostupné aj ako HTTP endpoint
        kind="song",
        map_fn=_map_song_http,
    )

    async def _poll_loop():
        backoff = 2.0
        while True:
            try:
                raw = _fetch_json(MELODY_HTTP_SONG_URL, timeout=10.0) or {}
                await emit_song(raw)   # deduplikáciu a pairing rieši app.py
                backoff = 2.0
                await asyncio.sleep(MELODY_POLL_SECS)
            except asyncio.CancelledError:
                break
            except Exception:
                # exponenciálny backoff pri dočasných výpadkoch
                await asyncio.sleep(min(backoff, 30.0))
                backoff = min(backoff * 2.0, 30.0)

    # app.py spustí tento poller ako task
    return _poll_loop
