import os
import json
import asyncio
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

# -----------------------------
# Konfigurácia (env premenné)
# -----------------------------
# PULL endpoint – podľa dokumentácie je LEN HTTP (nie HTTPS)
EXPRES_LISTENERS_URL = os.getenv(
    "EXPRES_LISTENERS_URL",
    "http://147.232.205.56:5010/api/current_listeners",
)
# Server generuje nové čísla cca každých 5 s
EXPRES_POLL_SECS = float(os.getenv("EXPRES_POLL_SECS", "5"))
LOG_WS_DEBUG = os.getenv("LOG_WS_DEBUG", "0") == "1"

# Voliteľné proxy (ak tvoja VM nemá egress na cieľovú IP)
HTTP_PROXY = os.getenv("EXPRES_LISTENERS_HTTP_PROXY") or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
HTTPS_PROXY = os.getenv("EXPRES_LISTENERS_HTTPS_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

CONNECT_TIMEOUT = float(os.getenv("EXPRES_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT = float(os.getenv("EXPRES_READ_TIMEOUT", "5"))


# -----------------------------
# Pomocné funkcie
# -----------------------------
def _normalize_http_url(u: str) -> str:
    """Doplní http:// ak chýba; pre danú IP vynúti HTTP (služba nemá HTTPS)."""
    if not u:
        return u
    s = u.strip()
    if not (s.startswith("http://") or s.startswith("https://")):
        s = "http://" + s.lstrip("/")
    if "147.232.205.56" in s and s.startswith("https://"):
        s = "http://" + s[len("https://") :]
    return s


def _build_opener():
    """Pripraví urllib opener s proxy, ak je nastavený."""
    handlers = []
    proxies = {}
    if HTTP_PROXY:
        proxies["http"] = HTTP_PROXY
    if HTTPS_PROXY:
        proxies["https"] = HTTPS_PROXY
    if proxies:
        handlers.append(urllib.request.ProxyHandler(proxies))
    return urllib.request.build_opener(*handlers) if handlers else None


def _fetch_json(url: str, timeout: float) -> Dict[str, Any]:
    """GET JSON z listeners API (HTTP-only)."""
    u = _normalize_http_url(url)
    req = urllib.request.Request(
        u,
        headers={
            "User-Agent": "/expres_webhook",
            "Accept": "application/json,text/plain,*/*",
            "Connection": "close",
        },
        method="GET",
    )
    opener = _build_opener()

    try:
        if opener:
            with opener.open(req, timeout=timeout) as r:
                data = r.read()
        else:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
    except urllib.error.HTTPError as e:
        if LOG_WS_DEBUG:
            print(f"[EXPRES] listeners HTTPError {e.code}: {e.reason} ({u})")
        raise
    except urllib.error.URLError as e:
        if LOG_WS_DEBUG:
            print(f"[EXPRES] listeners URLError: {e.reason} ({u})")
        raise

    # tolerantné dekódovanie
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        try:
            return json.loads(data.decode("utf-8", errors="replace"))
        except Exception:
            return {}


def _as_str(x: Any) -> str:
    return "" if x is None else str(x)


def _iso_like(s: str) -> bool:
    return isinstance(s, str) and ("T" in s or "-" in s)


# -----------------------------
# Mapovanie SONG (PUSH webhook)
# -----------------------------
def _map_song(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Očakávaný tvar podľa dokumentácie:
    {
      "song": "...",
      "artists": ["...","..."],
      "isrc": "...",
      "start_time": "YYYY-MM-DD hh:mm:ss",
      "radio": "expres"
    }
    """
    if not isinstance(payload, dict):
        return {}
    raw = dict(payload)
    low = {k.lower(): k for k in raw.keys()}

    # song
    if "song" not in raw:
        for cand in ("song", "title", "name"):
            if cand in low:
                raw["song"] = _as_str(raw[low[cand]])
                break

    # artists (list[str] alebo list[dict{name:...}])
    if "artists" not in raw:
        if "artist" in low:
            val = raw[low["artist"]]
            if isinstance(val, str):
                raw["artists"] = [val]
        elif "artists" in low:
            val = raw[low["artists"]]
            if isinstance(val, list):
                out = []
                for it in val:
                    if isinstance(it, str):
                        out.append(it)
                    elif isinstance(it, dict):
                        nm = it.get("name") or it.get("artist_name")
                        if isinstance(nm, str):
                            out.append(nm)
                if out:
                    raw["artists"] = out

    # start_time – zober priamo, ak príde pod iným kľúčom tiež akceptuj
    if "start_time" not in raw:
        for a in ("start_time", "played_at", "time", "datetime", "timestamp"):
            if a in low:
                v = _as_str(raw[low[a]])
                if v:
                    raw["start_time"] = v
                    break

    # isrc
    if "isrc" not in raw:
        for a in ("isrc", "isrc_code", "track_isrc"):
            if a in low:
                raw["isrc"] = _as_str(raw[low[a]])
                break

    # radio (ak chýba, doplň)
    if "radio" not in raw:
        raw["radio"] = "expres"

    return raw


# -----------------------------
# Mapovanie LISTENERS (PULL)
# -----------------------------
def _map_listeners(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Očakávaný tvar:
    {
      "timestamp": "YYYY-MM-DD hh:mm:ss",
      "listeners": 7840,
      "radio": "expres"
    }
    – nechávame RAW, app si pridá collected_at/rotáciu.
    """
    if not isinstance(payload, dict):
        return {}
    # minimálna validácia – ak chýba listeners alebo timestamp, zaloguj
    if LOG_WS_DEBUG:
        if "listeners" not in payload or "timestamp" not in payload:
            print(f"[EXPRES] listeners payload missing keys: {list(payload.keys())}")
    # doplň radio, ak chýba
    out = dict(payload)
    out.setdefault("radio", "expres")
    return out


# -----------------------------
# register() – integrácia do app
# -----------------------------
def register(add_ws_consumer, add_hook):
    # SONG: webhook PUSH z Expresu
    add_hook(
        station="expres",
        path="/hook/expres/song",
        kind="song",
        map_fn=_map_song,
    )

    # LISTENERS: (1) interný PUSH endpoint – môžeš ho kŕmiť z inej siete,
    #            (2) PULL poller nižšie
    emit_listeners = add_hook(
        station="expres",
        path="/hook/expres/listeners",
        kind="listeners",
        map_fn=_map_listeners,
    )

    async def _poll_loop():
        url = _normalize_http_url(EXPRES_LISTENERS_URL)
        backoff = 2.0
        if LOG_WS_DEBUG:
            proxy_dbg = f"http={HTTP_PROXY or '-'}, https={HTTPS_PROXY or '-'}"
            print(f"[EXPRES] listeners poll start -> {url} every {EXPRES_POLL_SECS}s, proxy: {proxy_dbg}")
        while True:
            try:
                raw = _fetch_json(url, timeout=CONNECT_TIMEOUT + READ_TIMEOUT) or {}
                # odošleme do vnútorného hooku -> zápis do R2 prebehne ako pri push
                await emit_listeners(raw)
                backoff = 2.0
                await asyncio.sleep(EXPRES_POLL_SECS)
            except asyncio.CancelledError:
                if LOG_WS_DEBUG:
                    print("[EXPRES] listeners poll cancelled")
                break
            except urllib.error.URLError as e:
                if LOG_WS_DEBUG:
                    print(f"[EXPRES] listeners poll URLError -> {e.reason}; retry in {backoff}s")
                await asyncio.sleep(min(backoff, 30.0))
                backoff = min(backoff * 2.0, 30.0)
            except Exception as e:
                if LOG_WS_DEBUG:
                    print(f"[EXPRES] listeners poll error {type(e).__name__}: {e}; retry in {backoff}s")
                await asyncio.sleep(min(backoff, 30.0))
                backoff = min(backoff * 2.0, 30.0)

    # app si túto korutínu spustí pri štarte
    return _poll_loop
