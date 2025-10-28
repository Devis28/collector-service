# app.py
import os, json, asyncio, signal, importlib, pkgutil, pathlib
from typing import Callable, Dict, Tuple, Optional, Any
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import PlainTextResponse, JSONResponse
from writer import JsonlRotator, utc_ts
import websockets

LOG_WS_DEBUG = os.getenv("LOG_WS_DEBUG", "0") == "1"
ROTATE_MAX_LINES = int(os.getenv("ROTATE_MAX_LINES", "20000"))
ROTATE_MAX_SECS  = int(os.getenv("ROTATE_MAX_SECS",  "300"))

app = FastAPI(title="Radio Collector -> Cloudflare R2", version="1.5.8")

# ---- Stav --------------------------------------------------------------------
state = {
    "ws_tasks": [],       # bežiace WS/poll tasky
    "hook_counts": {},    # (station,kind) -> count
    "ws_counts": {},      # (station,kind) -> count
    "recent": [],         # do /healthz (max ~50)
}

# pairing[station] = {"key": str|None, "song": dict|None, "written": bool}
pairing: Dict[str, Dict] = {}
writers: Dict[tuple[str,str], JsonlRotator] = {}  # (station,kind) -> writer

# pripojíme až v startup (po registrácii adaptérov)
router = APIRouter()

def _writer(station:str, kind:str) -> JsonlRotator:
    key = (station, kind)
    if key not in writers:
        writers[key] = JsonlRotator(
            station=station, kind=kind,
            rotate_max_lines=ROTATE_MAX_LINES, rotate_max_secs=ROTATE_MAX_SECS
        )
    return writers[key]

def _bump(counter:Dict, key):
    counter[key] = counter.get(key, 0) + 1

def _remember_recent(station, kind, payload):
    try:
        state["recent"].append({
            "ts": utc_ts(), "station": station, "kind": kind,
            "preview": payload if isinstance(payload, dict) else str(payload)[:300]
        })
        state["recent"] = state["recent"][-50:]
    except Exception:
        pass

# ---- kľúč na deduplikáciu songu (z RAW) -------------------------------------
def _derive_song_key(station: str, uni: Dict[str, Any]) -> str:
    # Melody/raw (ploché)
    if all(k in uni for k in ("artist", "title", "date", "time")):
        a = uni.get("artist") or ""
        t = uni.get("title") or ""
        d = uni.get("date") or ""
        tm = uni.get("time") or ""
        return f"{a}|{t}|{d}|{tm}"

    # Rock/Fun/raw (vnorené song + last_update)
    song_obj = uni.get("song")
    if isinstance(song_obj, dict) and ("musicAuthor" in song_obj or "musicTitle" in song_obj or "startTime" in song_obj):
        a = song_obj.get("musicAuthor") or ""
        t = song_obj.get("musicTitle") or ""
        tm = song_obj.get("startTime") or ""
        lu = uni.get("last_update")
        d = lu.split(" ")[0] if isinstance(lu, str) and " " in lu else ""
        return f"{a}|{t}|{d}|{tm}"

    # Beta/raw (ploché s timestampom)
    if all(k in uni for k in ("interpreters", "title", "start_time")):
        a = uni.get("interpreters") or ""
        t = uni.get("title") or ""
        tm = uni.get("start_time") or ""
        ts = uni.get("timestamp") or ""
        d = ts.split("T")[0] if isinstance(ts, str) and "T" in ts else ""
        return f"{a}|{t}|{d}|{tm}"

    # Expres/raw: song (string), artists (list), start_time (string), isrc (string)
    if isinstance(uni.get("song"), str) and "start_time" in uni:
        song_title = uni.get("song") or ""
        artists = "|".join(uni.get("artists") or [])
        start_time = uni.get("start_time") or ""
        isrc = uni.get("isrc") or ""
        return f"{artists}|{song_title}|{start_time}|{isrc}"

    # Jazz/raw (vnorené v "song"; collected_at ignorujeme)
    if isinstance(song_obj, dict) and ("title" in song_obj) and (("artist" in song_obj) or ("artists" in song_obj)) and ("play_time" in song_obj):
        arts = song_obj.get("artist") if "artist" in song_obj else song_obj.get("artists")
        if isinstance(arts, list):
            a = "|".join(arts)
        else:
            a = str(arts or "")
        t = song_obj.get("title") or ""
        d = song_obj.get("play_date") or ""
        tm = song_obj.get("play_time") or ""
        return f"{a}|{t}|{d}|{tm}"

    # Fallback
    return json.dumps(uni, sort_keys=True)

# ---- Jednotná ingest logika --------------------------------------------------
def ingest_event(station: str, kind: str, uni: dict) -> Tuple[bool, Optional[str], Optional[dict]]:
    """
    map_fn v adaptéri môže prepísať typ udalosti cez __kind = 'song' | 'listeners'
    """
    override = uni.pop("__kind", None)
    if override in ("song", "listeners"):
        kind = override

    if kind == "song":
        # BETA: keď is_playing=false -> zapíš, ale zruš pairing (žiadny listeners k tichu)
        if station == "beta" and isinstance(uni, dict) and (uni.get("is_playing") is False):
            _writer(station, "song").write_obj(dict(uni))
            pairing.pop(station, None)
            if LOG_WS_DEBUG:
                print(f"[PAIR] BETA idle stored (no pairing).")
            return True, "song", dict(uni)

        key = _derive_song_key(station, uni)
        st = pairing.get(station, {"key": None, "song": None, "written": False})
        if key and key != st.get("key"):
            _writer(station, "song").write_obj(dict(uni))           # RAW song
            pairing[station] = {"key": key, "song": dict(uni), "written": False}
            if LOG_WS_DEBUG:
                print(f"[PAIR] NEW SONG {station} key={key} -> wrote RAW song")
            return True, "song", dict(uni)
        else:
            if LOG_WS_DEBUG:
                print(f"[PAIR] DUP SONG ignored {station} key={key}")
            return False, None, None

    elif kind == "listeners":
        st = pairing.get(station)
        if st and st.get("song") and not st.get("written"):
            _writer(station, "listeners").write_obj(dict(uni))      # RAW listeners
            st["written"] = True
            pairing[station] = st
            if LOG_WS_DEBUG:
                print(f"[PAIR] LISTENERS wrote once for {station} key={st['key']} val={uni.get('listeners')}")
            return True, "listeners", dict(uni)
        else:
            if LOG_WS_DEBUG:
                print(f"[PAIR] LISTENERS ignored for {station} (no pending or already written)")
            return False, None, None

    else:
        _writer(station, kind).write_obj(dict(uni))
        return True, kind, dict(uni)

# ---- WS consumer -------------------------------------------------------------
def add_ws_consumer(*, station:str, url:str, kind:str, map_fn:Callable[[dict],dict]):
    async def _consume():
        backoff = 2
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                    if LOG_WS_DEBUG:
                        print(f"[WS] connected {station}/{kind} -> {url}")
                    backoff = 2
                    async for msg in ws:
                        data = json.loads(msg) if isinstance(msg, (str, bytes)) else msg
                        uni = map_fn(data) or {}
                        if not uni:
                            continue
                        wrote, row_kind, row_payload = ingest_event(station, kind, uni)
                        if wrote and row_kind and row_payload is not None:
                            _bump(state["ws_counts"], (station, row_kind))
                            _remember_recent(station, row_kind, row_payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if LOG_WS_DEBUG:
                    print(f"[WS] {station}/{kind} error: {type(e).__name__}: {e}")
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff*2, 30)
    state["ws_tasks"].append(asyncio.create_task(_consume()))

# ---- Hook + emit pre adaptér -------------------------------------------------
def add_hook(*, station:str, path:str, kind:str, map_fn:Callable[[dict],dict]):
    @router.post(path)
    @router.post(path + "/")   # toleruj aj trailing slash
    async def _hook(req: Request):
        try:
            body = await req.json()
        except Exception:
            return JSONResponse({"error": "invalid json"}, status_code=400)

        uni = map_fn(body) or {}
        if not uni:
            return {"ok": True}
        wrote, row_kind, row_payload = ingest_event(station, kind, uni)
        if wrote and row_kind and row_payload is not None:
            _bump(state["hook_counts"], (station, row_kind))
            _remember_recent(station, row_kind, row_payload)
        return {"ok": True}

    async def emit(payload: dict):
        uni = map_fn(payload) or {}
        if not uni:
            return
        wrote, row_kind, row_payload = ingest_event(station, kind, uni)
        if wrote and row_kind and row_payload is not None:
            _bump(state["hook_counts"], (station, row_kind))
            _remember_recent(station, row_kind, row_payload)

    return emit

# univerzálny RAW hook
@router.post("/hook/{station}/{kind}")
@router.post("/hook/{station}/{kind}/")
async def generic_hook(station: str, kind: str, req: Request):
    try:
        body = await req.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    if not body:
        return {"ok": True}
    wrote, row_kind, row_payload = ingest_event(station, kind, body)
    if wrote and row_kind and row_payload is not None:
        _bump(state["hook_counts"], (station, row_kind))
        _remember_recent(station, row_kind, row_payload)
    return {"ok": True}

# ---- Načítanie adapterov -----------------------------------------------------
def load_adapters():
    tasks = []
    pkg = "adapters"
    base = (pathlib.Path(__file__).parent / pkg).resolve()
    print(f"[BOOT] scanning adapters dir: {base}")
    if not base.exists():
        print("[BOOT] adapters folder NOT found in container – check .dockerignore / COPY")
        return tasks
    mods = [name for _, name, ispkg in pkgutil.iter_modules([str(base)]) if not ispkg]
    print(f"[BOOT] found adapter modules: {mods}")
    for mod in mods:
        try:
            m = importlib.import_module(f"{pkg}.{mod}")
            if hasattr(m, "register"):
                maybe = m.register(add_ws_consumer, add_hook)
                if callable(maybe):
                    tasks.append(asyncio.create_task(maybe()))
                print(f"[BOOT] adapter registered: {mod}")
            else:
                print(f"[BOOT] adapter {mod} has no register()")
        except Exception as e:
            print(f"[BOOT] adapter load failed: {mod}: {type(e).__name__}: {e}")
    return tasks

# ---- Endpoints ---------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {
        "ok": True,
        "ws_counts": {f"{k[0]}/{k[1]}": v for k,v in state["ws_counts"].items()},
        "hook_counts": {f"{k[0]}/{k[1]}": v for k,v in state["hook_counts"].items()},
        "recent": state["recent"],
        "pairing": {st: {"key": v.get("key"), "written": v.get("written", False)} for st,v in pairing.items()},
    }

@app.get("/")
def root():
    return PlainTextResponse("radio-collector -> R2 OK\n")

# ---- Startup/Shutdown --------------------------------------------------------
@app.on_event("startup")
async def _startup():
    extra_tasks = load_adapters()
    state["ws_tasks"].extend(extra_tasks)
    app.include_router(router)  # pripoj až po registrácii hookov

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_shutdown()))

async def _shutdown():
    for t in list(state["ws_tasks"]):
        t.cancel()
    try:
        await asyncio.wait(state["ws_tasks"], timeout=5)
    except Exception:
        pass
    for w in list(writers.values()):
        try: w.close()
        except: pass
