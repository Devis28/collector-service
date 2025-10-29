import asyncio
import datetime
import json
from zoneinfo import ZoneInfo

import websockets


async def get_current_listeners(session_id=None):
    listeners_data = None
    uri = "wss://rock-server.fly.dev/ws/push/status"

    try:
        async with websockets.connect(uri, ping_timeout=10, close_timeout=10) as websocket:
            print("Connected to ROCK WebSocket for listeners")

            # Nastavíme timeout na celú operáciu
            async with asyncio.timeout(15):
                async for message in websocket:
                    data = json.loads(message)
                    msg_type = data.get('type', 'unknown')

                    if msg_type == 'listeners_update':
                        print(f"Received listeners update: {data.get('listeners')}")
                        listeners_data = {
                            "listeners": data.get("listeners"),
                            "last_update": data.get("last_update"),
                            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
                            "raw_valid": True,
                            "song_session_id": session_id
                        }
                        break

    except asyncio.TimeoutError:
        print("Timeout: No listeners update received within 15 seconds")
    except websockets.exceptions.ConnectionClosed:
        print("WebSocket connection closed unexpectedly")
    except Exception as e:
        print(f"Error in ROCK WebSocket: {e}")

    if listeners_data is None:
        listeners_data = {
            "listeners": None,
            "recorded_at": datetime.now(ZoneInfo("Europe/Bratislava")).strftime("%d.%m.%Y %H:%M:%S"),
            "raw_valid": False,
            "song_session_id": session_id
        }

    return listeners_data