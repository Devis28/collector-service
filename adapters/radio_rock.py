import asyncio
import websockets
import json

async def debug_ws():
    uri = "wss://rock-server.fly.dev/ws/push/listenership"
    async with websockets.connect(uri) as websocket:
        print("Connected! Printing all incoming messages:")
        while True:
            try:
                msg = await asyncio.wait_for(websocket.recv(), timeout=30)
                print("RAW WS MSG:", msg)
                data = json.loads(msg)
                print("PARSED:", data)
            except Exception as e:
                print("ERROR/WS-TIMEOUT:", e)
                break

if __name__ == "__main__":
    asyncio.run(debug_ws())
