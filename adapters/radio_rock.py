import time

def fetch_current_song():
    retries = 3
    for i in range(retries):
        try:
            resp = requests.get(SONG_URL, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Attempt {i+1}/{retries} failed: {e}")
            time.sleep(5)
    return None
