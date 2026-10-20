import threading
import time
import webbrowser
import os

from video_studio_signature_fix import app, HOST, PORT, init_db, DOWNLOAD_DIR

def run_server():
    init_db()
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    app.run(host=HOST, port=PORT, debug=False, threaded=True)

if __name__ == '__main__':
    t = threading.Thread(target=run_server, daemon=True)
    t.start()
    
    time.sleep(2.5)
    webbrowser.open(f'http://{HOST}:{PORT}')
    
    while True:
        time.sleep(1)
