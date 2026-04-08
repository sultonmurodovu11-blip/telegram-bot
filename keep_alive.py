from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_health: dict = {
    "service": "starting",
    "bot": "starting",
    "db": "unknown",
    "last_error": "",
}
_lock = threading.Lock()


def set_health_state(**kwargs):
    with _lock:
        _health.update(kwargs)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with _lock:
            state = dict(_health)

        if self.path == "/health":
            body = str(state).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        # Loglarni jimib qo'yamiz (har ping logga tushmasin)
        pass


def keep_alive(host: str = "0.0.0.0", port: int = 8080):
    server = HTTPServer((host, port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
