"""Exercise the built menu against an isolated gateway, never live credentials."""

import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


def main():
    root = Path(__file__).resolve().parent.parent
    menu = root / "bin/Claude Gateway Menu.app/Contents/MacOS/GatewayMenu"
    proxy = root / "bin/claude-proxy"
    calls = []

    class NeverCalled(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(500)
            self.end_headers()

        do_POST = do_GET

        def log_message(self, *_):
            pass

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), NeverCalled)
    Thread(target=upstream.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory(prefix="gateway-menu-smoke-") as directory:
            path = Path(directory)
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            config = {
                "version": 1, "listen": f"127.0.0.1:{port}", "stateDir": "./state",
                "client": {"command": ["must-not-run"], "configDir": "./client", "model": "worker"},
                "providers": {"local": {"protocol": "openai-chat-completions", "billing": "local", "baseURL": f"http://127.0.0.1:{upstream.server_port}", "auth": {"type": "env", "name": "SMOKE_UNUSED_TOKEN"}}},
                "models": {"worker": {"provider": "local", "upstream": "test-model"}},
                "chains": {"worker": {"steps": [{"model": "worker"}]}}, "aliases": {"worker": "worker"},
            }
            config_path, menu_path = path / "gateway.json", path / "menubar.json"
            config_path.write_text(json.dumps(config))
            menu_path.write_text(json.dumps({"version": 1, "gatewayURL": f"http://127.0.0.1:{port}"}))
            environment = {k: v for k, v in os.environ.items() if not k.startswith(("ANTHROPIC_", "CLAUDE_"))}
            environment.pop("SMOKE_UNUSED_TOKEN", None)
            with (path / "server.log").open("wb") as log:
                process = subprocess.Popen([str(proxy), "--config", str(config_path), "serve"], env=environment, stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        if process.poll() is not None:
                            raise RuntimeError("isolated gateway exited during startup")
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                                break
                        except OSError:
                            if time.monotonic() > deadline:
                                raise RuntimeError("isolated gateway did not start")
                            time.sleep(0.05)
                    result = subprocess.run([str(menu), "--config", str(menu_path), "--check"], env=environment, capture_output=True, text=True, timeout=15)
                    if result.returncode != 0 or "Dashboard v1: 0 accounts, 1 providers" not in result.stdout or calls:
                        raise RuntimeError("menu/gateway contract failed or provider was contacted")
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                offline = subprocess.run([str(menu), "--config", str(menu_path), "--check"], env=environment, capture_output=True, timeout=15)
                if offline.returncode == 0:
                    raise RuntimeError("offline gateway incorrectly appeared healthy")

                class Redirect(BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.send_response(302)
                        self.send_header("Location", f"http://127.0.0.1:{upstream.server_port}/must-not-follow")
                        self.end_headers()

                    def log_message(self, *_):
                        pass

                redirect = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
                Thread(target=redirect.serve_forever, daemon=True).start()
                try:
                    menu_path.write_text(json.dumps({"gatewayURL": f"http://127.0.0.1:{redirect.server_port}"}))
                    result = subprocess.run([str(menu), "--config", str(menu_path), "--check"], env=environment, capture_output=True, timeout=15)
                    if result.returncode == 0 or calls:
                        raise RuntimeError("menu accepted or followed a gateway redirect")
                finally:
                    redirect.shutdown()
                    redirect.server_close()
        print("MENUBAR_SMOKE_PASSED: real dashboard, custom port, offline/redirect rejection, zero provider calls")
    finally:
        upstream.shutdown()
        upstream.server_close()


if __name__ == "__main__":
    main()
