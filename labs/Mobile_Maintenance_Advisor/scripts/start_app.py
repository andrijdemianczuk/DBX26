#!/usr/bin/env python3
"""
Start script for running frontend and backend processes concurrently.

Requirements:
1. Not reporting ready until BOTH frontend and backend processes are ready
2. Exiting as soon as EITHER process fails
3. Printing error logs if either process fails

Usage:
    start-app [OPTIONS]

All options are passed through to the backend server (start-server).
See 'uv run start-server --help' for available options.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv

# Readiness patterns
BACKEND_READY = [r"Uvicorn running on", r"Application startup complete", r"Started server process"]
FRONTEND_READY = [r"Server is running on http://localhost"]


class ProcessManager:
    def __init__(self, port=8000):
        self.backend_process = None
        self.frontend_process = None
        self.backend_ready = False
        self.frontend_ready = False
        self.failed = threading.Event()
        self.backend_log = None
        self.frontend_log = None
        self.port = port

    def start_process(self, cmd, name, log_file, ready_patterns, cwd=None):
        """Start a subprocess and begin monitoring its stdout for readiness."""
        print(f"Starting {name}: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=cwd,
            env=os.environ.copy(),
        )
        t = threading.Thread(
            target=self.monitor_process,
            args=(proc, name, log_file, ready_patterns),
            daemon=True,
        )
        t.start()
        return proc

    def print_logs(self, filepath, max_lines=200):
        """Best-effort log dump for troubleshooting in the Apps UI."""
        try:
            if not Path(filepath).exists():
                return
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            tail = lines[-max_lines:] if len(lines) > max_lines else lines
            print("\n" + "-" * 42)
            print(f"Last {len(tail)} lines of {filepath}:")
            print("-" * 42)
            for l in tail:
                print(l.rstrip())
        except Exception as e:
            print(f"Could not read {filepath}: {e}")

    def monitor_process(self, process, name, log_file, patterns):
        is_ready = False
        try:
            for line in iter(process.stdout.readline, ""):
                if not line:
                    break

                line = line.rstrip()
                log_file.write(line + "\n")
                print(f"[{name}] {line}")

                # Check readiness
                if not is_ready and any(re.search(p, line, re.IGNORECASE) for p in patterns):
                    is_ready = True
                    if name == "backend":
                        self.backend_ready = True
                    else:
                        self.frontend_ready = True
                    print(f"✓ {name.capitalize()} is ready!")

                    if self.backend_ready and self.frontend_ready:
                        print("\n" + "=" * 50)
                        print("✓ Both frontend and backend are ready!")
                        print(f"✓ Open the frontend at http://localhost:{self.port}")
                        print("=" * 50 + "\n")

            process.wait()
            if process.returncode != 0:
                self.failed.set()

        except Exception as e:
            print(f"Error monitoring {name}: {e}")
            self.failed.set()

    def clone_frontend_if_needed(self):
        """Legacy hook.

        The original template clones `e2e-chatbot-app-next` at runtime.

        This project instead vendors a local frontend in `chat-ui/` so:
        - you can customize it freely
        - builds are reproducible
        - the app can run without git access

        This function keeps the old name to avoid changing other logic.
        """
        if Path("chat-ui").exists():
            return True

        print("ERROR: Missing ./chat-ui frontend folder.")
        print("Expected a local frontend at: chat-ui/")
        return False
    def cleanup(self):
        print("\n" + "=" * 42)
        print("Shutting down both processes...")
        print("=" * 42)

        for proc in [self.backend_process, self.frontend_process]:
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except (subprocess.TimeoutExpired, Exception):
                    proc.kill()

        if self.backend_log:
            self.backend_log.close()
        if self.frontend_log:
            self.frontend_log.close()

    def run(self, backend_args=None):
        load_dotenv(dotenv_path=".env.local", override=True)

        if not self.clone_frontend_if_needed():
            return 1

        # Set API_PROXY environment variable for frontend to connect to backend
        os.environ["API_PROXY"] = f"http://localhost:{self.port}/invocations"

        # Open log files
        self.backend_log = open("backend.log", "w", buffering=1)
        self.frontend_log = open("frontend.log", "w", buffering=1)

        try:
            # Build backend command, passing through all arguments
            backend_cmd = ["uv", "run", "start-server"]
            if backend_args:
                backend_cmd.extend(backend_args)

            # Start backend
            self.backend_process = self.start_process(
                backend_cmd, "backend", self.backend_log, BACKEND_READY
            )

            # Setup and start frontend
            frontend_dir = Path("chat-ui")
            for cmd, desc in [("npm install", "install"), ("npm run build", "build")]:
                print(f"Running npm {desc}...")
                result = subprocess.run(
                    cmd.split(), cwd=frontend_dir, capture_output=True, text=True
                )
                if result.returncode != 0:
                    print(f"npm {desc} failed: {result.stderr}")
                    return 1

            self.frontend_process = self.start_process(
                ["npm", "run", "start"],
                "frontend",
                self.frontend_log,
                FRONTEND_READY,
                cwd=frontend_dir,
            )

            print(
                f"\nMonitoring processes (Backend PID: {self.backend_process.pid}, Frontend PID: {self.frontend_process.pid})\n"
            )

            # Wait for failure
            while not self.failed.is_set():
                time.sleep(0.1)
                for proc in [self.backend_process, self.frontend_process]:
                    if proc.poll() is not None:
                        self.failed.set()
                        break

            # Determine which failed
            failed_name = "backend" if self.backend_process.poll() is not None else "frontend"
            failed_proc = (
                self.backend_process if failed_name == "backend" else self.frontend_process
            )
            exit_code = failed_proc.returncode if failed_proc else 1

            print(
                f"\n{'=' * 42}\nERROR: {failed_name} process exited with code {exit_code}\n{'=' * 42}"
            )
            self.print_logs("backend.log")
            self.print_logs("frontend.log")
            return exit_code

        except KeyboardInterrupt:
            print("\nInterrupted")
            return 0

        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Start agent frontend and backend",
        usage="%(prog)s [OPTIONS]\n\nAll options are passed through to start-server. "
        "Use 'uv run start-server --help' for available options."
    )
    # Parse known args (none currently) and pass remaining to backend
    _, backend_args = parser.parse_known_args()

    # Extract port from backend_args if specified
    port = 8000
    for i, arg in enumerate(backend_args):
        if arg == "--port" and i + 1 < len(backend_args):
            try:
                port = int(backend_args[i + 1])
            except ValueError:
                pass
            break

    sys.exit(ProcessManager(port=port).run(backend_args))


if __name__ == "__main__":
    main()
