import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from ingest import ALL_SUPPORTED_EXTENSIONS as SUPPORTED

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

RESEARCH_DIR = Path(__file__).parent.parent / 'research'
INGEST_SCRIPT = Path(__file__).parent / 'ingest.py'
WIKI_DIR = Path(__file__).parent
DEBOUNCE_SECONDS = 3


class ResearchHandler(FileSystemEventHandler):
    def __init__(self):
        self._pending: dict[str, float] = {}
        self._lock = threading.Lock()

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def _schedule(self, path: str) -> None:
        if Path(path).suffix.lower() in SUPPORTED:
            with self._lock:
                self._pending[path] = time.time()

    def flush(self) -> None:
        now = time.time()
        with self._lock:
            ready = [p for p, t in self._pending.items() if now - t >= DEBOUNCE_SECONDS]
            for path in ready:
                del self._pending[path]
        for path in ready:
            print(f'Wiki: ingesting {Path(path).name}...')
            result = subprocess.run(
                ['uv', 'run', '--project', str(WIKI_DIR), 'python', str(INGEST_SCRIPT), path],
                check=False,
            )
            if result.returncode != 0:
                with self._lock:
                    self._pending[path] = time.time()  # re-enqueue for retry


if __name__ == '__main__':
    handler = ResearchHandler()
    observer = Observer()
    observer.schedule(handler, str(RESEARCH_DIR), recursive=True)
    observer.start()
    print(f'Watching {RESEARCH_DIR} for changes (Ctrl-C to stop)...')
    try:
        while True:
            time.sleep(1)
            handler.flush()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
