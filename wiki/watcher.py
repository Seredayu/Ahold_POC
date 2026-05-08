import subprocess
import sys
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

RESEARCH_DIR = Path(__file__).parent.parent / 'research'
INGEST_SCRIPT = Path(__file__).parent / 'ingest.py'
SUPPORTED = {'.md', '.txt', '.pdf', '.docx', '.pptx', '.xlsx', '.png', '.jpg', '.jpeg'}
DEBOUNCE_SECONDS = 3


class ResearchHandler(FileSystemEventHandler):
    def __init__(self):
        self._pending: dict[str, float] = {}

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def _schedule(self, path: str) -> None:
        if Path(path).suffix.lower() in SUPPORTED:
            self._pending[path] = time.time()

    def flush(self) -> None:
        now = time.time()
        ready = [p for p, t in self._pending.items() if now - t >= DEBOUNCE_SECONDS]
        for path in ready:
            del self._pending[path]
            print(f'Wiki: ingesting {Path(path).name}...')
            subprocess.run([sys.executable, str(INGEST_SCRIPT), path], check=False)


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
