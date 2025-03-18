import time
import threading
from queue import Queue
from datetime import datetime

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from fireball_clustering.data_ingestion.local_fetcher import ingestFromTarball
from fireball_clustering.database.db_writes import insertFRs, insertFieldsums

class UploadHandler(FileSystemEventHandler):
    def __init__(self, queue) -> None:
        super().__init__()
        self.queue = queue

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if isinstance(event.src_path, str) and event.src_path.endswith(".tar.bz2"):
            self.queue.put(event.src_path)

class FileWatcher():
    def __init__(self) -> None:
        self.queue = Queue()

        # File event watching and handling (producer)
        self.event_handler = UploadHandler(self.queue)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, "/home/armaan/school/Thesis/gmn-fireball-clustering/", recursive=True)
        self.observer.start()

        # Queue handler
        self.consumer = QueueConsumer(self.queue)
        self.consumer.start()
    
    def start_file_watcher(self):
        try:
            while True:
                time.sleep(1)
        finally:
            self.observer.stop()
            self.observer.join()
            self.consumer.join()
        
class QueueConsumer():
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.thread = threading.Thread(target=self.consumer_loop)

    def consumer_loop(self):
        while True:
            src_path = self.queue.get(timeout=10) if not self.queue.empty() else None
            if src_path == None:
                continue

            station_data, fr_files = ingestFromTarball(src_path)
            print(station_data.getDataframe())
            
            split_path = src_path.split('_')
            station_id = split_path[0]    
            date_str = split_path[1]
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            insertFieldsums(station_id, date_obj, station_data)
            insertFRs(station_id, date_obj, fr_files)

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()

def main():
    file_watcher = FileWatcher()
    file_watcher.start_file_watcher()

if __name__ == "__main__":
    main()
