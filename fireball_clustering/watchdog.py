import os
import time
import threading
from queue import Queue
from datetime import datetime

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from fireball_clustering.data_ingestion.local_fetcher import ingestFromTarball
from fireball_clustering.database.db_writes import insertFRs, insertFieldsums, setDataToIngested
from fireball_clustering import parameters

# Handler for FS upload events
class UploadHandler(FileSystemEventHandler):
    def __init__(self, queue) -> None:
        super().__init__()
        self.queue = queue

    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if isinstance(event.src_path, str) and event.src_path.endswith(".tar.bz2"):
            self.queue.put(event.src_path)
            print(f"Added {event.src_path} to the queue for ingestion.")

# Starts producer(FS upload handler) and consumer(FS ingestion) threads
class FileWatcher():
    def __init__(self) -> None:
        print("FILEWATCHERINIT???")
        self.queue = Queue()

        # File event watching and handling (producer)
        self.event_handler = UploadHandler(self.queue)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, parameters.PATH, recursive=True)
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

class FileWatcherProducer():
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.thread = threading.Thread(target=self.producer_loop)
        self.latest_timestamp = time.time()

    def producer_loop(self):
        while True:
            time.sleep(5)
            # Check for new files by comparing to most recent upload
            new_latest_timestamp = self.latest_timestamp
            for root, _, files in os.walk(parameters.PATH):
                for file in files:
                    path = os.path.join(root, file)
                    stat = os.stat(path)
                    if path.endswith('tar.bz2') and stat.st_mtime > self.latest_timestamp:
                        self.queue.put(path)
                        new_latest_timestamp = max(new_latest_timestamp, stat.st_mtime)
            self.latest_timestamp = new_latest_timestamp

class QueueConsumer():
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.thread = threading.Thread(target=self.consumer_loop)

    def consumer_loop(self):
        while True:
            src_path = self.queue.get(timeout=10) if not self.queue.empty() else None
            if src_path == None:
                continue
            print(f'Ingesting files from {src_path}')
            station_data, fr_files = ingestFromTarball(src_path)
            
            # src_path of format path/to/fieldsums/dir/AU000X_239123_19.tar.bz2
            split_path = src_path.split('_')
            station_id_path = split_path[0]
            station_id = station_id_path.split('/')[1]
            date_str = split_path[1]
            date_obj = datetime.strptime(date_str, '%Y%m%d')

            insertFieldsums(station_id, date_obj, station_data)
            insertFRs(station_id, date_obj, fr_files)
            setDataToIngested([(station_id, date_obj)])
            print(f'Files ingested from {src_path}')
            

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()
