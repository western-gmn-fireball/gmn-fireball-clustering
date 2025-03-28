import os
import time
import threading
import traceback
from queue import Queue
from datetime import datetime

from fireball_clustering.data_ingestion.local_fetcher import ingestFromTarball
from fireball_clustering.database.db_writes import insertFRs, insertFieldsums, setDataToIngested
from fireball_clustering import parameters

# Starts producer(FS upload handler) and consumer(FS ingestion) threads
class FileWatcher():
    def __init__(self) -> None:
        self.queue = Queue()

        # File event watching and handling (producer)
        self.observer = FileWatcherProducer(self.queue)
        self.observer.start()

        # Queue handler
        self.consumer = QueueConsumer(self.queue)
        self.consumer.start()
    
    def start_file_watcher(self):
        try:
            while True:
                time.sleep(1)
        finally:
            # self.observer.stop()
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
            for path, mtime in self.fast_scan(parameters.PATH):
                if mtime > self.latest_timestamp:
                    self.queue.put(path)
                    print(f'[Watchdog] PUT path: {path} in the queue.')
                    new_latest_timestamp = max(mtime, new_latest_timestamp)
            self.latest_timestamp = new_latest_timestamp

    def fast_scan(self, dir):
        with os.scandir(dir) as it:
            for entry in it:
                entry_stat = entry.stat()

                if entry.is_file() and entry.path.endswith('tar.bz2'):
                    yield entry.path, entry_stat.st_mtime
                
                # Only check processed directory if it has been modified recently
                if entry.is_dir() and entry.name.lower() == 'processed' and entry_stat.st_mtime > self.latest_timestamp:
                    yield from self.fast_scan(entry.path)
                elif entry.is_dir() and entry.name.lower() != 'processed' and len(entry.name) == 6: 
                    yield from self.fast_scan(entry.path)

    def start(self):
        self.thread.start()
        print('[Watchdog] Producer started.')

    def join(self):
        self.thread.join()

class QueueConsumer():
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.thread = threading.Thread(target=self.consumer_loop)

    def consumer_loop(self):
        while True:
            src_path = self.queue.get(timeout=10) if not self.queue.empty() else None
            if src_path == None:
                continue
            print(f'[Watchdog] Ingesting files from {src_path}')
            try:
                station_data, fr_files = ingestFromTarball(src_path)
                
                # src_path of format path/to/fieldsums/dir/AU000X_239123_19.tar.bz2
                split_path = src_path.split('_')
                station_id_path = split_path[0]
                station_id = station_id_path.split('/')[-1]
                date_str = split_path[1]
                date_obj = datetime.strptime(date_str, '%Y%m%d')

                insertFieldsums(station_id, date_obj, station_data)
                insertFRs(station_id, date_obj, fr_files)
                setDataToIngested([(station_id, date_obj)])
                print(f'[Watchdog] Files ingested from {src_path}')
            except Exception as e:
                print(f'Error: {e}')
                traceback.print_exc()

    def start(self):
        self.thread.start()
        print('[Watchdog] Consumer started.')

    def join(self):
        self.thread.join()

