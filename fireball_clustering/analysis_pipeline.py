from fireball_clustering.perseus.perseus import Perseus
from fireball_clustering.database import db_queries, db_setup, db_writes

from queue import Queue
import threading 
import time

class AnalysisProducer():
    def __init__(self, queue: Queue) -> None:
        self.thread = threading.Thread(target=self.producer_loop)
        self.queue = queue

    def producer_loop(self):
        while True:
            time.sleep(10)
            count = 0
            for stations_to_process in db_queries.getIngestedRadii():
                self.queue.put(stations_to_process)
                count += 1
            print(f'[AnalysisPipeline] Added {count} clusters of stations to the pipeline.')

    def start(self):
        self.thread.start()
        print('[AnalysisPipeline] Producer started.')

    def join(self):
        self.thread.join()

class AnalysisConsumer():
    def __init__(self, queue: Queue) -> None:
        self.queue = queue
        self.thread = threading.Thread(target=self.consumer_loop)
        self.perseus = Perseus()

    def consumer_loop(self):
        while True:
            time.sleep(10)
            stations_to_process = self.queue.get() if not self.queue.empty() else None
            if stations_to_process == None:
                continue
            
            all_candidates = []
            for station_date in stations_to_process:
                station_id, date = station_date
                print(station_id, date)
                print(f'[AnalysisPipeline] Processing Station: {station_id} for Date: {date}')
                if db_queries.isProcessed(station_id, date): 
                    all_candidates.extend(db_queries.getFireballsByStationDate(station_id, date))
                
                try:
                    db_writes.setDataToProcessing([(station_id, date)])

                    station_data = self.perseus.ingestFieldsumsDB(station_id, date)
                    fr_timestamps = self.perseus.ingestFrDB(station_id, date)
                    processed_station_data = self.perseus.process(station_data)
                    filtered_candidates = self.perseus.identify(station_id, processed_station_data, fr_timestamps)
                    all_candidates.extend(filtered_candidates)
                    db_writes.setDataToProcessed([(station_id, date)])
                except Exception as e:
                    print(f'[AnalysisPipeline] ERROR: {e}')
            try: 
                positive_fireballs = self.perseus.cluster(all_candidates)
                print(positive_fireballs)
            except Exception as e:
                print(f'[AnalysisPipeline] ERROR: {e}')

    def start(self):
        self.thread.start()
        print("[AnalysisPipeline] Consumer started.")

    def join(self):
        self.thread.join()

class Analysis():
    def __init__(self) -> None:
        self.queue = Queue()
        self.producer = AnalysisProducer(self.queue)
        self.consumer = AnalysisConsumer(self.queue)

    # def start(self):
    #     try:
    #         while True:
    #             time.sleep(1)
    #     finally:
    #         self.producer.join()
    #         self.consumer.join()

    def start(self):
        self.producer.start()
        self.consumer.start()

    def join(self):
        self.producer.join()
        self.consumer.join()

def main():
    analysis = Analysis()
    analysis.start()

if __name__ == "__main__":
    main()
