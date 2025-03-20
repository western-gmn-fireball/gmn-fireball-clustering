import multiprocessing

from fireball_clustering import watchdog
from fireball_clustering import analysis_pipeline

def run_watchdog():
    file_watcher = watchdog.FileWatcher()
    file_watcher.start_file_watcher()

def run_analysis_pipeline():
    analysis = analysis_pipeline.Analysis()
    analysis.start()

if __name__ == "__main__":
    p1 = multiprocessing.Process(target=run_watchdog)
    p2 = multiprocessing.Process(target=run_analysis_pipeline)

    p1.start()
    p2.start()

    p1.join()
    p2.join()
