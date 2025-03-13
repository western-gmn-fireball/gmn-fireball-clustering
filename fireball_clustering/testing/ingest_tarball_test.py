from fireball_clustering.data_ingestion.local_fetcher import ingestFromTarball
from fireball_clustering.data_processing.preprocessing import ingestStationData

import time

def main():
    start = time.time()
    folder = ingestStationData('/home/armaan/school/Thesis/gmn-fireball-clustering/fieldsums/AU0002_20221107/')
    print(f'From folder: {time.time()-start}')

    start = time.time()
    tar = ingestFromTarball('/home/armaan/school/Thesis/gmn-fireball-clustering/tmp/AU0002_20221107_111129_639666_detected.tar.bz2')
    print(f'From TAR: {time.time()-start}')
    assert tar.datetimes == folder.datetimes
    assert tar.intensities == folder.intensities
if __name__ == "__main__":
    main()
