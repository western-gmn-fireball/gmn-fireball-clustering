from fireball_clustering.data_ingestion.local_fetcher import ingestFromTarball
from fireball_clustering.data_processing.preprocessing import ingestStationData
from fireball_clustering.database.db_writes import insertFieldsums
from fireball_clustering.database.db_queries import getStationDataByDate 
from fireball_clustering.database.db_setup import initializeEmptyDatabase, insertStations

import time
import datetime
import os

def main():
    initializeEmptyDatabase()
    insertStations()
    start = time.time()
    folder = ingestStationData('/home/armaan/school/Thesis/gmn-fireball-clustering/fieldsums/AU0002_20221107/')
    print(f'From folder: {time.time()-start}')

    start = time.time()
    tar = ingestFromTarball('/home/armaan/school/Thesis/gmn-fireball-clustering/tmp/AU0002_20221107_111129_639666_detected.tar.bz2')
    print(f'From TAR: {time.time()-start}')
    assert tar.datetimes == folder.datetimes
    assert tar.intensities == folder.intensities

    insertFieldsums('AU0002', datetime.datetime(2022, 11, 7), tar) 
    getStationDataByDate('AU0002', datetime.datetime(2022, 11, 7)) 

if __name__ == "__main__":
    main()
