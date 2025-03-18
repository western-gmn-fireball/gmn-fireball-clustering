from fireball_clustering.database.db_writes import insertFieldsums, insertFRs
from fireball_clustering.database.db_queries import getStationDataByDate, getFrTimestampsByDate
from fireball_clustering.database.db_setup import *
from fireball_clustering.dataclasses.models import StationData

import datetime

def testFieldsums():
    insertFieldsums('XXYYYY', 
                    datetime.datetime(2000, 10, 10), 
                    StationData([datetime.datetime(2000, 10, 10)],
                                [100])
                    )
    station_data = getStationDataByDate('XXYYYY', datetime.datetime(2000, 10, 10))
    print(station_data)

def testFrFiles():
    insertFRs('XXYYYY',
              datetime.datetime(2000, 10, 10),
              ['fr_timestamp1', 'fr_tiemstamp2', 'fr_tiemstamps23'])
    fr_timestamps = getFrTimestampsByDate('XXYYYY', datetime.datetime(2000, 10, 10))
    print(fr_timestamps)

def main():
    initializeEmptyDatabase()
    insertStations()
    testFieldsums()
    testFrFiles()

if __name__ == "__main__":
    main()
