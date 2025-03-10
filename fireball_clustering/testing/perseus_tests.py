from os import access
from fireball_clustering.perseus.perseus import Perseus
import datetime
import time
from fireball_clustering.dataclasses.models import Fireball 

stations = [
    "AU0006",
    "AU0007",
    "AU0009",
    "AU000A",
    "AU000C",
    "AU000X",
    "AU000Y",
    "AU0010",
    "AU000Q",
    "AU004B",
    "AU004K",
    "AU0047",
    "AU000D",
    "AU000Z",
    "AU001A",
    "AU0038",
    "AU000E",
    "AU000F",
    "AU000V",
    "AU000U",
    "AU000W",
    "AU000G",
    "AU0002",
    "AU0003",
]

dates = [
    # '20221114',
    '20241212',
    '20240506',
    '20221204',
    '20230319',
    '20221107',
]

perseus = Perseus(fieldsums_path='/home/armaan/school/Thesis/gmn-fireball-clustering/fieldsums',
                  fr_path='/home/armaan/school/Thesis/gmn-fireball-clustering/fr_files')
access_times = []
for date in dates:
    fireballs: list[Fireball] = []
    for station in stations:
        print(f'Processing {date}_{station}')
        datetime_date = datetime.datetime.strptime(date, '%Y%m%d')

        # Ingestion
        start_time = time.time()
        station_data = perseus.ingestFieldsums(station, datetime_date)
        fr = perseus.ingestFR(station, datetime_date)
        elapsed_time = time.time() - start_time
        access_times.append(elapsed_time)

        # Processing and Identifying
        processed_station_data = perseus.process(station_data)
        curr_fireballs = perseus.identify(station, processed_station_data, fr)
        fireballs.extend(curr_fireballs)     

    # Cluster
    positive = perseus.cluster(fireballs)
    positive.to_csv(f"./csv/{date}.csv")
    print(f'Positives for {date}:\n{positive}')

average = sum(access_times) / len(access_times)
print(f'Average access_time: {average}')
