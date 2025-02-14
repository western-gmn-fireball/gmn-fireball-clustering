from ..data_processing import preprocessing, clustering, visualizations

from dataclasses import dataclass

import multiprocessing
import numpy
import pandas as pd
import pickle
import os
import argparse
import datetime

DATA_DIR = './station_data/fieldsums/'

def au_ingestion(fireball, stations, fieldsums_folder, target_time):
    print(f'[INGESTION] Ingesting fieldsums for {fireball}')
    if os.path.exists(f'./pickle/{fireball}.pkl'): 
        print('[INGESTION] Ingestion not necessary. Files have already been ingested.')
        return
    stations_data = {}
    for station, fieldsum_file in stations.items():
        dataset = preprocessing.ingestStationData(f'{DATA_DIR}{fieldsums_folder}/{fieldsum_file}')
        # Select out window and then pickle
        stations_data[station] = select_time_window(dataset, target_time) 
        visualizations.plot_intensities({station: stations_data[station]}, f'./plots/{fireball}_{station}_initial_data', f'{fireball}: {station} @ {target_time}')
        print(f'[INGESTION] Fieldsums for {station} ingested.')

    visualizations.plot_intensities(stations_data, f'./plots/{fireball}_initial_data', f'Intensities for Fireball at {target_time}')
    
    # Pickle datasets
    pickle.dump(stations_data, open(f'./pickle/{fireball}.pkl', 'wb'))
    print(f'Files ingested and written to ./pickle/{fireball}.pkl')

def select_time_window(dataset, target_time):
    # Filter out for manual analysis
    start_target = target_time - datetime.timedelta(seconds=5)
    end_target = target_time + datetime.timedelta(seconds=5)

    datetimes = dataset['datetimes']
    intensities = dataset['intensities']    

    windowed_dataset = {'datetimes': [], 'intensities': []}

    for i in range(len(datetimes)):
        if datetimes[i] >= start_target and datetimes[i] <= end_target:
            windowed_dataset['datetimes'].append(datetimes[i])
            windowed_dataset['intensities'].append(intensities[i])
    
    return windowed_dataset

def au_preprocessing(fireball, avg_window, std_window, FILE_NAME, target_time):
    print(f'[PRE] Beginning preprocessing for {fireball}')
    detrended_datasets = {}
    with open(f'./pickle/{fireball}.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        for station, data in stations_data.items():
            detrended_data = preprocessing.preprocessFieldsums(data, avg_window, std_window)

            # Filter out for manual analysis
            start_target = target_time - datetime.timedelta(seconds=5)
            end_target = target_time + datetime.timedelta(seconds=5)

            df = pd.DataFrame.from_dict(detrended_data)
            df['datetimes'] = pd.to_datetime(df['datetimes'])
            filtered = df[(df['datetimes'] > start_target) & (df['datetimes'] < end_target)]
            filtered.to_csv(f'./csv/{station}_preprocessed_target_window.csv')
            

            detrended_datasets[station] = detrended_data
            print(f'[PRE] Preprocessing for {station} finished.')

        pickle.dump(detrended_datasets, open(f'./pickle/{FILE_NAME}_processed.pkl', 'wb'))
        visualizations.plot_intensities(detrended_datasets, f'./plots/{FILE_NAME}', f'Intensities for Fireball at {target_time}')
    print(f'[PRE] Preprocessing for {fireball} completed')

def au_fireball_clustering(fireball, FILE_NAME):
    print(f'[CLUSTERING] Clustering {fireball} starting...')
    with open(f'./pickle/{FILE_NAME}_processed.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        fireballs = []
        for station, dataset in stations_data.items():
            fireballs.extend(clustering.identifyFireballs(station, dataset, save_to_db=False))

        clusters = clustering.clusterFireballs(fireballs)
        return clusters
    print(f'[CLUSTERING] Clustering {fireball} completed.')

@dataclass
class Fireball:
    name: str
    fieldsums_folder: str
    target_time: datetime.datetime
    stations: dict

def serial_main():
    fireballs = [
        Fireball('au20221114', 'AU_20221114', datetime.datetime(2022, 11, 14, 19, 58, 21), { # Australia, 2022/11/14 - 19:58:21
            "AU0006": f"AU0006_20221114",
            "AU0007": f"AU0007_20221114",
            "AU0009": f"AU0009_20221114",
            "AU000A": f"AU000A_20221114",
            "AU000C": f"AU000C_20221114",
            "AU000X": f"AU000X_20221114",
            "AU000Y": f"AU000Y_20221114",
            "AU0010": f"AU0010_20221114",
        }),
        Fireball('au20241212', 'AU_20241212', datetime.datetime(2024, 12, 12, 11, 39, 54), { # Australia, 2024/12/12 - 11:39:54
            "AU000Q": "AU000Q_20241212", 
            "AU004B": "AU004B_20241212",
            "AU004K": "AU004K_20241212",
            "AU0047": "AU0047_20241212",
        }),
        Fireball('au20240506', 'AU_20240506', datetime.datetime(2024, 5, 6, 12, 24, 40), { # Australia, 2024/05/06 - 12:24:40
            "AU000D": "AU000D_20240506", 
            "AU000Z": "AU000Z_20240506", 
            "AU001A": "AU001A_20240506", 
            "AU003D": "AU003D_20240506", 
            "AU0038": "AU0038_20240506", 
        }),
        Fireball('au20230319', 'AU_20230319', datetime.datetime(2023, 3, 19, 18, 43, 50), {
            "AU000E": "AU000E_20230319",
            "AU000F": "AU000F_20230319",
            "AU000V": "AU000V_20230319",
            "AU000U": "AU000U_20230319",
            "AU000W": "AU000W_20230319",
            "AU000Z": "AU000Z_20230319",
            "AU0006": "AU0006_20230319",
        }),
        Fireball('au20221204', 'AU_20221204', datetime.datetime(2022, 12, 4, 17, 59, 30), {
            "AU000A": "AU000A_20221204",
            "AU000E": "AU000E_20221204",
            "AU000G": "AU000G_20221204",
            "AU000V": "AU000V_20221204",
            "AU000X": "AU000X_20221204",
            "AU0002": "AU0002_20221204",
            "AU0003": "AU0003_20221204",
            "AU0010": "AU0010_20221204",
        }),
        Fireball('au20221107', 'AU_20221107', datetime.datetime(2022, 11, 7, 19, 0, 14), {
            "AU000X": "AU000X_20221107",
            "AU000Y": "AU000Y_20221107",
            "AU0006": "AU0006_20221107",
            "AU0007": "AU0007_20221107",
            "AU0009": "AU0009_20221107",
            "AU0010": "AU0010_20221107",
        })
    ]

    avg_window = 30
    std_window = 30
    for fireball in fireballs:
        name = fireball.name
        stations = fireball.stations
        fieldsums_folder = fireball.fieldsums_folder
        target_time = fireball.target_time

        file_name = f'{fireball.name}_{avg_window}sec'
        print(f'Processing: {file_name}')
        print(f'Analyzing and clustering {name}')
        au_ingestion(name, stations, fieldsums_folder, target_time)
        au_preprocessing(name, avg_window, std_window, file_name, target_time)
        clusters = au_fireball_clustering(name, file_name)
        clusters.to_csv(f'./csv/{file_name}.csv')
        print(f'Analysis for {name} is complete.')

def process_fireball(fireball, avg_window, std_window):
    """
    Processes a single fireball: ingest, preprocess, perform clustering,
    and save the results to CSV.
    """
    name = fireball.name
    stations = fireball.stations
    fieldsums_folder = fireball.fieldsums_folder
    target_time = fireball.target_time

    file_name = f'{fireball.name}_{avg_window}sec'
    print(f'Processing: {file_name}')
    print(f'Analyzing and clustering {name}')

    # Process the fireball data
    au_ingestion(name, stations, fieldsums_folder)
    au_preprocessing(name, avg_window, std_window, file_name, target_time)
    print(f'Ingestion and preprocessing for {name} is complete.')
    
    # Optionally, return a result or status message
    return file_name

def parallel_main():
    fireballs = [
        Fireball('au20221114', 'AU_20221114', datetime.datetime(2022, 11, 14, 19, 58, 21), { # Australia, 2022/11/14 - 19:58:21
            "AU0006": f"AU0006_20221114",
            "AU0007": f"AU0007_20221114",
            "AU0009": f"AU0009_20221114",
            "AU000A": f"AU000A_20221114",
            "AU000C": f"AU000C_20221114",
            "AU000X": f"AU000X_20221114",
            "AU000Y": f"AU000Y_20221114",
            "AU0010": f"AU0010_20221114",
        }),
        # Fireball('au20241212', 'AU_20241212', datetime.datetime(2024, 12, 12, 11, 39, 54), { # Australia, 2024/12/12 - 11:39:54
        #     "AU000Q": "AU000Q_20241212", 
        #     "AU004B": "AU004B_20241212",
        #     "AU004K": "AU004K_20241212",
        #     "AU0047": "AU0047_20241212",
        # }),
        # Fireball('au20240506', 'AU_20240506', datetime.datetime(2024, 5, 6, 12, 24, 40), { # Australia, 2024/05/06 - 12:24:40
        #     "AU000D": "AU000D_20240506", 
        #     "AU000Z": "AU000Z_20240506", 
        #     "AU001A": "AU001A_20240506", 
        #     "AU003D": "AU003D_20240506", 
        #     "AU0038": "AU0038_20240506", 
        # }),
        # Fireball('au20230319', 'AU_20230319', datetime.datetime(2023, 3, 19, 18, 43, 50), {
        #     "AU000E": "AU000E_20230319",
        #     "AU000F": "AU000F_20230319",
        #     "AU000V": "AU000V_20230319",
        #     "AU000U": "AU000U_20230319",
        #     "AU000W": "AU000W_20230319",
        #     "AU000Z": "AU000Z_20230319",
        #     "AU0006": "AU0006_20230319",
        # }),
        # Fireball('au20221204', 'AU_20221204', datetime.datetime(2022, 12, 4, 17, 59, 30), {
        #     "AU000A": "AU000A_20221204",
        #     "AU000E": "AU000E_20221204",
        #     "AU000G": "AU000G_20221204",
        #     "AU000V": "AU000V_20221204",
        #     "AU000X": "AU000X_20221204",
        #     "AU0002": "AU0002_20221204",
        #     "AU0003": "AU0003_20221204",
        #     "AU0010": "AU0010_20221204",
        # }),
        # Fireball('au20221107', 'AU_20221107', datetime.datetime(2022, 11, 7, 19, 0, 14), {
        #     "AU000X": "AU000X_20221107",
        #     "AU000Y": "AU000Y_20221107",
        #     "AU0006": "AU0006_20221107",
        #     "AU0007": "AU0007_20221107",
        #     "AU0009": "AU0009_20221107",
        #     "AU0010": "AU0010_20221107",
        # })
    ]
    avg_window = 30
    std_window = 30

    # Create a list of argument tuples for each fireball
    tasks = [(fireball, avg_window, std_window) for fireball in fireballs]

    # Use a multiprocessing Pool to process fireballs concurrently.
    with multiprocessing.Pool() as pool:
        # starmap allows us to pass multiple arguments to the process_fireball function.
        results = pool.starmap(process_fireball, tasks)
    
    # Optionally, print the results (the file names processed)
    for result in results:
        print(f'Finished processing: {result}')
    for fireball in fireballs:
        name = fireball.name

        file_name = f'{fireball.name}_{avg_window}sec'
        clusters = au_fireball_clustering(name, file_name)
        
        # Save the clustering results
        clusters.to_csv(f'./csv/{file_name}.csv')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--parallel", default=False)
    args = parser.parse_args()
    if args.parallel:
        parallel_main()
    else:
        serial_main()