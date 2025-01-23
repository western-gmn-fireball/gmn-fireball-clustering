from ..data_processing import preprocessing, clustering

import numpy
import pickle

def uk_ingestion():
    DATA_DIR = './station_data/fieldsums/'
    stations = {'UK0079': f'UK0079_20230212',
                'UK008B': f'UK008B_20230212'}
    stations_data = {}
    for station, fieldsum_path in stations.items():
        stations_data[station] = preprocessing.ingestStationData(f'{DATA_DIR}{fieldsum_path}')

    # Output statistics of datasets
    for station, data in stations_data.items():
        int_mean = numpy.mean(data['intensities'])
        int_median = numpy.median(data['intensities'])
        int_std = numpy.std(data['intensities'])
        print(f'Ingested Data Stats for {station}')
        print(f'\tMean: {int_mean}')
        print(f'\tMedian: {int_median}')
        print(f'\tStandard Deviation: {int_std}')
    
    # Pickle datasets
    pickle.dump(stations_data, open('./pickle/UK_20230212.pkl', 'wb'))

def uk_preprocessing():
    detrended_datasets = {}
    with open('./pickle/UK_20230212.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        for station, data in stations_data.items():
            detrended_data = preprocessing.preprocessFieldsums(data)
            detrended_datasets[station] = detrended_data
        
        # Output statistics of datasets
        for station, data in stations_data.items():
            int_mean = numpy.mean(data['intensities'])
            int_median = numpy.median(data['intensities'])
            int_std = numpy.std(data['intensities'])
            print(f'Ingested Data Stats for {station}')
            print(f'\tMean: {int_mean}')
            print(f'\tMedian: {int_median}')
            print(f'\tStandard Deviation: {int_std}')

        pickle.dump(detrended_datasets, open('./pickle/UK_20230212_processed.pkl', 'wb'))

def uk_fireball_id():
    with open('./pickle/UK_20230212_processed.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        for station, dataset in stations_data.items():
            clustering.identifyFireballs(station, dataset)

def main():
    print('Running tests...')
    uk_preprocessing()
    uk_fireball_id()
    print('Tests Complete')

if __name__ == '__main__':
    main()