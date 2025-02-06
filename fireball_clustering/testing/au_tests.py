from ..data_processing import preprocessing, clustering, visualizations

import numpy
import pickle

def au_ingestion():
    DATA_DIR = './station_data/fieldsums/'
    stations = {
                'AU0006': f'AU0006_20221114',
                'AU0007': f'AU0007_20221114',
                'AU0009': f'AU0009_20221114',
                'AU000A': f'AU000A_20221114',
                'AU000C': f'AU000C_20221114',
                'AU000X': f'AU000X_20221114',
                'AU000Y': f'AU000Y_20221114',
                'AU0010': f'AU0010_20221114'
                }
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
    pickle.dump(stations_data, open('./pickle/AU_20221114.pkl', 'wb'))

def au_preprocessing():
    detrended_datasets = {}
    with open('./pickle/AU_20221114.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        for station, data in stations_data.items():
            detrended_data = preprocessing.preprocessFieldsums(data)
            detrended_datasets[station] = detrended_data
        
        # visualizations.plot_intensities(detrended_datasets)

        # Output statistics of datasets
        for station, data in stations_data.items():
            int_mean = numpy.mean(data['intensities'])
            int_median = numpy.median(data['intensities'])
            int_std = numpy.std(data['intensities'])
            print(f'Ingested Data Stats for {station}')
            print(f'\tMean: {int_mean}')
            print(f'\tMedian: {int_median}')
            print(f'\tStandard Deviation: {int_std}')

        pickle.dump(detrended_datasets, open('./pickle/AU_20221114_processed.pkl', 'wb'))

def au_fireball_clustering():
    with open('./pickle/AU_20221114_processed.pkl', 'rb') as station_data_file:
        stations_data = pickle.load(station_data_file)
        fireballs = []
        for station, dataset in stations_data.items():
            fireballs.extend(clustering.identifyFireballs(station, dataset))

        clusters = clustering.clusterFireballs(fireballs)
        clusters.to_csv('./csv/clusters.csv')

    return clusters

def main():
    print('Running tests...')
    au_ingestion()
    au_preprocessing()
    au_fireball_clustering()
    print('Tests Complete')

if __name__ == '__main__':
    main()