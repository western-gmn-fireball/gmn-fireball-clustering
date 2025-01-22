'''
    Clustering for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import numpy as np

'''
    @todo: Correct to work with new data format and implement DB usage
'''
def identifyFireballs(station_data, save_to_db=True):
    '''
    Args:
        station_data (dict): Dictionary with processed station data.
        save_to_db (bool): If True, saves the fireball data to the database.
    '''
    first_timestamp = min(station_data['datetimes'])
    features = []
    for station, data in datasets.items():

        # Get standard deviation
        std = np.std(data['detrended_intensities'])

        # Modify into feature space
        up = True
        down = False
        feature = [] # (start_time, end_time, station_id)
        '''
            Iterates through the intensities
            Keep track of when the std is crossed in both directions
            Each spike (i.e in between an increasing slope cross of std and 
            decreasing slope cross of std) is added to the feature set
        '''
        for idx in range(len(data['intensities'])):
            if data['detrended_intensities'][idx] >= 2*std and up:
                up = False
                down = True

                time_seconds = (data['datetimes'][idx] - first_timestamp).total_seconds()
                feature.append(time_seconds)
            if data['detrended_intensities'][idx] <= 2*std and down:
                down = False
                up = True

                time_seconds = (data['datetimes'][idx] - first_timestamp).total_seconds()
                feature.append(time_seconds)
                feature.append(station)
                features.append(feature)
                
                feature = []

    return features

def clusterFireballs():
    pass