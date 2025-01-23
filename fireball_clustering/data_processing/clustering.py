'''
    Clustering for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import numpy as np
import datetime

from ..database import db_writes 

'''
    @todo: Add lat and lng into the data considerations (could just include it in station_data to have it for reference everywhere)
'''
def identifyFireballs(station_name, station_data, save_to_db=True):
    '''
    Args:
        station_data (dict): Dictionary with processed station data.
        save_to_db (bool): If True, saves the fireball data to the database.
    '''
    first_timestamp = min(station_data['datetimes'])
    fireballs = []
        
    # Get standard deviation
    std = np.std(station_data['detrended_intensities'])

    # Modify into feature space
    up = True
    down = False
    event = [] # (start_time, end_time, station_id)
    '''
        This comment is super ugly
        Iterates through the intensities
        Keep track of when the std is crossed in both directions
        Each spike (i.e in between an increasing slope cross of std and 
        decreasing slope cross of std) is added to the feature set
    '''
    for idx in range(len(station_data['intensities'])):
        if station_data['detrended_intensities'][idx] >= 2*std and up:
            up = False
            down = True

            # time_seconds = (station_data['datetimes'][idx] - first_timestamp).total_seconds()
            # event.append(time_seconds)
            event.append(station_data['datetimes'][idx])
        if station_data['detrended_intensities'][idx] <= 2*std and down:
            down = False
            up = True

            # time_seconds = (station_data['datetimes'][idx] - first_timestamp).total_seconds()
            # event.append(time_seconds)
            event.append(station_data['datetimes'][idx])
            event.append(station_name)
            fireballs.append(event)
            
            event = []
    
    # Add fireball events to the DB
    db_writes.insertFireballs([(station_name, 
                                datetime.datetime.isoformat(start_time), 
                                datetime.datetime.isoformat(end_time)) 
                                for start_time, end_time, station_name in fireballs])

    return fireballs 

def clusterFireballs():
    pass