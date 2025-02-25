'''
    Clustering for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import numpy as np
import datetime
import pandas as pd
from sklearn.cluster import DBSCAN

from ..database import db_writes, db_queries
from .. import parameters

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
    # std = np.std(station_data['detrended_intensities'])

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
    CUTOFF = parameters.CUTOFF # multiple of sigma
    for idx in range(len(station_data['intensities'])):
        std = station_data['moving_std'][idx]
        if station_data['detrended_intensities'][idx] >= CUTOFF * std and up:
            up = False
            down = True

            event.append(station_data['datetimes'][idx])
        if station_data['detrended_intensities'][idx] <= CUTOFF * std and down:
            down = False
            up = True

            event.append(station_data['datetimes'][idx])
            event.append(station_name)
            fireballs.append(event)
            
            event = []
    
    # Add fireball events to the DB
    fireball_ids = db_writes.insertFireballs([(station_name, 
                                datetime.datetime.isoformat(start_time), 
                                datetime.datetime.isoformat(end_time)) 
                                for start_time, end_time, station_name in fireballs])

    for i in range(len(fireballs)):
        fireballs[i].append(fireball_ids[i])

    return fireballs

def clusterFireballs(fireballs):
    '''
    Clusters fireballs using a 2 stage approach:
        1. Cluster based on time(sec) from the start of the year
        2. Cluster based on the haversine distance between stations
    
    Args:
        fireballs (array of tuples: (start_time, end_time, station_name)): processed fireballs as outputted by clustering.identifyFireballs()
    '''
    # Get stations coordinates in radians map station_id to them
    stations = list(set([station for _, _, station, _ in fireballs])) #####
    stations_with_coords = db_queries.getStations(stations)
    stations_coords_map = {}
    for station in stations_with_coords:
        stations_coords_map[station[0]] = np.radians([station[1], station[2]])

    # Convert start and end to delta since beginning of year and add in station coordinate data
    earliest_time: datetime.datetime = min([start_time for start_time, _, _, _ in fireballs])
    earliest_year = earliest_time.year
    start_of_year = datetime.datetime(earliest_year, 1, 1, 0, 0, 0, 0)
    modified_fireballs = []
    for start, end, station_id, fireball_id in fireballs:
        new_start = (start - start_of_year).total_seconds()
        new_end = (end - start_of_year).total_seconds()
        lat_rads, lng_rads = stations_coords_map[station_id]
        modified_fireballs.append([new_start, new_end, lat_rads, lng_rads, station_id, fireball_id])
    
    df = pd.DataFrame(modified_fireballs, columns=['start', 'end', 'lat_rads', 'lng_rads', 'station_id', 'fireball_id']) 
    df.to_csv('initial_data.csv')

    # Temporal clustering
    x_temp = df[['start', 'end']].values

    temporal_model = DBSCAN(eps=10, min_samples=2)
    df['temporal_cluster'] = temporal_model.fit_predict(x_temp)
    temporal_clusters_df = df[df['temporal_cluster'] >= 0].copy()
    # temporal_clusters_df.to_csv('./csv/temporal_clusters.csv')
    temporal_clusters = temporal_clusters_df.groupby('temporal_cluster')

    # Spatial clustering for each temporal cluster
    spatiotemporal_clusters_list = []
    spatiotemporal_cluster_id = 0
    for name, group in temporal_clusters:
        group = group.copy()

        # Spatial clustering
        x_space = group[['lat_rads', 'lng_rads']].values

        spatial_model = DBSCAN(eps=1000/6371.0088, min_samples=2, metric='haversine')
        group['spatial_cluster'] = spatial_model.fit_predict(x_space)
        spatiotemporal_cluster = group[group['spatial_cluster'] >= 0]
        
        # Only consider cluster if it has >= 2 unique station observers
        if spatiotemporal_cluster['station_id'].nunique() >= 2:
            spatiotemporal_cluster['spatiotemporal_cluster_id'] = spatiotemporal_cluster_id
            spatiotemporal_cluster_id += 1

            spatiotemporal_clusters_list.append(spatiotemporal_cluster)
    
    # Create and return a dataframe with the results, including familiar format timestamps
    if spatiotemporal_clusters_list:
        spatiotemporal_clusters = pd.concat(spatiotemporal_clusters_list, ignore_index=True)

        # Convert timestamps back to iso format
        spatiotemporal_clusters['start_iso'] = pd.to_datetime(spatiotemporal_clusters['start'], unit='s', origin=start_of_year)
        spatiotemporal_clusters['end_iso'] = pd.to_datetime(spatiotemporal_clusters['end'], unit='s', origin=start_of_year)
        spatiotemporal_clusters['start_iso_str'] = spatiotemporal_clusters['start_iso'].astype(str)
        spatiotemporal_clusters['end_iso_str'] = spatiotemporal_clusters['end_iso'].astype(str)
    else:
        spatiotemporal_clusters = pd.DataFrame()
    return spatiotemporal_clusters 