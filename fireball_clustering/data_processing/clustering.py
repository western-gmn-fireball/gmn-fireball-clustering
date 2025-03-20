'''
    Clustering for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import numpy as np
import json
import datetime
import bisect
import pandas as pd
from sklearn.cluster import DBSCAN

from ..database import db_writes, db_queries
from fireball_clustering.database import db_connection
from .. import parameters
from ..dataclasses.models import ProcessedStationData, Fireball

# TODO: Add lat and lng into the data considerations (could just include it in station_data to have it for reference everywhere)
def identifyFireballs(station_name: str, station_data: ProcessedStationData, save_to_db=True) -> list[Fireball]:
    '''
    Args:
        station_data (dict): Dictionary with processed station data.
        save_to_db (bool): If True, saves the fireball data to the database.
    '''
    if len(station_data.datetimes) == 0: return [] 
    
    fireballs = []

    # Modify into feature space
    up = True
    down = False
    event = [] # (start_time, end_time, station_id)
    
    # Peak detection
    # TODO: optimize
    CUTOFF = parameters.CUTOFF # multiple of sigma
    for idx in range(len(station_data.intensities)):
        std = station_data.moving_std[idx]
        if station_data.detrended_intensities[idx] >= CUTOFF * std and up:
            up = False
            down = True

            event.append(station_data.datetimes[idx])
        if station_data.detrended_intensities[idx] <= CUTOFF * std and down:
            down = False
            up = True

            event.append(station_data.datetimes[idx])
            event.append(station_name)
            fireballs.append(event)
            
            event = []
    
    # Add fireball events to the DB
    fireball_ids = db_writes.insertFireballs([(station_name,
                                datetime.datetime.isoformat(start_time),
                                datetime.datetime.isoformat(end_time))
                                for start_time, end_time, station_name in fireballs])
    
    # TODO: create better method for returning fireballs without getting them from the db
    # Convert to fireball dataclass
    for i in range(len(fireballs)):
        fireballs[i].append(fireball_ids[i])
    
    fireballs_dataclass = [Fireball(
        station_name=station_name,
        start_time=start_time,
        end_time=end_time,
        id=id
    ) for start_time, end_time, station_name, id in fireballs]

    return fireballs_dataclass 

def filterFireballsWithFR(fireballs: list[Fireball], fr_timestamps: list[datetime.datetime]):
    '''
    Filters fireballs based on temporal proximity to FR event timestamps.

    Args:
        fireball list[Fireball]: list of candidate fireballs  
        fr_timestamps list[datetime.datetime]

    Returns:
        list[fireball] Candidate fireballs filtered based on FR events
    '''
    if len(fr_timestamps) == 0: return []

    candidates: list[Fireball] = []
    for fireball in fireballs:
        start_time = fireball.start_time
        
        # Find temporally nearest FR events to fireball
        closest_fr_idx = bisect.bisect_left(fr_timestamps, start_time)
 
        left = closest_fr_idx if closest_fr_idx < len(fr_timestamps) else len(fr_timestamps) - 1
        right = closest_fr_idx + 1 if closest_fr_idx + 1 < len(fr_timestamps) else None
        
        left_delta = abs(fr_timestamps[left] - start_time)
        right_delta = abs(fr_timestamps[right] - start_time) if right else None
        
        # Filter for fireballs within MAX_DELTA seconds of FR events
        # TODO: change seconds to a parameter in parameters
        MAX_DELTA = datetime.timedelta(seconds=parameters.FR_EVENT_PROXIMITY)
        if left_delta <= MAX_DELTA or (right_delta and right_delta <= MAX_DELTA):
            candidates.append(fireball)

    db_writes.insertCandidateFireballs(candidates)
        
    return candidates

# TODO: write clusters to DB
# TODO: clean function up
# TODO: dataclass for return values?
def clusterFireballs(fireballs: list[Fireball]):
    '''
    Clusters fireballs using a 2 stage approach:
        1. Cluster based on time(sec) from the start of the year
        2. Cluster based on the haversine distance between stations
    
    Args:
        fireballs (array of tuples: (start_time, end_time, station_name)): processed fireballs as outputted by clustering.identifyFireballs()
    '''
    # Get stations coordinates in radians map station_id to them
    stations = list(set([fireball.station_name for fireball in fireballs]))
    stations_with_coords = db_queries.getStationsDataByID(stations)
    stations_coords_map = {}
    for station in stations_with_coords:
        stations_coords_map[station[0]] = np.radians([station[1], station[2]])

    # Convert start and end to delta since beginning of year and add in station coordinate data
    earliest_time: datetime.datetime = min([fireball.start_time for fireball in fireballs])
    earliest_year = earliest_time.year
    start_of_year = datetime.datetime(earliest_year, 1, 1, 0, 0, 0, 0)
    modified_fireballs = []
    for fireball in fireballs:
        start = fireball.start_time
        end = fireball.end_time
        station_id = fireball.station_name
        fireball_id = fireball.id
        new_start = (start - start_of_year).total_seconds()
        new_end = (end - start_of_year).total_seconds()
        lat_rads, lng_rads = stations_coords_map[station_id]
        modified_fireballs.append([new_start, new_end, lat_rads, lng_rads, station_id, fireball_id])
    
    df = pd.DataFrame(modified_fireballs, columns=['start', 'end', 'lat_rads', 'lng_rads', 'station_id', 'fireball_id']) 

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
    for _, group in temporal_clusters:
        group = group.copy()

        # Spatial clustering
        x_space = group[['lat_rads', 'lng_rads']].values

        spatial_model = DBSCAN(eps=1000/6371.0088, min_samples=2, metric='haversine')
        group['spatial_cluster'] = spatial_model.fit_predict(x_space)
        spatiotemporal_cluster = group[group['spatial_cluster'] >= 0]
        
        # Only consider cluster if it has >= 2 unique station observers
        if spatiotemporal_cluster['station_id'].nunique() >= parameters.MIN_OBSERVERS:
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
    
    sql_st_clusters = spatiotemporal_clusters[['station_id', 'start_iso_str', 'end_iso_str', 'spatiotemporal_cluster_id']]
    # sql_st_clusters_collapsed = sql_st_clusters.groupby('spatiotemporal_cluster_id')['station_id'].apply(lambda x: x.to_json(orient='records')).reset_index()
    sql_st_clusters_collapsed = sql_st_clusters.groupby('spatiotemporal_cluster_id', as_index=False).agg({
        'station_id': lambda x: json.dumps(list(x)),
        'start_iso_str': lambda x: min(list(x)),
        'end_iso_str': lambda x: max(list(x)),
    })

    db = db_connection.Database()
    conn = db.conn
    with db.lock:
        sql_st_clusters_collapsed.to_sql('pandas_clusters', conn, if_exists='append', index=False,
                                         dtype={'cluster_id': 'INTEGER PRIMARY KEY AUTOINCREMENT','station_id': 'TEXT', 'start_iso_str': 'TEXT', 'end_iso_str': 'TEXT'})
    db.conn.close()
    return spatiotemporal_clusters 
