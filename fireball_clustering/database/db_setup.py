'''
    This file contains functions to create and initialize the database with
    the following tables:
        Stations:
            - station_id (TEXT): Unique identifier for the station.
            - latitude (real): Latitude of the station
            - longitude (real): Longitude of the station
        Fireballs:
            - fireball_id (int): Unique identifier for the fireball
            - station_id (FOREIGN KEY INT): Identifier of observing station
            - start_time (TEXT): ISO8601 representation of fireball start_time
            - end_time (TEXT): ISO8601 representation of fireball end_time
        Clusters:
            - cluster_id (PRIMARY KEY INT): Unique identifier for the cluster
            - start_time (TEXT): ISO8601 representation of cluster start_time
            - end_time (TEXT): ISO8601 representation of cluster end_time
        Cluster_Fireballs:
            - fireball_id (FOREIGN KEY INT)
            - cluster_id (FOREIGN KEY INT) 
'''

import sqlite3
import requests
import datetime

from . import db_writes

def initializeEmptyDatabase():
    # Initialize connection
    con = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = con.cursor()

    # Create tables
    cursor.execute("""
                   CREATE TABLE stations(
                        station_id TEXT PRIMARY KEY,
                        latitude REAL NOT NULL,
                        longitude REAL NOT NULL 
                   )
                   """)
    cursor.execute("""
                   CREATE TABLE fireballs(
                        fireball_id INTEGER PRIMARY KEY,
                        station_id TEXT NOT NULL,
                        start_time TEXT,
                        end_time TEXT,
                        FOREIGN KEY (station_id) REFERENCES stations(station_id)
                   )
                   """)
    cursor.execute("""
                   CREATE TABLE clusters(
                        cluster_id INTEGER PRIMARY KEY,
                        start_time TEXT,
                        end_time TEXT
                   )
                   """)
    cursor.execute("""
                   CREATE TABLE clusters_fireballs(
                        cluster_id INTEGER NOT NULL,
                        fireball_id INTEGER NOT NULL,
                        FOREIGN KEY (cluster_id) REFERENCES clusters(cluster_id)
                        FOREIGN KEY (fireball_id) REFERENCES fireballs(fireball_id)
                   )
                   """)
    con.commit()

def insertStations():
    '''
    Inserts stations from the GMNs public station listings.
    Source: https://globalmeteornetwork.org/data/kml_fov/GMN_station_coordinates_public.json
    '''
    STATIONS_URL = "https://globalmeteornetwork.org/data/kml_fov/GMN_station_coordinates_public.json"

    response = requests.get(STATIONS_URL)

    if response.status_code == 200:
        station_metadata = response.json()
    else:
        raise f'Could not fetch station metadata: {response.status_code}'
    
    filtered_metadata = []
    for station, times in station_metadata.items():
        most_recent = datetime.datetime(2000, 1, 1, 0, 0, 0)
        most_recent_iso = ''
        for iso_time in times:
            time = datetime.datetime.fromisoformat(iso_time)
            if time > most_recent:
                most_recent = time
                most_recent_iso = iso_time
        
        lat = station_metadata[station][most_recent_iso]['lat']
        lon = station_metadata[station][most_recent_iso]['lon']
        filtered_metadata.append((station, lat, lon))
    
    db_writes.insertStations(filtered_metadata)