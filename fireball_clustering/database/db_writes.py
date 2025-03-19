'''
    Handles all logic related to writing to the gmn_fireball_clustering database

    Author: Armaan Mahajan
'''

import numpy as np
import sqlite3
import pickle
from datetime import datetime

from fireball_clustering.dataclasses.models import StationData 

def insertStations(stations):
    '''
    Inserts 1+ station(s) into the stations table of the database.

    Args:
        stations (list of tuples): List of tuples with the format (station_id, latitude, longitude, status)
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO stations (station_id, latitude, longitude) VALUES(?, ?, ?)', stations)
    conn.commit()
    conn.close()

def insertRadius(radii: dict):
    '''
    Args:
        radii: dictionary of station_id: list<stations_within_radius>
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    to_insert = [(k, pickle.dumps(v)) for k, v in radii.items()]
    cursor.executemany('INSERT INTO radius (station_id, stations_within_radius) VALUES(?, ?)', to_insert)
    conn.commit()
    conn.close()

def updateRadius(station_id, stations_within_radius):
    '''
    Args:
        station_id: station ID that the stations within radius need to be updated for
        stations_within_radius: list of stations within a radius of station with station id
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.executemany('UPDATE radius SET stations_within_radius = ? WHERE station_id = ?', (pickle.dumps(stations_within_radius), station_id))
    conn.commit()
    conn.close()

def insertFieldsums(station_id: str, date: datetime, station_data: StationData):
    '''
    Inserts 1+ fieldsum arrays into the fieldsums table of the database.

    Args:
        station_id
        date: the earliest datetime object for the date being passed (00:00:00)
    '''
    dts = pickle.dumps([dt.isoformat() for dt in station_data.datetimes])
    ints = pickle.dumps(station_data.intensities)

    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO fieldsums (station_id, date, datetimes, intensities) VALUES(?, ?, ?, ?)', 
                   (station_id, date.isoformat(), dts, ints))
    conn.commit()
    conn.close()

def insertFRs(station_id: str, date: datetime, fr_timestamps: list):
    fr_dump = pickle.dumps(fr_timestamps)

    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO fr_files (station_id, date, fr_timestamps) VALUES(?, ?, ?)', 
                   (station_id, date.isoformat(), fr_dump))
    conn.commit()
    conn.close()

def insertFireballs(fireballs):
    '''
    Inserts one or more fireballs into the fireballs table of the database.

    Args:
        fireballs (list of tuples): List of tuples with following format(station_id, start_time, end_time))
    
    Returns:
        Array of primary keys for each fireball in the same order as they were inserted.
    '''
    res = [] # Array of IDs

    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    for fireball in fireballs:
        cursor.execute('INSERT INTO fireballs (station_id, start_time, end_time) VALUES(?, ?, ?)', fireball)
        res.append(cursor.lastrowid)
    conn.commit()
    conn.close()

    return res

def insertClusters(clusters):
    '''
    Inserts 1+ cluster(s) into the clusters table of the database and updates FireballsClusters to reflect the relationship.

    Args:
        clusters (list of tuples): List of tuples with the format (start_time, end_time)
    Clusters:
        - cluster_id (PRIMARY KEY INT): Unique identifier for the cluster
        - start_time (TEXT): ISO8601 representation of cluster start_time
        - end_time (TEXT): ISO8601 representation of cluster end_time
    
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
