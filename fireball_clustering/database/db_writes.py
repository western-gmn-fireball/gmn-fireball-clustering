'''
    Handles all logic related to writing to the gmn_fireball_clustering database

    Author: Armaan Mahajan
'''

import sqlite3

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

def insertFireballs(fireballs):
    '''
    Inserts one or more fireballs into the fireballs table of the database.

    Args:
        fireballs (list of tuples): List of tuples with following format(station_id, start_time, end_time))
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO fireballs (station_id, start_time, end_time) VALUES(?, ?, ?)', fireballs)
    conn.close()

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
    pass

def insertFireballClusters():
    pass