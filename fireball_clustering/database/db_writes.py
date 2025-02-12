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

def insertFieldsums(fieldsums):
    '''
    Inserts 1+ fieldsum arrays into the fieldsums table of the database.

    Args:
        fieldsums (list of tuples): List of tuples with format (station_id, date, np.array(fieldsums))  
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO fieldsums (station_id, date, fieldsums) VALUES(?, ?, ?)', fieldsums)
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
