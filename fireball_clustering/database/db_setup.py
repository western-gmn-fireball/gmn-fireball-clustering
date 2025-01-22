'''
    This file contains functions to create and initialize the database with
    the following tables:
        Stations:
            - station_id (int): Unique identifier for the station.
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

def initializeEmptyDatabase():
    # Initialize connection
    con = sqlite3.connect('gmn_fireball_clustering.db')
    cursor = con.cursor()

    # Create tables
    cursor.execute("""
                   CREATE TABLE stations(
                        station_id INTEGER PRIMARY KEY,
                        latitude REAL NOT NULL,
                        longitude REAL NOT NULL 
                   )
                   """)
    cursor.execute("""
                   CREATE TABLE fireballs(
                        fireball_id INTEGER PRIMARY KEY,
                        station_id INTEGER NOT NULL,
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