'''
    Queries for the SQLite DB used in this project.

    Author: Armaan Mahajan
'''
import sqlite3

def getStations(station_ids):
    '''
    Fetches the station data from the database for a given set of station IDs.

    Args:
        station_ids (str[]): An array of station ID strings
    
    Returns:
        An array of tuples with form (station_id, latitude, longitude)
    '''
    placeholders = ",".join('?' for _ in station_ids) 
    QUERY = f"SELECT * FROM stations WHERE station_id IN ({placeholders})"

    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cur = conn.cursor()
    stations = cur.execute(QUERY, station_ids)
    res = [station for station in stations]
    return res