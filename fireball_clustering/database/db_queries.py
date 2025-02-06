'''
    Queries for the SQLite DB used in this project.

    Author: Armaan Mahajan
'''
import sqlite3
import datetime

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

def getFireballsByDate(date):
    '''
    Fetches all recorded fireball candidates for a given date in iso format.

    Args:
        date (string): A string of the date to be fetched in ISO YYYY-MM-DD format.

    Returns:
        An array of tuples that match the given date.
    '''
    # Input validation
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        return None

    QUERY = f"SELECT * FROM fireballs WHERE start_time LIKE '{date}%'"

    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cur = conn.cursor()
    fireballs = cur.execute(QUERY)
    res = [fireball for fireball in fireballs]
    return res