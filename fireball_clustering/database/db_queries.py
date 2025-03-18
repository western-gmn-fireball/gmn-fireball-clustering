'''
    Queries for the SQLite DB used in this project.

    Author: Armaan Mahajan
'''
import sqlite3
import datetime
import pickle
from datetime import datetime

from fireball_clustering.dataclasses.models import StationData

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

def getStationDataByDate(station_id: str, date: datetime) -> StationData:
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cur = conn.cursor()
    cur.execute('SELECT * from fieldsums WHERE station_id = ? AND date = ?', (station_id, date.isoformat()))
    row = cur.fetchone()
    
    if row:
        dts = pickle.loads(row[3])
        ints = pickle.loads(row[4])
    else:
        raise ValueError(f"No fieldsum data found for {station_id} - {date}")

    return StationData(datetimes=dts, intensities=ints)

def getFrTimestampsByDate(station_id: str, date: datetime) -> list:
    '''
    Gets all fr_timestamps for a given station on a given day.

    Args:
        station_id: str of station id
        date: datetime object for earliest time of the given day (00:00:00)
    '''
    conn = sqlite3.connect('gmn_fireball_clustering.db')
    cur = conn.cursor()
    cur.execute('SELECT * from fr_files WHERE station_id = ? AND date = ?', (station_id, date.isoformat()))
    row = cur.fetchone()
    
    if row:
        fr_timestamps = pickle.loads(row[2])
    else:
        raise ValueError(f"No fr_file data found for {station_id} - {date}")

    return fr_timestamps

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
