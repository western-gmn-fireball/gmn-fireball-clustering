'''
    Queries for the SQLite DB used in this project.

    Author: Armaan Mahajan
'''
import sqlite3
import datetime
import pickle
import math
from datetime import datetime

from fireball_clustering.dataclasses.models import StationData
from fireball_clustering.database.db_connection import Database
from fireball_clustering.utils.fieldsum_handlers import filenameToDatetime
from fireball_clustering import parameters
 
def getAllStations() -> list[tuple[str, float, float]]:
    '''
    Returns:
        List of tuples of form (<STATION_ID>, <LAT>, <LON>)
    '''
    db = Database()
    conn = db.conn
    cur = db.cur
    stations = cur.execute('SELECT * FROM stations')
    res = [row for row in stations]
    return res

def getStationsDataByID(station_ids):
    '''
    Fetches the station data from the database for a given set of station IDs.

    Args:
        station_ids (str[]): An array of station ID strings
    
    Returns:
        An array of tuples with form (station_id, latitude, longitude)
    '''
    placeholders = ",".join('?' for _ in station_ids) 
    QUERY = f"SELECT * FROM stations WHERE station_id IN ({placeholders})"
    db = Database()
    conn = db.conn
    cur = db.cur
    stations = cur.execute(QUERY, station_ids)
    res = [station for station in stations]
    return res

def getStationDataByDate(station_id: str, date: datetime) -> StationData:
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * from fieldsums WHERE station_id = ? AND date = ?', (station_id, date.isoformat()))
    row = cur.fetchone()
    
    if row:
        dts = pickle.loads(row[3])
        ints = pickle.loads(row[4])
    else:
        raise ValueError(f"No fieldsum data found for {station_id} - {date}")

    return StationData(datetimes=dts, intensities=ints)

def getStationsWithinRadius(station_id: str) -> list[str]:
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * FROM radius WHERE station_id = ?', (station_id,))
    row = cur.fetchone()
    if row:
        _, stations_within_radius_blob = row
        stations = pickle.loads(stations_within_radius_blob)
    else:
        raise ValueError(f"No stations within radius information found for: {station_id}")

    return stations

def getIngestedStations() -> list[tuple[str, datetime]]:
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * FROM analysis WHERE status="ingested"')
    rows = cur.fetchall()
    ingested_stations = [(station_id, datetime.fromisoformat(date)) for station_id, date, _ in rows]
    return ingested_stations

# TODO: clean this up
def getIngestedRadii() -> list[list[tuple[str, datetime]]]:
    '''
    Gets all stations that have all radius stations ingested.

    Returns:
        List of tuples with form (<STATION_ID>, <DATE_OBJ>)
    '''
    ingested_stations = set()
    stations_dates_map = {}
    for station_id, date_obj in getIngestedStations():
        ingested_stations.add(station_id)
        stations_dates_map[station_id] = date_obj

    res = []
    all_station_ids = [station_id for station_id, _, _ in getAllStations()]
    for station_id in all_station_ids:
        # Get number of stations within radius that are ingested
        radius_set = set(getStationsWithinRadius(station_id))
        ingested_within_radius = radius_set.intersection(ingested_stations)
        num_ingested_within_radius = len(ingested_within_radius)
        # If enough are ingested, add them to the return value
        if num_ingested_within_radius >= math.floor(len(radius_set) * parameters.MIN_CAMERAS):
            stations_to_process = []
            for station in ingested_within_radius:
                if station in stations_dates_map:
                    stations_to_process.append((station, stations_dates_map[station]))
            if len(stations_to_process) != 0:
                res.append(stations_to_process)
    
    return res

def getRadiusStations(station_id: str):
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * FROM radius WHERE station_id=?', (station_id,))
    row = cur.fetchone()
    if row:
        return pickle.loads(row[1])
    else:
        return []

def isProcessed(station_id: str, date: str) -> bool:
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * FROM analysis WHERE station_id=? AND date=?', 
                (station_id, date))
    row = cur.fetchone()
    print(row)
    return True if row[2] == 'processed' else False

def getFrTimestampsByDate(station_id: str, date: datetime) -> list[datetime]:
    '''
    Gets all fr_timestamps for a given station on a given day.

    Args:
        station_id: str of station id
        date: datetime object for earliest time of the given day (00:00:00)
    '''
    db = Database()
    conn = db.conn
    cur = db.cur
    cur.execute('SELECT * from fr_files WHERE station_id = ? AND date = ?', (station_id, date.isoformat()))
    row = cur.fetchone()
    
    if row:
        fr_files = pickle.loads(row[2])
        fr_datetimes = [filenameToDatetime(file.split('/')[1]) for file in fr_files]
    else:
        raise ValueError(f"No fr_file data found for {station_id} - {date}")
    
    return fr_datetimes

# def getFireballsByDate(date):
#     '''
#     Fetches all recorded fireball candidates for a given date in iso format.
#
#     Args:
#         date (string): A string of the date to be fetched in ISO YYYY-MM-DD format.
#
#     Returns:
#         An array of tuples that match the given date.
#     '''
#     # Input validation
#     try:
#         datetime.date.fromisoformat(date)
#     except ValueError:
#         return None
#
#     QUERY = f"SELECT * FROM fireballs WHERE start_time LIKE '{date}%'"
#
#     conn = sqlite3.connect('gmn_fireball_clustering.db')
#     cur = conn.cursor()
#     fireballs = cur.execute(QUERY)
#     res = [fireball for fireball in fireballs]
#     return res
