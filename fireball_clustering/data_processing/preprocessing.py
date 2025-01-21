'''
    Preprocessing module for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import pandas as pd
import datetime
from scipy import signal

from ..utils import fieldsum_handlers as fh

FPS = 25 # FPS of camera, assumed to be 25

def ingestStationData(station_file_path: str):
    '''
    Args:
        station_file_path (str): Path to the station data file. 
    Returns:
        dict: Dictionary with station data 'datetimes', 'intensities'.
    '''
    station_data = {'datetimes': None, 'intensities': None}
    timestamp = fh.filenameToDatetime(station_file_path)
    half_frames, intensity_arr = fh.readFieldIntensitiesBin(station_file_path)

    datapoints = []
    for i in range(len(half_frames)):
        timestamp += datetime.timedelta(seconds=1/FPS)
        datapoints.append((timestamp, intensity_arr[i]))
    datapoints.sort(key = lambda x: x[0])

    station_data['datetimes'] = [x[0] for x in datapoints]
    station_data['intensities'] = [x[1] for x in datapoints]

    return station_data

def ingestMultipleStations(stations: dict) -> pd.DataFrame:
    '''
    Wrapper for ingestStationData that allows ingestion of multiple stations
    with one call.

    Args:
        stations (dict): Keys as station IDs and values of station data file paths. 
    Returns:
        pandas.DataFrame: Dictionary with station data 'datetimes', 'intensities'.
    '''
    pass

def preprocessFieldsums(station_data: dict) -> dict:
    '''
    Applies a bandpass filter and detrends the fieldsum data for a single station.

    Args:
        station_data (dict): Dictionary with station data 'datetimes', 'intensities'.
    Returns:
        dict: Dictionary with bandpass filter and detrending applied.
    '''
    # Input validation
    if 'datetimes' not in station_data: raise 'Datetimes expected in dataframe.'
    if 'intensities' not in station_data: raise 'Intensities expected in dataframe.'

    # Bandpass Filter
    b, a = signal.butter(2, [1/10, 1], btype='band', fs=FPS)
    station_data['intensities'] = signal.filtfilt(b, a, station_data['intensities'])

    # Ensure datetime var is in datetime format
    df = pd.DataFrame(station_data)
    df['datetimes'] = pd.to_datetime(df['datetimes'])
    df = df.set_index('datetimes')

    # Calculate moving avg
    window_size = f'{30}s'
    df['30s_moving_avg'] = df['intensities'].rolling(window=window_size).mean()

    # Subtract moving avg
    df['detrended_intensities'] = df['intensities'] - df['30s_moving_avg']

    # Return updated dataset
    df = df.reset_index()
    df = df.drop(columns=['30s_moving_avg'])
    processed_data = df.to_dict(orient='list')
    return processed_data