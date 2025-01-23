'''
    Preprocessing module for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import os
import pandas as pd
import datetime
from scipy import signal

from ..utils import fieldsum_handlers as fh

FPS = 25 # FPS of camera, assumed to be 25

def ingestStationData(fieldsums_path: str):
    '''
    Args:
        fieldsums_path (str): Path to the directory containing the fieldsums. 
    Returns:
        dict: Dictionary with station data 'datetimes', 'intensities'.
    '''
    station_data = {}
    datapoints = []

    # Iterate through fieldsum files
    for f in os.listdir(fieldsums_path):
        # Get starting timestamp and fieldsum data
        timestamp = fh.filenameToDatetime(f)
        half_frames, intensity_arr = fh.readFieldIntensitiesBin(fieldsums_path, f)
        
        # Add each time step and intensity to in memory data
        for i in range(len(half_frames)):
            timestamp += datetime.timedelta(seconds=1/FPS)
            datapoints.append((timestamp, intensity_arr[i]))

    # Sort data by time 
    datapoints.sort(key = lambda x: x[0])

    # Add data to return variable
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