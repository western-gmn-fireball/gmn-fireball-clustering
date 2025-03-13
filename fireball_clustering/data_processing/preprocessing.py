'''
    Preprocessing module for the GMN fieldsum analysis pipeline.
    Author: Armaan Mahajan
    Date: 2025-01-30
'''
import os
import pandas as pd
import datetime
from scipy import signal
from concurrent.futures import ThreadPoolExecutor

from ..utils import fieldsum_handlers as fh
from ..dataclasses.models import StationData, ProcessedStationData    

# TODO: change from default 25
FPS = 25 # FPS of camera, assumed to be 25

def ingestStationData(fieldsums_path: str):
    '''
    Args:
        fieldsums_path (str): Path to the directory containing the fieldsums. 
    Returns:
        StationData: StationData dataclass with fieldsum data.
    '''
    datapoints = []
    station_data = StationData(datetimes=[], intensities=[])

    try:
        fieldsum_files = os.listdir(fieldsums_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return station_data

    # Iterate through fieldsum files
    for f in fieldsum_files:
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
    station_data.datetimes = [x[0] for x in datapoints]
    station_data.intensities = [x[1] for x in datapoints]

    return station_data

def ingestFRFiles(fr_file_path: str):
    """
    Get the datetime of detected FR events. 

    Args:
        fr_file_path (str): The path to the FR file containing filenames.

    Returns:
        List[datetime]: A list of datetime objects derived from filenames.
    """
    try:
        with open(fr_file_path, 'r') as file:
            fr_files = [line.strip('./') for line in file]
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return []
    fr_timestamps = [fh.filenameToDatetime(i) for i in fr_files]
    return fr_timestamps

# TODO: convert to dataframe before handling data
def preprocessFieldsums(station_data: StationData, avg_window=30, std_window=30) -> ProcessedStationData:
    '''
    Applies a bandpass filter and detrends the fieldsum data for a single station.

    Args:
        station_data (dict): Dictionary with station data 'datetimes', 'intensities'.
        window (int): The window size to be used for detrending and the std.
    Returns:
        ProcessedStationData: A ProcessedStationData object.
    '''
    if len(station_data.intensities) == 0:
        return ProcessedStationData([], [], [], [])

    # Convert station data to dataframe
    df = pd.DataFrame({ 
        'datetimes': station_data.datetimes,
        'intensities': station_data.intensities,
    })
    df['datetimes'] = pd.to_datetime(df['datetimes'])
    df = df.set_index('datetimes')

    # Bandpass Filter
    b, a = signal.butter(4, [1/10, 1], btype='bandpass', fs=FPS)
    df['bandpass_intensities'] = signal.filtfilt(b, a, df['intensities'])
    df['bandpass_intensities'] = abs(df['bandpass_intensities'])

    # Calculate and subtract moving avg
    avg_window_size = f'{avg_window}s'
    std_window_size = f'{std_window}s'

    df[f'{avg_window_size}_moving_avg'] = df['bandpass_intensities'].rolling(window=avg_window_size).mean()
    df['detrended_intensities'] = abs(df['bandpass_intensities'] - df[f'{avg_window_size}_moving_avg'])

    # Calculate moving standard dev
    df[f'moving_std'] = df['detrended_intensities'].rolling(window=std_window_size).std()

    # Return updated dataset
    df = df.reset_index()
    df = df.drop(columns=[f'{avg_window_size}_moving_avg'])
    processed_data = df.to_dict(orient='list')

    processed_station_data = ProcessedStationData(
        datetimes = processed_data['datetimes'],
        intensities = processed_data['intensities'],
        detrended_intensities = processed_data['detrended_intensities'],
        moving_std = processed_data['moving_std'],
    )

    return processed_station_data 

