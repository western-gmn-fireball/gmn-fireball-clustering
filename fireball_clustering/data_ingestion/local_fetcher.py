'''
    Data ingestion from a local file system.
'''
import os
import tarfile
import io
import datetime

from fireball_clustering.utils.fieldsum_handlers import readFieldIntensitiesBytes, filenameToDatetime
from fireball_clustering.dataclasses.models import StationData

FPS = 25

def ingestFromTarball(path: str) -> StationData:
    if not os.path.exists(path):
        raise FileNotFoundError
    
    datapoints = []
    station_data = StationData(datetimes=[], intensities=[])

    with tarfile.open(path, 'r:bz2') as tarball:
        for member in tarball.getmembers():
            if member.name.startswith('./FS') and member.name.endswith('.tar.bz2'):
                inner_file = tarball.extractfile(member)
                with tarfile.open(fileobj=inner_file, mode='r:bz2') as fs_tarball:
                    for fs_member in fs_tarball.getmembers():
                        fs_file = fs_tarball.extractfile(fs_member)
                        if fs_file == None: continue
                        fs_file_bytes = io.BytesIO(fs_file.read())

                        timestamp = filenameToDatetime(fs_member.name.split('/')[1])
                        half_frames, intensity_arr = readFieldIntensitiesBytes(fs_file_bytes)

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
