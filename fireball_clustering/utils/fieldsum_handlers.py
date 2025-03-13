'''
    This file incorporates code from the RPi Meteor Station project:
    Copyright (C) 2015 Dario Zubovic
    Licensed under the GNU General Public License v3.0 or later.
    Original Source: 
        https://github.com/CroatianMeteorNetwork/RMS/blob/master/RMS/Formats/FRbin.py
        https://github.com/CroatianMeteorNetwork/RMS/blob/master/RMS/Formats/FieldIntensities.py
'''

import datetime
import os
import io
import numpy as np

def filenameToDatetime(file_name, microseconds='auto'):
    """ Converts FF bin file name to a datetime object.

    Arguments:
        file_name: [str] Name of a FF file.

    Keyword arguments:
        microseconds: [str/bool] If auto, the function will try to guess if the last number in the file name
            is in milliseconds or microseconds. If True, the file name contains microseconds instead of 
            milliseconds.

    Return:
        [datetime object] Date and time of the first frame in the FF file.

    """

    # e.g.  FF499_20170626_020520_353_0005120.bin
    # or FF_CA0001_20170626_020520_353_0005120.fits

    file_name = file_name.split('_')

    # Check the number of list elements, and the new fits format has one more underscore
    i = 0
    if len(file_name[0]) == 2:
        i = 1

    date = file_name[i + 1]
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:8])

    time = file_name[i + 2]
    hour = int(time[:2])
    minute = int(time[2:4])
    seconds = int(time[4:6])

    # Read the last number (either ms or us)
    ms_us_str = file_name[i + 3]

    # Check if the last number is in milliseconds or microseconds
    if microseconds == 'auto':
        if len(ms_us_str) == 6:
            microseconds = True
        else:
            microseconds = False

    if microseconds:
        us = int(file_name[i + 3])
    
    else:
        us = 1000*int(file_name[i + 3])


    return datetime.datetime(year, month, day, hour, minute, seconds, us)

def readFieldIntensitiesBin(dir_path, file_name, deinterlace=False):
    """ Read the field intensities form a binary file.

    Arguments:
        dir_path: [str] Path to the directory where the file is located.
        file_name: [str] Name of the file.
    """

    with open(os.path.join(dir_path, file_name), 'rb') as fid:
        # Read the number of entries
        n_entries = int(np.fromfile(fid, dtype=np.uint16, count = 1))

        intensity_array = np.zeros(n_entries, dtype=np.uint32)
        half_frames = np.zeros(n_entries)

        if deinterlace:
            deinterlace_flag = 2.0
        else:
            deinterlace_flag = 1.0
        
        # fid.seek(0)
        # Read individual entries
        for i in range(n_entries):

            # Calculate the half frame
            half_frames[i] = float(i)/deinterlace_flag

            # Read the summed field intensity
            intensity_array[i] = int(np.fromfile(fid, dtype=np.uint32, count = 1))

        return half_frames, intensity_array

def readFieldIntensitiesBytes(bytes: io.BytesIO, deinterlace=False):
    # Read the number of entries
    bytes.seek(0)
    n_entries = int(np.frombuffer(bytes.read(2), dtype=np.uint16, count = 1))

    intensity_array = np.zeros(n_entries, dtype=np.uint32)
    half_frames = np.zeros(n_entries)

    if deinterlace:
        deinterlace_flag = 2.0
    else:
        deinterlace_flag = 1.0

    # Read individual entries
    for i in range(n_entries):

        # Calculate the half frame
        half_frames[i] = float(i)/deinterlace_flag

        # Read the summed field intensity
        intensity_array[i] = int(np.frombuffer(bytes.read(4), dtype=np.uint32, count = 1))

    return half_frames, intensity_array
