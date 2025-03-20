'''
This file contains some quality of life utility to easily grab test files from
    the GMN server.

    Author: Armaan Mahajan
'''
from os.path import isdir
import paramiko
import tarfile
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import time

from fireball_clustering.database import db_queries

def _getSftpClient():
    HOST = os.getenv('GMN_HOST')
    USER = os.getenv('GMN_USER')
    PASS = os.getenv('GMN_PASS')
    KEY_FILE = os.getenv('GMN_KEY')

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if HOST and USER and PASS and KEY_FILE:
        ssh.connect(hostname=HOST, username=USER, password=PASS, port=22, key_filename=KEY_FILE)

    sftp = ssh.open_sftp()
    return sftp

def _getFieldsums(SFTP: paramiko.SFTPClient, target_dir: str, remote_dir: str):
    if not os.path.isdir('tmp'): os.mkdir('tmp')

    SFTP.get(remotepath=remote_dir, localpath=f'tmp/{remote_dir}')
    with tarfile.open(f'tmp/{remote_dir}', mode='r:bz2') as file:
        file.extractall(path=target_dir, members=[i for i in file.getmembers() if i.name.startswith('./FS') and i.name.endswith('tar.bz2')], filter='fully_trusted')

def _extractFieldsum(tar_file_path, folder_name):
    if not os.path.isdir('fieldsums'): os.mkdir('fieldsums')

    if not os.path.isdir(f'./fieldsums/{folder_name}'): os.mkdir(f'./fieldsums/{folder_name}')
    with tarfile.open(f'./fieldsums/{tar_file_path}', mode='r:bz2') as tar:
        tar.extractall(f'./fieldsums/{folder_name}', filter='fully_trusted')

def _getFrFiles(target_file, tar):
    # Ensure dirs exist
    if not os.path.isdir('./fr_files'): os.mkdir('./fr_files')

    with tarfile.open(f'tmp/{tar}', mode='r:bz2') as file:
        # Write FR file names to fr_files folder
        FR_FILES = [i.name for i in file.getmembers() if i.name.startswith('./FR')]
        with open(f'./fr_files/{target_file}', 'w') as f:
            for item in FR_FILES:
                f.write(item + '\n')

def fetchFiles(nights):
    '''
    Fetches files for a given list of stations and dates

    Args:
        nights (dict): Dict of form <STATION_ID> : <DATE(YYYYMMDD)>
    '''
    SFTP = _getSftpClient()
    SFTP.chdir('..')

    # Fetch remote station files for given date and download to local
    for station_id, date in nights.items():
        station_id_lower = station_id.lower()
        SFTP.chdir(f'{station_id_lower}/files/processed')
        for tar in SFTP.listdir():
            if tar.startswith(f'{station_id}_{date}'):
                start_time = time.time()
                print(f'Extracting {station_id}_{date}')
                _getFieldsums(SFTP, './fieldsums', tar)
                _getFrFiles(f'{station_id}_{date}', tar)
                seconds = round(time.time() - start_time)
                print(f'Extracted {station_id}_{date}. Took {seconds}s.')
        SFTP.chdir('/home')

    extracted = []
    # The expected file structure is COUNTRY_DATE -> STATION_ID_DATE -> fieldsum files
    for station_id, date in nights.items():
        write_path = f"./fieldsums/{station_id}_{date}"
        if not os.path.isdir(write_path): os.mkdir(write_path)

        tar_files = [file for file in os.listdir('./fieldsums') if file.startswith(f'FS_{station_id}') and file.endswith('.tar.bz2')]
        for tar_file in tar_files:
            _extractFieldsum(tar_file, f"{station_id}_{date}")
            extracted.append(tar_file)

    return extracted

def cleanup():
    fieldsum_dirs = []
    unique_dirs = set()
    parents = []
    children = []

    for item in os.listdir('./fieldsums'):
        if item.endswith('.tar.bz2'): os.remove(os.path.join('./fieldsums', item))
        if os.path.isdir(os.path.join('./fieldsums', item)):
            fieldsum_dirs.append(item)
            children.append(os.path.join('./fieldsums', item))

    for i in fieldsum_dirs:
        country_date = i[0] + i[1] + "_" + i.split('_')[-1]
        unique_dirs.add(country_date)

    for dir in unique_dirs:
        full_path = os.path.join('./fieldsums', dir)
        parents.append(full_path)
        if not os.path.isdir(full_path):
            os.makedirs(full_path, exist_ok=True)

    for parent in parents:
        for child in children:
            if parent.split('_')[-1] == child.split('_')[-1]:
                shutil.move(child, parent)

    shutil.rmtree('/tmp', ignore_errors=True)

def _splitNameDate(strings):
    res = {}
    for string in strings:
        station_id, date = string.split('_')
        res[station_id] = date
    return res

def main():
    # stations = [
    #     "AU0006",
    #     "AU0007",
    #     "AU0009",
    #     "AU000A",
    #     "AU000C",
    #     "AU000X",
    #     "AU000Y",
    #     "AU0010",
    #     "AU000Q",
    #     "AU004B",
    #     "AU004K",
    #     "AU0047",
    #     "AU000D",
    #     "AU000Z",
    #     "AU001A",
    #     "AU0038",
    #     "AU000E",
    #     "AU000F",
    #     "AU000V",
    #     "AU000U",
    #     "AU000W",
    #     "AU000G",
    #     "AU0002",
    #     "AU0003",
    # ]
    #
    # dates = [
    #     '20221114',
    #     '20241212',
    #     '20240506',
    #     '20221204',
    #     '20230319',
    #     '20221107',
    # ]
    stations = db_queries.getRadiusStations('AU000X')
    dates = [
        '20221114'
    ]
    
    all_nights = []
    for date in dates:
        nights = {}
        for station in stations:
              nights[station] = date
        all_nights.append(nights)

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetchFiles, all_nights)
    
    # extracted = fetchFiles(nights)
    #
    # all_files = set([f"{station}_{date}" for station, date in nights.items()])
    # extracted_files = set([string.split('_')[2] for string in extracted])
    # try:
    #     assert all_files == extracted_files
    # except AssertionError:
    #     longer = all_files if len(all_files) > len(extracted_files) else extracted_files
    #     shorter = all_files if len(all_files) < len(extracted_files) else extracted_files
    #     print(f'Failed to extract files {longer - shorter}')
    

if __name__=='__main__':
    main()
