'''
    This file contains some quality of life utility to easily grab test files from
    the GMN server.

    Author: Armaan Mahajan
'''
from os.path import isdir
import paramiko
import tarfile
import os
import shutil

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

    print(f'Fetching {remote_dir} from server...')
    SFTP.get(remotepath=remote_dir, localpath=f'tmp/{remote_dir}')
    print('Finished fetching tar file from server.')
    print('Opening tarfile.')
    with tarfile.open(f'tmp/{remote_dir}', mode='r:bz2') as file:
        print('File opened. Searching for fieldsums.')
        
        
        file.extractall(path=target_dir, members=[i for i in file.getmembers() if i.name.startswith('./FS') and i.name.endswith('tar.bz2')])
    print(f'Extracted fieldsum file from {remote_dir}.')

def _extractFieldsum(tar_file_path, folder_name):
    if not os.path.isdir('fieldsums'): os.mkdir('fieldsums')

    print(f'Extracting {tar_file_path}')
    if not os.path.isdir(f'./fieldsums/{folder_name}'): os.mkdir(f'./fieldsums/{folder_name}')
    with tarfile.open(f'./fieldsums/{tar_file_path}', mode='r:bz2') as tar:
        tar.extractall(f'./fieldsums/{folder_name}')
    print(f'Finished extracting {tar_file_path}')

def _getFrFiles(target_file, tar):
    # Ensure dirs exist
    if not os.path.isdir('./fr_files'): os.mkdir('./fr_files')

    
    with tarfile.open(f'tmp/{tar}', mode='r:bz2') as file:
        # Write FR file names to fr_files folder
        FR_FILES = [i.name for i in file.getmembers() if i.name.startswith('./FR')]
        with open(f'./fr_files/{target_file}.txt', 'w') as f:
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
                _getFieldsums(SFTP, './fieldsums', tar)
                _getFrFiles(f'{station_id}_{date}', tar)
        SFTP.chdir('/home')
    
    
    # The expected file structure is COUNTRY_DATE -> STATION_ID_DATE -> fieldsum files
    for station_id, date in nights.items():
        write_path = f"./fieldsums/{station_id}_{date}"
        if not os.path.isdir(write_path): os.mkdir(write_path)
        
        tar_files = [file for file in os.listdir('./fieldsums') if file.startswith(f'FS_{station_id}') and file.endswith('.tar.bz2')]
        for tar_file in tar_files:
            _extractFieldsum(tar_file, f"{station_id}_{date}")

def cleanup():
    fieldsum_dirs = []
    unique_dirs = set()
    parents = []
    children = []

    for item in os.listdir('./fieldsums'):
        if item.endswith('.tar.bz2'): os.remove(os.path.join('./fieldsums', item))
        if os.path.isdir(os.path.join('./fieldsums', item)):
            print(item)
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
    au_files = {
        "AU0006": f"AU0006_20221114",
        # "AU0007": f"AU0007_20221114",
        # "AU0009": f"AU0009_20221114",
        # "AU000A": f"AU000A_20221114",
        # "AU000C": f"AU000C_20221114",
        # "AU000X": f"AU000X_20221114",
        # "AU000Y": f"AU000Y_20221114",
        # "AU0010": f"AU0010_20221114",
    }

    fetchFiles(_splitNameDate([string for _, string in au_files.items()]))
    cleanup()

if __name__=='__main__':
    main()
