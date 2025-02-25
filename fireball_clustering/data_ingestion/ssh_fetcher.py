'''
    This file contains some quality of life utility to easily grab test files from
    the GMN server.

    Author: Armaan Mahajan
'''
import paramiko
import tarfile
import os
import io

def _getSftpClient():
    HOST = os.getenv('GMN_HOST')
    USER = os.getenv('GMN_USER')
    PASS = os.getenv('GMN_PASS')
    KEY_FILE = os.getenv('GMN_KEY')

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
    print(f'Extracting {tar_file_path}')
    if not os.path.isdir(f'./fieldsums/{folder_name}'): os.mkdir(f'./fieldsums/{folder_name}')
    with tarfile.open(f'./fieldsums/{tar_file_path}', mode='r:bz2') as tar:
        tar.extractall(f'./fieldsums/{folder_name}')
    print(f'Finished extracting {tar_file_path}')

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
        SFTP.chdir('/home')
    
    # Get list of fieldsums tarballs and extract them
    tar_files = [file for file in os.listdir('./fieldsums') if file.startswith('FS') and file.endswith('.tar.bz2')]
    for tar_file in tar_files:
        # TODO: extract to folder for specific station
        _extractFieldsum(tar_file, f'{station_id}_{date}')

def _splitNameDate(strings):
    res = {}
    for string in strings:
        station_id, date = string.split('_')
        res[station_id] = date
    return res

def main():
    au_files = {
        "AU0006": f"AU0006_20221114",
        "AU0007": f"AU0007_20221114",
        "AU0009": f"AU0009_20221114",
        "AU000A": f"AU000A_20221114",
        "AU000C": f"AU000C_20221114",
        "AU000X": f"AU000X_20221114",
        "AU000Y": f"AU000Y_20221114",
        "AU0010": f"AU0010_20221114",
    }

    fetchFiles(_splitNameDate([string for _, string in au_files.items()]))

if __name__=='__main__':
    main()