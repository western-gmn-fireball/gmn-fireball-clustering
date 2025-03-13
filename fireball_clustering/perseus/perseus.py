from fireball_clustering.dataclasses.models import StationData, ProcessedStationData, Fireball
from fireball_clustering.data_processing.preprocessing import ingestFRFiles, ingestStationData, preprocessFieldsums
from fireball_clustering.data_processing.clustering import filterFireballsWithFR, identifyFireballs, clusterFireballs
from fireball_clustering.database import db_setup

import datetime
import os

class Perseus:
    def __init__(self, fieldsums_path: str = './fieldsums', fr_path: str = './fr_files') -> None:
        self.fs_path = fieldsums_path
        self.fr_path = fr_path
        if not os.path.exists('./gmn_fireball_clustering.db'):
            db_setup.initializeEmptyDatabase()
            db_setup.insertStations()
     
    def checkExists(self):
        # Checks if the station and date already exists (i.e has been analyzed already)
        pass

    def getStationsWithinRadius(self):
        # Util function for getting list of stations within a certain radius of a station
        pass

    def ingestFieldsums(self, station_id: str, date: datetime.datetime) -> StationData:
        """
        Ingest field sum data for a specific station on a given date.

        Args:
            station_id (str): Unique identifier for the station
            date (datetime.datetime): Date of the data to be ingested

        Returns:
            StationData: Object containing the ingested station data
        
        Note:
            This method constructs and processes field sum data files based on the provided station ID and date.
        """
        file_name = f'{station_id}_{date.strftime('%Y%m%d')}'
        file_path = os.path.join(self.fs_path, file_name)
        ingestedStationData: StationData = StationData([], []) 
        ingestedStationData = ingestStationData(file_path)
        return ingestedStationData
    
    def ingestFR(self, station_id: str, date: datetime.datetime) -> list[datetime.datetime]:
        file_name = f'{station_id}_{date.strftime('%Y%m%d')}'
        file_path = os.path.join(self.fr_path, file_name)
        fr_timestamps = ingestFRFiles(file_path)
        return fr_timestamps

    def process(self, station_data: StationData) -> ProcessedStationData:
        processed_station_data = preprocessFieldsums(station_data)
        return processed_station_data

    def identify(self, 
                 station_id: str, 
                 processed_station_data: ProcessedStationData, 
                 fr_timestamps: list[datetime.datetime]
                 ) -> list[Fireball]:

        candidate_fireballs = identifyFireballs(station_id, processed_station_data)
        filtered_candidate_fireballs = filterFireballsWithFR(candidate_fireballs, fr_timestamps)
        return filtered_candidate_fireballs

    def cluster(self, fireballs: list[Fireball]):
        positive_fireballs = clusterFireballs(fireballs)
        return positive_fireballs
    
    # TODO: this
    def identificationPipeline(self):
        pass

    # TODO: this
    def clusteringPipeline(self):
        pass

