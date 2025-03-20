from dataclasses import dataclass
from datetime import datetime
import pandas as pd

@dataclass
class StationData:
    datetimes: list[datetime]
    intensities: list[int | float]
    
    def getDataframe(self):
        return pd.DataFrame({
            'datetime': self.datetimes,
            'intensity': self.intensities,
        })

@dataclass
class ProcessedStationData:
    datetimes: list[datetime]
    intensities: list[int | float]
    detrended_intensities: list[int | float]
    moving_std: list[float]

    def getDataframe(self):
        return pd.DataFrame({
            'datetime': self.datetimes,
            'intensity': self.intensities,
            'detrended_intensities': self.detrended_intensities,
            'moving_std': self.moving_std
        })

@dataclass
class Fireball:
    station_name: str
    start_time: datetime
    end_time: datetime
    id: int


