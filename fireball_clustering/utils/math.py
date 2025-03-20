import math
import json

def haversineRadiusPoint(lat, lon, distance_km, bearing_degrees):
    R = 6371.0
    # Convert latitude, longitude, and bearing to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    bearing_rad = math.radians(bearing_degrees)

    # Distance ratio (distance divided by Earth's radius)
    distance_ratio = distance_km / R

    # New latitude calculation
    new_lat_rad = math.asin(math.sin(lat_rad) * math.cos(distance_ratio) +
                            math.cos(lat_rad) * math.sin(distance_ratio) * math.cos(bearing_rad))

    # New longitude calculation
    new_lon_rad = lon_rad + math.atan2(math.sin(bearing_rad) * math.sin(distance_ratio) * math.cos(lat_rad),
                                       math.cos(distance_ratio) - math.sin(lat_rad) * math.sin(new_lat_rad))

    # Convert radians back to degrees
    new_lat = math.degrees(new_lat_rad)
    new_lon = math.degrees(new_lon_rad)

    return new_lat, new_lon

def stationsWithinRadius(stations: list[tuple[str, float, float]], lat: float, lon: float, radius_km: int) -> list:
    ''' Returns a list of stations within a radius of the given coordinates

    Arguments:
        station: [str] the central station
        radius_km: [int] the search radius around the central station in km

    Return:
        [list] Array of stations in the radius
    
    ''' 

    # Find northmost, southmost, eastmost, westmost points 
    north_lat, _ = haversineRadiusPoint(lat, lon, radius_km, 0)
    south_lat, _ = haversineRadiusPoint(lat, lon, radius_km, 180)
    _, east_lon = haversineRadiusPoint(lat, lon, radius_km, 90)
    _, west_lon = haversineRadiusPoint(lat, lon, radius_km, 270)

    res = []

    for station_id, other_lat, other_lon in stations:
        # Check if current station is within bounds
        if (south_lat <= other_lat <= north_lat and 
            west_lon <= other_lon <= east_lon):
            res.append(station_id)

    return res
