import sqlite3
import pandas as pd
from keplergl import KeplerGl

# def fetch_data(database_path):
#     """
#     Connects to the SQLite database and fetches nodes and events data.
#     """
#     conn = sqlite3.connect(database_path)

#     # Fetch nodes and events
#     stations = pd.read_sql_query("SELECT station_id, latitude, longitude FROM stations", conn)
#     events = pd.read_sql_query("SELECT fireball_id, start_time, end_time FROM fireballs", conn)

#     conn.close()

#     # Merge nodes and events data
#     data = events.merge(stations, left_on="station_id", right_on="station_id")
#     return data

def fetch_data(database_path):
    conn = sqlite3.connect(database_path)

    # Fetch data from tables
    fireballs = pd.read_sql_query("SELECT * FROM fireballs", conn)
    stations = pd.read_sql_query("SELECT * FROM stations", conn)

    # Merge fireballs with station data
    data = fireballs.merge(stations, on="station_id")

    conn.close()
    return data

def process_data(data):
    """
    Processes the data by converting timestamps and expanding rows for animation.
    """
    # Expand rows for timelapse (start and end times)
    rows = []
    for _, row in data.iterrows():
        rows.append({
            "station_id": row["station_id"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "timestamp": row["start_time"],
            "event": "True"
        })
        rows.append({
            "station_id": row["station_id"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "timestamp": row["end_time"],
            "event": "False"
        })

    return pd.DataFrame(rows)


def generate_kepler_config():
    """
    Generates the Kepler.gl configuration for the timelapse map.
    """
    return {
        "version": "v1",
        "config": {
            "visState": {
                "layers": [
                    {
                        "id": "nodes",
                        "type": "point",
                        "config": {
                            "dataId": "Node Events",
                            "label": "Nodes",
                            "color": [0, 128, 255],
                            "columns": {
                                "lat": "latitude",
                                "lng": "longitude",
                                "altitude": None
                            },
                            "isVisible": True,
                            "visConfig": {
                                "radius": 10,
                                "colorRange": {
                                    "colors": ["#d3d3d3", "#ff4500"]  # Neutral and highlighted colors
                                },
                                "radiusRange": [1, 20]
                            }
                        }
                    }
                ],
                "animationConfig": {
                    "enabled": True,
                    "speed": 1
                }
            },
            "mapState": {
                "latitude": 0,
                "longitude": 0,
                "zoom": 2
            }
        }
    }


def create_kepler_map(data, config, output_file):
    """
    Creates and saves a Kepler.gl map.
    """
    # Initialize a Kepler.gl map
    map_ = KeplerGl(height=1280, config=config)

    # Add data to the map
    map_.add_data(data=data, name="Node Events")

    # Save the map to an HTML file
    map_.save_to_html(file_name=output_file)


if __name__ == "__main__":
    # Define database path and output file
    DATABASE_PATH = "gmn_fireball_clustering.db"  # Replace with the path to your SQLite database
    OUTPUT_FILE = "kepler_map.html"

    # Step 1: Fetch and process data
    raw_data = fetch_data(DATABASE_PATH)
    processed_data = process_data(raw_data)

    # Step 2: Generate Kepler.gl configuration
    kepler_config = generate_kepler_config()

    # Step 3: Create and save the Kepler.gl map
    create_kepler_map(processed_data, kepler_config, OUTPUT_FILE)

    print(f"Kepler.gl map has been saved to {OUTPUT_FILE}")
