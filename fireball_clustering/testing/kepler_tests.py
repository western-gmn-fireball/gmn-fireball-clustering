from .integration_tests import *
from ..database.db_setup import *
from ..data_processing.visualizations import * 
import os

def main():
    print('Nuking Database')
    try:
        os.remove('gmn_fireball_clustering.db')
    except OSError:
        print('Database did not exist.')

    # Database setup 
    initializeEmptyDatabase()
    insertStations()

    # Data handling
    uk_preprocessing()
    uk_fireball_clustering()

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


if __name__ == '__main__':
    main()