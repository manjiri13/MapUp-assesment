import argparse
import pandas as pd
from datetime import datetime, timedelta
import os

def process_gps_data(parquet_file, output_dir):
    # Load the Parquet file into a Pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Sort the DataFrame by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Initialize variables to track trips
    current_unit = None
    trip_number = 0
    last_timestamp = None

    # Iterate through rows and create trip-specific CSV files
    for index, row in df.iterrows():
        if current_unit != row['unit']:
            # New unit encountered, reset trip information
            current_unit = row['unit']
            trip_number = 0
            last_timestamp = None

        if last_timestamp is not None:
            # Convert timestamp values to datetime objects
            timestamp_current = datetime.strptime(row['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
            timestamp_last = datetime.strptime(last_timestamp, '%Y-%m-%dT%H:%M:%SZ')

            # Calculate time difference in hours
            time_diff = (timestamp_current - timestamp_last).total_seconds() / 3600.0

            # Check if a new trip should start
            if time_diff > 7:
                trip_number += 1

        # Update last timestamp for the next iteration
        last_timestamp = row['timestamp']

        # Create a CSV file for the trip
        trip_filename = f"{current_unit}_{trip_number}.csv"
        trip_filepath = os.path.join(output_dir, trip_filename)

        # Write the row to the CSV file or create a new file if it doesn't exist
        if not os.path.exists(trip_filepath):
            row.to_frame().transpose().to_csv(trip_filepath, index=False, header=True, mode='w')
        else:
            row.to_frame().transpose().to_csv(trip_filepath, index=False, header=False, mode='a')

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process GPS data and extract trip information.")
    parser.add_argument("--to_process", required=True, help="Path to the Parquet file to be processed.")
    parser.add_argument("--output_dir", required=True, help="The folder to store the resulting CSV files.")
    args = parser.parse_args()

    # Ensure the output directory exists, create if not
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Process the GPS data and create trip-specific CSV files
    process_gps_data(args.to_process, args.output_dir)
