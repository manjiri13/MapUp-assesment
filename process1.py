import argparse
import pandas as pd
from datetime import datetime, timedelta
import os

def process_gps_data(parquet_file, output_dir):
    df = pd.read_parquet(parquet_file)
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    current_unit = None
    trip_number = 0
    last_timestamp = None
    for index, row in df.iterrows():
        if current_unit != row['unit']:
            current_unit = row['unit']
            trip_number = 0
            last_timestamp = None

        if last_timestamp is not None:
            timestamp_current = datetime.strptime(row['timestamp'], '%Y-%m-%dT%H:%M:%SZ')
            timestamp_last = datetime.strptime(last_timestamp, '%Y-%m-%dT%H:%M:%SZ')

            time_diff = (timestamp_current - timestamp_last).total_seconds() / 3600.0

            if time_diff > 7:
                trip_number += 1

        last_timestamp = row['timestamp']

        trip_filename = f"{current_unit}_{trip_number}.csv"
        trip_filepath = os.path.join(output_dir, trip_filename)

        if not os.path.exists(trip_filepath):
            row.to_frame().transpose().to_csv(trip_filepath, index=False, header=True, mode='w')
        else:
            row.to_frame().transpose().to_csv(trip_filepath, index=False, header=False, mode='a')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GPS data and extract trip information.")
    parser.add_argument("--to_process", required=True, help="Path to the Parquet file to be processed.")
    parser.add_argument("--output_dir", required=True, help="The folder to store the resulting CSV files.")
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    process_gps_data(args.to_process, args.output_dir)
