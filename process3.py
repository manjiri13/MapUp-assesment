import os
import json
import csv
import argparse

def process_json_files(input_dir, output_dir):
    # Initialize a list to store processed data for CSV
    csv_data = []

    # Process each JSON file in the input directory
    for json_file in os.listdir(input_dir):
        if json_file.endswith('.json'):
            json_filepath = os.path.join(input_dir, json_file)

            with open(json_filepath, 'r') as file:
                # Load JSON data
                json_data = json.load(file)

                # Check if there are tolls in the route
                if 'route' in json_data and 'tolls' in json_data['route']:
                    for toll in json_data['route']['tolls']:
                        # Extract relevant data
                        unit = json_data.get('summary', {}).get('vehicleType', '')
                        trip_id = json_file.replace('.json', '')
                        toll_loc_id_start = toll.get('start', {}).get('id', '')
                        toll_loc_id_end = toll.get('end', {}).get('id', '')
                        toll_loc_name_start = toll.get('start', {}).get('name', '')
                        toll_loc_name_end = toll.get('end', {}).get('name', '')
                        toll_system_type = toll.get('type', '')
                        entry_time = toll.get('start', {}).get('timestamp_formatted', '')
                        exit_time = toll.get('end', {}).get('timestamp_formatted', '')
                        tag_cost = toll.get('tagCost', '')
                        cash_cost = toll.get('cashCost', '')
                        license_plate_cost = toll.get('licensePlateCost', '')

                        # Append data to the list
                        csv_data.append([unit, trip_id, toll_loc_id_start, toll_loc_id_end,
                                         toll_loc_name_start, toll_loc_name_end, toll_system_type,
                                         entry_time, exit_time, tag_cost, cash_cost, license_plate_cost])

    # Check if there is any toll data to process
    if csv_data:
        # Create the output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Write the processed data to the CSV file
        csv_filepath = os.path.join(output_dir, 'transformed_data.csv')
        with open(csv_filepath, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            
            # Write header
            csv_writer.writerow(['unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end',
                                 'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
                                 'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost'])

            # Write data
            csv_writer.writerows(csv_data)

        print(f"CSV file 'transformed_data.csv' has been created in {output_dir}")
    else:
        print("No toll data found. CSV file not created.")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Extract toll information from JSON files and transform to CSV.")
    parser.add_argument("--to_process", required=True, help="Path to the JSON responses folder.")
    parser.add_argument("--output_dir", required=True, help="The folder where the final CSV file will be stored.")
    args = parser.parse_args()

    # Process JSON files and generate CSV
    process_json_files(args.to_process, args.output_dir)
