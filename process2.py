import os
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor

def upload_to_tollguru(file_path, output_dir):
    api_key = os.getenv("TOLLGURU_API_KEY")
    api_url = os.getenv("TOLLGURU_API_URL")

    endpoint_url = f"{api_url}&vehicleType=5AxlesTruck"
    
    headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}

    response_filename = os.path.basename(file_path).replace('.csv', '.json')
    response_filepath = os.path.join(output_dir, response_filename)

    with open(file_path, 'rb') as file:
        response = requests.post(endpoint_url, data=file, headers=headers)

        with open(response_filepath, 'w') as json_file:
            json_file.write(response.text)

def process_csv_files(csv_folder, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with ThreadPoolExecutor() as executor:
        csv_files = [file for file in os.listdir(csv_folder) if file.endswith('.csv')]
        futures = []
        for csv_file in csv_files:
            file_path = os.path.join(csv_folder, csv_file)
            futures.append(executor.submit(upload_to_tollguru, file_path, output_dir))

        for future in futures:
            future.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload GPS tracks to TollGuru API.")
    parser.add_argument("--to_process", required=True, help="Path to the CSV folder.")
    parser.add_argument("--output_dir", required=True, help="The folder where JSON files will be stored.")
    args = parser.parse_args()

    process_csv_files(args.to_process, args.output_dir)
