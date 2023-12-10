import os
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor
#from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

def upload_to_tollguru(file_path, output_dir):
    # Get TollGuru API key and URL from environment variables
    api_key = os.getenv("TOLLGURU_API_KEY")
    api_url = os.getenv("TOLLGURU_API_URL")

    # API endpoint for uploading CSV files
    endpoint_url = f"{api_url}&vehicleType=5AxlesTruck"
    
    # Set headers
    headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}

    # Prepare file name for JSON response
    response_filename = os.path.basename(file_path).replace('.csv', '.json')
    response_filepath = os.path.join(output_dir, response_filename)

    with open(file_path, 'rb') as file:
        # Send a POST request to TollGuru API
        response = requests.post(endpoint_url, data=file, headers=headers)

        # Save the JSON response to the specified output directory
        with open(response_filepath, 'w') as json_file:
            json_file.write(response.text)

def process_csv_files(csv_folder, output_dir):
    # Ensure the output directory exists, create if not
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Process CSV files concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        csv_files = [file for file in os.listdir(csv_folder) if file.endswith('.csv')]
        futures = []
        for csv_file in csv_files:
            file_path = os.path.join(csv_folder, csv_file)
            futures.append(executor.submit(upload_to_tollguru, file_path, output_dir))

        # Wait for all tasks to complete
        for future in futures:
            future.result()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Upload GPS tracks to TollGuru API.")
    parser.add_argument("--to_process", required=True, help="Path to the CSV folder.")
    parser.add_argument("--output_dir", required=True, help="The folder where JSON files will be stored.")
    args = parser.parse_args()

    # Process CSV files and upload to TollGuru API
    process_csv_files(args.to_process, args.output_dir)
