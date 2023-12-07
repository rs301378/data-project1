'''
@author: Rohit Sharma
Date: 06-12-2023
Decription: script copy the data from bronze folder to silver folder in Google Storage after applying some transformations in the data.
'''
import json
from google.cloud import storage

def transform_employee_data(employee_data):
    # Transformation 1: Adding a suffix to employee names
    for employee in employee_data:
        employee['empname'] = employee['empname'] + '_Suffix'

    # Transformation 2: Anonymizing email addresses
        employee['email'] = 'anonymous@example.com'

    # Transformation 3: Adding a country code to phone numbers
        employee['phone'] = '+1 ' + employee['phone']

    # Transformation 4: Capitalizing the address field
        employee['address'] = employee['address'].capitalize()

    return employee_data

def copy_and_transform_data(source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path):
    # Initialize the GCS client
    client = storage.Client()

    # Get the source and destination buckets
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    # List objects in the source folder
    blobs = source_bucket.list_blobs(prefix=source_folder_path)

    # Iterate through each object in the source folder
    for blob in blobs:
        # Download the content of the object (assuming it's in JSON format)
        data = json.loads(blob.download_as_text())

        # Perform data transformation
        transformed_data = transform_employee_data(data)

        # Upload the transformed data to the destination folder
        destination_blob_name = blob.name.replace(source_folder_path, destination_folder_path)
        destination_blob = destination_bucket.blob(destination_blob_name)
        destination_blob.upload_from_string(json.dumps(transformed_data), content_type='application/json')

        print(f"Transformed and copied data from {blob.name} to {destination_blob_name}")

if __name__ == "__main__":
    # Replace these values with your actual GCS information
    source_bucket_name = "cnx_test_project1"
    source_folder_path = "cnx_test_project1/data/bronze/"
    destination_bucket_name = "cnx_test_project1"
    destination_folder_path = "cnx_test_project1/data/silver/"

    copy_and_transform_data(source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path)
