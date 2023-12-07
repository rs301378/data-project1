'''
@author: Rohit Sharma
Date: 06-12-2023
Decription: script copy the data from landing folder to bronze folder in Google Storage.
'''

from google.cloud import storage

def copy_data_to_bronze(source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path):
    # Initialize the GCS client
    client = storage.Client()

    # Get the source and destination buckets
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)

    # List objects in the source folder
    blobs = source_bucket.list_blobs(prefix=source_folder_path)

    # Copy each object to the destination folder
    for blob in blobs:
        destination_blob_name = blob.name.replace(source_folder_path, destination_folder_path)
        destination_blob = destination_bucket.blob(destination_blob_name)

        # Copy the blob
        destination_blob.compose([blob])

        print(f"Copied {blob.name} to {destination_blob_name}")

if __name__ == "__main__":
    # Replace these values with your actual GCS information
    source_bucket_name = "cnx_test_project1"
    source_folder_path = "cnx_test_project1/data/landing"
    destination_bucket_name = "cnx_test_project1"
    destination_folder_path = "cnx_test_project1/data/bronze"

    copy_data_to_bronze(source_bucket_name, source_folder_path, destination_bucket_name, destination_folder_path)
