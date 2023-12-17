import requests
from google.cloud import storage

def extract_data(url):
    response = requests.get(url)
    return response.json()

def upload_to_gcs(data, bucket_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(str(data))

URL = "URL_API"
BUCKET_NAME = "raw-bucket-en-gcp"
DESTINATION_BLOB_NAME = "datos_incendios/raw_data.json"

data = extract_data(URL)
upload_to_gcs(data, BUCKET_NAME, DESTINATION_BLOB_NAME)
