
import os
from dotenv import load_dotenv
from pathlib import Path
from BlobStorage.BlobStorage import AzureBlobStorage

# Load env values from .env file
load_dotenv()

account_url = os.getenv("AZURE_STORAGE_ACCOUNT")
container_name = os.getenv("AZURE_CONTAINER_NAME")

# print(f"account_url: {account_url}")
# print(f"container_name: {container_name}")

# Initialize BlobStorage
blob_storage = AzureBlobStorage(account_url, container_name)

# Upload files
files = ["./input/sample.txt"]
blob_storage.upload_files(files)

blob_names = ["./input/input.txt"]
blob_storage.fetch_blobs(blob_names)

# Store blobs in a local path
blob_names = ["./input/input.txt"]
local_path = "./output/"

# Create an absolute path
local_path = Path(local_path).resolve()

# Call the `store_blobs_in_local_path` method
blob_storage.store_blobs(blob_names, local_path)