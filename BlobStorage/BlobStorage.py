from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import List
from BlobStorage.IBlobStorage import IBlobStorage
from pathlib import Path

class AzureBlobStorage(IBlobStorage):
    def __init__(self, account_url: str, container_name: str):
        self.account_url = account_url
        self.container_name = container_name
        self.token_credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url,
            credential=self.token_credential)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def upload_files(self, files: List[str]) -> None:
        for file in files:
            blob_client = self.blob_service_client.get_blob_client(self.container_name, file)
            if not blob_client.exists():  # Check if blob exists
                with open(file, "rb") as data:
                    blob_client.upload_blob(data)
                print(f"File {file} uploaded successfully.")
            else:
                print(f"File {file} already exists in the container.")

    def fetch_blobs(self, blob_names: List[str]) -> None:
        for blob_name in blob_names:
            blob_client = self.blob_service_client.get_blob_client(self.container_name, blob_name)
            if blob_client.exists():  # Check if blob exists
                with open(f"./{blob_name}", "wb") as file:
                    download_stream = blob_client.download_blob()
                    file.write(download_stream.readall())
                print(f"Blob {blob_name} fetched successfully.")
            else:
                print(f"Blob {blob_name} does not exist in the container.")
        for blob_name in blob_names:
            blob_client = self.blob_service_client.get_blob_client(self.container_name, blob_name)
            if blob_client.exists():  # Check if blob exists
                with open(f"./{blob_name}", "wb") as file:
                    download_stream = blob_client.download_blob()
                    file.write(download_stream.readall())
                print(f"Blob {blob_name} downloaded successfully.")
            else:
                print(f"Blob {blob_name} does not exist in the container.")

    def store_blobs(self, blob_names: List[str], local_path: str) -> None:
        for blob_name in blob_names:
            blob_client = self.blob_service_client.get_blob_client(self.container_name, blob_name)
            if blob_client.exists():  # Check if blob exists
                local_file_path = Path(local_path) / blob_name
                with open(local_file_path, "wb") as file:
                    download_stream = blob_client.download_blob()
                    file.write(download_stream.readall())
                print(f"Blob {blob_name} stored in local path {local_path} successfully.")
            else:
                print(f"Blob {blob_name} does not exist in the container.")