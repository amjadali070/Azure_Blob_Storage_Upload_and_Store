from abc import ABC, abstractmethod
from typing import List
class IBlobStorage(ABC):
    @abstractmethod
    def upload_files(self, files: List[str]) -> None:
        pass

    @abstractmethod
    def fetch_blobs(self, blob_names: List[str]) -> None:
        pass

    @abstractmethod
    def store_blobs(self, blob_names: List[str], local_path: str) -> None:
        pass