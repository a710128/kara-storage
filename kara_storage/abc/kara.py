from typing import Any
from .storage import StorageBase
from .dataset import Dataset
from .serializer import Serializer
import warnings

class KaraStorageBase:
    def open_dataset(self, 
        namespace : str, key : str, mode : str = "r", version="latest", 
        serialization : Serializer = None, **kwargs
    ) -> Dataset:
        raise NotImplementedError()

    def open(self, *args, **kwargs) -> Dataset:
        warnings.warn(
            "KaraStorage.open is deprecated, and will be removed in the future.\n" +
            "Please use KaraStorage.open_dataset instead."
        )
        return self.open_dataset(*args, **kwargs)

    def load_directory(self, namespace : str, key : str, local_path : str, version = "latest", progress_bar=True) -> str:
        raise NotImplementedError()
    
    def save_directory(self, namespace : str, key : str, local_path : str, version = None, progress_bar=True) -> str:
        raise NotImplementedError()

    def get_row_meta(self, namespace : str, key : str):
        raise NotImplementedError()
    
    def get_object_meta(self, namespace : str, key : str):
        raise NotImplementedError()
    
    def put_row_meta(self, namespace : str, key : str, meta : Any):
        raise NotImplementedError()
    
    def put_object_meta(self, namespace : str, key : str, meta : Any):
        raise NotImplementedError()