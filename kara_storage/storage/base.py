
from kara_storage.storage import local
from typing import Any, Generator, Union
from urllib.parse import urlparse
import os
from ..dataset import Dataset
from ..serialization import Serializer, JSONSerializer

class RowDataset:
    def __init__(self, ds : Dataset, serializer : Serializer):
        self.__ds = ds
        self.__serializer = serializer

    @property
    def closed(self):
        return self.__ds.closed
    
    def close(self):
        return self.__ds.close()
    
    def flush(self):
        return self.__ds.flush()
    
    def write(self, data : Any):
        return self.__ds.write( self.__serializer.serialize(data) )
    
    def read(self) -> Union[Any, None]:
        v = self.__ds.read()
        if v is None or len(v) == 0:
            return None
        return self.__serializer.deserialize( v )
    
    def seek(self, offset : int, whence : int) -> int:
        return self.__ds.seek(offset, whence)
    
    def pread(self, offset : int) -> Union[Any, None]:
        v = self.__ds.pread(offset)
        if len(v) == 0:
            return None
        return self.__serializer.deserialize( v )
    
    def size(self) -> int:
        return self.__ds.size()
    
    def tell(self) -> int:
        return self.__ds.tell()
    
    def __len__(self) -> int:
        return self.size()
    
    def __iter__(self) -> Generator[Any, None, None]:
        while True:
            v = self.read()
            if v is None:
                break
            yield v
    
    def __getitem__(self, key : int) -> Any:
        if not isinstance(key, int):
            raise TypeError("Dataset index must be int")
        return self.pread(key)
        


class KaraStorage:
    def __init__(self, url : str, **kwargs) -> None:
        uri = urlparse(url)
        if uri.scheme == "file":
            path = ""
            if uri.netloc == "":
                path = uri.path
            else:
                path = os.path.join( os.path.abspath(uri.netloc), uri.path)
            from .local import LocalStorage
            self.__storage = LocalStorage(path, **kwargs)
        elif uri.scheme == "oss":
            from .oss import OSSStorage
            self.__storage = OSSStorage(uri.path[1:], "http://" + uri.netloc, kwargs["app_key"], kwargs["app_secret"])
            
    
    def open(self, namespace, key, mode="r", version="latest", serialization=None, **kwargs) -> RowDataset:
        version = str(version)
        if serialization is None:
            serialization = JSONSerializer()
        return RowDataset(self.__storage.open(namespace, key, mode, version, **kwargs), serialization)

    def loadDirectory(self, namespace, key, local_path, version="latest"):
        version = str(version)
        return self.__storage.loadDirectory(namespace, key, local_path, version)
    
    def saveDirectory(self, namespace, key, local_path, version=None):
        if version is not None:
            version = str(version)
        return self.__storage.saveDirectory(namespace, key, local_path, version)