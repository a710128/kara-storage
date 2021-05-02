from typing import Any
from .storage import StorageBase
import io

class Dataset:    
    @property
    def closed(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()
    
    def flush(self):
        raise NotImplementedError()
    
    def write(self, data : Any):
        raise NotImplementedError()
    
    def read(self) -> Any:
        raise NotImplementedError()

    def seek(self, offset : int, whence : int = io.SEEK_SET) -> int:
        raise NotImplementedError()
            
    def pread(self, offset : int) -> Any:
        raise NotImplementedError()
    
    def size(self) -> int:
        raise NotImplementedError()
    
    def tell(self) -> int:
        raise NotImplementedError()
    