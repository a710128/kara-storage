
class Dataset:
    def __init__(self, index_controller ) -> None:
        pass
    
    @property
    def closed(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError
    
    def flush(self):
        raise NotImplementedError
    
    def write(self, data : bytes):
        raise NotImplementedError
    
    def read(self) -> bytes:
        raise NotImplementedError
    
    def seek(self, offset : int, whence : int) -> int:
        raise NotImplementedError
    
    def pread(self, offset : int) -> bytes:
        raise NotImplementedError
    
    def size(self) -> int:
        raise NotImplementedError
    
    def tell(self) -> int:
        raise NotImplementedError
    