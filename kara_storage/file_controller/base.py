class FileController:
    def __init__(self) -> None:
        pass

    def read(self, length : int) -> bytes:
        raise NotImplementedError
    
    def write(self, data : bytes) -> None:
        raise NotImplementedError
    
    def seek(self, offset):
        raise NotImplementedError
    
    def flush(self):
        raise NotImplementedError
    
    @property
    def tell(self) -> int:
        raise NotImplementedError
    
    @property
    def size(self) -> int:
        raise NotImplementedError
    
    def close(self) -> None:
        raise NotImplementedError