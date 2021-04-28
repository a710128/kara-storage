import io
from typing import Optional

class FileController(io.RawIOBase):
    def __init__(self, mode) -> None:
        self.__mode = mode
    
    def readable(self) -> bool:
        return "r" in self.__mode
    def writable(self) -> bool:
        return "w" in self.__mode
    def seekable(self) -> bool:
        return "r" in self.__mode
    
    def readinto(self, __buffer) -> Optional[int]:
        raise NotImplementedError
    
    def write(self, __b) -> Optional[int]:
        raise NotImplementedError
    
    def seek(self, __offset: int, __whence: int) -> int:
        raise NotImplementedError
    
    @property
    def closed(self):
        raise NotImplementedError

    def tell(self) -> int:
        raise NotImplementedError
    
    def close(self) -> None:
        raise NotImplementedError
    
    @property
    def size(self) -> int:
        raise NotImplementedError

    def pread(self, offset : int, length : int) -> bytes:
        raise NotImplementedError