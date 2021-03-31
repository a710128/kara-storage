from _typeshed import WriteableBuffer, ReadableBuffer
import io
from typing import Union, Optional

class FileController(io.RawIOBase):
    def __init__(self) -> None:
        pass
    
    def readable(self) -> bool:
        return True
    def writable(self) -> bool:
        return True
    def seekable(self) -> bool:
        return True
    
    def readinto(self, __buffer: WriteableBuffer) -> Optional[int]:
        raise NotImplementedError
    
    def write(self, __b: ReadableBuffer) -> Optional[int]:
        raise NotImplementedError
    
    def seek(self, __offset: int, __whence: int) -> int:
        raise NotImplementedError
    
    @property
    def closed(self):
        raise NotImplementedError
    @property
    def tell(self) -> int:
        raise NotImplementedError
    def close(self) -> None:
        raise NotImplementedError
    
    @property
    def size(self) -> int:
        raise NotImplementedError

    def pread(self, offset : int, length : int) -> bytes:
        raise NotImplementedError