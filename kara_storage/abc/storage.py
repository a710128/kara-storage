
import io
from typing import Union, Optional

class StorageFileBase:
    def __init__(self, mode) -> None:
        self.__readable = ("r" in mode)
        self.__writable = ("w" in mode) or ("a" in mode)

    def append(self, data : bytes):
        if self.__writable:
            raise NotImplementedError()
        else:
            raise RuntimeError("StorageFile is not writable")

    def readinto(self, __buffer) -> Optional[int]:
        if self.__readable:
            raise NotImplementedError()
        else:
            raise RuntimeError("StorageFile is not readable")
    
    def flush(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()


class StorageBase:
    def __init__(self):
        pass
    
    def open(self, path : str, mode : str, begin : int = None, end : int = None) -> StorageFileBase:
        raise NotImplementedError()
    
    def filesize(self, path : str) -> Union[int, None]:
        raise NotImplementedError()
    
    def put(self, path : str, data : Union[bytes, io.IOBase]) -> None:
        raise NotImplementedError()
    
    def readfile(self, path : str, chunk_size = 128 * 1024) -> bytes:
        fp = self.open(path, "r")

        buffer = io.BytesIO()
        memview = bytearray(chunk_size)
        while True:
            lw = fp.readinto(memview)
            if lw == 0 or lw is None:
                break
            buffer.write( memview[:lw] )
        fp.close()
        return buffer.getvalue()
