import io
import os
from typing import Union
from ..abc import StorageBase, StorageFileBase

class LocalFile(StorageFileBase):
    def __init__(self, fp : io.RawIOBase, mode) -> None:
        super().__init__(mode)

        self.__fp = fp

    def append(self, data : bytes):
        rest_length = len(data)
        while rest_length > 0:
            wrt_len = self.__fp.write(data[-rest_length:])
            rest_length -= wrt_len
        

    def readinto(self, __buffer):
        return self.__fp.readinto(__buffer)
    
    def flush(self):
        self.__fp.flush()
    
    def close(self):
        self.__fp.close()

class LocalFileStorage(StorageBase):
    def __init__(self):
        pass
    
    def open(self, path, mode, begin=None, end=None) -> LocalFile:
        if mode == "r":
            fp = open(path, "rb")
            if begin is not None:
                fp.seek(begin, io.SEEK_SET)
        elif mode == "a":
            os.makedirs(os.path.dirname( os.path.abspath(path) ), exist_ok=True)
            fp = open(path, "ab")
        else:
            raise ValueError("Unknown mode: `%s`" % mode)
        return LocalFile(fp, mode)
        
    
    def filesize(self, path):
        if not os.path.exists(path):
            return None
        return os.stat(path).st_size
    
    def put(self, path: str, data: Union[bytes, io.RawIOBase, io.BufferedIOBase]):
        # make dir for new file
        os.makedirs(os.path.dirname( os.path.abspath(path) ), exist_ok=True)

        if isinstance(data, bytes):
            open(path, "wb").write(data)
        else:
            buf = bytearray(4 * 1024 * 1024)
            fout = open(path, "wb")
            while True:
                lw = data.readinto(buf)
                if lw is None or lw == 0:
                    break
                fout.write(buf[:lw])
            fout.close()