import io
from typing import Union
from ..abc import StorageBase, StorageFileBase
import oss2

class OSSFile(StorageFileBase):
    pass

class OSSReadableFile(OSSFile):
    def __init__(self, fp : oss2.models.GetObjectResult) -> None:
        super().__init__("r")
        self.__fp = fp

    def readinto(self, __buffer):
        try:
            v = self.__fp.read(len(__buffer))
            __buffer[:len(v)] = v
            return len(v)
        except KeyboardInterrupt as e:
            raise e
        except InterruptedError as e:
            raise e
        except Exception:
            return 0
        
    def flush(self):
        return
    
    def close(self):
        self.__fp.close()
    
class OSSAppendableFile(StorageFileBase):
    def __init__(self, bucket : oss2.Bucket, path) -> None:
        super().__init__("a")
        self.__path = path
        self.__bucket = bucket
        try:
            self.__infile_offset = self.__bucket.get_object_meta(path).content_length
        except oss2.exceptions.NoSuchKey:
            self.__infile_offset = 0
        

    def append(self, data : Union[bytes, memoryview]):
        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytes):
            pass
        else:
            raise TypeError("Invalid data type: %s (require bytes/memoryview)" % type(data))
        self.__bucket.append_object(self.__path, self.__infile_offset, data)
        self.__infile_offset += len(data)
    
    def flush(self):
        return
    
    def close(self):
        return

class OSSStorage(StorageBase):
    def __init__(self, bucket : str, endpoint : str, app_key : str, app_secret : str):
        self.__bucket_name = bucket
        self.__endpoint = endpoint
        self.auth = oss2.Auth(app_key, app_secret)
        self.bucket = oss2.Bucket(self.auth, endpoint, bucket)
        
    
    def open(self, path, mode, begin=None, end=None) -> OSSFile:
        if path.startswith("/"):
            path = path[1:]
        if mode == "r":
            if end is not None:
                return OSSReadableFile(self.bucket.get_object(path, byte_range=(begin, end - 1))) 
            else:
                return OSSReadableFile(self.bucket.get_object(path, byte_range=(begin, None)))
        elif mode == "a":
            # self.bucket is a stateful object with a session of signature
            
            return OSSAppendableFile(oss2.Bucket(self.auth, self.__endpoint, self.__bucket_name), path)
        else:
            raise ValueError("Unknown mode: `%s`" % mode)
        
    
    def filesize(self, path : str):
        if path.startswith("/"):
            path = path[1:]
        try:
            return self.bucket.get_object_meta(path).content_length
        except oss2.exceptions.NoSuchKey:
            return None
    
    def put(self, path: str, data: Union[bytes, io.IOBase] ):
        if path.startswith("/"):
            path = path[1:]
        self.bucket.put_object(path, data)
