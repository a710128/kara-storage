import io
from typing import Any, Generator
from ..abc import Dataset, Serializer
import multiprocessing.connection
import atexit

class RowDatasetProxy(Dataset):
    def __init__(self, serial_id : int, pipe : multiprocessing.connection.Connection, serialization : Serializer) -> None:
        self.__serial_id = serial_id
        self.__pipe = pipe
        self.__serialization = serialization
        atexit.register(self.__handle_exit)

    @property
    def closed(self):
        self.__pipe.send({"op": "closed"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return ret["data"]
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def close(self):
        self.__pipe.send({"op": "close"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def flush(self):
        self.__pipe.send({"op": "flush"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def write(self, data : Any):
        byte_data = self.__serialization.serialize(data)
        
        self.__pipe.send({"op": "write", "data": byte_data})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def read(self) -> Any:
        self.__pipe.send({"op": "read"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            if ret["data"] is None:
                raise EOFError()
            return self.__serialization.deserialize(ret["data"])
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)

    def seek(self, offset : int, whence : int = io.SEEK_SET) -> int:
        self.__pipe.send({"op": "seek", "data": (offset, whence)})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return ret["data"]
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
            
    def pread(self, offset : int) -> Any:
        self.__pipe.send({"op": "pread", "data": offset})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            if ret["data"] is None:
                raise EOFError()
            return self.__serialization.deserialize(ret["data"])
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def size(self) -> int:
        self.__pipe.send({"op": "size"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return ret["data"]
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def tell(self) -> int:
        self.__pipe.send({"op": "tell"})
        ret = self.__pipe.recv()
        if ret["code"] == 0:
            return ret["data"]
        elif ret["code"] == 1:
            raise ret["data"]
        else:
            raise ValueError("Unknown response from dataset server: %s" % ret)
    
    def __handle_exit(self):
        self.__pipe.send({"op": "exit", "data": self.__serial_id})
    
    def __del__(self):
        atexit.unregister(self.__handle_exit)
        self.__handle_exit()
    
    def __len__(self) -> int:
        return self.size()
    
    def __iter__(self) -> Generator[Any, None, None]:
        while True:
            try:
                v = self.read()
            except EOFError:
                break
            yield v
    
    def __getitem__(self, key : int) -> Any:
        if not isinstance(key, int):
            raise TypeError("Dataset index must be int")
        try:
            return self.pread(key)
        except EOFError:
            raise IndexError("Index `%d` is out of range" % key)