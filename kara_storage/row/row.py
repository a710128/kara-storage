import io
from kara_storage.row.proxy import RowDatasetProxy
import multiprocessing
from multiprocessing.connection import Connection
from typing import Any, Dict, Generator, Optional
from .dataset import RawDataset
from ..abc import StorageBase, Dataset, Serializer
from ..serialization import JSONSerializer
import threading
from multiprocessing.reduction import ForkingPickler

class RowDataset(Dataset):
    """
    RowDataset adds multi-threading, multi-processing and slicing capabilities to RawDataset.
    """
    def __init__(self, 
            storage: StorageBase, 
            prefix: str, 
            mode: str, 
            serialization : Serializer = None,
            start : int = None,
            length : int = None,
            **kwargs
        ) -> None:

        # record initial parameters
        self.__storage = storage
        self.__prefix = prefix
        self.__mode = mode
        self.__kwargs = kwargs
        self.__writable = ("w" in mode)
        self.__readable = ("r" in mode)

        # create RawDataset
        self.__ds = RawDataset(storage, prefix, mode, **kwargs)

        # initialize serializer
        if serialization is None:
            serialization = JSONSerializer()
        self.__serialization = serialization

        # init lock
        self.__lock = threading.Lock()
        
        # init ipc
        self.__ipc_lock = threading.Lock()
        self.__ipc_server = None
        self.__ipc_client_cnt = 0
        self.__ipc_serial = 0
        self.__ipc_pipes : Dict[int, Connection] = {}
        self.__ipc_pipes_back : Dict[int, Connection] = {}

        # init slice
        if self.__readable:
            if start is None:
                start = 0
            if length is None:
                length = self.__ds.size() - start
            
            self.__begin = start
            if self.__begin > self.__ds.size():
                self.__begin = self.__ds.size()
            self.__end = start + length
            if self.__end > self.__ds.size():
                self.__end = self.__ds.size()
            self.__length = self.__end - self.__begin
            self.__tell = 0
            self.__ds.seek(self.__begin, io.SEEK_SET)
        else:
            self.__length = self.__ds.size()
    
    def _read_raw(self) -> Optional[bytes]:
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")

        with self.__lock:
            if self.__tell == self.__length:
                return None
            self.__tell += 1
            return self.__ds.read()
    
    def _pread_raw(self, offset : int) -> Optional[bytes]:
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")
        if offset >= self.__length:
            return None

        with self.__lock:
            return self.__ds.pread(offset + self.__begin)
    
    def _write_raw(self, data : bytes):
        if not self.__writable:
            raise RuntimeError("Dataset is not writable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")

        with self.__lock:
            self.__ds.write(data)
    
    @property
    def closed(self):
        with self.__lock:
            return self.__ds.closed
    
    def close(self):
        with self.__lock:
            self.__ds.close()
    
    def flush(self):
        with self.__lock:
            self.__ds.flush()
    
    def write(self, data : Any):
        byte_data = self.__serialization.serialize(data)
        self._write_raw(byte_data)
    
    def read(self) -> Any:
        byte_ret = self._read_raw()
        if byte_ret is None:
            raise EOFError()
        return self.__serialization.deserialize(byte_ret)

    def seek(self, offset : int, whence : int = io.SEEK_SET) -> int:
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")

        if whence == io.SEEK_SET:
            ds_offset = self.__begin + offset
        elif whence == io.SEEK_CUR:
            ds_offset = self.__tell + self.__begin + offset
        elif whence == io.SEEK_END:
            ds_offset = self.__end - offset
        if ds_offset < self.__begin:
            ds_offset = self.__begin
        if ds_offset > self.__end:
            ds_offset = self.__end

        with self.__lock:
            self.__tell = ds_offset - self.__begin
            return self.__ds.seek(ds_offset, io.SEEK_SET)
            
    def pread(self, offset : int) -> Any:
        byte_ret = self._pread_raw(offset)
        if byte_ret is None:
            raise EOFError()
        return self.__serialization.deserialize(byte_ret)
    
    def size(self) -> int:
        with self.__lock:
            return self.__length
    
    def tell(self) -> int:
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        with self.__lock:
            return self.__tell
    
    def __ipc_main(self):
        import select
        while True:
            with self.__ipc_lock:
                if self.__ipc_client_cnt == 0:
                    # remove server if all clients exists
                    self.__ipc_server = None
                    assert len(self.__ipc_pipes) == 0
                    self.__ipc_serial = 0
                    break

                pipes = list(self.__ipc_pipes.values())
            
            readable, _, _ = select.select(pipes, [], [])
            for pipe in readable:
                cmd = pipe.recv()
                try:
                    if cmd["op"] == "closed":
                        pipe.send({
                            "code": 0,
                            "data": self.closed
                        })
                    elif cmd["op"] == "close":
                        self.close()
                        pipe.send({"code": 0})
                    elif cmd["op"] == "flush":
                        self.flush()
                        pipe.send({"code": 0})
                    elif cmd["op"] == "write":
                        self._write_raw(cmd["data"])
                        pipe.send({"code": 0})
                    elif cmd["op"] == "read":
                        pipe.send({
                            "code": 0,
                            "data": self._read_raw()
                        })
                    elif cmd["op"] == "seek":
                        pipe.send({
                            "code": 0,
                            "data": self.seek(*cmd["data"])
                        })
                    elif cmd["op"] == "pread":
                        pipe.send({
                            "code": 0,
                            "data": self._pread_raw(cmd["data"])
                        })
                    elif cmd["op"] == "size":
                        pipe.send({
                            "code": 0,
                            "data": self.size()
                        })
                    elif cmd["op"] == "size":
                        pipe.send({
                            "code": 0,
                            "data": self.tell()
                        })
                    elif cmd["op"] == "exit":
                        serial_id = cmd["data"]
                        with self.__ipc_lock:
                            self.__ipc_client_cnt -= 1
                            self.__ipc_pipes[serial_id].close()
                            self.__ipc_pipes_back[serial_id].close()

                            del self.__ipc_pipes[serial_id]
                            del self.__ipc_pipes_back[serial_id]
                    else:
                        raise ValueError("Unknown cmd: %s" % cmd)
                except Exception as e:
                    pipe.send({
                        "code": 1,
                        "data": e
                    })

    
    def _reduce_dataset(self):
        with self.__ipc_lock:
            self.__ipc_client_cnt += 1
            self.__ipc_serial += 1

            serial_id = self.__ipc_serial

            p1, p2 = multiprocessing.Pipe()
            self.__ipc_pipes[serial_id] = p1
            self.__ipc_pipes_back[serial_id] = p2
            if self.__ipc_server is None:
                self.__ipc_server = threading.Thread(target=self.__ipc_main)
                self.__ipc_server.start()
        return RowDatasetProxy, (serial_id, p2, self.__serialization)
    
    def __len__(self) -> int:
        return self.__length
    
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
    
    def slice(self, start : int = 0, length : int = None):
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")

        if length is None:
            length = self.__length - start
        return RowDataset(
            self.__storage, 
            self.__prefix, 
            self.__mode, 
            self.__serialization, 
            self.__begin + start,
            length,
            **self.__kwargs
        )
    
    def slice_(self, start : int = 0, length : int = None):
        if not self.__readable:
            raise RuntimeError("Dataset is not readable")
        if self.__ds.closed:
            raise RuntimeError("Dataset is closed")

        if length is None:
            length = self.__length - start
        
        
        with self.__lock:
            self.__begin += start
            if self.__begin > self.__ds.size():
                self.__begin = self.__ds.size()

            self.__end = self.__begin + length
            if self.__end > self.__ds.size():
                self.__end = self.__ds.size()

            self.__length = self.__end - self.__begin
            self.__tell = 0
            self.__ds.seek(self.__begin, io.SEEK_SET)
    
    def __del__(self):
        self.close()

def register_forking():
    ForkingPickler.register(RowDataset, RowDataset._reduce_dataset)

register_forking()
    
