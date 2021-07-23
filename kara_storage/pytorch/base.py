from typing import Dict, Generator, Any, Iterator, Type
import torch.utils.data as data
from ..row import RowDataset
from ..abc import DatasetIterator
from multiprocessing.connection import Connection
import multiprocessing as mp
import threading
import atexit
import logging

logger = logging.getLogger(__name__)

class KaraPytorchDatasetProxy(data.IterableDataset):
    def __init__(self, serial_id, pipe):
        self.__serial_id = serial_id
        self.__pipe = pipe
        atexit.register(self.__handle_exit)
        logger.debug("KARA Pytorch proxy %d created" % self.__serial_id)
    
    def __iter__(self):
        while True:
            try:
                self.__pipe.send({
                    "op": 0,
                })
                data = self.__pipe.recv()
                if data["code"] == 0:
                    pass
                elif data["code"] == 1:
                    raise data["data"]
                else:
                    raise ValueError("Unknown IPC server response %s" % data)

            except StopIteration:
                break
            else:
                yield data["data"]
    
    def __handle_exit(self):
        self.__pipe.send({
            "op": 1,
            "data": self.__serial_id
        })
        logger.debug("KARA Pytorch proxy %d exited" % self.__serial_id)
    
    def __del__(self):
        atexit.unregister(self.__handle_exit)
        self.__handle_exit()
    

class KaraPytorchDatasetBase(data.IterableDataset):
    def __init__(self, dataset : RowDataset, iter_tool : Type[DatasetIterator], **kwargs) -> None:
        self.__ds = dataset
        self.__iter_tool = iter_tool
        self.__kwargs = kwargs
        self.__iter = None

        # read lock
        self.__lock = threading.Lock()

        # ipc server
        self.__ipc_server = None
        self.__ipc_client_cnt = 0
        self.__ipc_serial = 0
        self.__ipc_pipes : Dict[int, Connection] = {}
        self.__ipc_pipes_back : Dict[int, Connection] = {}
        self.__ipc_lock = threading.Lock()

        self.set_epoch(0)

    
    def set_epoch(self, epoch_num : int):
        with self.__lock:
            if self.__iter is not None:
                del self.__iter
                self.__iter = None
            self.__iter = self.__iter_tool(self.__ds, epoch_num, **self.__kwargs)
    
    def __read_next(self):
        if self.__iter is None:
            raise StopIteration()
        with self.__lock:
            return self.__iter.next()

    def __iter__(self) -> Iterator[Any]:
        while True:
            try:
                v = self.__read_next()
            except StopIteration:
                break
            else:
                yield v
    
    def __len__(self) -> int:
        raise len(self.__ds)
    
    def __ipc_main(self):
        import select
        logger.debug("KARA Pytorch IPC server started")
        while True:
            with self.__ipc_lock:
                if self.__ipc_client_cnt == 0:
                    # remove server if all clients exists
                    assert len(self.__ipc_pipes) == 0
                    assert len(self.__ipc_pipes_back) == 0

                    self.__ipc_server = None
                    self.__ipc_serial = 0
                    break

                pipes = list(self.__ipc_pipes.values())
            
            readable, _, _ = select.select(pipes, [], [])
            for pipe in readable:
                cmd = pipe.recv()
                try:
                    if cmd["op"] == 0:
                        # read
                        pipe.send({
                            "code": 0,
                            "data": self.__read_next()
                        })
                    elif cmd["op"] == 1:
                        # exit
                        serial_id = cmd["data"]
                        logger.debug("KARA Pytorch IPC server recived proxy %d exited" % serial_id)
                        with self.__ipc_lock:
                            self.__ipc_client_cnt -= 1
                            self.__ipc_pipes[serial_id].close()
                            self.__ipc_pipes_back[serial_id].close()
                            del self.__ipc_pipes[serial_id]
                            del self.__ipc_pipes_back[serial_id]
                    else:
                        logger.debug("KARA Pytorch received invalid ipc cmd: %s", cmd)
                        raise ValueError("Unknown IPC proto")
                except Exception as e:
                    pipe.send({
                        "code": 1,
                        "data": e,
                    })
        # ipc server exits
        logger.debug("KARA Pytorch IPC server stoped")
                    
    
    def __reduce__(self):
        with self.__ipc_lock:
            self.__ipc_client_cnt += 1
            self.__ipc_serial += 1

            serial_id = self.__ipc_serial

            p1, p2 = mp.Pipe()
            self.__ipc_pipes[serial_id] = p1
            self.__ipc_pipes_back[serial_id] = p2
            if self.__ipc_server is None:
                self.__ipc_server = threading.Thread(target=self.__ipc_main)
                self.__ipc_server.start()
        return KaraPytorchDatasetProxy, (serial_id, p2)
        