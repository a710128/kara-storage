from typing import Dict
from .base import KaraPytorchDatasetBase
import multiprocessing as mp
from multiprocessing.connection import Connection
from multiprocessing.reduction import ForkingPickler
import threading
import atexit

class DatasetMultiprocessingProxy(KaraPytorchDatasetBase):
    def __init__(self, serial_id, pipe : Connection, length: int) -> None:
        super().__init__()
        self.__serial_id = serial_id
        self.__pipe = pipe
        self.__length = length
        self.lock = threading.Lock()
        atexit.register( self.__handle_exit )
    
    def set_epoch(self, epoch_num: int):
        with self.lock:
            self.__pipe.send({
                "code": 2,
                "epoch_num": epoch_num
            })
            assert self.__pipe.recv()["code"] == 0
    
    def __iter__(self):
        while True:
            with self.lock:
                self.__pipe.send({"code": 0})
                v = self.__pipe.recv()
            
            if v["code"] == 0:
                yield v["data"]
            elif v["code"] == 1:
                return
            else:
                raise RuntimeError("Unknown IPC response: %s" % v)

    def __handle_exit(self):
        with self.lock:
            self.__pipe.send({"code": 1, "serial": self.__serial_id})

    def __del__(self):
        atexit.unregister(self.__handle_exit)
        self.__handle_exit()

    def __len__(self):
        return self.__length

class LockedDatasetWrapper(KaraPytorchDatasetBase):
    def __init__(self, dataset : KaraPytorchDatasetBase) -> None:
        super().__init__()
        self.__dataset = dataset
        
        self.__ipc_server = None
        self.__pipes : Dict[int, Connection] = {}
        self.__pipe_back = {}

        self.__pipe_serial = 0

        self.__ipc_client_cnt = 0

        self.__lock = threading.Lock()
        self.__ipc_lock = threading.Lock()

        atexit.register(self.__handle_exit)

        self.set_epoch(0)
    
    def set_epoch(self, epoch_num):
        with self.__lock:
            self.__dataset.set_epoch(epoch_num)
            self.__it = iter(self.__dataset)

    def __read_next(self):
        with self.__lock:
            return next(self.__it)

    def __iter__(self):
        while True:
            yield self.__read_next()
    
    def __len__(self):
        return len(self.__dataset)
    
    def __ipc_main(self):
        import select
        while True:
            with self.__ipc_lock:
                if self.__ipc_client_cnt == 0:
                    # remove server if all clients exists
                    self.__ipc_server = None
                    assert len(self.__pipes) == 0
                    self.__pipe_serial = 0
                    pipes = []
                    break

                pipes = list(self.__pipes.values())

            readable, _, _ = select.select(pipes, [], [])
            for it in readable:
                op = it.recv()
                if op["code"] == 0:
                    # client read
                    try:
                        it.send({"code": 0, "data": self.__read_next()})
                    except StopIteration as e:
                        it.send({"code": 1})
                elif op["code"] == 1:
                    # client exit
                    with self.__ipc_lock:
                        self.__pipes[ op["serial"] ].close()
                        self.__pipe_back[ op["serial"] ].close()
                        del self.__pipes[ op["serial"] ]
                        del self.__pipe_back[ op["serial"] ]
                        self.__ipc_client_cnt -= 1
                elif op["code"] == 2:
                    self.set_epoch(op["epoch_num"])
                    it.send({"code": 0})
                else:
                    raise RuntimeError("Unknown op: %s" % op)

    def _reduce_dataset(self):
        with self.__ipc_lock:
            self.__ipc_client_cnt += 1
            self.__pipe_serial += 1
            serial_id = self.__pipe_serial
            
            p1, p2 = mp.Pipe()
            self.__pipes[serial_id] = p1
            self.__pipe_back[serial_id] = p2
            if self.__ipc_server is None:
                # create ipc server if not exists
                self.__ipc_server = threading.Thread(target=self.__ipc_main)
                self.__ipc_server.start()

        return  DatasetMultiprocessingProxy, (serial_id, p2, len(self))
    
    def __handle_exit(self):
        if self.__ipc_server is not None:
            self.__ipc_server.join()

    def __del__(self):
        atexit.unregister(self.__handle_exit)
        self.__handle_exit()
        

def register_forking():
    ForkingPickler.register(LockedDatasetWrapper, LockedDatasetWrapper._reduce_dataset)

register_forking()