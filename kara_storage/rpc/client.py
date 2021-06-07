import io
from typing import Any
from . import kara_storage_pb2_grpc, kara_storage_pb2
from ..abc import KaraStorageBase, Dataset
import grpc
import threading
from .op import *
import json

class RequestIterator:
    def __init__(self):
        self.__lock = threading.Event()

    
    def set_request(self, request):
        self.__request = request
        self.__lock.set()
    
    def __next__(self):
        self.__lock.wait()
        self.__lock.clear()
        return self.__request

class KaraStorageClientDataset(Dataset):
    def __init__(self, client : kara_storage_pb2_grpc.KaraGatewayStub, namespace, key, mode, version, **kwargs):
        self.__req = RequestIterator()
        self.__iter = client.StreamDataset(self.__req)
        self.lock = threading.Lock()

        self._request(KARA_OP_OPEN_DS, json.dumps({
            "namespace": namespace,
            "key": key,
            "mode": mode,
            "version": version,
            **kwargs
        }).encode("utf-8"))
    
    def _request(self, op, data = None) -> bytes:
        with self.lock:
            if data is None:
                self.__req.set_request( kara_storage_pb2.KaraRequest(op=op) )
            else:
                self.__req.set_request( kara_storage_pb2.KaraRequest(op=op, data=data) )
            ret = next(self.__iter)
            if ret.code == 1:
                raise RuntimeError(ret.data.decode("utf-8"))
            elif ret.code == 0:
                return ret.data
            elif ret.code == 2:
                raise EOFError()
            else:
                raise RuntimeError("Unknown ret code %d" % ret.code)

    @property
    def closed(self):
        return json.loads(self._request(KARA_OP_CLOSED_DS).decode("utf-8")) 
    
    def close(self):
        ret = self._request(KARA_OP_CLOSE_DS).decode("utf-8")
        if ret != "ok":
            raise RuntimeError(ret)
    
    def flush(self):
        ret = self._request(KARA_OP_FLUSH_DS).decode("utf-8")
        if ret != "ok":
            raise RuntimeError(ret)
    
    def write(self, data : bytes):
        ret = self._request(KARA_OP_WRITE_DS, data).decode("utf-8")
        if ret != "ok":
            raise RuntimeError(ret)
    
    def read(self) -> bytes:
        return self._request(KARA_OP_READ_DS)

    def seek(self, offset : int, whence : int = io.SEEK_SET) -> int:
        return  json.loads(self._request(KARA_OP_SEEK_DS, json.dumps({
            "offset": offset,
            "whence": whence,
        }).encode("utf-8") ).decode("utf-8"))
            
    def pread(self, offset : int) -> bytes:
        return self._request(KARA_OP_PREAD_DS, json.dumps({
            "offset": offset
        }).encode("utf-8"))
    
    def size(self) -> int:
        return json.loads(self._request(KARA_OP_SIZE_DS).decode("utf-8"))
    
    def tell(self) -> int:
        return json.loads(self._request(KARA_OP_TELL_DS).decode("utf-8"))
    
    

class KaraStorageClient(KaraStorageBase):
    def __init__(self, address) -> None:
        self.channel = grpc.insecure_channel(address)
        self.client = kara_storage_pb2_grpc.KaraGatewayStub(self.channel)

    def open_dataset(self, namespace: str, key: str, mode : str = "r", version="latest", **kwargs):
        return KaraStorageClientDataset(self.client, namespace, key, mode, str(version), **kwargs)
    
    def _response_wrapper(self, resp) -> str :
        if resp.code == 0:
            return resp.data.decode("utf-8")
        elif resp.code == 1:
            raise RuntimeError( resp.data.decode("utf-8") )
        else:
            raise RuntimeError("Unknown response code: %d" % resp.code)

    def __get_meta(self, type_, namespace, key):
        resp = self.client.GetMeta(kara_storage_pb2.KaraRequest(data=json.dumps({
            "type": type_,
            "namespace": namespace,
            "key": key
        }).encode("utf-8")))
        
        return json.loads(self._response_wrapper(resp))

    def get_row_meta(self, namespace : str, key : str):
        return self.__get_meta("row", namespace, key)
    
    def get_object_meta(self, namespace : str, key : str):
        return self.__get_meta("obj", namespace, key)
    
    def __put_meta(self, type_, namespace, key, meta):
        resp = self.client.PutMeta(kara_storage_pb2.KaraRequest(data=json.dumps({
            "type": type_,
            "namespace": namespace,
            "key": key,
            "meta": meta
        }).encode("utf-8")))
        self._response_wrapper(resp)

    def put_row_meta(self, namespace : str, key : str, meta : Any):
        self.__put_meta("row", namespace, key, meta)
    
    def put_object_meta(self, namespace : str, key : str, meta : Any):
        self.__put_meta("obj", namespace, key, meta)
