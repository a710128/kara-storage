
from typing import Dict, Iterable

import io
from . import kara_storage_pb2
from . import kara_storage_pb2_grpc
import grpc
from ..storage import KaraStorage
from ..serialization import NoSerializer
import json
from concurrent import futures
from .op import *


class KaraService(kara_storage_pb2_grpc.KaraGatewayServicer):
    def __init__(self, storage : KaraStorage) -> None:
        self._storage = storage

    def StreamDataset(self, request_iterator : Iterable[kara_storage_pb2.KaraRequest], context : grpc.RpcContext):
        dataset = None
        def close_dataset():
            if dataset is not None:
                dataset.close()

        context.add_callback(close_dataset)
        for request in request_iterator:
            try:
                if request.op == KARA_OP_OPEN_DS:
                    if dataset is not None:
                        dataset.close()
                        dataset = None
                    req = json.loads( request.data.decode("utf-8") )

                    kwargs = {}
                    if "buffer_size" in req:
                        kwargs["buffer_size"] = req["buffer_size"]

                    dataset = self._storage.open_dataset( req["namespace"], req["key"], req["mode"], req["version"], serialization=NoSerializer(), **kwargs)
                    yield kara_storage_pb2.KaraResponse(code=0, data= b"ok" )

                elif request.op == KARA_OP_CLOSE_DS:
                    dataset.close()
                    yield kara_storage_pb2.KaraResponse(code=0, data= b"ok" )

                elif request.op == KARA_OP_CLOSED_DS:
                    yield kara_storage_pb2.KaraResponse(code=0, data=b"true" if dataset.closed else b"false" )
                
                elif request.op == KARA_OP_FLUSH_DS:
                    dataset.flush()
                    yield kara_storage_pb2.KaraResponse(code=0, data= b"ok" )
                elif request.op == KARA_OP_WRITE_DS:
                    dataset.write(request.data)
                    yield kara_storage_pb2.KaraResponse(code=0, data= b"ok" )
                elif request.op == KARA_OP_READ_DS:
                    try:
                        yield kara_storage_pb2.KaraResponse(code=0, data=dataset.read())
                    except EOFError:
                        yield kara_storage_pb2.KaraResponse(code=2)
                elif request.op == KARA_OP_SEEK_DS:
                    req = json.loads( request.data.decode("utf-8") )
                    yield kara_storage_pb2.KaraResponse(code=0, data=json.dumps(dataset.seek(req["offset"], req["whence"])).encode("utf-8"))
                elif request.op == KARA_OP_PREAD_DS:
                    try:
                        req = json.loads( request.data.decode("utf-8") )
                    except EOFError:
                        yield kara_storage_pb2.KaraResponse(code=2)
                    yield kara_storage_pb2.KaraResponse(code=0, data=dataset.pread( req["offset"] ))
                elif request.op == KARA_OP_SIZE_DS:
                    yield kara_storage_pb2.KaraResponse(code=0, data=json.dumps(dataset.size()).encode("utf-8"))
                elif request.op == KARA_OP_TELL_DS:
                    yield kara_storage_pb2.KaraResponse(code=0, data=json.dumps(dataset.tell()).encode("utf-8"))
                else:
                    yield kara_storage_pb2.KaraResponse(code=2, data=b"unknown op %d" % request.op)
            except KeyboardInterrupt:
                break
            except Exception as e:
                yield kara_storage_pb2.KaraResponse(code=1, data= ("%s" % e).encode("utf-8") )
    
    def StreamObject(self, request_iterator : Iterable[kara_storage_pb2.KaraRequest], context : grpc.RpcContext):
        uploads = {}
        downloads = {}
        namespace = None
        key = None
        version = None
        remote_prefix = None
        buffer_size = None
        buffer = None

        first_time = True

        for request in request_iterator:
            try:
                if first_time:
                    assert request.op == KARA_OP_STORAGE_OBJ
                    qry = json.loads( request.data.decode("utf-8") )
                    namespace = qry["namespace"]
                    key = qry["key"]
                    version = qry["version"]
                    is_upload = qry["is_upload"]
                    buffer_size = qry["buffer_size"]
                    buffer = bytearray(buffer_size)

                    if is_upload:
                        try:
                            config = self._storage.get_object_meta(namespace, key)
                        except FileNotFoundError:
                            config = {
                                "latest": None,
                                "versions": [],
                                "api": 2
                            }
                        if version is None:
                            # auto generate version
                            cnt = 0
                            while ("%d" % cnt) in config["versions"]:
                                cnt += 1
                            version = '%d' % cnt
                        
                        version = str(version)
                        if version == "latest":
                            if config["latest"] is None:
                                raise ValueError("No available version found in object storage `%s`." % key)
                            version = config["latest"]

                        config["latest"] = version
                        if version not in config["versions"]:
                            config["versions"].append(version)
                        self._storage.put_object_meta(namespace, key, config)
                    else:
                        try:
                            config = self._storage.get_object_meta(namespace, key)
                        except FileNotFoundError:
                            raise ValueError("Dataset not exists")
                        version = str(version)
                        if version == "latest":
                            if config["latest"] is None:
                                raise ValueError("No available version found in object storage `%s`." % key)
                            version = config["latest"]

                        if version not in config["versions"]:
                            raise ValueError("Object version `%s` not found in storage `%s`" % (version, key))

                    first_time = False
                    remote_prefix = self._storage.prefix + ("obj/%s/%s" % (namespace, key))
                    
                    yield kara_storage_pb2.KaraResponse(code=0, data=version.encode("utf-8") )
                else:
                    # not first time
                    if request.op == KARA_OP_HAS_FILE_OBJ:
                        fname = remote_prefix + "/data/" + request.fname
                        has_file = self._storage._storage.filesize(fname) is not None
                        yield kara_storage_pb2.KaraResponse( code=0, data=json.dumps(has_file).encode("utf-8") )
                    elif request.op == KARA_OP_READ_OBJ:
                        fname = remote_prefix + "/data/" + request.fname
                        if fname not in downloads:
                            downloads[fname] = self._storage._storage.open(fname, "r")
                        lw = downloads[fname].readinto(memoryview(buffer))
                        yield kara_storage_pb2.KaraResponse(code=0, data=bytes(buffer[:lw]))
                    elif request.op == KARA_OP_UPLOAD_OBJ:
                        fname = remote_prefix + "/data/" + request.fname
                        if fname not in uploads:
                            uploads[fname] = io.BytesIO()
                        uploads[fname].write(request.data)
                        yield kara_storage_pb2.KaraResponse( code=0, data=("%d" % uploads[fname].tell()).encode("utf-8") )
                    elif request.op == KARA_OP_FINISH_UPLOAD_OBJ:
                        fname = remote_prefix + "/data/" + request.fname
                        self._storage._storage.put(fname, uploads[fname].getvalue())
                        del uploads[fname]
                        yield kara_storage_pb2.KaraResponse(code=0, data=b"ok")
                    elif request.op == KARA_OP_GET_VER_OBJ:
                        fname = remote_prefix + "/vers/" + request.fname + ".json"
                        yield  kara_storage_pb2.KaraResponse( code=0, data=self._storage._storage.readfile(fname))
                    elif request.op == KARA_OP_SET_VER_OBJ:
                        fname = remote_prefix + "/vers/" + request.fname + ".json"
                        self._storage._storage.put(fname, request.data)
                        yield kara_storage_pb2.KaraResponse(code=0, data=b"ok")
                    else:
                        yield kara_storage_pb2.KaraResponse(code=2, data= "unknown op %d" % request.op )
            except KeyboardInterrupt:
                break
            except Exception as e:
                yield kara_storage_pb2.KaraResponse(code=1, data= ("%s" % e).encode("utf-8") )
    
    def PutMeta(self, request, context):
        try:
            req = json.loads(request.data.decode("utf-8"))
            if req["type"] == "obj":
                self._storage.put_object_meta(req["namespace"], req["key"], req["meta"])
            elif req["type"] == "row":
                self._storage.put_row_meta(req["namespace"], req["key"], req["meta"])
            else:
                raise TypeError("Unknown storage type: %s" % req["type"])
            return kara_storage_pb2.KaraResponse(code=0, data=b"ok" )
        except Exception as e:
            return kara_storage_pb2.KaraResponse(code=1, data= ("%s" % e).encode("utf-8") )
    def GetMeta(self, request, context):
        try:
            req = json.loads(request.data.decode("utf-8"))
            if req["type"] == "obj":
                meta = self._storage.get_object_meta(req["namespace"], req["key"])
            elif req["type"] == "row":
                meta = self._storage.get_row_meta(req["namespace"], req["key"])
            else:
                raise TypeError("Unknown storage type: %s" % req["type"])
            return kara_storage_pb2.KaraResponse(code=0, data=json.dumps(meta).encode("utf-8") )
        except Exception as e:
            return kara_storage_pb2.KaraResponse(code=1, data= ("%s" % e).encode("utf-8") )

class KaraStorageServer:
    def __init__(self, uri, num_workers : int = 10, **kwargs) -> None:
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_workers))
        kara_storage_pb2_grpc.add_KaraGatewayServicer_to_server(
            KaraService( KaraStorage(uri, **kwargs) ),
            self.server
        )
    
    def bind(self, address, server_credentials=None):
        if server_credentials is None:
            return self.server.add_insecure_port(address)
        else:
            return self.server.add_secure_port(address, server_credentials)
    
    def start(self):
        self.server.start()
    
    def join(self):
        self.server.wait_for_termination()

