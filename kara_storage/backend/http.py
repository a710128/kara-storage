import io
import os
from typing import Union
from ..abc import StorageBase, StorageFileBase
import urllib
import urllib.request
import urllib.error

class HTTPFile(StorageFileBase):
    def __init__(self, fp) -> None:
        super().__init__("r")
        self.__fp = fp

    def append(self, data : bytes):
        raise RuntimeError("HTTP/HTTPS files are read-only")
        

    def readinto(self, __buffer):
        return self.__fp.readinto(__buffer)
    
    def flush(self):
        return
    
    def close(self):
        self.__fp.close()

class HTTPStorage(StorageBase):
    def __init__(self, url_prefix : str, headers = {}):
        if not url_prefix.endswith("/"):
            url_prefix = url_prefix + "/"
        self.__url_prefix = url_prefix
        self.__custom_headers = headers
    
    def __range_to_str(self, v):
        if v is None:
            return ""
        return str(v)
    
    def open(self, path : str, mode, begin=None, end=None) -> HTTPFile:
        if path.startswith("/"):
            path = path[1:]
        if mode == "r":
            if begin is None and end is None:
                req = urllib.request.Request(self.__url_prefix + path, headers=self.__custom_headers)
            else:
                if end is not None:
                    end = end - 1
                req = urllib.request.Request(self.__url_prefix + path, headers={
                    **self.__custom_headers,
                    "Range": "bytes=%s-%s" % (
                        self.__range_to_str(begin),
                        self.__range_to_str(end)
                    )
                })
            resp = urllib.request.urlopen(req)
            if begin is not None or end is not None:
                if 'content-range' not in resp.headers:
                    raise RuntimeError("Storage server seems doesn't support range header")
            return HTTPFile(resp)
        elif mode == "a":
            raise ValueError("HTTP/HTTPS Storages are read-only")
        else:
            raise ValueError("Unknown mode: `%s`" % mode)
        
    
    def filesize(self, path : str):
        if path.startswith("/"):
            path = path[1:]
        try:
            resp = urllib.request.urlopen(urllib.request.Request(self.__url_prefix + path, headers=self.__custom_headers))
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise RuntimeError("Unexpected response code: %d" % e.code)
            return None
        if not "content-length" in resp.headers:
            raise RuntimeError("Storage server does not support `Content-Length` header")

        return int(resp.headers["content-length"])
    
    def put(self, path: str, data: Union[bytes, io.RawIOBase, io.BufferedIOBase]):
        raise ValueError("HTTP/HTTPS Storages are read-only")