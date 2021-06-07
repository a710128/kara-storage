from kara_storage.abc.serializer import Serializer
from typing import Any
from urllib.parse import urlparse
import os
import json
from ..row import RowDataset
from ..object import ObjectDataset
from ..abc import StorageBase, KaraStorageBase


class KaraStorage(KaraStorageBase):
    def __init__(self, url, **kwargs) -> None:
        uri = urlparse(url)
        if uri.scheme == "file":
            path = ""
            if uri.netloc == "":
                path = uri.path
            else:
                path = os.path.abspath(uri.netloc) + uri.path
            from ..backend.file import LocalFileStorage
            self.__prefix = path
            self.__storage = LocalFileStorage()
        elif uri.scheme == "oss":
            path =  uri.path.split("/")
            from ..backend.oss import OSSStorage
            if "use_ssl" in kwargs and kwargs["use_ssl"]:
                self.__storage = OSSStorage(path[1], "https://" + uri.netloc, kwargs["app_key"], kwargs["app_secret"])
            else:
                self.__storage = OSSStorage(path[1], "http://" + uri.netloc, kwargs["app_key"], kwargs["app_secret"])
            self.__prefix = "/".join(path[2:])    
        elif uri.scheme == "http" or uri.scheme == "https":
            from ..backend.http import HTTPStorage
            headers = {}
            if "headers" in kwargs:
                headers = kwargs["headers"]
            self.__storage = HTTPStorage( uri.scheme + "://" + uri.netloc, headers=headers)
            self.__prefix = uri.path
        else:
            raise ValueError("Unknown scheme `%s`" % uri.scheme)
        
        self.__object_dataset = ObjectDataset(self.__storage)
        
        if not self.__prefix.endswith("/"):
            self.__prefix = self.__prefix + "/"
    
    @property
    def prefix(self) -> str:
        return self.__prefix
        
    @property
    def _storage(self) -> StorageBase:
        return self.__storage
    
    def __get_meta(self, storage_type : str, namespace : str, key : str):
        version_path = self.__prefix + "%s/%s/%s/meta.json" % (storage_type, namespace, key)
        if self.__storage.filesize(version_path) is None:
            raise FileNotFoundError("Dataset not exists")

        return json.loads(self.__storage.readfile(version_path).decode("utf-8"))
    
    def get_row_meta(self, namespace : str, key : str):
        return self.__get_meta("row", namespace, key)
    
    def get_object_meta(self, namespace : str, key : str):
        return self.__get_meta("obj", namespace, key)
    
    def __put_meta(self, storage_type : str, namespace : str, key : str, meta : Any):
        version_path = self.__prefix + "%s/%s/%s/meta.json" % (storage_type, namespace, key)
        self.__storage.put( version_path, json.dumps(meta).encode("utf-8") )
    
    def put_row_meta(self, namespace : str, key : str, meta : Any):
        self.__put_meta("row", namespace, key, meta)
    
    def put_object_meta(self, namespace : str, key : str, meta : Any):
        self.__put_meta("obj", namespace, key, meta)
    
    def open_dataset(self, 
        namespace : str, key : str, mode : str = "r", version="latest", 
        serialization : Serializer = None, **kwargs
    ) -> RowDataset:

        version = str(version)
        try:
            config = self.get_row_meta(namespace, key)
        except FileNotFoundError:
            if "w" not in mode:
                raise ValueError("Dataset not exists")
            config = {
                "latest": None,
                "versions": [],
                "api": 2,
            }
        
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in dataset `%s`. Please specify a version if you want to create a new dataset" % key)
            version = config["latest"]

        if "w" in mode:
            config["latest"] = version
            if version not in config["versions"]:
                config["versions"].append(version)
            self.put_row_meta(namespace, key, config)
            
        if "r" in mode:
            if version not in config["versions"]:
                raise ValueError("Dataset version `%s` not found in dataset `%s`" % (version, key))
        
        return RowDataset(self.__storage, self.__prefix + "row/%s/%s/%s/" % (namespace, key, version), mode, serialization=serialization, **kwargs)

    def load_directory(self, namespace : str, key : str, local_path : str, version = "latest", progress_bar=True):
        
        try:
            config = self.get_object_meta(namespace, key)
        except FileNotFoundError:
            raise ValueError("Dataset not exists")
        
        version = str(version)
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in object storage `%s`." % key)
            version = config["latest"]

        if version not in config["versions"]:
            raise ValueError("Object version `%s` not found in storage `%s`" % (version, key))
        
        version_info = json.loads(self.__storage.readfile(
            self.__prefix + "obj/%s/%s/vers/%s.json" % (namespace, key, version)
        ).decode("utf-8"))
        self.__object_dataset.download(
            self.__prefix + "obj/%s/%s/data/" % (namespace, key), 
            version_info,
            local_path,
            progress_bar=progress_bar
        )
        return version
    
    def save_directory(self, namespace : str, key : str, local_path : str, version = None, progress_bar=True) -> str:
        
        try:
            config = self.get_object_meta(namespace, key)
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
        self.put_object_meta(namespace, key, config)

        version_info = self.__object_dataset.upload(
            self.__prefix + "obj/%s/%s/data/" % (namespace, key),
            local_path,
            progress_bar=progress_bar
        )
        self.__storage.put(
            self.__prefix + "obj/%s/%s/vers/%s.json" % (namespace, key, version),
            json.dumps( version_info ).encode("utf-8")
        )
        return version

    

    


        


