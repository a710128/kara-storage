import os, json
from ..dataset import HTTPDataset, Dataset
import urllib
import urllib.request
import urllib.error

class HTTPStorage:
    def __init__(self, url_with_prefix : str) -> None:
        if not url_with_prefix.endswith("/"):
            url_with_prefix = url_with_prefix + "/"
        self.prefix = url_with_prefix
        
    
    def open(self, namespace, key, mode, version, **kwargs) -> Dataset:
        if mode != "r":
            raise ValueError("HTTP/HTTPS storages are read-only")
        dataset_meta = self.prefix + "row/%s/%s/meta.json" % (namespace, key)
        try:
            config = json.loads(urllib.request.urlopen(dataset_meta).read())
        except urllib.error.HTTPError:
            raise ValueError("Dataset `%s` not found" % key)
        
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in dataset `%s`. Please specify a version if you want to create a new dataset" % key)
            version = config["latest"]

        if version not in config["versions"]:
            raise ValueError("Dataset version `%s` not found in dataset `%s`" % (version, key))

        return HTTPDataset( self.prefix +"row/%s/%s/%s/" % (namespace, key, version), mode, **kwargs )


    def loadDirectory(self, namespace, key, local_path, version) -> str:
        raise NotImplementedError()
    
    def saveDirectory(self, namespace, key, local_path, version) -> str:
        raise NotImplementedError()
