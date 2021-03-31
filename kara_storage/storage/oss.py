import os, json
from ..dataset import OSSDataset, Dataset


class OSSRowStorage:
    def __init__(self, bucket, endpoint, APP_KEY, APP_SECRET) -> None:
        import oss2
        self.end_point = endpoint
        self.APP_KEY = APP_KEY
        self.APP_SECRET = APP_SECRET

        self.auth = oss2.Auth(self.APP_KEY, self.APP_SECRET)
        self.bucket = oss2.Bucket(self.auth, endpoint, bucket )
        
    
    def open(self, namespace, key, mode, version, **kwargs) -> Dataset:
        dataset_meta = "/row/%s/%s/meta.json" % (namespace, key)
        if not self.bucket.object_exists(dataset_meta):
            if "w" not in mode:
                raise ValueError("Dataset not exists")
            config = {
                "latest": None,
                "versions": []
            }
        else:
            config = json.loads( self.bucket.get_object(dataset_meta).read().decode("utf-8") )
        
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in dataset `%s`. Please specify a version if you want to create a new dataset" % key)
            version = config["latest"]

        if "w" in mode:
            config["latest"] = version
            if version not in config["versions"]:
                config["versions"].append(version)
            self.bucket.put_object(dataset_meta, json.dumps(config, ensure_ascii=False).encode('utf-8'))
        if "r" in mode:
            if version not in config["versions"]:
                raise ValueError("Dataset version `%s` not found in dataset `%s`" % (version, key))

        return OSSDataset( "/row/%s/%s/" % (namespace, key), self.bucket, **kwargs )