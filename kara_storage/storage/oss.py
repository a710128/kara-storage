import os, json
import shutil
from ..dataset import OSSDataset, Dataset


class OSSStorage:
    def __init__(self, bucket, endpoint, APP_KEY, APP_SECRET) -> None:
        import oss2
        self.end_point = endpoint
        self.APP_KEY = APP_KEY
        self.APP_SECRET = APP_SECRET

        self.auth = oss2.Auth(self.APP_KEY, self.APP_SECRET)
        self.bucket = oss2.Bucket(self.auth, endpoint, bucket )
        
    
    def open(self, namespace, key, mode, version, **kwargs) -> Dataset:
        dataset_meta = "row/%s/%s/meta.json" % (namespace, key)
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

        return OSSDataset( "row/%s/%s/%s/" % (namespace, key, version), self.bucket, mode, **kwargs )


    def loadDirectory(self, namespace, key, local_path, version) -> str:
        dataset_meta = "obj/%s/%s/meta.json" % (namespace, key)
        if not self.bucket.object_exists(dataset_meta):
            raise ValueError("Object storage not exists")
        
        config = json.loads( self.bucket.get_object(dataset_meta).read().decode("utf-8") )
        
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in object storage `%s`." % key)
            version = config["latest"]
        
        if version not in config["versions"]:
            raise ValueError("Object version `%s` not found in storage `%s`" % (version, key))
        
        import oss2
        data_prefix = "obj/%s/%s/%s/" % (namespace, key, version)
        
        dirs = []
        files = []
        for obj in oss2.ObjectIteratorV2(self.bucket, prefix=data_prefix):
            if not obj.key.startswith(data_prefix):
                continue
            if obj.key.endswith("/"):
                dirs.append(obj.key[len(data_prefix):])
            else:
                files.append(obj.key[len(data_prefix):])
        dirs = sorted(dirs, key=len)

        for d in dirs:
            subdirs = d.split("/")
            os.makedirs(os.path.join( local_path, *subdirs), exist_ok=True)
        for f in files:
            subdirs = f.split("/")
            self.bucket.get_object_to_file(data_prefix + f, os.path.join(local_path, *subdirs) )
        return version
    
    def saveDirectory(self, namespace, key, local_path, version) -> str:
        dataset_meta = "obj/%s/%s/meta.json" % (namespace, key)
        if not self.bucket.object_exists(dataset_meta):
            config = {
                "latest": None,
                "versions": []
            }
        else:
            config = json.loads( self.bucket.get_object(dataset_meta).read().decode("utf-8") )

        if version is None:
            # auto generate version
            cnt = 0
            while ("%d" % cnt) in config["versions"]:
                cnt += 1
            version = '%d' % cnt

        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in object storage `%s`." % key)
            version = config["latest"]
        
        config["latest"] = version
        if version not in config["versions"]:
            config["versions"].append(version)
        
        self.bucket.put_object(dataset_meta, json.dumps(config, ensure_ascii=False).encode('utf-8'))

        data_prefix = "obj/%s/%s/%s/" % (namespace, key, version)
        
        def search_in_file(path, db_path):
            self.bucket.put_object(db_path, b"")
            for fname in os.listdir(path):
                fullname = os.path.join(path, fname)
                if os.path.isdir(fullname):
                    search_in_file(fullname, db_path + ("%s/" % fname))
                else:
                    self.bucket.put_object( db_path + ("%s" % fname), open(fullname, "rb"))
        
        search_in_file(local_path, data_prefix)
        return version
