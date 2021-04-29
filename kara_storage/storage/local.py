from kara_storage.dataset import local
import os, json, shutil
from ..dataset import LocalDataset, Dataset


class LocalStorage:
    def __init__(self, base_dir) -> None:
        self.base_dir = os.path.abspath(base_dir)
        if not os.path.exists( self.base_dir ):
            os.makedirs( self.base_dir )
    
    def open(self, namespace, key, mode, version, **kwargs) -> Dataset:
        ds_base = os.path.join(self.base_dir, "row", namespace, key)
        if not os.path.exists(ds_base):
            if "w" not in mode:
                raise ValueError("Dataset not exists")
            os.makedirs(ds_base)
            json.dump({
                "latest": None,
                "versions": []
            }, open( os.path.join(ds_base, "meta.json"), "w"))
        
        config = json.load(open(os.path.join(ds_base, "meta.json"), "r"))
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in dataset `%s`. Please specify a version if you want to create a new dataset" % key)
            version = config["latest"]
        

        if "w" in mode:
            config["latest"] = version
            if version not in config["versions"]:
                config["versions"].append(version)
            json.dump(config,  open(os.path.join(ds_base, "meta.json"), "w"))
        if "r" in mode:
            if version not in config["versions"]:
                raise ValueError("Dataset version `%s` not found in dataset `%s`" % (version, key))

        return LocalDataset( os.path.join(ds_base, version), mode, **kwargs )
    
    def loadDirectory(self, namespace, key, local_path, version):
        ds_base = os.path.join(self.base_dir, "obj", namespace, key)
        if not os.path.exists(ds_base):
            raise ValueError("Object storage not exists")
        
        config = json.load(open(os.path.join(ds_base, "meta.json"), "r"))
        if version == "latest":
            if config["latest"] is None:
                raise ValueError("No available version found in object storage `%s`." % key)
            version = config["latest"]
        
        if version not in config["versions"]:
            raise ValueError("Object version `%s` not found in storage `%s`" % (version, key))
        
        storage_base = os.path.join(ds_base, version)
        if os.path.isdir(storage_base):
            shutil.copytree(storage_base, local_path, symlinks=False, ignore_dangling_symlinks=True, dirs_exist_ok=True)
        else:
            shutil.copy(storage_base, local_path)
        return version
    
    def saveDirectory(self, namespace, key, local_path, version):
        ds_base = os.path.join(self.base_dir, "obj", namespace, key)
        if not os.path.exists(ds_base):
            os.makedirs(ds_base)
            json.dump({
                "latest": None,
                "versions": []
            }, open( os.path.join(ds_base, "meta.json"), "w"))
        config = json.load(open(os.path.join(ds_base, "meta.json"), "r"))

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
        json.dump(config,  open(os.path.join(ds_base, "meta.json"), "w"))
        
        storage_base = os.path.join(ds_base, version)
        os.makedirs(storage_base, exist_ok=True)

        if os.path.isdir(local_path):
            shutil.copytree(local_path, storage_base, symlinks=False, ignore_dangling_symlinks=True, dirs_exist_ok=True)
        else:
            shutil.copy(local_path, storage_base)
        return version
