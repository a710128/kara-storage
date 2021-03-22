import os, json
from ..dataset import LocalDataset, Dataset


class LocalRowStorage:
    def __init__(self, base_dir) -> None:
        self.base_dir = os.path.abspath(base_dir)
        if not os.path.exists( self.base_dir ):
            os.makedirs( self.base_dir )
    
    def open(self, namespace, key, mode, version, **kwargs) -> Dataset:
        ds_base = os.path.join(self.base_dir, namespace, key)
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