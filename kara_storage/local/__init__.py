from .._C import LocalDataset as C_LocalDataset
import os, json

class LocalDataset:
    def __init__(self, dir : str, mode : str, trunk_size : int = 32 * 1024 * 1024, trunks_per_file : int = 4) -> None:
        dir = os.path.abspath(dir)

        if not os.path.exists(dir):
            if "w" not in mode:
                raise ValueError("Dataset not exists!")
            os.makedirs(dir)
            json.dump({
                "trunk_size": trunk_size,
                "trunks_per_file ":  trunks_per_file
            }, open( os.path.join(dir, "meta.json"), "w" ) )

        if not os.path.exists( os.path.join(dir, "meta.json") ):
            raise ValueError("Broken dataset: missing meta.json")
            
        self.__config = json.load(open( os.path.join(dir, "meta.json"), "r" ))
        data_dir = os.path.join(dir, "data")
        index_dir = os.path.join(dir, "index")
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        if not os.path.exists(index_dir):
            os.mkdir(index_dir)
        self.__ds = C_LocalDataset(dir, "w" in mode, self.__config["trunk_size"], self.__config["trunks_per_file"])
        self.__closed = False
    
    @property
    def closed(self):
        return self.__closed
    
    def close(self):
        if not self.__closed:
            self.__closed = True
            del self.__ds
    
    def flush(self):
        if self.__closed:
            return
        self.__ds.flush()
    
    def write(self, data):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        self.__ds.write(data, len(data))
    
    def read(self):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        return self.__ds.read()
    
    def seek(self, offset, whence):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        self.__ds.seek(offset, whence)
        return self.__ds.tell()
    
    def pread(self, offset):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        return self.__ds.pread(offset)
    
    def size(self):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        return self.__ds.size()
    
    def tell(self):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        return self.__ds.tell()