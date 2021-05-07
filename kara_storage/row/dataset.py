from .trunk import TrunkController
from ..abc import StorageBase, Dataset
import struct
import io

class RawDataset(Dataset):
    def __init__(self, storage : StorageBase, prefix : str, mode : str, buffer_size : int = 1024 * 1024, **kwargs) -> None:
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        self.__closed = True
        self.__mode = mode
        self.__writable = ("w" in mode)
        self.__readable = ("r" in mode)
        self.__index_controller = TrunkController(storage, prefix + "index/" , mode=mode, **kwargs)
        self.__data_controller = TrunkController(storage, prefix + "data/" , mode=mode, **kwargs)
        
        if self.__readable:
            self.__index_reader = io.BufferedReader(self.__index_controller, buffer_size=buffer_size)
            self.__data_reader = io.BufferedReader(self.__data_controller, buffer_size=buffer_size)
        if self.__writable:
            self.__index_writer = io.BufferedWriter(self.__index_controller, buffer_size=buffer_size)
            self.__data_writer = io.BufferedWriter(self.__data_controller, buffer_size=buffer_size)
        
        self.__closed = False
        self.__last_read_pos = 0

        self.__real_data_size = self.__data_controller.size
        self.__tell = 0
        self.__size = self.__index_controller.size // 8
    
    def __del__(self):
        self.close()
    
    @property
    def closed(self):
        return self.__closed
    
    def close(self):
        if not self.__closed:
            if self.__readable:
                self.__index_reader.close()
                self.__data_reader.close()
            if self.__writable:
                self.__index_writer.close()
                self.__data_writer.close()
            self.__closed = True
    
    def flush(self):
        if self.__writable:
            if self.__closed:
                raise RuntimeError("Dataset closed")
            self.__index_writer.flush()
            self.__data_writer.flush()
    
    def write(self, data : bytes):
        if self.__closed:
            raise RuntimeError("Dataset closed")
        if not self.__writable:
            raise RuntimeError("Dataset not writable in mode `%s`" % self.__mode)

        self.__data_writer.write(data)
        self.__real_data_size += len(data)
        self.__size += 1
        self.__index_writer.write( struct.pack("Q", self.__real_data_size) )

    
    def read(self) -> bytes:
        if self.__closed:
            raise RuntimeError("Dataset closed")
        if not self.__readable:
            raise RuntimeError("Dataset not readable in mode `%s`" % self.__mode)
        if self.__tell == self.__size:
            return None
        v = self.__index_reader.read(8)
        if v is None:
            return None
        if len(v) != 8:
            raise RuntimeError("Dataset is broken at index offset %d, got length %d" % (self.__tell * 8, len(v)))
        cur_read_pos = struct.unpack("Q", v)[0]
        length = cur_read_pos - self.__last_read_pos
        ret = self.__data_reader.read(length)
        if len(ret) != length:
            raise RuntimeError("Dataset is broken at data offset %d ~ %d" % (self.__last_read_pos, cur_read_pos))
        self.__last_read_pos = cur_read_pos
        self.__tell += 1
        return ret
        

    
    def seek(self, offset : int, whence : int) -> int:
        if self.__closed:
            raise RuntimeError("Dataset closed")
        if not self.__readable:
            raise RuntimeError("Dataset not seekable in mode `%s`" % self.__mode)

        nw_pos = None
        if whence == io.SEEK_SET:
            nw_pos = offset
        elif whence == io.SEEK_CUR:
            nw_pos = self.__tell + offset
        elif whence == io.SEEK_END:
            nw_pos = self.__size - offset
        else:
            raise ValueError("Invalid whence: %d" % whence)
        if nw_pos < 0:
            nw_pos = 0
        if nw_pos > self.__size:
            nw_pos = self.__size
        if nw_pos > 0:
            self.__index_reader.seek((nw_pos - 1) * 8, io.SEEK_SET)
            self.__last_read_pos = struct.unpack("Q", self.__index_reader.read(8))[0]
        else:
            self.__index_reader.seek(0, io.SEEK_SET)
            self.__last_read_pos = 0

        self.__data_reader.seek(self.__last_read_pos, io.SEEK_SET)
        self.__tell = nw_pos

        return self.__tell
            
    def pread(self, offset : int) -> bytes:
        if self.__closed:
            raise RuntimeError("Dataset closed")
        if not self.__readable:
            raise RuntimeError("Dataset not readable in mode `%s`" % self.__mode)
        if offset >= self.__size:
            raise IndexError("Offset %d is out of range [0, %d)" % (offset ,self.__size))

        if offset > 0:
            bf = self.__index_controller.pread((offset - 1) * 8, 16)
            if len(bf) != 16:
                raise RuntimeError("Dataset is broken at index offset %d, go length %d" % ((offset - 1) * 8, len(bf)))
            last_pos = struct.unpack("Q", bf[:8])[0]
            curr_pos = struct.unpack("Q", bf[8:])[0]
        else:
            bf = self.__index_controller.pread(0, 8)
            if len(bf) != 8:
                raise RuntimeError("Dataset is broken at index offset %d" % 0)
            last_pos = 0
            curr_pos = struct.unpack("Q", bf)[0]
            
        ret = self.__data_controller.pread( last_pos, curr_pos - last_pos )
        if len(ret) != curr_pos - last_pos:
            raise RuntimeError("Dataset is broken at data offset %d ~ %d" % (last_pos, curr_pos))
        return ret
    
    def size(self) -> int:
        return self.__size
    
    def tell(self) -> int:
        return self.__tell
    