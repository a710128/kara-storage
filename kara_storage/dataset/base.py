from ..file_controller import FileController
import struct
import io

class Dataset:
    def __init__(self, index_controller : FileController, data_controller : FileController, mode : str, buffer_size = 128 * 1024) -> None:
        self.__closed = True
        self.__mode = mode
        self.__writable = ("w" in mode)
        self.__readable = ("r" in mode)
        self.__index_controller = index_controller
        self.__data_controller = data_controller
        if self.__readable:
            self.__index_reader = io.BufferedReader(index_controller, buffer_size=buffer_size)
            self.__data_reader = io.BufferedReader(data_controller, buffer_size=buffer_size)
        if self.__writable:
            self.__index_writer = io.BufferedWriter(index_controller, buffer_size=buffer_size)
            self.__data_writer = io.BufferedWriter(data_controller, buffer_size=buffer_size)
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
            self.flush()
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
        
        v = self.__index_reader.read(8)
        if v is None or len(v) != 8:
            return None
        cur_read_pos = struct.unpack("Q", v)[0]
        length = cur_read_pos - self.__last_read_pos
        ret = self.__data_reader.read(length)
        self.__last_read_pos = cur_read_pos
        self.__tell += 1
        return ret
        

    
    def seek(self, offset : int, whence : int) -> int:
        if self.__closed:
            raise RuntimeError("Dataset closed")

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

        if offset > 0:
            bf = self.__index_controller.pread((offset - 1) * 8, 16)
            last_pos = struct.unpack("Q", bf[:8])[0]
            curr_pos = struct.unpack("Q", bf[8:])[0]
        else:
            bf = self.__index_controller.pread(0, 8)
            last_pos = 0
            curr_pos = struct.unpack("Q", bf)[0]
        return self.__data_controller.pread( last_pos, curr_pos - last_pos )
    
    def size(self) -> int:
        return self.__size
    
    def tell(self) -> int:
        return self.__tell
    