import os, io
from typing import Optional
from .base import FileController
class LocalFileController(FileController):
    def __init__(self, base_dir, max_file_size = 128 * 1024 * 1024):
        self.base_dir = base_dir
        self.max_file_size = max_file_size
        self.__closed = False

        self.read_pos = 0
        self.num_trunks = 0
        self.__size = 0

        last_fs = max_file_size
        while os.path.exists(os.path.join( self.base_dir, "%d.blk" % self.num_trunks)):
            fs = os.stat(os.path.join( self.base_dir, "%d.blk" % self.num_trunks))
            self.__size += fs.st_size
            if last_fs != max_file_size:
                raise RuntimeError("Broken trunk %d (size: %d)" % (self.num_trunks - 1, last_fs))
            last_fs = fs.st_size
            self.num_trunks += 1

        if self.num_trunks == 0:
            self.num_trunks += 1
            last_fs = 0
        self.__write_in_file_size = last_fs
        self.write_fd = open(os.path.join( self.base_dir, "%d.blk" % (self.num_trunks - 1) ), "ab")
        self.write_fd.flush()
        self.read_fd = open(os.path.join( self.base_dir, "%d.blk" % 0), "rb")
        

    def readinto(self, __buffer) -> Optional[int]:
        lw = self.read_fd.readinto(__buffer)
        if lw is None:
            return None
        self.read_pos += lw
        if self.read_pos % self.max_file_size == 0:
            nx_trunk = self.read_pos // self.max_file_size
            if nx_trunk < self.num_trunks:
                self.read_fd.close()
                self.read_fd = open(os.path.join( self.base_dir, "%d.blk" % nx_trunk), "rb")
        return lw
    
    def write(self, __b) -> Optional[int]:
        wrt_len = min( len(__b), self.max_file_size - self.__write_in_file_size )
        self.write_fd.write( __b[:wrt_len] )
        
        self.__size += wrt_len
        self.__write_in_file_size += wrt_len
        if self.__write_in_file_size == self.max_file_size:
            self.write_fd.close()
            self.write_fd = open(os.path.join( self.base_dir, "%d.blk" % self.num_trunks ), "ab")
            self.num_trunks += 1
            self.__write_in_file_size = 0
        return wrt_len
    
    def seek(self, __offset: int, __whence: int) -> int:
        nw_pos = __offset
        if __whence == io.SEEK_CUR:
            nw_pos = self.read_pos + __offset
        elif __whence == io.SEEK_END:
            nw_pos = self.__size - __offset
        if nw_pos < 0:
            nw_pos = 0
        if nw_pos > self.__size:
            nw_pos = self.__size
        
        nx_trunk =  nw_pos // self.max_file_size
        self.read_fd.close()
        self.read_fd = open(os.path.join( self.base_dir, "%d.blk" % nx_trunk), "rb")
        self.read_fd.seek( nw_pos % self.max_file_size )
        self.read_pos = nw_pos
        return self.read_pos
    
    def flush(self):
        self.write_fd.flush()
    
    @property
    def tell(self) -> int:
        return self.read_pos

    def close(self) -> None:
        if not self.__closed:
            self.read_fd.close()
            self.read_fd = None
            self.write_fd.close()
            self.write_fd = None
            self.__closed = True
    
    @property
    def closed(self):
        return self.__closed

    @property
    def size(self) -> int:
        return self.__size
    
    def pread(self, offset : int, length : int) -> bytes:
        trunk_id = offset // self.max_file_size
        rest_length = length
        read_pos = 0

        ret = io.BytesIO()
        while rest_length > 0:
            try:
                f = open(os.path.join( self.base_dir, "%d.blk" % trunk_id), "rb")
            except FileNotFoundError:
                print(os.path.join( self.base_dir, "%d.blk" % trunk_id))
                break
            f.seek( (offset + read_pos) % self.max_file_size, io.SEEK_SET )
            v = f.read(length)

            read_pos += len(v)
            rest_length -= len(v)
            trunk_id += 1
            ret.write(v)
        return ret.getvalue()