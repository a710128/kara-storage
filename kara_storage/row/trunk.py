import io
from typing import Optional
from ..abc import StorageBase
class TrunkController(io.RawIOBase):
    def __init__(self, storage : StorageBase, prefix : str, mode : str, max_file_size : int = 128 * 1024 * 1024) -> None:
        if mode != "r" and mode != "w":
            raise ValueError("Unknown mode `%s`" % mode)
        
        # normalize prefix
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        
        # store parameters
        self.__storage = storage
        self.__prefix = prefix
        self.__max_file_size = max_file_size
        self.__closed = False

        # initialize mode
        self.__readable = ("r" in mode)
        self.__writable = ("w" in mode)

        # initialize file pointer
        self.__tell = 0
        self.__size = 0
        self.__file_sizes = []
    
        # calc trunks

        self.__num_trunks = 0
        while True:
            file_size = self.__storage.filesize( self.__prefix + "%d.blk" % self.__num_trunks )
            if file_size is None:
                # returns None if file not exists
                break
            
            # else
            self.__file_sizes.append(file_size)
            self.__num_trunks += 1
        
        self.__size += sum(self.__file_sizes)

        if self.__readable:
            if self.__num_trunks == 0:
                raise RuntimeError("Empty dataset !")
            self.__fp_read = self.__storage.open(self.__prefix + "0.blk", "r")
            self.__infile_offset = 0
            self.__curr_file = 0
        if self.__writable:
            if self.__num_trunks == 0:
                self.__file_sizes.append(0)
                self.__num_trunks += 1

            self.__fp_write = self.__storage.open(self.__prefix + "%d.blk" % (self.__num_trunks - 1), "a")
            self.__infile_offset = self.__file_sizes[-1]
            self.__curr_file = self.__num_trunks - 1
    
    def readable(self) -> bool:
        return self.__readable
    
    def writable(self) -> bool:
        return self.__writable
    
    def seekable(self) -> bool:
        return self.__readable

    def readinto(self, __buffer, max_retry=3) -> Optional[int]:
        if not self.__readable:
            raise RuntimeError("Dataset not readable")
        if self.__closed:
            raise RuntimeError("Dataset is closed")

        lw = self.__fp_read.readinto(__buffer)

        # If the object is in non-blocking mode and no bytes are available, None is returned.
        # https://docs.python.org/3/library/io.html#io.RawIOBase
        if lw is None:
            return None

        self.__tell += lw
        self.__infile_offset += lw

        if self.__infile_offset == self.__file_sizes[self.__curr_file]:
            # reached the end of current file
            if self.__curr_file + 1 < self.__num_trunks:
                # open next trunk
                self.__fp_read.close()
                self.__curr_file += 1
                self.__infile_offset = 0
                self.__fp_read = self.__storage.open(self.__prefix + "%d.blk" % self.__curr_file, "r")
        else:
            if lw == 0:
                # __infile_offset < __file_size[__curr_file] but got EOF
                # retry if connection timeout
                if max_retry > 0:
                    # reopen connection
                    self.__fp_read = self.__storage.open(self.__prefix + "%d.blk" % self.__curr_file, "r", begin=self.__infile_offset)
                    # retry
                    return self.readinto(__buffer, max_retry - 1)
                else:
                    raise RuntimeError("File size not aligned: expected %d more bytes" % (self.__file_sizes[self.__curr_file] - self.__infile_offset))
        return lw
    
    def write(self, __b : bytes) -> Optional[int]:
        if not self.__writable:
            raise RuntimeError("Dataset not writable")
        if self.__closed:
            raise RuntimeError("Dataset is closed")

        wrt_len = min( len(__b), self.__max_file_size - self.__infile_offset )
        self.__fp_write.append( __b[:wrt_len] )

        self.__size += wrt_len
        self.__infile_offset += wrt_len

        if self.__infile_offset == self.__max_file_size:
            self.__fp_write.close()
            self.__fp_write = self.__storage.open( self.__prefix + "%d.blk" % self.__num_trunks, "a")
            self.__num_trunks += 1
            self.__infile_offset = 0
        return wrt_len
    
    def seek(self, __offset: int, __whence: int = io.SEEK_SET) -> int:
        if not self.__readable:
            raise RuntimeError("Dataset not readable")
        if self.__closed:
            raise RuntimeError("Dataset is closed")

        nw_pos = __offset
        if __whence == io.SEEK_CUR:
            nw_pos = self.__tell + __offset
        elif __whence == io.SEEK_END:
            nw_pos = self.__size - __offset
        if nw_pos < 0:
            nw_pos = 0
        if nw_pos > self.__size:
            nw_pos = self.__size
        
        rest_size = nw_pos
        self.__curr_file = 0

        while rest_size > self.__file_sizes[self.__curr_file]:
            rest_size -= self.__file_sizes[self.__curr_file]
            self.__curr_file += 1
        
        # 处理最后相等的情况
        if rest_size == self.__file_sizes[self.__curr_file]:
            # 处理最后一个文件的情况
            if self.__curr_file + 1 < self.__num_trunks:
                self.__curr_file += 1
                rest_size = 0

        self.__fp_read.close()
        if rest_size == self.__file_sizes[self.__curr_file]:
            self.__fp_read = io.BytesIO()
        else:
            self.__fp_read = self.__storage.open(self.__prefix + "%d.blk" % self.__curr_file, "r", begin=rest_size)
        self.__infile_offset = rest_size
        self.__tell = nw_pos

        return self.__tell

    def flush(self) -> None:
        if not self.__writable:
            return # ignore flush
        if self.__closed:
            raise RuntimeError("Dataset is closed")
        self.__fp_write.flush()
    
    def tell(self) -> int:
        return self.__tell
    
    def close(self):
        if not self.__closed:
            if self.__readable:
                self.__fp_read.close()
                self.__fp_read = None
            if self.__writable:
                self.__fp_write.close()
                self.__fp_write = None
            self.__closed = True
    
    @property
    def closed(self) -> bool:
        return self.__closed
    
    @property
    def size(self) -> int:
        return self.__size

    def pread(self, offset : int, length : int) -> bytes:
        if not self.__readable:
            raise RuntimeError("Dataset not readable")
        if self.__closed:
            raise RuntimeError("Dataset is closed")

        trunk_id = 0
        while offset >= self.__file_sizes[trunk_id] and trunk_id < self.__num_trunks:
            offset -= self.__file_sizes[trunk_id]
            trunk_id += 1
        
        if trunk_id >= self.__num_trunks:
            return b""
        
        rest_length = length
        
        ret = bytearray(length)
        read_offset = 0
        view = memoryview(ret)


        while rest_length > 0 and trunk_id < self.__num_trunks:
            ed = self.__file_sizes[trunk_id]
            if offset + rest_length < ed:
                ed = offset + rest_length
            fp = self.__storage.open(self.__prefix + "%d.blk" % trunk_id, "r", offset, ed)
            offset = ed % self.__file_sizes[trunk_id]

            
            vlen = fp.readinto(view[read_offset:])

            read_offset += vlen
            rest_length -= vlen
            trunk_id += 1
            fp.close()
        return bytes( ret[:read_offset] )

    