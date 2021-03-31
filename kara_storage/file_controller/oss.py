import io
from typing import  Optional
from .base import FileController

import oss2

class OSSFileController(FileController):
    def __init__(self, prefix : str, bucket : oss2.Bucket, max_file_size = 1024 * 1024 * 1024) -> None:
        
        self.bucket = bucket

        self.__closed = False

        if not prefix.endswith("/"):
            prefix = prefix + "/"

        self.prefix = prefix
        self.max_file_size = max_file_size
        self.num_trunks = 0
        for _ in oss2.ObjectIteratorV2(self.bucket, prefix=self.prefix):
            self.num_trunks += 1
        
        if self.num_trunks == 0:
            self.num_trunks = 1
            self.bucket.append_object(self.prefix + "%d.blk" % 0, 0, b"")
        self.__wrin_file_length = self.bucket.get_object_meta(self.prefix + "%d.blk" % (self.num_trunks - 1)).content_length
        self.__size = (self.num_trunks - 1) * self.max_file_size + self.__wrin_file_length
        self.__tell = 0
        self.read_fd = self.bucket.get_object(self.prefix + "%d.blk" % 0)

        
    
    def readinto(self, __buffer : memoryview) -> Optional[int]:
        v = self.read_fd.read(len(__buffer))
        if len(v) == 0:
            return None
        __buffer[:len(v)] = v
        self.__tell += len(v)
        
        if self.__tell % self.max_file_size == 0:
            self.read_fd.close()
            self.read_fd = self.bucket.get_object(self.prefix + "%d.blk" % ( self.__tell // self.max_file_size ))
            
        return len(v)
    
    def write(self, data : memoryview) -> Optional[int]:
        wrt_len = min( len(data), self.max_file_size - self.__wrin_file_length )
        self.bucket.append_object(self.prefix + "%d.blk" % (self.num_trunks - 1), self.__wrin_file_length, data[:wrt_len].tobytes())

        self.__size += wrt_len
        self.__wrin_file_length += wrt_len
        if self.__wrin_file_length == self.max_file_size:
            self.num_trunks += 1
            self.__wrin_file_length = 0
        return wrt_len
    
    def seek(self, __offset: int, __whence: int) -> int:
        nw_pos = __offset
        if __whence == io.SEEK_CUR:
            nw_pos = self.__tell + __offset
        elif __whence == io.SEEK_END:
            nw_pos = self.__size - __offset
        if nw_pos < 0:
            nw_pos = 0
        if nw_pos > self.__size:
            nw_pos = self.__size
        nx_trunk =  nw_pos // self.max_file_size
        self.read_fd.close()
        self.read_fd = self.bucket.get_object(
            self.prefix + "%d.blk" % nx_trunk, 
            byte_range=(nw_pos % self.max_file_size, self.max_file_size - 1),
            headers = {'x-oss-range-behavior':'standard'}
        )
        self.__tell = nw_pos
        return self.__tell
    
    @property
    def closed(self):
        return self.__closed
    
    def tell(self) -> int:
        return self.__tell

    def close(self) -> None:
        if not self.__closed:
            self.read_fd.close()
            self.read_fd = None
            self.__closed = True
    
    @property
    def size(self) -> int:
        return self.__size

    def pread(self, offset : int, length : int) -> bytes:
        trunk_id = offset // self.max_file_size
        rest_length = length
        read_pos = 0

        ret = io.BytesIO()

        while rest_length > 0:
            st = (offset + read_pos) % self.max_file_size
            ed = min(st + rest_length, self.max_file_size)
            v = self.bucket.get_object(self.prefix + "%d.blk" % trunk_id, byte_range=(st, ed - 1))

            read_pos += ed - st
            rest_length -= ed - st
            trunk_id += 1
            ret.write(v.read())
        return ret.getvalue()