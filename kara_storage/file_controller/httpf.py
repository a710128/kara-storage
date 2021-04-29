import io
from typing import Optional

from .base import FileController
import urllib
import urllib.error
import urllib.request

class HTTPController(FileController):
    def __init__(self, url_with_prefix : str, mode : str):
        super().__init__(mode=mode)

        if not url_with_prefix.endswith("/"):
            url_with_prefix = url_with_prefix + "/"
        self.prefix = url_with_prefix
        if mode != "r":
            raise ValueError("HTTP/HTTPS storages are read-only")
        self.__closed = False

        
        self.num_trunks = 0
        self.__size = 0

        self.file_sizes = []
        
        while True:
            try:
                r = urllib.request.urlopen(self.prefix + "%d.blk" % self.num_trunks)
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise RuntimeError("Unexpected response code: %d" % e.code)
                break
            if not "content-length" in r.headers:
                raise RuntimeError("Storage server does not support `Content-Length` header")
            self.file_sizes.append(int(r.headers["content-length"]))
            self.num_trunks += 1
        
        self.__size = sum(self.file_sizes)
        self.__tell = 0
        self.__curr_file = 0
        self.__infile_offset = 0

        if self.__size > 0:
            self.read_fd = urllib.request.urlopen(self.prefix + "0.blk")
        else:
            self.read_fd = None
        

    def readinto(self, __buffer) -> Optional[int]:
        lw = self.read_fd.readinto(__buffer)
        if lw is None:
            return None
        
        self.__tell += lw
        self.__infile_offset += lw

        if self.__infile_offset == self.file_sizes[self.__curr_file]:
            # reach the end of this file
            if self.__curr_file + 1 < self.num_trunks:
                # open the next trunk
                self.read_fd.close()
                self.__curr_file += 1
                self.__infile_offset = 0
                self.read_fd = urllib.request.urlopen(self.prefix + "%d.blk" % self.__curr_file)
                if "content-length" not in self.read_fd.headers or int(self.read_fd.headers["content-length"]) != self.file_sizes[self.__curr_file]:
                    raise RuntimeError("File size not aligned (expected %d, trunk: %d)" % ( self.file_sizes[self.__curr_file], self.__curr_file))
        return lw
    
    def write(self, __b) -> Optional[int]:
        raise RuntimeError("HTTP/HTTPS storages are read-only")
    
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
        
        rest_size = nw_pos
        self.__curr_file = 0
        while rest_size > self.file_sizes[self.__curr_file]:
            rest_size -= self.file_sizes[self.__curr_file]
            self.__curr_file += 1
        
        # 处理最后相等的情况
        if rest_size == self.file_sizes[self.__curr_file]:
            # 处理最后一个文件的情况
            if self.__curr_file + 1 < self.num_trunks:
                self.__curr_file += 1
                rest_size = 0
        
        
        self.read_fd.close()
        if rest_size == self.file_sizes[self.__curr_file]:
            self.read_fd = io.BytesIO()
        else:
            self.read_fd = urllib.request.urlopen(
                urllib.request.Request(self.prefix + "%d.blk" % self.__curr_file, headers={
                    "Range": "bytes=%d-" % rest_size
                })
            )
            if "content-length" not in self.read_fd.headers or int(self.read_fd.headers["content-length"]) != (self.file_sizes[self.__curr_file] - rest_size):
                raise RuntimeError("Storage server does not support `Range` header")
            
        self.__tell = nw_pos
        return self.__tell
    
    def flush(self):
        pass
    
    def tell(self) -> int:
        return self.__tell

    def close(self) -> None:
        if not self.__closed:
            self.read_fd.close()
            self.read_fd = None
            self.__closed = True
    
    @property
    def closed(self):
        return self.__closed

    @property
    def size(self) -> int:
        return self.__size
    
    def pread(self, offset : int, length : int) -> bytes:
        trunk_id = 0
        while offset >= self.file_sizes[trunk_id]:
            offset -= self.file_sizes[trunk_id]
            trunk_id += 1
        
        if trunk_id >= self.num_trunks:
            return b""
        
        rest_length = length

        ret = io.BytesIO()
        while rest_length > 0:
            ed = self.file_sizes[trunk_id]
            if offset + rest_length < ed:
                ed = offset + rest_length

            r =  urllib.request.urlopen(
                urllib.request.Request(self.prefix + "%d.blk" % trunk_id, headers={
                    "Range": "bytes=%d-%d" % (offset, ed - 1)
                })
            )
            offset = ed % self.file_sizes[trunk_id]
            if "content-length" not in r.headers or int(r.headers["content-length"]) != ed - offset:
                raise RuntimeError("Storage server does not support `Range` header")
            
            v = r.read(rest_length)
            rest_length -= len(v)
            trunk_id += 1
            ret.write(v)
            r.close()
        return ret.getvalue()