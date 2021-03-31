import os, io
from .base import FileController
class LocalFileController(FileController):
    def __init__(self, base_dir, max_file_size = 128 * 1024 * 1024, buffer_size = 128 * 1024):
        self.__base_dir = base_dir
        self.__max_file_size = max_file_size
        self.__buffer_size = buffer_size

        self.__tell = 0
        self.__rdin_file_size = 0
        self.__read_fd = None

        self.__size = 0
        self.__wrin_file_size = 0

        cnt = 0

        while os.path.exists( os.path.join(self.__base_dir, "%d.blk" % cnt) ):
            cnt += 1
            self.__size += os.stat( os.path.join(self.__base_dir, "%d.blk" % cnt) ).st_size
        self.__num_trunks = cnt

        self.__wrin_file_size = self.__size % max_file_size
        self.__write_fd = io.BufferedWriter(open( os.path.join(self.__base_dir, "%d.blk" % (self.__num_trunks - 1)), "wb" ), buffer_size=self.__buffer_size)

    
    def read(self, length : int ) -> bytes:
        if self.__tell + length > self.__size:
            length = self.__size - self.__tell
        rest_length = length

        ret = io.BytesIO()
        while rest_length > 0:
            curr_len = min(rest_length, self.__max_file_size - self.__rdin_file_size)
            ret.write( self.__read_fd.read(curr_len) )
            
            rest_length -= curr_len
            self.__rdin_file_size += curr_len
            self.__tell += curr_len

            if self.__rdin_file_size == self.__max_file_size:
                self.open_trunk( self.__tell // self.__max_file_size )
                self.__rdin_file_size = 0
        return ret.getvalue()
    
    def write(self, data : bytes):
        while len(data) > 0:
            curr_len = min( len(data), self.__max_file_size - self.__wrin_file_size )
            self.__write_fd.write( data[:curr_len] )

            data = data[curr_len:]
            self.__wrin_file_size += curr_len
            self.__size += curr_len

            if self.__wrin_file_size == self.__max_file_size:
                self.__write_fd.close()
                self.__write_fd = io.BufferedWriter(open( os.path.join(self.__base_dir, "%d.blk" % self.__num_trunks), "wb" ), buffer_size=self.__buffer_size)
                self.__num_trunks += 1
                self.__wrin_file_size = 0
    
    def seek(self, offset):
        self.__tell = offset
        self.__rdin_file_size = self.__tell % self.__max_file_size
        self.open_trunk(self.__tell // self.__max_file_size, self.__rdin_file_size)
    
    @property
    def tell(self):
        return self.__tell
    
    @property
    def size(self):
        return self.__size

    def open_trunk(self, trunk_id, in_trunk_offset=0):
        if self.__read_fd is not None:
            self.__read_fd.close()
            self.__read_fd = None
        fd = open( os.path.join(self.__base_dir, "%d.blk" % trunk_id), "rb" )
        fd.seek(in_trunk_offset, io.SEEK_SET)
        self.__read_fd = io.BufferedReader(fd, buffer_size=self.__buffer_size)

    def close(self):
        if self.__read_fd is not None:
            self.__read_fd.close()
            self.__read_fd = None
        self.__write_fd.close()
        self.__write_fd = None