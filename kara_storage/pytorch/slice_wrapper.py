from ..storage import RowDataset

class SliceDatasetWrapper(RowDataset):
    def __init__(self, ds : RowDataset, start : int, length : int):
        self.__dataset = ds
        self.__start = start
        self.__length = length
        self.__end = start + length
        if self.__end > len(self.__dataset):
            self.__end = len(self.__dataset)
            self.__length = self.__end - self.__start
        self.__cur = None

        self.seek(0, 0)
    
    def seek(self, offset, whence):
        if whence == 0:
            self.__cur = offset + self.__start
        elif whence == 1:
            self.__cur += offset
        else:
            self.__cur = self.__end - offset
        if self.__cur < self.__start:
            self.__cur = self.__start
        if self.__cur > self.__end:
            self.__cur = self.__end
        self.__dataset.seek(self.__cur, 0)

    def read(self):
        if self.__cur >= self.__end:
            return None
        self.__cur += 1
        return self.__dataset.read()
    
    def __len__(self):
        return self.__length

