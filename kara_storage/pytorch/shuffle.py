import random

from ..abc import DatasetIterator
from ..row import RowDataset


class ShuffleIterator(DatasetIterator):
    def __init__(self, ds : RowDataset, epoch: int, seed : int = 0, buffer_size = 4096) -> None:
        self.__g = random.Random(epoch + seed)
        self.__ds = ds
        
        self.__ds.seek(0)
        
        self.__pool = []
        for i in range(buffer_size):
            try:
                self.__pool.append(self.__ds.read())
            except EOFError:
                break
    
    def next(self):
        if len(self.__pool) == 0:
            raise StopIteration()

        idx = int(self.__g.random() * len(self.__pool))
        ret = self.__pool[idx]

        try:
            nwval = self.__ds.read()
        except EOFError:
            self.__pool[idx] = self.__pool[-1]
            self.__pool.pop()
        else:
            self.__pool[idx] = nwval
        return ret