from ..row import RowDataset
from ..abc import DatasetIterator

class SequentialIterator(DatasetIterator):
    def __init__(self, ds : RowDataset, epoch: int):
        self.__ds = ds
        self.__ds.seek(0)
    
    def next(self):
        try:
            return self.__ds.read()
        except EOFError:
            raise StopIteration()
