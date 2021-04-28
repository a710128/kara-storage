from .base import KaraPytorchDatasetBase
import multiprocessing as mp

class LockedDatasetWrapper(KaraPytorchDatasetBase):
    def __init__(self, dataset : KaraPytorchDatasetBase) -> None:
        super().__init__()
        self.__dataset = dataset
        self.lock = mp.Lock()
    
    def set_epoch(self, epoch_num):
        with self.lock:
            self.__dataset.set_epoch(epoch_num)

    def __iter__(self):
        it = iter(self.__dataset)
        while True:
            try:
                with self.lock:
                    value = next(it)
            except StopIteration:
                return
            yield value
    
    def __len__(self):
        return len(self.__dataset)
