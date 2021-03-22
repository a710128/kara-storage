import torch.utils.data as data
from ..storage import RowDataset

class IterDatasetWrapper(data.IterableDataset):
    def __init__(self, dataset : RowDataset) -> None:
        super().__init__()
        self.__dataset = dataset
        self.set_epoch(0)
    
    def set_epoch(self, epoch_num):
        self.__dataset.seek(0, 0)

    
    def __iter__(self):
        while True:
            v = self.__dataset.read()
            if v is None:
                break
            yield v
        return
    
    def __len__(self):
        return len(self.__dataset)
