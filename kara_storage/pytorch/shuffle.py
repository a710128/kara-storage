import random
import torch.utils.data as data
from ..storage import RowDataset

class ShuffleDatasetWrapper(data.IterableDataset):
    def __init__(self, dataset : RowDataset, seed : int = 0, buffer_size : int = 10240, shuffle_ratio : float = 0.1) -> None:
        super().__init__()
        self.__dataset = dataset
        self.__g = random.Random()
        self.__seed = seed
        self.__buffer_size = buffer_size
        self.__fetch_num = int(buffer_size * shuffle_ratio)
        self.set_epoch(0)
        
        if self.__fetch_num <= 1:
            raise ValueError("Shuffle ratio is too small")

    
    def set_epoch(self, epoch_num):
        self.__g.seed(self.__seed + epoch_num)
        self.__dataset.seek(0, 0)

    
    def __iter__(self):
        buffer = []

        # preload
        for _ in range(self.__buffer_size - self.__fetch_num):
            v = self.__dataset.read()
            if v is None:
                break
            buffer.append(v)

        while True:
            # fetch new data
            for _ in range(self.__fetch_num):
                v = self.__dataset.read()
                if v is None:
                    break
                buffer.append(v)
            
            # get fetch size
            fetch_size = min( self.__fetch_num, len(buffer) )

            if fetch_size == 0:
                break
            
             # random swap
            for i in range(fetch_size):
                v = self.__g.randint(0, len(buffer) - 1)
                buffer[i], buffer[v] = buffer[v], buffer[i]
            for i in range(fetch_size):
                yield buffer[i]
            buffer = buffer[fetch_size:]
        return
    
    def __len__(self):
        return len(self.__dataset)
