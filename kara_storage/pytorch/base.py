from typing import Generator, Any
import torch.utils.data as data

class KaraPytorchDatasetBase(data.IterableDataset):
    def __init__(self) -> None:
        super().__init__()
    
    def set_epoch(self, epoch_num : int):
        raise NotImplementedError()

    
    def __iter__(self) -> Generator[Any, None, None]:
        raise NotImplementedError()
    
    def __len__(self) -> int:
        raise NotImplementedError()
