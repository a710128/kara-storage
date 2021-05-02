from typing import TYPE_CHECKING
from ..row import RowDataset

if TYPE_CHECKING:
    from .base import KaraPytorchDatasetBase

def make_torch_dataset(ds : RowDataset, shuffle=False, auto_distributed=True, **kwargs) -> 'KaraPytorchDatasetBase':
    
    import torch
    import torch.distributed
    from .base import KaraPytorchDatasetBase
    from .iter import SequentialIterator
    from .shuffle import ShuffleIterator

    if torch.distributed.is_initialized() and auto_distributed:
        rank = torch.distributed.get_rank()
        size = torch.distributed.get_world_size()

        total_length = ds.size()

        ds.slice_(total_length * rank // size, total_length // size)
    if shuffle:
        ret = KaraPytorchDatasetBase(ds,  ShuffleIterator, **kwargs)
    else:
        ret = KaraPytorchDatasetBase(ds, SequentialIterator, **kwargs)
    return ret
