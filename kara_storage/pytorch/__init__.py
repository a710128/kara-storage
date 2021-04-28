from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .base import KaraPytorchDatasetBase

def make_torch_dataset(ds, shuffle=False, distributed=True, begin=None, end=None, **kwargs) -> 'KaraPytorchDatasetBase':
    from .shuffle import ShuffleDatasetWrapper
    from .iter import IterDatasetWrapper

    from .slice_wrapper import SliceDatasetWrapper
    from .lock_wrapper import LockedDatasetWrapper

    
    import torch
    import torch.distributed

    if begin is None:
        begin = 0
    if end is None:
        end = len(ds)

    if torch.distributed.is_initialized() and distributed:
        rank = torch.distributed.get_rank()
        size = torch.distributed.get_world_size()

        total_length = end - begin

        ds = SliceDatasetWrapper(ds, total_length * rank // size + begin, total_length // size + begin)
    else:
        if begin != 0 or end != len(ds):
            ds = SliceDatasetWrapper( ds, begin, end - begin )
    if shuffle:
        ret =ShuffleDatasetWrapper(ds, **kwargs)
    else:
        ret = IterDatasetWrapper(ds)
    return LockedDatasetWrapper(ret)
