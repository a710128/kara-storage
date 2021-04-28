def make_torch_dataset(ds, shuffle=False, distributed=True, begin=None, end=None, **kwargs):
    from .shuffle import ShuffleDatasetWrapper
    from .iter import IterDatasetWrapper
    from .slice import SliceDatasetWrapper

    
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
        return ShuffleDatasetWrapper(ds, **kwargs)
    else:
        return IterDatasetWrapper(ds)
