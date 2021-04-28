def make_torch_dataset(ds, shuffle=False, distributed=True, **kwargs):
    from .shuffle import ShuffleDatasetWrapper
    from .iter import IterDatasetWrapper
    from .slice import SliceDatasetWrapper

    
    import torch
    import torch.distributed

    if torch.distributed.is_initialized() and distributed:
        rank = torch.distributed.get_rank()
        size = torch.distributed.get_world_size()

        total_length = len(ds)

        ds = SliceDatasetWrapper(ds, total_length * rank // size, total_length // size)

    if shuffle:
        return ShuffleDatasetWrapper(ds, **kwargs)
    else:
        return IterDatasetWrapper(ds)
