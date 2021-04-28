from .base import Dataset
from ..file_controller import OSSFileController

class OSSDataset(Dataset):
    def __init__(self,  prefix : str, bucket, mode, buffer_size = 1 * 1024 * 1024, **kwargs) -> None:
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        
        idx_controller = OSSFileController(prefix + "index/", bucket, mode, **kwargs)
        dat_controller = OSSFileController(prefix +  "data/", bucket, mode, **kwargs)

        super().__init__(idx_controller, dat_controller, mode, buffer_size=buffer_size)
    