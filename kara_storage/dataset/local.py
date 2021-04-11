from .base import Dataset
from ..file_controller import LocalFileController
import os

class LocalDataset(Dataset):
    def __init__(self, base_dir, mode, buffer_size = 128 * 1024, **kwargs) -> None:
        if not os.path.exists( os.path.join(base_dir, "data") ):
            os.makedirs(  os.path.join(base_dir, "data")  )
        if not os.path.exists( os.path.join(base_dir, "index") ):
            os.makedirs(  os.path.join(base_dir, "index")  )
        idx_controller = LocalFileController( os.path.join(base_dir, "index"), mode , **kwargs)
        dat_controller = LocalFileController( os.path.join(base_dir, "data"), mode  , **kwargs)

        super().__init__(idx_controller, dat_controller, mode, buffer_size=buffer_size)
    