from kara_storage.file_controller import base
from .base import Dataset
from ..file_controller import LocalFileController
import os

class LocalDataset(Dataset):
    def __init__(self, base_dir, buffer_size, **kwargs) -> None:
        if not os.path.exists( os.path.join(base_dir, "data") ):
            os.makedirs(  os.path.join(base_dir, "data")  )
        if not os.path.exists( os.path.join(base_dir, "index") ):
            os.makedirs(  os.path.join(base_dir, "index")  )
        idx_controller = LocalFileController( os.path.join(base_dir, "index") , **kwargs)
        dat_controller = LocalFileController( os.path.join(base_dir, "data")  , **kwargs)

        super().__init__(idx_controller, dat_controller, buffer_size=buffer_size)
    