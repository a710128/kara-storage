from .base import Dataset
from ..file_controller import HTTPController

class HTTPDataset(Dataset):
    def __init__(self,  url_with_prefix : str, mode, buffer_size = 1 * 1024 * 1024, **kwargs) -> None:
        if not url_with_prefix.endswith("/"):
            url_with_prefix = url_with_prefix + "/"
        
        idx_controller = HTTPController(url_with_prefix + "index/", mode, **kwargs)
        dat_controller = HTTPController(url_with_prefix +  "data/", mode, **kwargs)

        super().__init__(idx_controller, dat_controller, mode, buffer_size=buffer_size)
    