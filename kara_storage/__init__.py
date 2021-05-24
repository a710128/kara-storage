import multiprocessing
multiprocessing.set_start_method("spawn", force=True)
# FIXME: force using spawn start method, use register_after_fork in the future.

from .storage import KaraStorage
from . import serialization
from .pytorch import make_torch_dataset
from .version import version
from .row import RowDataset