from typing import Any


class DatasetIterator:
    def __init__(self, ds, epoch : int) -> None:
        raise NotImplementedError()
    def next(self) -> Any:
        raise NotImplementedError()