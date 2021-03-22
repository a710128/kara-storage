from typing import Any


class Serializer:
    def serialize(self, x : Any) -> bytes:
        raise NotImplementedError
    
    def deserialize(self, x : bytes) -> Any:
        raise NotImplementedError