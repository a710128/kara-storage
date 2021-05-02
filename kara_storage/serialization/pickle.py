from typing import Any
from ..abc import Serializer
import pickle

class PickleSerializer(Serializer):
    def serialize(self, x : Any) -> bytes:
        return pickle.dumps(x)
    
    def deserialize(self, x : bytes) -> Any:
        return pickle.loads(x)