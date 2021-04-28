from typing import Any
from .base import Serializer
import mujson

class JSONSerializer(Serializer):
    def serialize(self, x : Any) -> bytes:
        v = mujson.dumps(x)
        if isinstance(v, str):
            v = v.encode("utf-8")
        return v
    
    def deserialize(self, x : bytes) -> Any:
        return mujson.loads( x )