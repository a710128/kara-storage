from typing import Any
from ..abc import Serializer
import orjson

class JSONSerializer(Serializer):
    def serialize(self, x : Any) -> bytes:
        return orjson.dumps(x)
    
    def deserialize(self, x : bytes) -> Any:
        return orjson.loads(x)
