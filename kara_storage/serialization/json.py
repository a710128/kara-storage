from typing import Any
from .base import Serializer
import json

class JSONSerializer(Serializer):
    def serialize(self, x : Any) -> bytes:
        return json.dumps(x).encode("utf-8")
    
    def deserialize(self, x : bytes) -> Any:
        return json.loads( x.decode("utf-8") )