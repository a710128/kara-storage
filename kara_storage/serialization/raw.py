from ..abc import Serializer

class NoSerializer(Serializer):
    def serialize(self, x : bytes) -> bytes:
        if not isinstance(x, bytes):
            raise TypeError("Serializer is required if you want to write Object data")
        return x
    
    def deserialize(self, x : bytes) -> bytes:
        return x