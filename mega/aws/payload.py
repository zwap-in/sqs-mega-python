from enum import Enum
from typing import Tuple, Union

from mega.aws.encoding import decode_value
from mega.event import MegaPayload, deserialize_mega_payload, matches_mega_payload

Payload = Union[bytes, str, dict, MegaPayload]


class PayloadType(Enum):
    NONE = 0
    BINARY = 1
    PLAINTEXT = 2
    DATA = 4
    MEGA = 5


def deserialize_payload(plaintext: str) -> Tuple[Payload, PayloadType]:
    value = decode_value(plaintext)
    value_type = type(value)

    if value_type == bytes:
        return value, PayloadType.BINARY
    elif value_type == str:
        return value, PayloadType.PLAINTEXT
    elif value_type == dict:
        if matches_mega_payload(value):
            return deserialize_mega_payload(value), PayloadType.MEGA
        return value, PayloadType.DATA

    raise ValueError("Don't know how to deserialize payload with value: {}".format(value_type))
