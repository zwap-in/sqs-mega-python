from enum import Enum
from typing import Tuple, Union

from mega.aws.decoding import decode
from mega.event import MegaPayload, deserialize_mega_payload

Payload = Union[bytes, str, dict, MegaPayload]


class PayloadType(Enum):
    NONE = 0
    BINARY = 1
    PLAINTEXT = 2
    DATA = 4
    MEGA = 5


# TODO: test
def parse_payload(body) -> Tuple[Payload, PayloadType]:
    decoded = decode(body)
    _type = type(decoded)
    if _type == bytes:
        return decoded, PayloadType.BINARY
    elif _type == str:
        return decoded, PayloadType.PLAINTEXT
    elif _type == dict:
        data = decoded
        if MegaPayload.matches(data):
            return deserialize_mega_payload(data), PayloadType.MEGA
        else:
            return data, PayloadType.DATA
    else:
        raise ValueError("Don't know how to parse: {}".format(_type))
