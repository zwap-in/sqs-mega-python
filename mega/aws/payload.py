from enum import Enum
from typing import Tuple, Union

import mega.event
from mega.aws.encoding import decode_value, encode_blob, encode_data

MessagePayload = Union[bytes, str, dict, mega.event.Payload]


class PayloadType(Enum):
    NONE = 0
    BINARY = 1
    PLAINTEXT = 2
    DATA = 4
    MEGA = 5


def deserialize_payload(plaintext: str) -> Tuple[MessagePayload, PayloadType]:
    value = decode_value(plaintext)
    _type = type(value)

    if _type == str:
        return value, PayloadType.PLAINTEXT

    if _type == bytes:
        return value, PayloadType.BINARY

    if _type == dict:
        if mega.event.matches_payload(value):
            return mega.event.deserialize_payload(value), PayloadType.MEGA
        return value, PayloadType.DATA

    raise ValueError("Don't know how to deserialize payload with type: {}".format(_type))


def serialize_payload(payload: MessagePayload, binary_encoding=False) -> str:
    if payload is None:
        raise ValueError("Payload can't be null")

    _type = type(payload)

    if _type == str:
        if binary_encoding:
            raise ValueError("Can't use binary encoding with a plaintext string")
        return payload

    if _type == bytes:
        return encode_blob(payload)

    if _type == dict:
        return encode_data(payload, binary_encoding)

    if _type == mega.event.Payload:
        data = mega.event.serialize_payload(payload)
        return encode_data(data, binary_encoding)

    raise ValueError("Don't know how to serialize payload with type: {}".format(_type))
