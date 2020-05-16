import binascii
import json
from base64 import b64decode
from typing import Optional, Tuple, Union

import bson


def try_decode_base64(plaintext: str) -> Tuple[Optional[bytes], Optional[Exception]]:
    try:
        return b64decode(plaintext), None
    except binascii.Error as e:
        return None, e


def try_decode_bson(blob: bytes) -> Tuple[Optional[dict], Optional[Exception]]:
    try:
        return bson.loads(blob), None
    except (IndexError, TypeError) as e:
        return None, e


def try_decode_json(plaintext: Union[str, bytes]) -> Tuple[Optional[dict], Optional[Exception]]:
    try:
        return json.loads(plaintext), None
    except json.decoder.JSONDecodeError as e:
        return None, e


# TODO: test
def decode_string(plaintext: str) -> Union[bytes, str, dict]:
    blob, _ = try_decode_base64(plaintext)
    if blob:
        data, _ = try_decode_bson(blob)
        if data is not None:
            return data
        else:
            return blob
    else:
        data, _ = try_decode_json(plaintext)
        if data is not None:
            return data
        else:
            return plaintext
