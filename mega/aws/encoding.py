import binascii
import json
from base64 import b64decode
from logging import getLogger
from typing import Optional, Tuple, Union

import bson

logger = getLogger(__name__)


def try_decode_base64(plaintext: str) -> Tuple[Optional[bytes], Optional[Exception]]:
    try:
        return b64decode(plaintext, validate=True), None
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


def _decode_value_from_blob(blob: bytes) -> Union[bytes, dict]:
    logger.debug('Trying to decode BSON')
    data, error = try_decode_bson(blob)
    if data is not None:
        return data

    logger.debug('Assuming Binary. Could not decode BSON: ' + str(error))
    return blob


def decode_value(plaintext: str) -> Union[bytes, str, dict]:
    if not plaintext:
        return plaintext

    logger.debug('Trying to decode JSON')
    data, error = try_decode_json(plaintext)
    if data is not None:
        return data

    logger.debug('Trying to decode Base64. Could not decode JSON: ' + str(error))
    blob, error = try_decode_base64(plaintext)
    if blob:
        return _decode_value_from_blob(blob)

    logger.debug('Assuming Plaintext. Could not decode Base64: ' + str(error))
    return plaintext
