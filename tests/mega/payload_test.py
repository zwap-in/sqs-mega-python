import json
from base64 import b64encode

import bson

from mega.aws.payload import deserialize_payload, PayloadType


def test_deserialize_plaintext_payload():
    plaintext = 'hello world!'
    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.PLAINTEXT
    assert type(payload) == str
    assert payload == plaintext


def test_deserialize_plaintext_json_payload():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2, 3, 'four']
    }
    plaintext = json.dumps(data)

    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.DATA
    assert type(payload) == dict
    assert payload == data


def test_deserialize_base64_encoded_bson_payload():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2, 3, 'four']
    }
    blob = bson.dumps(data)
    plaintext = b64encode(blob).decode()

    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.DATA
    assert type(payload) == dict
    assert payload == data


def test_deserialize_binary_payload():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
    )
    plaintext = b64encode(blob).decode()

    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.BINARY
    assert type(payload) == bytes
    assert payload == blob


def test_deserialize_empty_payload():
    plaintext = ''
    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.PLAINTEXT
    assert type(payload) == str
    assert payload == plaintext
