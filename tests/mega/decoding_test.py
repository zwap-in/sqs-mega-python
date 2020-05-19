import binascii
import json
from base64 import b64encode

import bson

from mega.aws.decoding import try_decode_base64, try_decode_bson, try_decode_json, decode


def test_decode_bytes_from_valid_base64():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3"
    )
    base64 = b64encode(blob)

    decoded, error = try_decode_base64(base64)
    assert decoded == blob
    assert error is None


def test_do_not_decode_invalid_base64_made_of_bytes():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3"
    )

    decoded, error = try_decode_base64(blob)
    assert decoded is None
    assert type(error) == binascii.Error
    assert str(error) == 'Incorrect padding'


def test_do_not_decode_invalid_base64_made_of_plaintext():
    plaintext = 'the quick brown fox jumps over the lazy dog'

    decoded, error = try_decode_base64(plaintext)
    assert decoded is None
    assert type(error) == binascii.Error
    assert str(error) == 'Incorrect padding'


def test_decode_data_from_valid_bson():
    data = {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
        }
    }
    blob = bson.dumps(data)

    decoded, error = try_decode_bson(blob)
    assert decoded == data
    assert error is None


def test_do_not_decode_data_from_invalid_bson_made_of_bytes():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3"
    )

    decoded, error = try_decode_bson(blob)
    assert decoded is None
    assert type(error) == IndexError
    assert str(error) == 'index out of range'


def test_do_not_decode_data_from_invalid_bson_made_of_plaintext():
    data = {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
        }
    }
    plaintext = json.dumps(data)

    decoded, error = try_decode_bson(plaintext)
    assert decoded is None
    assert type(error) == TypeError
    assert str(error) == "a bytes-like object is required, not 'str'"


def test_decode_data_from_valid_json_made_of_plaintext():
    data = {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
        }
    }
    plaintext = json.dumps(data)

    decoded, error = try_decode_json(plaintext)
    assert decoded == data
    assert error is None


def test_decode_data_from_valid_json_made_of_bytes():
    data = {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
        }
    }
    blob = json.dumps(data).encode()

    decoded, error = try_decode_json(blob)
    assert decoded == data
    assert error is None


def test_do_not_decode_data_from_invalid_json_made_of_plaintext():
    plaintext = """
    {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
    }
    """

    decoded, error = try_decode_json(plaintext)
    assert decoded is None
    assert type(error) == json.decoder.JSONDecodeError
    assert str(error) == 'Expecting property name enclosed in double quotes: line 3 column 9 (char 15)'


def test_do_not_decode_data_from_invalid_json_made_of_bytes():
    blob = """
    {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2, 'three', 49]
    }
    """.encode()

    decoded, error = try_decode_json(blob)
    assert decoded is None
    assert type(error) == json.decoder.JSONDecodeError
    assert str(error) == 'Expecting property name enclosed in double quotes: line 3 column 9 (char 15)'


def test_decode_plaintext():
    plaintext = 'the quick brown fox jumps over the lazy dog'
    decoded = decode(plaintext)

    assert type(decoded) == str
    assert decoded == plaintext


def test_decode_binary():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3\xa1[2\xd0\x9f|V\xb0\xc3"
        b"wb6X\x9cz\xab\xb5\r\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0"
    )

    plaintext = b64encode(blob).decode()
    decoded = decode(plaintext)

    assert type(decoded) == bytes
    assert decoded == blob


def test_decode_data_from_plaintext_json():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2, 3, 'four']
    }

    plaintext = json.dumps(data)
    decoded = decode(plaintext)

    assert type(decoded) == dict
    assert decoded == data


def test_decode_data_from_base64_encoded_binary_bson():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2, 3, 'four']
    }

    blob = bson.dumps(data)
    plaintext = b64encode(blob).decode()
    decoded = decode(plaintext)

    assert type(decoded) == dict
    assert decoded == data
