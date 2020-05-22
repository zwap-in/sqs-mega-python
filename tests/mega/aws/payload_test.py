import json
from base64 import b64encode, b64decode

import bson
import dateutil.parser
import pytest

from mega.aws.payload import deserialize_payload, PayloadType, serialize_payload
from mega.event import MegaPayload, MegaEvent, MegaObject
from mega.event.v1.schema import MegaSchemaError


def test_deserialize_plaintext_payload():
    plaintext = 'hello world!'
    payload, payload_type = deserialize_payload(plaintext)

    assert payload_type == PayloadType.PLAINTEXT
    assert type(payload) == str
    assert payload == plaintext


def test_deserialize_plaintext_payload_with_non_ascii_characters():
    plaintext = '<áéíóúãõâêîôûàèìòùçÁÉÍÓÚÃÕÂÊÎÔÛÀÈÌÒÙÇ!@#$%^&*()-_=+`~\'";:.,/>'
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


def test_deserialize_plaintext_json_payload_with_non_ascii_characters():
    data = {
        'foo': '<áéíóúãõâêîôûàèìòùçÁÉÍÓÚÃÕÂÊÎÔÛÀÈÌÒÙÇ!@#$%^&*()-_=+`~\'";:.,/>',
        'bar': True
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


def test_serialize_plaintext_payload_as_itself():
    plaintext = '{"the quick brown fox jumps overt the lazy dog!"}'
    serialized = serialize_payload(plaintext)
    assert type(serialized) == str
    assert serialized == plaintext


def test_fail_to_serialize_plaintext_payload_with_binary_encoding():
    plaintext = '{"the quick brown fox jumps overt the lazy dog!"}'

    with pytest.raises(ValueError) as e:
        serialize_payload(plaintext, binary_encoding=True)

    assert str(e.value) == "Can't use binary encoding with a plaintext string"


def test_serialize_binary_payload_as_base64():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq"
    )

    serialized = serialize_payload(blob)
    assert type(serialized) == str
    assert b64decode(serialized) == blob


def test_serialize_data_payload_as_json_string():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2.01, 3, 'four', {}]
    }

    serialized = serialize_payload(data)
    assert type(serialized) == str
    assert json.loads(serialized) == data


def test_serialize_data_payload_as_bson_encoded_as_base64_string():
    data = {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2.01, 3, 'four', {}]
    }

    serialized = serialize_payload(data, binary_encoding=True)
    assert type(serialized) == str
    decoded = b64decode(serialized)
    assert bson.loads(decoded) == data


def test_serialize_mega_payload_as_json_string():
    timestamp = '2020-05-20T20:07:22.589063'
    payload = MegaPayload(
        event=MegaEvent(
            name='foo.bar',
            timestamp=dateutil.parser.parse(timestamp),
            subject='991'
        ),
        object=MegaObject(
            current={'foo': 'bar'}
        )
    )

    serialized = serialize_payload(payload)
    assert type(serialized) == str

    data = json.loads(serialized)
    assert data == {
        'protocol': 'mega',
        'version': 1,
        'event': {
            'name': 'foo.bar',
            'version': 1,
            'timestamp': timestamp,
            'subject': '991'
        },
        'object': {
            'version': 1,
            'current': {'foo': 'bar'}
        }
    }


def test_serialize_mega_payload_as_bson_encoded_as_base64_string():
    timestamp = '2020-05-20T20:07:22.589063'
    payload = MegaPayload(
        event=MegaEvent(
            name='foo.bar',
            timestamp=dateutil.parser.parse(timestamp),
            subject='991'
        ),
        object=MegaObject(
            current={'foo': 'bar'}
        )
    )

    serialized = serialize_payload(payload, binary_encoding=True)
    assert type(serialized) == str

    decoded = b64decode(serialized)
    data = bson.loads(decoded)
    assert data == {
        'protocol': 'mega',
        'version': 1,
        'event': {
            'name': 'foo.bar',
            'version': 1,
            'timestamp': timestamp,
            'subject': '991'
        },
        'object': {
            'version': 1,
            'current': {'foo': 'bar'}
        }
    }


def test_fail_to_serialize_invalid_mega_payload():
    payload = MegaPayload(
        event=MegaEvent(
            name='foo.bar',
            subject='991'
        ),
        object=MegaObject(
            current={'foo': 'bar'}
        )
    )
    payload.event = None

    with pytest.raises(MegaSchemaError) as e:
        serialize_payload(payload)

    assert str(e.value) == "Invalid MEGA payload: {'event': ['Missing data for required field.']}"


def test_fail_to_serialize_null_payload():
    with pytest.raises(ValueError) as e:
        serialize_payload(None)
    assert str(e.value) == "Payload can't be null"
