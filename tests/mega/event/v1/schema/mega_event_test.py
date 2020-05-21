from datetime import timedelta

import dateutil.parser
import pytest
from parameterized import parameterized

from mega.event.v1.payload import MegaEvent
from mega.event.v1.schema import MegaEventSchema, MegaSchemaError


def build_mega_event_attributes():
    return {
        'item_id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
        'quantity': 5,
        'active': True
    }


def build_mega_event_data(**kwargs):
    data = {
        'name': 'shopping_cart.item.added',
        'version': 1,
        'timestamp': '2020-05-04T15:53:23.123',
        'domain': 'shopping_cart',
        'subject': '987650',
        'publisher': 'shopping-cart-service',
        'attributes': build_mega_event_attributes()
    }
    data.update(kwargs)
    return data


def test_deserialize_full_event_data():
    data = build_mega_event_data()

    event = MegaEventSchema().load(data)

    assert event.name == data['name']
    assert event.version == data['version']
    assert event.domain == data['domain']
    assert event.subject == data['subject']
    assert event.publisher == data['publisher']
    assert event.attributes == data['attributes']

    assert event.timestamp == dateutil.parser.parse(data['timestamp'])
    assert event.timestamp.utcoffset() is None
    assert event.timestamp.tzinfo is None


def test_deserialize_event_data_ignoring_unknown_attributes():
    data = build_mega_event_data()
    data['foo'] = 'bar'
    data['one'] = 1

    payload = MegaEventSchema().load(data)

    assert payload is not None
    assert isinstance(payload, MegaEvent)


@parameterized.expand([
    ['version', 1],
    ['domain', None],
    ['subject', None],
    ['publisher', None],
    ['attributes', {}]
])
def test_deserialize_event_data_without_optional_attribute(attribute_key, expected_value):
    data = build_mega_event_data()
    del data[attribute_key]
    event = MegaEventSchema().load(data)
    assert getattr(event, attribute_key) == expected_value


@parameterized.expand([
    ['version', 1],
    ['domain', None],
    ['subject', None],
    ['publisher', None],
    ['attributes', {}]
])
def test_deserialize_event_data_with_optional_attribute_set_to_null(attribute_key, expected_value):
    data = build_mega_event_data()
    data[attribute_key] = None
    event = MegaEventSchema().load(data)
    assert getattr(event, attribute_key) == expected_value


def test_deserialize_event_data_with_timezone_aware_timestamp():
    timestamp = '2020-05-15T20:35:45.000+05:00'
    data = build_mega_event_data(timestamp=timestamp)

    event = MegaEventSchema().load(data)

    assert event.timestamp == dateutil.parser.parse(timestamp)
    assert event.timestamp.utcoffset() == timedelta(hours=5)


@parameterized.expand([
    ['name'],
    ['timestamp']
])
def test_fail_to_deserialize_event_data_without_required_attribute(attribute_key):
    data = build_mega_event_data()
    del data[attribute_key]

    with pytest.raises(MegaSchemaError) as e:
        MegaEventSchema().load(data)

    assert str(e.value) == (
        "Invalid MEGA payload. There is an error in the 'event' section: "
        "{{'{0}': ['Missing data for required field.']}}".format(attribute_key)
    )


@parameterized.expand([
    ['name'],
    ['timestamp']
])
def test_fail_to_deserialize_event_data_with_required_attribute_set_to_null(attribute_key):
    data = build_mega_event_data()
    data[attribute_key] = None

    with pytest.raises(MegaSchemaError) as e:
        MegaEventSchema().load(data)

    assert str(e.value) == (
        "Invalid MEGA payload. There is an error in the 'event' section: "
        "{{'{0}': ['Field may not be null.']}}".format(attribute_key)
    )


def test_fail_to_deserialize_event_data_with_invalid_version():
    data = build_mega_event_data(version='foobar')

    with pytest.raises(MegaSchemaError) as e:
        MegaEventSchema().load(data)

    assert "{'version': ['Not a valid integer.']}" in str(e.value)


def test_fail_to_deserialize_event_data_with_invalid_timestamp():
    data = build_mega_event_data(timestamp='2020-05-15TFOOBAR')

    with pytest.raises(MegaSchemaError) as e:
        MegaEventSchema().load(data)

    assert "{'timestamp': ['Not a valid datetime.']}" in str(e.value)


def test_fail_to_deserialize_event_data_with_invalid_attribute_key():
    data = build_mega_event_data(
        attributes={
            'foo': 'bar',
            666: 'bogus'
        }
    )

    with pytest.raises(MegaSchemaError) as e:
        MegaEventSchema().load(data)

    assert "{'attributes': defaultdict(<class 'dict'>, {666: {'key': ['Not a valid string.']}})}" in str(e.value)
