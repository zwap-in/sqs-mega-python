import dateutil.parser
import pytest
from parameterized import parameterized

from mega.event.v1.payload import EventObject, Event, MegaPayload
from mega.event.v1.schema import deserialize_mega_payload, MegaSchemaError, matches_mega_payload
from tests.mega.event.v1.schema.event_object_test import build_event_object_data
from tests.mega.event.v1.schema.event_test import build_event_data


def build_mega_payload_data(**kwargs):
    data = {
        'protocol': 'mega',
        'version': 1,
        'event': build_event_data(),
        'object': build_event_object_data(),
        'extra': {
            'channel': 'web/desktop',
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                          'Version/13.1 Safari/605.1.15',
            'user_ip_address': '177.182.205.103'
        }
    }
    data.update(kwargs)
    return data


def test_deserialize_full_mega_payload_data():
    data = build_mega_payload_data()

    payload = deserialize_mega_payload(data)

    event_data = data['event']
    assert payload.event is not None
    assert isinstance(payload.event, Event)
    assert payload.event.name == event_data['name']
    assert payload.event.version == event_data['version']
    assert payload.event.domain == event_data['domain']
    assert payload.event.subject == event_data['subject']
    assert payload.event.publisher == event_data['publisher']
    assert payload.event.attributes == event_data['attributes']
    assert payload.event.timestamp == dateutil.parser.parse(event_data['timestamp'])

    object_data = data['object']
    assert payload.object is not None
    assert isinstance(payload.object, EventObject)
    assert payload.object.type == object_data['type']
    assert payload.object.id == object_data['id']
    assert payload.object.version == object_data['version']
    assert payload.object.current == object_data['current']
    assert payload.object.previous == object_data['previous']

    assert payload.extra is not None
    assert payload.extra == data['extra']


def test_deserialize_mega_payload_ignoring_unknown_attributes():
    data = build_mega_payload_data()
    data['foo'] = 'bar'
    data['one'] = 1

    payload = deserialize_mega_payload(data)

    assert payload is not None
    assert isinstance(payload, MegaPayload)


@parameterized.expand([
    ['object', None],
    ['extra', {}]
])
def test_deserialize_mega_payload_data_without_optional_attribute(attribute_key, expected_value):
    data = build_mega_payload_data()
    del data[attribute_key]
    payload = deserialize_mega_payload(data)
    assert getattr(payload, attribute_key) == expected_value


@parameterized.expand([
    ['object', None],
    ['extra', {}]
])
def test_deserialize_mega_payload_data_with_optional_attribute_set_to_null(attribute_key, expected_value):
    data = build_mega_payload_data()
    data[attribute_key] = None
    payload = deserialize_mega_payload(data)
    assert getattr(payload, attribute_key) == expected_value


def test_fail_to_deserialize_mega_payload_data_without_event_attribute():
    data = build_mega_payload_data()
    del data['event']

    with pytest.raises(MegaSchemaError) as e:
        deserialize_mega_payload(data)

    assert str(e.value) == "Invalid MEGA payload: {'event': ['Missing data for required field.']}"


def test_fail_to_deserialize_event_data_with_event_set_to_null():
    data = build_mega_payload_data(event=None)

    with pytest.raises(MegaSchemaError) as e:
        deserialize_mega_payload(data)

    assert str(e.value) == "Invalid MEGA payload: {'event': ['Field may not be null.']}"


def test_fail_to_deserialize_mega_payload_data_with_invalid_event_section():
    bogus_event_data = build_event_data()
    del bogus_event_data['name']

    data = build_mega_payload_data(event=bogus_event_data)

    with pytest.raises(MegaSchemaError) as e:
        deserialize_mega_payload(data)

    assert str(e.value) == "Invalid MEGA payload. Could not deserialize the 'event' section: " \
                           "{'name': ['Missing data for required field.']}"


def test_fail_to_deserialize_mega_payload_data_with_invalid_object_section():
    bogus_object_data = build_event_object_data()
    bogus_object_data['current'] = None

    data = build_mega_payload_data(object=bogus_object_data)

    with pytest.raises(MegaSchemaError) as e:
        deserialize_mega_payload(data)

    assert str(e.value) == "Invalid MEGA payload. Could not deserialize the 'object' section: " \
                           "{'current': ['Field may not be null.']}"


def test_mega_payload_matches_data_when_protocol_and_version_match():
    data = {
        'protocol': 'mega',
        'version': 1
    }
    assert matches_mega_payload(data) is True


@parameterized.expand([
    [None],
    [''],
    ['MEGA'],
    ['Mega'],
    ['MeGa'],
    ['_mega'],
    [' mega'],
    ['mega_'],
    ['123foobar']
])
def test_mega_payload_does_not_match_data_when_protocol_is(protocol):
    data = {
        'protocol': protocol,
        'version': 1
    }
    assert matches_mega_payload(data) is False


@parameterized.expand([
    [None],
    ['1'],
    [2],
    [0],
    [-1]
])
def test_mega_payload_does_not_match_data_when_version_is(version):
    data = {
        'protocol': 'mega',
        'version': version
    }
    assert matches_mega_payload(data) is False


def test_mega_payload_does_match_data_when_protocol_is_missing():
    data = {'version': 1}
    assert matches_mega_payload(data) is False


def test_mega_payload_does_match_data_when_version_is_missing():
    data = {'protocol': 'mega'}
    assert matches_mega_payload(data) is False


def test_null_does_not_match_mega_payload():
    assert matches_mega_payload(None) is False
