import dateutil.parser
import freezegun
import pytest
from parameterized import parameterized

from mega.event.v1.payload import ObjectData, Event, Payload
from mega.event.v1.schema import deserialize_payload, SchemaError, matches_payload, serialize_payload
from tests.mega.event.v1.schema.event_test import build_event_data, build_event_attributes
from tests.mega.event.v1.schema.object_test import build_object_data, build_previous_object_data, \
    build_current_object_data


def build_extra_data():
    return {
        'channel': 'web/desktop',
        'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                      'Version/13.1 Safari/605.1.15',
        'user_ip_address': '177.182.205.103'
    }


def build_payload_data(**kwargs):
    data = {
        'protocol': 'mega',
        'version': 1,
        'event': build_event_data(),
        'object': build_object_data(),
        'extra': build_extra_data()
    }
    data.update(kwargs)
    return data


def test_deserialize_full_payload_data():
    data = build_payload_data()

    payload = deserialize_payload(data)

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
    assert isinstance(payload.object, ObjectData)
    assert payload.object.type == object_data['type']
    assert payload.object.id == object_data['id']
    assert payload.object.version == object_data['version']
    assert payload.object.current == object_data['current']
    assert payload.object.previous == object_data['previous']

    assert payload.extra is not None
    assert payload.extra == data['extra']


def test_deserialize_payload_ignoring_unknown_attributes():
    data = build_payload_data()
    data['foo'] = 'bar'
    data['one'] = 1

    payload = deserialize_payload(data)

    assert payload is not None
    assert isinstance(payload, Payload)


@parameterized.expand([
    ['object', None],
    ['extra', {}]
])
def test_deserialize_payload_data_without_optional_attribute(attribute_key, expected_value):
    data = build_payload_data()
    del data[attribute_key]
    payload = deserialize_payload(data)
    assert getattr(payload, attribute_key) == expected_value


@parameterized.expand([
    ['object', None],
    ['extra', {}]
])
def test_deserialize_payload_data_with_optional_attribute_set_to_null(attribute_key, expected_value):
    data = build_payload_data()
    data[attribute_key] = None
    payload = deserialize_payload(data)
    assert getattr(payload, attribute_key) == expected_value


def test_fail_to_deserialize_payload_data_without_event_attribute():
    data = build_payload_data()
    del data['event']

    with pytest.raises(SchemaError) as e:
        deserialize_payload(data)

    assert str(e.value) == "Invalid MEGA payload: {'event': ['Missing data for required field.']}"


def test_fail_to_deserialize_event_data_with_event_set_to_null():
    data = build_payload_data(event=None)

    with pytest.raises(SchemaError) as e:
        deserialize_payload(data)

    assert str(e.value) == "Invalid MEGA payload: {'event': ['Field may not be null.']}"


def test_fail_to_deserialize_payload_data_with_invalid_event_section():
    bogus_event_data = build_event_data()
    del bogus_event_data['name']

    data = build_payload_data(event=bogus_event_data)

    with pytest.raises(SchemaError) as e:
        deserialize_payload(data)

    assert str(e.value) == "Invalid MEGA payload. There is an error in the 'event' section: " \
                           "{'name': ['Missing data for required field.']}"


def test_fail_to_deserialize_payload_data_with_invalid_object_section():
    bogus_object_data = build_object_data()
    bogus_object_data['current'] = None

    data = build_payload_data(object=bogus_object_data)

    with pytest.raises(SchemaError) as e:
        deserialize_payload(data)

    assert str(e.value) == "Invalid MEGA payload. There is an error in the 'object' section: " \
                           "{'current': ['Field may not be null.']}"


def test_payload_matches_data_when_protocol_and_version_match():
    data = {
        'protocol': 'mega',
        'version': 1
    }
    assert matches_payload(data) is True


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
def test_payload_does_not_match_data_when_protocol_is(protocol):
    data = {
        'protocol': protocol,
        'version': 1
    }
    assert matches_payload(data) is False


@parameterized.expand([
    [None],
    ['1'],
    [2],
    [0],
    [-1]
])
def test_payload_does_not_match_data_when_version_is(version):
    data = {
        'protocol': 'mega',
        'version': version
    }
    assert matches_payload(data) is False


def test_payload_does_match_data_when_protocol_is_missing():
    data = {'version': 1}
    assert matches_payload(data) is False


def test_payload_does_match_data_when_version_is_missing():
    data = {'protocol': 'mega'}
    assert matches_payload(data) is False


def test_null_does_not_match_payload():
    assert matches_payload(None) is False


def test_serialize_full_valid_payload():
    event = Event(
        name='shopping_cart.item.added',
        version=2,
        timestamp=dateutil.parser.parse('2020-05-04T15:53:23.123'),
        domain='shopping_cart',
        subject='987650',
        publisher='shopping-cart-service',
        attributes=build_event_attributes()
    )
    _object = ObjectData(
        type='shopping_cart',
        id='18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        version=3,
        current=build_current_object_data(),
        previous=build_previous_object_data()
    )
    extra = build_extra_data()
    payload = Payload(
        event=event,
        object=_object,
        extra=extra
    )

    data = serialize_payload(payload)

    assert data['protocol'] == 'mega'
    assert data['version'] == 1

    assert data['event']['name'] == event.name
    assert data['event']['version'] == event.version
    assert dateutil.parser.parse(data['event']['timestamp']) == event.timestamp
    assert data['event']['domain'] == event.domain
    assert data['event']['subject'] == event.subject
    assert data['event']['publisher'] == event.publisher
    assert data['event']['attributes'] == event.attributes
    assert data['object']['type'] == _object.type
    assert data['object']['id'] == _object.id
    assert data['object']['version'] == _object.version
    assert data['object']['current'] == _object.current
    assert data['object']['previous'] == _object.previous
    assert data['extra'] == extra


def test_serialize_minimal_valid_payload():
    timestamp = '2020-05-19T18:43:52.424566'
    with freezegun.freeze_time(timestamp):
        event = Event(name='shopping_cart.item.added')
    payload = Payload(event=event)

    data = serialize_payload(payload)

    assert data == {
        'protocol': 'mega',
        'version': 1,
        'event': {
            'name': 'shopping_cart.item.added',
            'version': 1,
            'timestamp': timestamp
        }
    }


def test_serialize_medium_valid_payload():
    timestamp = '2020-05-19T18:43:52.424566'
    with freezegun.freeze_time(timestamp):
        event = Event(
            name='shopping_cart.item.removed',
            version=2,
            subject='235078',
            item_id='b23c5670-e4d9-40cd-94e0-b7776e08b104',
            all=True
        )

    _object = ObjectData(
        type='shopping_cart',
        current=build_current_object_data()
    )

    payload = Payload(
        event=event,
        object=_object,
        ip_address='172.217.162.174'
    )

    data = serialize_payload(payload)

    assert data == {
        'protocol': 'mega',
        'version': 1,
        'event': {
            'name': 'shopping_cart.item.removed',
            'timestamp': timestamp,
            'version': 2,
            'subject': '235078',
            'attributes': {
                'item_id': 'b23c5670-e4d9-40cd-94e0-b7776e08b104',
                'all': True
            }
        },
        'object': {
            'type': 'shopping_cart',
            'version': 1,
            'current': _object.current
        },
        'extra': {
            'ip_address': '172.217.162.174'
        }
    }


def test_fail_to_serialize_invalid_payload():
    event = Event(name='shopping_cart.item.added')
    event.name = None
    payload = Payload(event=event)

    with pytest.raises(SchemaError) as e:
        serialize_payload(payload)

    assert str(e.value) == "Invalid MEGA payload. " \
                           "There is an error in the 'event' section: {'name': ['Missing data for required field.']}"
