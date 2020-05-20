import pytest
from parameterized import parameterized

from mega.event.v1.payload import EventObject
from mega.event.v1.schema import EventObjectSchema, MegaSchemaError


def build_current_object_data():
    return {
        'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        'user_id': 987650,
        'items': [
            {
                'id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
                'price': '19.99',
                'quantity': 10
            },
            {
                'id': '3c7f8798-1d3d-47de-82dd-c6c5e0de74ee',
                'price': '102.50',
                'quantity': 1
            },
            {
                'id': 'bba76edc-8afc-4fde-b4c4-ea58a230c5d6',
                'price': '24.99',
                'quantity': 3
            }
        ],
        'currency': 'USD',
        'value': '377.37',
        'discount': '20.19',
        'subtotal': '357.18',
        'estimated_shipping': '10.00',
        'estimated_tax': '33.05',
        'estimated_total': '400.23',
        'created_at': '2020-05-03T12:20:23.000',
        'updated_at': '2020-05-04T15:52:01.000'
    }


def build_previous_object_data():
    return {
        'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        'user_id': 987650,
        'items': [
            {
                'id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
                'price': '19.99',
                'quantity': 5
            },
            {
                'id': '3c7f8798-1d3d-47de-82dd-c6c5e0de74ee',
                'price': '102.50',
                'quantity': 1
            },
            {
                'id': 'bba76edc-8afc-4fde-b4c4-ea58a230c5d6',
                'price': '24.99',
                'quantity': 3
            }
        ],
        'currency': 'USD',
        'value': '277.42',
        'discount': '10.09',
        'subtotal': '267.33',
        'estimated_shipping': '10.00',
        'estimated_tax': '24.96',
        'estimated_total': '302.29',
        'created_at': '2020-05-03T12:20:23.000',
        'updated_at': '2020-05-04T13:47:08.000'
    }


def build_event_object_data(**kwargs):
    data = {
        'type': 'shopping_cart',
        'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        'version': 2,
        'current': build_current_object_data(),
        'previous': build_previous_object_data()
    }
    data.update(kwargs)
    return data


def test_deserialize_full_event_object_data():
    data = build_event_object_data()

    event_object = EventObjectSchema().load(data)

    assert event_object.type == data['type']
    assert event_object.id == data['id']
    assert event_object.version == data['version']
    assert event_object.current == data['current']
    assert event_object.previous == data['previous']


def test_deserialize_event_data_ignoring_unknown_attributes():
    data = build_event_object_data()
    data['foo'] = 'bar'
    data['one'] = 1

    payload = EventObjectSchema().load(data)

    assert payload is not None
    assert isinstance(payload, EventObject)


@parameterized.expand([
    ['type', None],
    ['id', None],
    ['version', 1],
    ['previous', None]
])
def test_deserialize_event_data_without_optional_attribute(attribute_key, expected_value):
    data = build_event_object_data()
    del data[attribute_key]
    event = EventObjectSchema().load(data)
    assert getattr(event, attribute_key) == expected_value


@parameterized.expand([
    ['type', None],
    ['id', None],
    ['version', 1],
    ['previous', None]
])
def test_deserialize_event_data_with_optional_attribute_set_to_null(attribute_key, expected_value):
    data = build_event_object_data()
    data[attribute_key] = None
    event = EventObjectSchema().load(data)
    assert getattr(event, attribute_key) == expected_value


def test_fail_to_deserialize_event_data_without_current_attribute():
    data = build_event_object_data()
    del data['current']

    with pytest.raises(MegaSchemaError) as e:
        EventObjectSchema().load(data)

    assert str(e.value) == (
        "Invalid MEGA payload. There is an error in the 'object' section: "
        "{'current': ['Missing data for required field.']}"
    )


def test_fail_to_deserialize_event_data_without_current_attribute_set_to_null():
    data = build_event_object_data(current=None)

    with pytest.raises(MegaSchemaError) as e:
        EventObjectSchema().load(data)

    assert str(e.value) == (
        "Invalid MEGA payload. There is an error in the 'object' section: "
        "{'current': ['Field may not be null.']}"
    )
