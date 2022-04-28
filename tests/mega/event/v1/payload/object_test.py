import pytest
from parameterized import parameterized

from sqs_mega_python_zwap.event.v1.payload import ObjectData


def build_object_kwargs():
    return dict(
        type='shopping_cart',
        id='18a3f92e-1fbf-45eb-8769-d836d0a1be55',
        version=3,
        current={
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
        },
        previous={
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
    )


def test_create_object():
    kwargs = build_object_kwargs()
    object_data = ObjectData(**kwargs)

    assert object_data.id == kwargs['id']
    assert object_data.type == kwargs['type']
    assert object_data.version == kwargs['version']
    assert object_data.current == kwargs['current']
    assert object_data.previous == kwargs['previous']


@parameterized.expand([
    ['type', None],
    ['id', None],
    ['version', 1],
    ['previous', None]
])
def test_create_object_without_optional_attribute_should_use_default(attribute_name, expected_default):
    kwargs = build_object_kwargs()
    del kwargs[attribute_name]

    object_data = ObjectData(**kwargs)
    assert getattr(object_data, attribute_name) == expected_default


@parameterized.expand([
    [None],
    [{}]
])
def test_fail_to_create_object_with_empty_current_attribute(bogus):
    with pytest.raises(AttributeError) as e:
        ObjectData(current=bogus)
    assert str(e.value) == 'Mega object attribute "current" has not been set, or set to an empty value'


def test_objects_are_equal_when_all_attributes_are_equal():
    kwargs = build_object_kwargs()

    this = ObjectData(**kwargs)
    that = ObjectData(**kwargs)

    assert this.__eq__(that) is True
    assert that.__eq__(this) is True
    assert this == that
    assert not (this != that)


@parameterized.expand([
    ['current', {'some': 'thing', 'else': True}],
    ['type', 'another_type'],
    ['id', 'some_other_id'],
    ['version', 666],
    ['previous', {'foo': 'bar'}],
])
def test_objects_are_not_equal_if_one_attribute_is_different(attribute_name, different_value):
    this_kwargs = build_object_kwargs()
    this = ObjectData(**this_kwargs)

    that_kwargs = build_object_kwargs()
    that_kwargs[attribute_name] = different_value
    that = ObjectData(**that_kwargs)

    assert this.__eq__(that) is False
    assert that.__eq__(this) is False
    assert this != that
    assert not (this == that)


@parameterized.expand([
    [None],
    [1],
    [object()],
    ['foobar']
])
def test_an_object_is_not_equal_to(another_thing):
    object_data = ObjectData(**build_object_kwargs())
    assert object_data != another_thing
