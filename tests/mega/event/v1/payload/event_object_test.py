from parameterized import parameterized

from mega.event.v1.payload import EventObject


def build_event_object_kwargs():
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


def test_create_event_object():
    kwargs = build_event_object_kwargs()
    event_object = EventObject(**kwargs)

    assert event_object.id == kwargs['id']
    assert event_object.type == kwargs['type']
    assert event_object.version == kwargs['version']
    assert event_object.current == kwargs['current']
    assert event_object.previous == kwargs['previous']


@parameterized.expand([
    ['type', None],
    ['id', None],
    ['version', 1],
    ['previous', None]
])
def test_create_event_object_without_optional_attribute_should_use_default(attribute_name, expected_default):
    kwargs = build_event_object_kwargs()
    del kwargs[attribute_name]

    event_object = EventObject(**kwargs)
    assert getattr(event_object, attribute_name) == expected_default
