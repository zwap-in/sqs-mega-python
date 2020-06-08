from datetime import datetime

import dateutil.parser
from freezegun import freeze_time

from mega.event.v1.payload import MegaPayload, MegaEvent, MegaObject
from mega.event.v1.build import PayloadBuilder


def test_build_minimal_mega_payload():
    timestamp = datetime.utcnow()

    with freeze_time(timestamp):
        payload = PayloadBuilder().with_event(
            name='shopping_cart.item.added',
            subject='123456'
        ).build()

    assert isinstance(payload, MegaPayload)
    assert payload.event is not None
    assert isinstance(payload.event, MegaEvent)
    assert payload.event.name == 'shopping_cart.item.added'
    assert payload.event.timestamp == timestamp
    assert payload.event.version == MegaEvent.DEFAULT_VERSION
    assert payload.event.domain is None
    assert payload.event.subject == '123456'
    assert payload.event.publisher is None
    assert payload.event.attributes == {}

    assert payload.object is None
    assert payload.extra == {}


def test_build_medium_mega_payload():
    timestamp = datetime.utcnow()

    current_object = {
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

    with freeze_time(timestamp):
        payload = PayloadBuilder().with_event(
            domain='shopping_cart',
            name='item.added',
            version=2,
            subject='235078',
            item_id='b23c5670-e4d9-40cd-94e0-b7776e08b104',
            all=True
        ).with_object(
            type='shopping_cart',
            current=current_object
        ).with_extra(
            ip_address='172.217.162.174'
        ).build()

    assert isinstance(payload, MegaPayload)

    assert payload.event is not None
    assert isinstance(payload.event, MegaEvent)
    assert payload.event.domain == 'shopping_cart'
    assert payload.event.name == 'item.added'
    assert payload.event.timestamp == timestamp
    assert payload.event.version == 2
    assert payload.event.subject == '235078'
    assert payload.event.attributes == dict(
        item_id='b23c5670-e4d9-40cd-94e0-b7776e08b104',
        all=True
    )
    assert payload.event.publisher is None

    assert payload.object is not None
    assert isinstance(payload.object, MegaObject)
    assert payload.object.type == 'shopping_cart'
    assert payload.object.current == current_object
    assert payload.object.id is None
    assert payload.object.previous is None
    assert payload.object.version is MegaObject.DEFAULT_VERSION

    assert payload.extra == dict(
        ip_address='172.217.162.174'
    )


def test_build_full_mega_payload():
    timestamp = dateutil.parser.parse('2020-05-04T15:53:23.123')

    current_object = {
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

    previous_object = {
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

    extra = {
        'channel': 'web/desktop',
        'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                      'Version/13.1 Safari/605.1.15',
        'user_ip_address': '177.182.205.103'
    }

    with freeze_time(timestamp):
        payload = PayloadBuilder().with_event(
            name='shopping_cart.item.added',
            version=2,
            timestamp=timestamp,
            domain='shopping_cart',
            subject='987650',
            publisher='shopping-cart-service',
            item_id='61fcc874-624e-40f8-8fd7-0e663c7837e8',
            attributes={
                'quantity': 5,
                'active': True
            }
        ).with_object(
            type='shopping_cart',
            id='18a3f92e-1fbf-45eb-8769-d836d0a1be55',
            version=3,
            current=current_object,
            previous=previous_object

        ).with_extra(
            **extra
        ).build()

    assert isinstance(payload, MegaPayload)

    assert payload.event is not None
    assert isinstance(payload.event, MegaEvent)
    assert payload.event.domain == 'shopping_cart'
    assert payload.event.name == 'shopping_cart.item.added'
    assert payload.event.timestamp == timestamp
    assert payload.event.version == 2
    assert payload.event.subject == '987650'
    assert payload.event.publisher == 'shopping-cart-service'
    assert payload.event.attributes == {
        'item_id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
        'quantity': 5,
        'active': True
    }

    assert payload.object is not None
    assert isinstance(payload.object, MegaObject)
    assert payload.object.type == 'shopping_cart'
    assert payload.object.id == '18a3f92e-1fbf-45eb-8769-d836d0a1be55'
    assert payload.object.version == 3
    assert payload.object.current == current_object
    assert payload.object.previous == previous_object

    assert payload.extra == extra
