from datetime import datetime

import dateutil.parser
import freezegun
import pytest
from parameterized import parameterized

from mega.event.v1.payload import Event


def build_event_kwargs():
    return dict(
        name='shopping_cart.item.added',
        timestamp=datetime.utcnow(),
        version=2,
        domain='shopping_cart',
        subject='987650',
        publisher='shopping-cart-service',
        attributes={
            'quantity': 5,
            'item_id': '0794bac2-e860-4e0d-b9cc-42ab21e2a851'
        }
    )


def test_create_event():
    kwargs = build_event_kwargs()
    event = Event(**kwargs)
    assert event.name == kwargs['name']
    assert event.timestamp == kwargs['timestamp']
    assert event.version == kwargs['version']
    assert event.domain == kwargs['domain']
    assert event.subject == kwargs['subject']
    assert event.publisher == kwargs['publisher']
    assert event.attributes == kwargs['attributes']


def test_create_event_without_timestamp_should_use_current_utc_datetime():
    frozen_time = dateutil.parser.parse('2020-05-15T15:45:59.123')

    kwargs = build_event_kwargs()
    del kwargs['timestamp']

    with freezegun.freeze_time(frozen_time):
        event = Event(**kwargs)

    assert event.timestamp == frozen_time


@parameterized.expand([
    ['version', 1],
    ['domain', None],
    ['subject', None],
    ['publisher', None],
    ['attributes', {}]
])
def test_create_event_without_optional_attribute_should_use_default(attribute_name, expected_default):
    kwargs = build_event_kwargs()
    del kwargs[attribute_name]

    event = Event(**kwargs)
    assert getattr(event, attribute_name) == expected_default


def test_create_event_with_additional_kwargs_should_be_added_to_the_event_attributes():
    event = Event(name='test-event', foo='bar', test=123)

    assert event.name == 'test-event'
    assert event.attributes == {
        'foo': 'bar',
        'test': 123
    }


@parameterized.expand([
    [''],
    [None]
])
def test_fail_to_create_event_with_empty_name(bogus):
    with pytest.raises(AttributeError) as e:
        Event(name=bogus)
    assert str(e.value) == 'Mega event attribute "name" has not been set, or set to an empty value'


def test_events_are_equal_when_all_attributes_are_equal():
    kwargs = build_event_kwargs()

    this = Event(**kwargs)
    that = Event(**kwargs)

    assert this.__eq__(that) is True
    assert that.__eq__(this) is True
    assert this == that
    assert not (this != that)


@parameterized.expand([
    ['name', 'some_other_event_name'],
    ['timestamp', dateutil.parser.parse('2020-05-10T14:30:05.123Z')],
    ['version', 10],
    ['domain', 'a_different_domain'],
    ['subject', 'a_different_subject'],
    ['publisher', 'a_different_publisher'],
    ['attributes', {'some': 'thing', 'different': True}]
])
def test_events_are_not_equal_if_one_attribute_is_different(attribute_name, different_value):
    this_kwargs = build_event_kwargs()
    this = Event(**this_kwargs)

    that_kwargs = build_event_kwargs()
    that_kwargs[attribute_name] = different_value
    that = Event(**that_kwargs)

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
def test_an_event_is_not_equal_to(another_thing):
    event = Event(**build_event_kwargs())
    assert event != another_thing
