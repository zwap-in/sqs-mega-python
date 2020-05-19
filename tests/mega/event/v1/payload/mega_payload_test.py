from parameterized import parameterized

from mega.event.v1.payload import MegaPayload, Event, EventObject
from tests.mega.event.v1.payload.event_object_test import build_event_object_kwargs
from tests.mega.event.v1.payload.event_test import build_event_kwargs


def build_mega_payload_kwargs():
    return dict(
        event=Event(**build_event_kwargs()),
        object=EventObject(**build_event_object_kwargs()),
        extra={
            'channel': 'web/desktop',
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) '
                          'AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15',
            'user_ip_address': '177.182.205.103'
        }
    )


def test_create_event():
    kwargs = build_mega_payload_kwargs()
    payload = MegaPayload(**kwargs)

    assert payload.event is not None
    assert isinstance(payload.event, Event)
    assert payload.event == kwargs['event']

    assert payload.object is not None
    assert isinstance(payload.object, EventObject)
    assert payload.object == kwargs['object']

    assert payload.extra is not None
    assert payload.extra == kwargs['extra']


@parameterized.expand([
    ['object', None],
    ['extra', {}]
])
def test_create_mega_payload_without_optional_attribute_should_use_default(attribute_name, expected_default):
    kwargs = build_mega_payload_kwargs()
    del kwargs[attribute_name]

    payload = MegaPayload(**kwargs)
    assert getattr(payload, attribute_name) == expected_default


def test_create_mega_payload_with_additional_kwargs_should_be_added_to_the_extra_dictionary():
    payload = MegaPayload(event=Event(**build_event_kwargs()), foo='bar', test=123)

    assert payload.extra == {
        'foo': 'bar',
        'test': 123,
    }


def test_mega_payloads_are_equal_when_all_attributes_are_equal():
    event = Event(**build_event_kwargs())
    event_object = EventObject(**build_event_object_kwargs())
    extra = {'foo': 'bar', 'one': 1}

    this = MegaPayload(
        event=event,
        object=event_object,
        extra=extra
    )

    that = MegaPayload(
        event=event,
        object=event_object,
        extra=extra
    )

    assert this.__eq__(that) is True
    assert that.__eq__(this) is True
    assert this == that
    assert not (this != that)


@parameterized.expand([
    ['event', Event(name='foo.bar')],
    ['object', EventObject(current={'foo': 'bar'})],
    ['extra', {'one': 1, 'two': [3, 4]}]
])
def test_mega_payloads_are_not_equal_if_one_attribute_is_different(attribute_name, different_value):
    this_kwargs = build_mega_payload_kwargs()
    this = MegaPayload(**this_kwargs)

    that_kwargs = build_mega_payload_kwargs()
    that_kwargs[attribute_name] = different_value
    that = MegaPayload(**that_kwargs)

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
def test_a_mega_payload_is_not_equal_to(another_thing):
    mega_payload = MegaPayload(**build_mega_payload_kwargs())
    assert mega_payload != another_thing
