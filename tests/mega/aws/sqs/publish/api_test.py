import json
import logging
import re
from base64 import b64decode
from urllib.parse import parse_qs

import bson
import dateutil.parser
import pytest

from mega.aws.sqs import LOGGER_NAME
from mega.aws.sqs.publish.api import SqsPublisher
from mega.event import MegaPayload, Event, EventObject, deserialize_mega_payload
from tests.vcr import build_vcr

vcr = build_vcr(
    path='aws/sqs/publish/api',
    match_on=['method', 'scheme', 'host', 'port', 'path', 'query']
)


@pytest.fixture
def sqs():
    return SqsPublisher(
        queue_url='https://sqs.us-east-2.amazonaws.com/424566909325/sqs-mega-test'
    )


def build_generic_data():
    return {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'true': True,
            'list': [
                'one',
                2.01,
                'three',
                None,
                49,
                {'a': 'ha!'}
            ]
        }
    }


def build_mega_payload():
    return MegaPayload(
        event=Event(
            name='user.updated',
            timestamp=dateutil.parser.parse('2020-05-04T15:53:27.823'),
            domain='user',
            subject='987650',
            publisher='user-service',
            attributes={
                'email': 'johndoe_84@example.com',
                'username': 'john.doe'
            }
        ),
        object=EventObject(
            current={
                'id': 987650,
                'full_name': 'John Doe',
                'username': 'john.doe',
                'email': 'johndoe_86@example.com',
                'ssn': '497279436',
                'birthdate': '1986-02-15',
                'created_at': '2020-05-03T12:20:23.000',
                'updated_at': '2020-05-04T15:52:01.000'
            }
        )
    )


def get_sqs_request_data(cassette):
    request_body = cassette.requests[0].body.decode()
    return parse_qs(request_body)


def get_queue_url(request_data):
    return request_data['QueueUrl'][0]


def get_message_body(request_data):
    return request_data['MessageBody'][0]


def get_sqs_response_body(cassette) -> str:
    return cassette.responses[0]['body']['string'].decode()


def get_message_id(response_body: str):
    match = re.search(r'<MessageId>(.+)</MessageId>', response_body)
    if not match:
        return None
    return match.group(1)


def test_send_raw_plaintext_message(sqs):
    plaintext = 'hello world!'

    with vcr.use_cassette('send_raw_plaintext_message') as cassette:
        sqs.send_raw_message(plaintext)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url
    assert get_message_body(request_data) == plaintext


def test_send_raw_json_message(sqs):
    data = {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'true': True,
            'list': [
                'one',
                2.01,
                'three',
                None,
                49,
                {'a': 'ha!'}
            ]
        }
    }
    plaintext_json = json.dumps(data, sort_keys=True)

    with vcr.use_cassette('send_raw_json_message') as cassette:
        sqs.send_raw_message(plaintext_json)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url
    assert get_message_body(request_data) == plaintext_json


def test_send_plaintext_payload(sqs):
    plaintext = 'hello world!'
    with vcr.use_cassette('send_plaintext_payload') as cassette:
        sqs.send_payload(plaintext)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url
    assert get_message_body(request_data) == plaintext


def test_send_blob_payload(sqs):
    blob = (
        b'\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{'
        b'wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94'
        b'\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1['
        b'2\xd0\x9f|V\xb0\xc3\xb0\xc3\xb0\xc3\xb0\xc3 '
    )

    with vcr.use_cassette('send_blob_payload') as cassette:
        sqs.send_payload(blob)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url
    assert b64decode(get_message_body(request_data)) == blob


def test_send_data_payload_as_plaintext_json(sqs):
    data = build_generic_data()

    with vcr.use_cassette('send_data_payload_as_plaintext_json') as cassette:
        sqs.send_payload(data)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url
    assert json.loads(get_message_body(request_data)) == data


def test_send_mega_payload_as_plaintext_json(sqs):
    mega = build_mega_payload()

    with vcr.use_cassette('send_mega_payload_as_plaintext_json') as cassette:
        sqs.send_payload(mega)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url

    message_body = get_message_body(request_data)
    assert deserialize_mega_payload(json.loads(message_body)) == mega


def test_send_data_payload_as_binary_bson(sqs):
    data = build_generic_data()

    with vcr.use_cassette('send_data_payload_as_binary_bson') as cassette:
        sqs.send_payload(data, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url

    message_body = get_message_body(request_data)
    blob = b64decode(message_body)
    assert bson.loads(blob) == data


def test_send_mega_payload_as_binary_bson(sqs):
    mega = build_mega_payload()

    with vcr.use_cassette('send_mega_payload_as_binary_bson') as cassette:
        sqs.send_payload(mega, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == sqs.queue_url

    message_body = get_message_body(request_data)
    blob = b64decode(message_body)
    assert deserialize_mega_payload(bson.loads(blob)) == mega


def test_publish_overriding_default_topic_arn(sqs):
    another_queue_url = 'https://sqs.us-east-2.amazonaws.com/424566909325/another-queue'
    data = build_generic_data()

    with vcr.use_cassette('send_overriding_default_queue_url') as cassette:
        sqs.send_payload(data, queue_url=another_queue_url)

    assert cassette.all_played
    request_data = get_sqs_request_data(cassette)
    assert get_queue_url(request_data) == another_queue_url


def test_fail_if_no_topic_arn_is_provided():
    sqs = SqsPublisher()

    with pytest.raises(ValueError) as e:
        sqs.send_payload('hello world!')

    assert str(e.value) == 'Missing Queue URL'


def test_log_sent_messages(sqs, caplog):
    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        with vcr.use_cassette('send_plaintext_payload') as cassette:
            sqs.send_payload('hello world!')

    response_body = get_sqs_response_body(cassette)
    message_id = get_message_id(response_body)

    records = caplog.records
    assert len(records) == 2
    assert records[0].levelno == logging.INFO
    assert records[0].message == '[{}][{}] Sent SQS message'.format(sqs.queue_url, message_id)
    assert records[1].levelno == logging.DEBUG
    assert records[1].message == '[{}][{}] hello world!'.format(sqs.queue_url, message_id)
