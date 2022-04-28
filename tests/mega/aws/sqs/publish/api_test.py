import json
import logging
from base64 import b64decode

import bson
import dateutil.parser
import pytest

import sqs_mega_python_zwap.event
from sqs_mega_python_zwap.aws.sqs.api import logger
from sqs_mega_python_zwap.aws.sqs.publish.api import SqsPublisher
from tests.mega.aws.sqs import get_sqs_request_data, get_queue_url_from_request, get_sqs_response_data
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
    return sqs_mega_python_zwap.event.Payload(
        event=sqs_mega_python_zwap.event.Event(
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
        object=sqs_mega_python_zwap.event.ObjectData(
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


def get_message_body_from_request(request_data):
    return request_data['MessageBody'][0]


def get_message_id_from_response(cassette):
    response = get_sqs_response_data(cassette)
    return response['SendMessageResponse']['SendMessageResult']['MessageId']


def test_publish_raw_plaintext_message(sqs):
    plaintext = 'hello world!'

    with vcr.use_cassette('publish_raw_plaintext_message') as cassette:
        message_id = sqs.publish_raw_message(plaintext)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url
    assert get_message_body_from_request(request_data) == plaintext
    assert get_message_id_from_response(cassette) == message_id


def test_publish_raw_json_message(sqs):
    data = build_generic_data()
    plaintext_json = json.dumps(data, sort_keys=True)

    with vcr.use_cassette('publish_raw_json_message') as cassette:
        message_id = sqs.publish_raw_message(plaintext_json)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url
    assert get_message_body_from_request(request_data) == plaintext_json
    assert get_message_id_from_response(cassette) == message_id


def test_publish_plaintext_payload(sqs):
    plaintext = 'hello world!'
    with vcr.use_cassette('publish_plaintext_payload') as cassette:
        message_id = sqs.publish(plaintext)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url
    assert get_message_body_from_request(request_data) == plaintext
    assert get_message_id_from_response(cassette) == message_id


def test_publish_blob_payload(sqs):
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

    with vcr.use_cassette('publish_blob_payload') as cassette:
        message_id = sqs.publish(blob)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url
    assert b64decode(get_message_body_from_request(request_data)) == blob
    assert get_message_id_from_response(cassette) == message_id


def test_publish_data_payload_as_plaintext_json(sqs):
    data = build_generic_data()

    with vcr.use_cassette('publish_data_payload_as_plaintext_json') as cassette:
        message_id = sqs.publish(data)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url
    assert json.loads(get_message_body_from_request(request_data)) == data
    assert get_message_id_from_response(cassette) == message_id


def test_publish_mega_payload_as_plaintext_json(sqs):
    mega_payload = build_mega_payload()

    with vcr.use_cassette('publish_mega_payload_as_plaintext_json') as cassette:
        message_id = sqs.publish(mega_payload)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url

    message_body = get_message_body_from_request(request_data)
    assert sqs_mega_python_zwap.event.deserialize_payload(json.loads(message_body)) == mega_payload
    assert get_message_id_from_response(cassette) == message_id


def test_publish_data_payload_as_binary_bson(sqs):
    data = build_generic_data()

    with vcr.use_cassette('publish_data_payload_as_binary_bson') as cassette:
        message_id = sqs.publish(data, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url

    message_body = get_message_body_from_request(request_data)
    blob = b64decode(message_body)
    assert bson.loads(blob) == data
    assert get_message_id_from_response(cassette) == message_id


def test_publish_mega_payload_as_binary_bson(sqs):
    mega_payload = build_mega_payload()

    with vcr.use_cassette('publish_mega_payload_as_binary_bson') as cassette:
        message_id = sqs.publish(mega_payload, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == sqs.queue_url

    message_body = get_message_body_from_request(request_data)
    blob = b64decode(message_body)
    assert sqs_mega_python_zwap.event.deserialize_payload(bson.loads(blob)) == mega_payload
    assert get_message_id_from_response(cassette) == message_id


def test_publish_overriding_default_topic_arn(sqs):
    another_queue_url = 'https://sqs.us-east-2.amazonaws.com/424566909325/another-queue'
    data = build_generic_data()

    with vcr.use_cassette('publish_overriding_default_queue_url') as cassette:
        message_id = sqs.publish(data, queue_url=another_queue_url)

    assert cassette.all_played
    request_data = get_sqs_request_data(cassette)
    assert get_queue_url_from_request(request_data) == another_queue_url
    assert get_message_id_from_response(cassette) == message_id


def test_fail_if_no_queue_url_is_provided():
    sqs = SqsPublisher()

    with pytest.raises(ValueError) as e:
        sqs.publish('hello world!')

    assert str(e.value) == 'Missing Queue URL'


def test_log_sent_messages(sqs, caplog):
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        with vcr.use_cassette('publish_plaintext_payload'):
            message_id = sqs.publish('hello world!')

    records = caplog.records
    assert len(records) == 2
    assert records[0].levelno == logging.INFO
    assert records[0].message == '[{}][{}] Sent SQS message'.format(sqs.queue_url, message_id)
    assert records[1].levelno == logging.DEBUG
    assert records[1].message == '[{}][{}] hello world!'.format(sqs.queue_url, message_id)
