import json
import logging
import re
from base64 import b64decode
from urllib.parse import parse_qs

import bson
import dateutil.parser
import pytest

from mega.aws.sns import LOGGER_NAME
from mega.aws.sns.publish.api import SnsPublishApi
from mega.event import deserialize_mega_payload, MegaPayload, Event, EventObject
from tests.vcr import build_vcr

vcr = build_vcr(
    path='aws/sns/publish/api',
    match_on=['method', 'scheme', 'host', 'port', 'path', 'query']
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


def get_sns_request_data(cassette) -> dict:
    request_body = cassette.requests[0].body.decode()
    return parse_qs(request_body)


def get_topic_arn(request_data: dict):
    return request_data['TopicArn'][0]


def get_message(request_data: dict):
    return request_data['Message'][0]


def get_sns_response_body(cassette) -> str:
    return cassette.responses[0]['body']['string'].decode()


def get_message_id(response_body: str):
    match = re.search(r'<MessageId>(.+)</MessageId>', response_body)
    if not match:
        return None
    return match.group(1)


@pytest.fixture
def sns():
    return SnsPublishApi(
        topic_arn='arn:aws:sns:us-east-2:424566909325:sqs-mega-test'
    )


def test_publish_raw_plaintext_message(sns):
    plaintext = 'hello world!'

    with vcr.use_cassette('publish_raw_plaintext_message') as cassette:
        sns.publish_raw_message(plaintext)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn
    assert get_message(request_data) == plaintext


def test_publish_raw_json_message(sns):
    data = build_generic_data()
    plaintext_json = json.dumps(data, sort_keys=True)

    with vcr.use_cassette('publish_raw_json_message') as cassette:
        sns.publish_raw_message(plaintext_json)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn
    assert json.loads(get_message(request_data)) == data


def test_publish_plaintext_payload(sns):
    plaintext = 'hello world!'
    with vcr.use_cassette('publish_plaintext_payload') as cassette:
        sns.publish_payload(plaintext)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn
    assert get_message(request_data) == plaintext


def test_publish_blob_payload(sns):
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
        sns.publish_payload(blob)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn
    assert b64decode(get_message(request_data)) == blob


def test_publish_data_payload_as_plaintext_json(sns):
    data = build_generic_data()

    with vcr.use_cassette('publish_data_payload_as_plaintext_json') as cassette:
        sns.publish_payload(data)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn
    assert json.loads(get_message(request_data)) == data


def test_publish_mega_payload_as_plaintext_json(sns):
    mega = build_mega_payload()

    with vcr.use_cassette('publish_mega_payload_as_plaintext_json') as cassette:
        sns.publish_payload(mega)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn

    message = get_message(request_data)
    assert deserialize_mega_payload(json.loads(message)) == mega


def test_publish_data_payload_as_binary_bson(sns):
    data = build_generic_data()

    with vcr.use_cassette('publish_data_payload_as_binary_bson') as cassette:
        sns.publish_payload(data, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn

    message = get_message(request_data)
    blob = b64decode(message)
    assert bson.loads(blob) == data


def test_publish_mega_payload_as_binary_bson(sns):
    mega = build_mega_payload()

    with vcr.use_cassette('publish_mega_payload_as_binary_bson') as cassette:
        sns.publish_payload(mega, binary_encoding=True)

    assert cassette.all_played

    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == sns.topic_arn

    message = get_message(request_data)
    blob = b64decode(message)
    assert deserialize_mega_payload(bson.loads(blob)) == mega


def test_publish_overriding_default_topic_arn(sns):
    another_topic_arn = 'arn:aws:sns:us-east-2:424566909325:another-test-topic'
    data = build_generic_data()

    with vcr.use_cassette('publish_overriding_default_topic_arn') as cassette:
        sns.publish_payload(data, topic_arn=another_topic_arn)

    assert cassette.all_played
    request_data = get_sns_request_data(cassette)
    assert get_topic_arn(request_data) == another_topic_arn


def test_fail_if_no_topic_arn_is_provided():
    sns = SnsPublishApi()

    with pytest.raises(ValueError) as e:
        sns.publish_payload('hello world!')

    assert str(e.value) == 'Missing Topic ARN'


def test_log_published_messages(sns, caplog):
    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        with vcr.use_cassette('publish_plaintext_payload') as cassette:
            sns.publish_payload('hello world!')

    response_body = get_sns_response_body(cassette)
    message_id = get_message_id(response_body)

    records = caplog.records
    assert len(records) == 2
    assert records[0].levelno == logging.INFO
    assert records[0].message == '[{}][{}] Published SNS message'.format(sns.topic_arn, message_id)
    assert records[1].levelno == logging.DEBUG
    assert records[1].message == '[{}][{}] hello world!'.format(sns.topic_arn, message_id)
