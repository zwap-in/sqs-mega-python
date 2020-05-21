import json
from base64 import b64encode

import bson
import pytest
from parameterized import parameterized

from mega.aws.message import MessageType
from mega.aws.payload import PayloadType
from mega.aws.sns.message import SnsNotification, SnsMessageType
from mega.aws.sqs.schema import deserialize_sqs_message, SqsSchemaError
from mega.event import deserialize_mega_payload
from tests.mega.aws.sns.schema_test import build_sns_notification_data


def build_sqs_message_data(**kwargs):
    data = {
        'MessageId': 'c58a3575-e78c-40e1-9bde-682162525ea0',
        'Body': 'Hello World!',
        'MD5OfBody': '61f4b24d8c081c9377b29a1b72ef214c',
        'ReceiptHandle': 'AQEBRsNTfXJDUq3LfP5jXhL9AORVveYfe0m28XiZS8x6BwGDugpWoabAe8x7AZsncLlI0Lu6lK+oZkNxEjcb2ggV1yzYS'
                         'BiPUF+sDeKcoMLPC9rBm3kldrFciYZeYjm4cx2ysmDGyEkOYSBvRp1FHlDUMJEionQlDlyUp4uEnTtqkdhnFzXSlQdkNr'
                         'c1FeLQeKc+TcQaXrQbVUQeBk8ZyX4Hrp7uolDcekfS3ZjHp1hwabn/UN+g+KN+lqa5H4+Cu6JOzRwaukPhSZeFu3b9lYb'
                         'rEJzP0iijcqCnz2CPjxdZwwJY005Y4HtV34dkahFPrTBhlhunNjA8YW8iGk3ptiQRfm+hORT1k41kplewD4Crsthgff+5'
                         'sXWfcLKGVWcO5uvBK/qUI81S7qSzYU0CigcB4Pl93H2G0rPfZqXmAJQS2Os='
    }
    data.update(**kwargs)
    return data


def build_generic_json_data():
    return {
        'foo': 'bar',
        'one': 1,
        'embedded': {
            'a': 'b',
            'list': ['one', 2.0, 'three', 49, {}]
        },
        '1': ['a']
    }


def build_mega_payload_data(**kwargs):
    data = {
        'protocol': 'mega',
        'version': 1,
        'event': {
            'name': 'shopping_cart.item.added',
            'version': 1,
            'timestamp': '2020-05-04T15:53:23.123',
            'domain': 'shopping_cart',
            'subject': '987650',
            'publisher': 'shopping-cart-service',
            'attributes': {
                'item_id': '61fcc874-624e-40f8-8fd7-0e663c7837e8',
                'quantity': 5
            }
        },
        'object': {
            'type': 'shopping_cart',
            'id': '18a3f92e-1fbf-45eb-8769-d836d0a1be55',
            'version': 2,
            'current': {
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
            'previous': {
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
        },
        'extra': {
            'channel': 'web/desktop',
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) '
                          'Version/13.1 Safari/605.1.15',
            'user_ip_address': '177.182.205.103'
        }
    }
    data.update(kwargs)
    return data


def assert_sqs_message_matches_data_attributes(message, data):
    assert message.message_id == data['MessageId']
    assert message.message_type == MessageType.SQS
    assert message.receipt_handle == data['ReceiptHandle']


def assert_sqs_message_has_embedded_sns_notification(sqs_message):
    assert sqs_message.embedded_message is not None
    assert sqs_message.embedded_message.message_type == MessageType.SNS
    assert isinstance(sqs_message.embedded_message, SnsNotification)
    assert sqs_message.embedded_message.sns_message_type == SnsMessageType.NOTIFICATION
    assert sqs_message.embedded_message.embedded_message is None


def test_deserialize_sqs_message_with_plaintext_payload():
    plaintext = '{"the quick brown fox jumps overt the lazy dog!"}'

    sqs_data = build_sqs_message_data(Body=plaintext)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.PLAINTEXT
    assert sqs_message.payload == plaintext
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_binary_payload():
    blob = b"\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3"

    sqs_data = build_sqs_message_data(Body=b64encode(blob).decode())
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.BINARY
    assert sqs_message.payload == blob
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_data_payload_encoded_as_plaintext_json():
    payload_data = build_generic_json_data()
    plaintext = json.dumps(payload_data)

    sqs_data = build_sqs_message_data(Body=plaintext)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.DATA
    assert sqs_message.payload == payload_data
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_mega_payload_encoded_as_plaintext_json():
    mega_payload = build_mega_payload_data()
    plaintext = json.dumps(mega_payload)

    sqs_data = build_sqs_message_data(Body=plaintext)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.MEGA
    assert sqs_message.payload == deserialize_mega_payload(mega_payload)
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_data_payload_encoded_as_binary_bson():
    payload_data = build_generic_json_data()
    blob = bson.dumps(payload_data)

    sqs_data = build_sqs_message_data(Body=b64encode(blob).decode())
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.DATA
    assert sqs_message.payload == payload_data
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_mega_payload_encoded_as_binary_bson():
    mega_payload = build_mega_payload_data()
    blob = bson.dumps(mega_payload)

    sqs_data = build_sqs_message_data(Body=b64encode(blob).decode())
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.MEGA
    assert sqs_message.payload == deserialize_mega_payload(mega_payload)
    assert sqs_message.embedded_message is None


def test_deserialize_sqs_message_with_embedded_sns_message_having_plaintext_payload():
    plaintext = 'foo-bar #132! {}'
    sns_data = build_sns_notification_data(Message=plaintext)
    sns_json = json.dumps(sns_data)

    sqs_data = build_sqs_message_data(Body=sns_json)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.PLAINTEXT
    assert sqs_message.payload == plaintext

    assert_sqs_message_has_embedded_sns_notification(sqs_message)
    assert sqs_message.embedded_message.payload_type == PayloadType.PLAINTEXT
    assert sqs_message.embedded_message.payload == plaintext


def test_deserialize_sqs_message_with_embedded_sns_message_having_data_payload_encoded_as_plaintext_json():
    payload_data = build_generic_json_data()
    sns_data = build_sns_notification_data(Message=json.dumps(payload_data))
    sns_json = json.dumps(sns_data)

    sqs_data = build_sqs_message_data(Body=sns_json)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.DATA
    assert sqs_message.payload == payload_data

    assert_sqs_message_has_embedded_sns_notification(sqs_message)
    assert sqs_message.embedded_message.payload_type == PayloadType.DATA
    assert sqs_message.embedded_message.payload == payload_data


def test_deserialize_sqs_message_with_embedded_sns_message_having_mega_payload_encoded_as_plaintext_json():
    mega_payload_data = build_mega_payload_data()
    sns_data = build_sns_notification_data(Message=json.dumps(mega_payload_data))
    sns_json = json.dumps(sns_data)

    sqs_data = build_sqs_message_data(Body=sns_json)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    expected_mega_payload = deserialize_mega_payload(mega_payload_data)

    assert sqs_message.payload_type == PayloadType.MEGA
    assert sqs_message.payload == expected_mega_payload

    assert_sqs_message_has_embedded_sns_notification(sqs_message)
    assert sqs_message.embedded_message.payload_type == PayloadType.MEGA
    assert sqs_message.embedded_message.payload == expected_mega_payload


def test_deserialize_sqs_message_with_embedded_sns_message_having_data_payload_encoded_as_binary_bson():
    payload_data = build_generic_json_data()
    payload_blob = bson.dumps(payload_data)

    sns_data = build_sns_notification_data(Message=b64encode(payload_blob).decode())
    sns_json = json.dumps(sns_data)

    sqs_data = build_sqs_message_data(Body=sns_json)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    assert sqs_message.payload_type == PayloadType.DATA
    assert sqs_message.payload == payload_data

    assert_sqs_message_has_embedded_sns_notification(sqs_message)
    assert sqs_message.embedded_message.payload_type == PayloadType.DATA
    assert sqs_message.embedded_message.payload == payload_data


def test_deserialize_sqs_message_with_embedded_sns_message_having_mega_payload_encoded_as_binary_bson():
    mega_payload_data = build_mega_payload_data()
    mega_payload_blob = bson.dumps(mega_payload_data)

    sns_data = build_sns_notification_data(Message=b64encode(mega_payload_blob).decode())
    sns_json = json.dumps(sns_data)

    sqs_data = build_sqs_message_data(Body=sns_json)
    sqs_message = deserialize_sqs_message(sqs_data)

    assert_sqs_message_matches_data_attributes(sqs_message, sqs_data)

    expected_mega_payload = deserialize_mega_payload(mega_payload_data)

    assert sqs_message.payload_type == PayloadType.MEGA
    assert sqs_message.payload == expected_mega_payload

    assert_sqs_message_has_embedded_sns_notification(sqs_message)
    assert sqs_message.embedded_message.payload_type == PayloadType.MEGA
    assert sqs_message.embedded_message.payload == expected_mega_payload


@parameterized.expand([
    ['MessageId'],
    ['ReceiptHandle'],
    ['Body']
])
def test_fail_to_deserialize_sns_message_without_required_attribute(attribute_name):
    data = build_sqs_message_data()
    del data[attribute_name]

    with pytest.raises(SqsSchemaError) as error:
        deserialize_sqs_message(data)

    assert str(error.value) == "Could not deserialize SQS message: " \
                               "{{'{0}': ['Missing data for required field.']}}".format(attribute_name)


@parameterized.expand([
    ['MessageId'],
    ['ReceiptHandle'],
    ['Body']
])
def test_fail_to_deserialize_sns_message_without_required_attribute_set_to_null(attribute_name):
    data = build_sqs_message_data()
    data[attribute_name] = None

    with pytest.raises(SqsSchemaError) as error:
        deserialize_sqs_message(data)

    assert str(error.value) == "Could not deserialize SQS message: " \
                               "{{'{0}': ['Field may not be null.']}}".format(attribute_name)
