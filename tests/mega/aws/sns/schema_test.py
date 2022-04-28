import json
from base64 import b64encode

import bson
import dateutil.parser
import pytest
from parameterized import parameterized

import sqs_mega_python_zwap.event
from sqs_mega_python_zwap.aws.message import MessageType
from sqs_mega_python_zwap.aws.payload import PayloadType
from sqs_mega_python_zwap.aws.sns.message import SnsNotification, SnsMessageType, SnsSubscriptionConfirmation, \
    SnsUnsubscribeConfirmation
from sqs_mega_python_zwap.aws.sns.schema import deserialize_sns_message, SnsSchemaError, matches_sns_message


def build_sns_notification_data(**kwargs):
    data = {
        'Type': 'Notification',
        'MessageId': '22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324',
        'TopicArn': 'arn:aws:sns:us-west-2:123456789012:MyTopic',
        'Subject': 'My First Message',
        'Message': 'Hello world!',
        'Timestamp': '2012-05-02T00:54:06.655Z',
        'SignatureVersion': '1',
        'Signature': 'EXAMPLEw6JRN...',
        'SigningCertURL':
            'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem',
        'UnsubscribeURL':
            'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:123456789012'
            ':MyTopic:c9135db0-26c4-47ec-8998-413945fb5a96'
    }
    data.update(**kwargs)
    return data


def build_sns_subscription_confirmation_data(**kwargs):
    data = {
        'Type': 'SubscriptionConfirmation',
        'MessageId': '165545c9-2a5c-472c-8df2-7ff2be2b3b1b',
        'Token': '2336412f37...',
        'TopicArn': 'arn:aws:sns:us-west-2:123456789012:MyTopic',
        'Message': 'You have chosen to subscribe to the topic arn:aws:sns:us-west-2:123456789012:MyTopic.\n'
                   'To confirm the subscription, visit the SubscribeURL included in this message.',
        'SubscribeURL': 'https://sns.us-west-2.amazonaws.com/?Action='
                        'ConfirmSubscription&TopicArn=arn:aws:sns:us-west-2:123456789012:MyTopic&Token=2336412f37...',
        'Timestamp': '2012-04-26T20:45:04.751Z',
        'SignatureVersion': '1',
        'Signature': 'EXAMPLEpH+DcEwjAPg8O9mY8dReBSwksfg2S7WKQcikcNKWLQjwu6A4VbeS0QHVCkhRS7fUQvi2egU3N858fiTDN6bkkOxY'
                     'DVrY0Ad8L10Hs3zH81mtnPk5uvvolIC1CXGu43obcgFxeL3khZl8IKvO61GWB6jI9b5+gLPoBc1Q=',
        'SigningCertURL': 'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-'
                          'f3ecfb7224c7233fe7bb5f59f96de52f.pem'
    }
    data.update(**kwargs)
    return data


def build_sns_unsubscribe_confirmation_data(**kwargs):
    data = {
        'Type': 'UnsubscribeConfirmation',
        'MessageId': '47138184-6831-46b8-8f7c-afc488602d7d',
        'Token': '2336412f37...',
        'TopicArn': 'arn:aws:sns:us-west-2:123456789012:MyTopic',
        'Message': 'You have chosen to deactivate subscription '
                   'arn:aws:sns:us-west-2:123456789012:MyTopic:2bcfbf39-05c3-41de-beaa-fcfcc21c8f55.\n'
                   'To cancel this operation and restore the subscription, visit the SubscribeURL '
                   'included in this message.',
        'SubscribeURL': 'https://sns.us-west-2.amazonaws.com/?Action=ConfirmSubscription&'
                        'TopicArn=arn:aws:sns:us-west-2:123456789012:MyTopic&Token=2336412f37fb6...',
        'Timestamp': '2012-04-26T20:06:41.581Z',
        'SignatureVersion': '1',
        'Signature': 'EXAMPLEHXgJm...',
        'SigningCertURL': 'https://sns.us-west-2.amazonaws.com/'
                          'SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem'
    }
    data.update(**kwargs)
    return data


def build_generic_json_data():
    return {
        'foo': 'bar',
        'one': 123,
        'two': {'aha': True},
        'three': [1, 2, 3, 'four']
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


def assert_is_sns_notification(message):
    assert message is not None
    assert isinstance(message, SnsNotification)
    assert message.message_type == MessageType.SNS
    assert message.sns_message_type == SnsMessageType.NOTIFICATION


def assert_sns_message_matches_data_attributes(message, data):
    assert message.message_id == data['MessageId']
    assert message.topic_arn == data['TopicArn']
    assert message.timestamp == dateutil.parser.parse(data['Timestamp'])


def assert_sns_notification_matches_data_attributes(message, data):
    assert_sns_message_matches_data_attributes(message, data)
    assert message.subject == data.get('Subject')
    assert message.unsubscribe_url == data['UnsubscribeURL']


def assert_has_mega_payload(message):
    assert message.payload_type == PayloadType.MEGA
    assert message.payload is not None
    assert isinstance(message.payload, sqs_mega_python_zwap.event.Payload)


def assert_mega_payload_matches_data(mega_payload, data):
    assert mega_payload.event.name == data['event']['name']
    assert mega_payload.event.timestamp == dateutil.parser.parse(data['event']['timestamp'])
    assert mega_payload.event.version == data['event']['version']
    assert mega_payload.event.domain == data['event']['domain']
    assert mega_payload.event.subject == data['event']['subject']
    assert mega_payload.event.publisher == data['event']['publisher']
    assert mega_payload.event.attributes == data['event']['attributes']

    assert mega_payload.object.type == data['object']['type']
    assert mega_payload.object.id == data['object']['id']
    assert mega_payload.object.version == data['object']['version']
    assert mega_payload.object.current == data['object']['current']
    assert mega_payload.object.previous == data['object']['previous']

    assert mega_payload.extra == data['extra']


def test_deserialize_sns_notification_with_plaintext_payload():
    plaintext = 'Hello world!'
    data = build_sns_notification_data(Message=plaintext)

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)

    assert message.payload == plaintext
    assert message.payload_type == PayloadType.PLAINTEXT


def test_deserialize_sns_notification_with_base64_encoded_binary_payload():
    blob = (
        b"\x01\x02\x03\x00xQ\xc4\xf2QF\xbfw~W\x1b\xf1\xf1Nq\xff\xc0\x94\x84ov\x9a0\x1dC\xcf\xd2\x06r4\n\xe7m\x01\nQ{"
        b"wb6X\x9cz\xab\xb5\x04\x97\x8e\xdf^cr\x81\xb1\x83s\xf2\xb0\xa1[2\xd0\x9f|V\xb0\xc3"
    )
    data = build_sns_notification_data(Message=b64encode(blob).decode())

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)

    assert message.payload == blob
    assert message.payload_type == PayloadType.BINARY


def test_deserialize_sns_notification_with_base64_encoded_bson_payload():
    payload_data = build_generic_json_data()
    payload_blob = bson.dumps(payload_data)
    data = build_sns_notification_data(Message=b64encode(payload_blob).decode())

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)

    assert message.payload_type == PayloadType.DATA
    assert message.payload == payload_data


def test_deserialize_sns_notification_with_generic_json_payload():
    payload_data = build_generic_json_data()
    payload_json = json.dumps(payload_data)
    data = build_sns_notification_data(Message=payload_json)

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)

    assert message.payload_type == PayloadType.DATA
    assert message.payload == payload_data


def test_deserialize_sns_notification_with_mega_payload_over_plaintext_json():
    mega_data = build_mega_payload_data()
    mega_json = json.dumps(mega_data)
    data = build_sns_notification_data(Message=mega_json)

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)
    assert_has_mega_payload(message)
    assert_mega_payload_matches_data(message.payload, mega_data)


def test_deserialize_sns_notification_with_mega_payload_over_binary_bson():
    mega_data = build_mega_payload_data()
    mega_bson = bson.dumps(mega_data)
    mega_blob = b64encode(mega_bson).decode()
    data = build_sns_notification_data(Message=mega_blob)

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert_sns_notification_matches_data_attributes(message, data)
    assert_has_mega_payload(message)
    assert_mega_payload_matches_data(message.payload, mega_data)


def test_deserialize_sns_notification_without_subject():
    data = build_sns_notification_data()
    del data['Subject']

    message = deserialize_sns_message(data)

    assert_is_sns_notification(message)
    assert message.subject is None


def test_deserialize_sns_subscription_confirmation():
    data = build_sns_subscription_confirmation_data()

    message = deserialize_sns_message(data)

    assert message is not None
    assert isinstance(message, SnsSubscriptionConfirmation)
    assert message.message_type == MessageType.SNS
    assert message.sns_message_type == SnsMessageType.SUBSCRIPTION_CONFIRMATION

    assert_sns_message_matches_data_attributes(message, data)

    assert message.payload is None
    assert message.payload_type == PayloadType.NONE
    assert message.token == data['Token']
    assert message.subscribe_url == data['SubscribeURL']
    assert message.raw_message == data['Message']


def test_deserialize_sns_unsubscribe_confirmation():
    data = build_sns_unsubscribe_confirmation_data()

    message = deserialize_sns_message(data)

    assert message is not None
    assert isinstance(message, SnsUnsubscribeConfirmation)
    assert message.message_type == MessageType.SNS
    assert message.sns_message_type == SnsMessageType.UNSUBSCRIBE_CONFIRMATION

    assert_sns_message_matches_data_attributes(message, data)

    assert message.payload is None
    assert message.payload_type == PayloadType.NONE
    assert message.token == data['Token']
    assert message.subscribe_url == data['SubscribeURL']
    assert message.raw_message == data['Message']


@parameterized.expand([
    ['Type'],
    ['MessageId'],
    ['TopicArn'],
    ['Timestamp'],
    ['Message']
])
def test_fail_to_deserialize_sns_message_without_required_attribute(attribute_name):
    data = build_sns_notification_data()
    del data[attribute_name]

    with pytest.raises(SnsSchemaError) as error:
        deserialize_sns_message(data)

    assert str(error.value) == "Could not deserialize SNS message: " \
                               "{{'{0}': ['Missing data for required field.']}}".format(attribute_name)


@parameterized.expand([
    ['Type'],
    ['MessageId'],
    ['TopicArn'],
    ['Timestamp'],
    ['Message']
])
def test_fail_to_deserialize_sns_message_without_required_attribute_set_to_null(attribute_name):
    data = build_sns_notification_data()
    data[attribute_name] = None

    with pytest.raises(SnsSchemaError) as error:
        deserialize_sns_message(data)

    assert str(error.value) == "Could not deserialize SNS message: " \
                               "{{'{0}': ['Field may not be null.']}}".format(attribute_name)


def test_fail_to_deserialize_sns_message_with_unknown_type():
    data = build_sns_notification_data(Type='FooBar')

    with pytest.raises(SnsSchemaError) as error:
        deserialize_sns_message(data)

    assert str(error.value) == (
        "Could not deserialize SNS message: "
        "{'Type': ['Must be one of: Notification, SubscriptionConfirmation, UnsubscribeConfirmation.']}"
    )


def test_sns_notification_data_matches_sns_message():
    data = build_sns_notification_data()
    assert matches_sns_message(data) is True


def test_sns_subscription_confirmation_data_matches_sns_message():
    data = build_sns_subscription_confirmation_data()
    assert matches_sns_message(data) is True


def test_sns_unsubscribe_confirmation_data_matches_sns_message():
    data = build_sns_unsubscribe_confirmation_data()
    assert matches_sns_message(data) is True


def test_mega_payload_does_not_match_sns_message():
    data = build_mega_payload_data()
    assert matches_sns_message(data) is False


def test_generic_json_payload_does_not_match_sns_message():
    data = build_generic_json_data()
    assert matches_sns_message(data) is False


def test_null_does_not_match_sns_message():
    assert matches_sns_message(None) is False
