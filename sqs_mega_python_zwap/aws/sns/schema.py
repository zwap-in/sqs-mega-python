from marshmallow import Schema, EXCLUDE, fields, post_load, validate

from sqs_mega_python_zwap.aws.payload import deserialize_payload
from sqs_mega_python_zwap.aws.sns.message import SnsMessage, SnsMessageType, SnsNotification, SnsSubscriptionConfirmation, \
    SnsUnsubscribeConfirmation


class SnsSchemaError(Exception):
    pass


class SnsMessageSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    type = fields.String(
        data_key='Type', required=True, allow_none=False,
        validate=validate.OneOf(SnsMessageType.values())
    )
    message_id = fields.String(data_key='MessageId', required=True, allow_none=False)
    topic_arn = fields.String(data_key='TopicArn', required=True, allow_none=False)
    timestamp = fields.DateTime(data_key='Timestamp', format='iso', required=True, allow_none=False)
    message = fields.String(data_key='Message', required=True, allow_none=False)

    # Only for SNS Notification messages
    subject = fields.String(data_key='Subject', required=False, allow_none=True)
    unsubscribe_url = fields.String(data_key='UnsubscribeURL', required=False)

    # Only for SNS Subscription/Unsubscribe Confirmation messages
    token = fields.String(data_key='Token', required=False)
    subscribe_url = fields.String(data_key='SubscribeURL', required=False)

    @post_load
    def build_object(self, data, **kwargs):
        type_ = data['type']

        if type_ == SnsMessageType.NOTIFICATION.value:
            return self.__build_sns_notification(data)
        elif type_ == SnsMessageType.SUBSCRIPTION_CONFIRMATION.value:
            return self.__build_sns_subscription_confirmation(data)
        elif type_ == SnsMessageType.UNSUBSCRIBE_CONFIRMATION.value:
            return self.__build_sns_unsubscribe_confirmation(data)
        else:
            raise ValueError("Don't know how to deserialize SNS message type: {0}".format(type_))

    @staticmethod
    def __build_sns_notification(data):
        payload, payload_type = deserialize_payload(data['message'])
        return SnsNotification(
            message_id=data['message_id'],
            topic_arn=data['topic_arn'],
            timestamp=data['timestamp'],
            payload=payload,
            payload_type=payload_type,
            subject=data.get('subject'),
            unsubscribe_url=data['unsubscribe_url']
        )

    @staticmethod
    def __build_sns_subscription_confirmation(data):
        return SnsSubscriptionConfirmation(
            message_id=data['message_id'],
            topic_arn=data['topic_arn'],
            timestamp=data['timestamp'],
            token=data['token'],
            subscribe_url=data['subscribe_url'],
            raw_message=data['message']
        )

    @staticmethod
    def __build_sns_unsubscribe_confirmation(data):
        return SnsUnsubscribeConfirmation(
            message_id=data['message_id'],
            topic_arn=data['topic_arn'],
            timestamp=data['timestamp'],
            token=data['token'],
            subscribe_url=data['subscribe_url'],
            raw_message=data['message']
        )

    def handle_error(self, exc, data, **kwargs):
        raise SnsSchemaError('Could not deserialize SNS message: {0}'.format(exc))


def deserialize_sns_message(data: dict) -> SnsMessage:
    return SnsMessageSchema().load(data)


def matches_sns_message(data: dict) -> bool:
    if not data:
        return False

    return (
            'MessageId' in data and
            'TopicArn' in data and
            'Type' in data and
            'Timestamp' in data and
            'Message' in data
    )
