from marshmallow import Schema, EXCLUDE, fields, post_load

from mega.aws.payload import deserialize_payload, PayloadType
from mega.aws.sns.schema import matches_sns_message, deserialize_sns_message
from mega.aws.sqs.message import SqsMessage


class SqsSchemaError(Exception):
    pass


class SqsMessageSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    message_id = fields.String(data_key='MessageId', required=True, allow_none=False)
    receipt_handle = fields.String(data_key='ReceiptHandle', required=True, allow_none=False)
    body = fields.String(data_key='Body', required=True, allow_none=False)

    @post_load
    def build_object(self, data, **kwargs):
        payload, payload_type = deserialize_payload(data['body'])
        if payload_type == PayloadType.DATA and matches_sns_message(payload):
            return self.__build_sqs_message_with_embedded_sns_message(data, payload)
        return self.__build_sqs_message_with_payload(data, payload, payload_type)

    @staticmethod
    def __build_sqs_message_with_embedded_sns_message(data, payload):
        sns_message = deserialize_sns_message(payload)
        return SqsMessage(
            message_id=data['message_id'],
            receipt_handle=data['receipt_handle'],
            payload=sns_message.payload,
            payload_type=sns_message.payload_type,
            embedded_message=sns_message
        )

    @staticmethod
    def __build_sqs_message_with_payload(data, payload, payload_type):
        return SqsMessage(
            message_id=data['message_id'],
            receipt_handle=data['receipt_handle'],
            payload=payload,
            payload_type=payload_type,
            embedded_message=None
        )

    def handle_error(self, exc, data, **kwargs):
        raise SqsSchemaError('Could not deserialize SQS message: {0}'.format(exc))


def deserialize_sqs_message(data: dict) -> SqsMessage:
    return SqsMessageSchema().load(data)
