from typing import Optional

from mega.aws.message import Message, PayloadType, Payload, MessageType
from mega.aws.payload import deserialize_payload
# TODO: test
from mega.aws.sns.schema import matches_sns_message, deserialize_sns_message


class SqsMessage(Message):
    def __init__(self, message_id: str, receipt_handle: str, body: str):
        self._message_id = message_id
        self._receipt_handle = receipt_handle

        payload, payload_type = deserialize_payload(body)
        if payload_type == PayloadType.DATA and matches_sns_message(payload):
            sns = deserialize_sns_message(payload)
            self._payload = sns.payload
            self._payload_type = sns.payload_type
            self._embedded_message = sns
        else:
            self._payload = payload
            self._payload_type = payload_type
            self._embedded_message = None

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def message_type(self) -> MessageType:
        return MessageType.SQS

    @property
    def payload_type(self) -> PayloadType:
        return self._payload_type

    @property
    def payload(self) -> Optional[Payload]:
        return self._payload

    @property
    def embedded_message(self) -> Optional[Message]:
        return self._embedded_message

    @property
    def receipt_handle(self) -> str:
        return self._receipt_handle

    # TODO: test
    @classmethod
    def matches(cls, data: dict) -> bool:
        return (
                'MessageId' in data and
                'ReceiptHandle' in data and
                'Body' in data
        )

    # TODO: test
    @classmethod
    def deserialize(cls, data: dict) -> 'SqsMessage':
        return SqsMessage(
            message_id=data['MessageId'],
            receipt_handle=data['ReceiptHandle'],
            body=data['Body']
        )
