from typing import Optional

from mega.aws.message import Message, PayloadType, Payload, MessageType
from mega.aws.payload import parse_payload
from mega.aws.sns.message import SnsMessage


# TODO: test
class SqsMessage(Message):
    def __init__(self, message_id: str, receipt_handle: str, body: str):
        self._id = message_id
        self._receipt_handle = receipt_handle

        payload, payload_type = parse_payload(body)
        if payload_type == PayloadType.DATA and SnsMessage.matches(payload):
            sns = SnsMessage.deserialize(payload)
            self._payload = sns.payload
            self._payload_type = sns.payload_type
            self._embedded_message = sns
        else:
            self._payload = payload
            self._payload_type = payload_type
            self._embedded_message = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> MessageType:
        return MessageType.SQS

    @property
    def receipt_handle(self) -> str:
        return self._receipt_handle

    @property
    def payload_type(self) -> PayloadType:
        return self._payload_type

    @property
    def payload(self) -> Payload:
        return self._payload

    @property
    def embedded_message(self) -> Optional[Message]:
        return self._embedded_message

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
