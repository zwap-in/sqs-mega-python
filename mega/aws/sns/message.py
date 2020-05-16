from datetime import datetime
from typing import Optional

from dateutil import parser

from mega.aws.message import Message, PayloadType, Payload, MessageType
from mega.aws.payload import parse_payload


# TODO: test
class SnsMessage(Message):
    def __init__(self, message_id: str, topic_arn: str, timestamp: datetime, raw_message: str):
        self._message_id = message_id
        self._topic_arn = topic_arn
        self._timestamp = timestamp

        payload, payload_type = parse_payload(raw_message)
        self._payload = payload
        self._payload_type = payload_type

    @property
    def id(self) -> str:
        return self._message_id

    @property
    def type(self) -> MessageType:
        return MessageType.SNS

    @property
    def payload_type(self) -> PayloadType:
        return self._payload_type

    @property
    def payload(self) -> Payload:
        return self._payload

    @property
    def embedded_message(self) -> Optional[Message]:
        return None

    @property
    def topic_arn(self) -> str:
        return self._topic_arn

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    # TODO: test
    @classmethod
    def matches(cls, data: dict):
        return (
                'MessageId' in data and
                'TopicArn' in data and
                'Message' in data and
                'Timestamp' in data
        )

    # TODO: test
    @classmethod
    def deserialize(cls, data: dict):
        return SnsMessage(
            message_id=data['MessageId'],
            topic_arn=data['TopicArn'],
            timestamp=parser.parse(data['Timestamp']),
            raw_message=data['Message']
        )
