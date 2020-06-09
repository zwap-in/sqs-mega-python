from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional

from mega.aws.message import Message, MessageType
from mega.aws.payload import PayloadType, MessagePayload


class SnsMessageType(Enum):
    NOTIFICATION = 'Notification'
    SUBSCRIPTION_CONFIRMATION = 'SubscriptionConfirmation'
    UNSUBSCRIBE_CONFIRMATION = 'UnsubscribeConfirmation'

    @classmethod
    def values(cls):
        return (
            cls.NOTIFICATION.value,
            cls.SUBSCRIPTION_CONFIRMATION.value,
            cls.UNSUBSCRIBE_CONFIRMATION.value
        )


class SnsMessage(Message, ABC):
    def __init__(
            self,
            message_id: str,
            topic_arn: str,
            timestamp: datetime
    ):
        self._message_id = message_id
        self._topic_arn = topic_arn
        self._timestamp = timestamp

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def message_type(self) -> MessageType:
        return MessageType.SNS

    @property
    def embedded_message(self) -> Optional[Message]:
        return None

    @property
    @abstractmethod
    def sns_message_type(self) -> SnsMessageType:
        pass

    @property
    def topic_arn(self) -> str:
        return self._topic_arn

    @property
    def timestamp(self) -> datetime:
        return self._timestamp


class SnsNotification(SnsMessage):

    def __init__(
            self,
            message_id: str,
            topic_arn: str,
            timestamp: datetime,
            payload: MessagePayload,
            payload_type: PayloadType,
            subject: Optional[str],
            unsubscribe_url: str
    ):
        super().__init__(
            message_id=message_id,
            topic_arn=topic_arn,
            timestamp=timestamp
        )
        self._payload = payload
        self._payload_type = payload_type
        self._subject = subject
        self._unsubscribe_url = unsubscribe_url

    @property
    def sns_message_type(self) -> SnsMessageType:
        return SnsMessageType.NOTIFICATION

    @property
    def payload_type(self) -> PayloadType:
        return self._payload_type

    @property
    def payload(self) -> Optional[MessagePayload]:
        return self._payload

    @property
    def subject(self) -> Optional[str]:
        return self._subject

    @property
    def unsubscribe_url(self) -> str:
        return self._unsubscribe_url


class SnsConfirmation(SnsMessage, ABC):

    def __init__(
            self,
            message_id: str,
            topic_arn: str,
            timestamp: datetime,
            token: str,
            subscribe_url: str,
            raw_message: str
    ):
        super().__init__(
            message_id=message_id,
            topic_arn=topic_arn,
            timestamp=timestamp
        )
        self._token = token
        self._subscribe_url = subscribe_url
        self._raw_message = raw_message

    @property
    def payload_type(self) -> PayloadType:
        return PayloadType.NONE

    @property
    def payload(self) -> Optional[MessagePayload]:
        return None

    @property
    def token(self) -> str:
        return self._token

    @property
    def subscribe_url(self) -> str:
        return self._subscribe_url

    @property
    def raw_message(self) -> str:
        return self._raw_message


class SnsSubscriptionConfirmation(SnsConfirmation):
    @property
    def sns_message_type(self) -> SnsMessageType:
        return SnsMessageType.SUBSCRIPTION_CONFIRMATION


class SnsUnsubscribeConfirmation(SnsConfirmation):
    @property
    def sns_message_type(self) -> SnsMessageType:
        return SnsMessageType.UNSUBSCRIBE_CONFIRMATION
