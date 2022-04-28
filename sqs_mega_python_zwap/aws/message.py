from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from sqs_mega_python_zwap.aws.payload import PayloadType, MessagePayload


class MessageType(Enum):
    SQS = 1
    SNS = 2


class Message(ABC):
    @property
    @abstractmethod
    def message_id(self) -> str:
        pass

    @property
    @abstractmethod
    def message_type(self) -> MessageType:
        pass

    @property
    @abstractmethod
    def payload_type(self) -> PayloadType:
        pass

    @property
    @abstractmethod
    def payload(self) -> Optional[MessagePayload]:
        pass

    @property
    @abstractmethod
    def embedded_message(self) -> Optional['Message']:
        pass
