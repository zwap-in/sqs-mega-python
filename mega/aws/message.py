from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from mega.aws.payload import PayloadType, Payload


class MessageType(Enum):
    SQS = 1
    SNS = 2


class Message(ABC):
    @abstractmethod
    @property
    def id(self) -> str:
        pass

    @abstractmethod
    @property
    def type(self) -> MessageType:
        pass

    @abstractmethod
    @property
    def payload_type(self) -> PayloadType:
        pass

    @abstractmethod
    @property
    def payload(self) -> Payload:
        pass

    @abstractmethod
    @property
    def embedded_message(self) -> Optional['Message']:
        pass
