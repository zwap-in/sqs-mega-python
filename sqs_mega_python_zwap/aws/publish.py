from abc import ABC, abstractmethod

from sqs_mega_python_zwap.aws.payload import MessagePayload


class Publisher(ABC):

    @abstractmethod
    def publish(self, payload: MessagePayload, binary_encoding=False, **kwargs) -> str:
        pass

    @abstractmethod
    def publish_raw_message(self, message: str, **kwargs) -> str:
        pass
