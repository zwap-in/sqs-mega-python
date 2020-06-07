from abc import ABC, abstractmethod

from mega.aws.payload import Payload


class Publisher(ABC):

    @abstractmethod
    def publish_payload(self, payload: Payload, binary_encoding=False, **kwargs) -> str:
        pass

    @abstractmethod
    def publish_raw_message(self, message: str, **kwargs) -> str:
        pass
