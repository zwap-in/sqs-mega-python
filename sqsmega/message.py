from abc import ABC, abstractmethod

from sqsmega.payload import Payload


class Message(ABC):
    @property
    @abstractmethod
    def message_id(self) -> str:
        pass

    @abstractmethod
    @property
    def payload(self) -> Payload:
        pass

    @property
    def has_data(self) -> bool:
        return self.payload.has_data

    @property
    def is_event(self) -> bool:
        return self.payload.is_event
