from abc import abstractmethod, ABC
from typing import Optional


class Payload(ABC):

    @abstractmethod
    @property
    def has_data(self) -> bool:
        pass

    @abstractmethod
    @property
    def is_event(self) -> bool:
        pass


class DataPayload(Payload):

    def __init__(self, data: dict):
        self.data = data

    @property
    def has_data(self): return True

    @property
    def is_event(self): return False

    def __getitem__(self, key: str):
        return self.data[key]

    def __contains__(self, key: str):
        return key in self.data

    def get(self, key: str, default=None):
        return self.__getitem__(key) if key in self.data else default


class EventPayload(DataPayload):

    def __init__(self, data: dict, event_name: str, version: Optional[str] = None, publisher: Optional[str] = None):
        super().__init__(data)
        self.event_name = event_name
        self.version = version
        self.publisher = publisher

    @property
    def is_event(self):
        return True


class ErrorPayload(Payload):

    def __init__(self, raw: str, error: str):
        self.raw = raw
        self.error = error

    @property
    def has_data(self):
        return False

    @property
    def is_event(self):
        return False
