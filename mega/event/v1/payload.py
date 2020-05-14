from datetime import datetime
from typing import Optional

from mega.event import PROTOCOL_NAME
from mega.event.v1 import PROTOCOL_VERSION


class Event:
    def __init__(
            self,
            name: str,
            timestamp: Optional[datetime] = None,
            version: int = 1,
            publisher: str = None,
            subject: str = None,
            **kwargs
    ):
        self.name = name
        self.timestamp = timestamp if timestamp else datetime.utcnow()
        self.version = version
        self.publisher = publisher
        self.subject = subject
        self.attributes = kwargs


class EventObject:

    def __init__(
            self,
            current,
            version=1,
            name=None,
            previous=None
    ):
        self.current = current
        self.version = version
        self.name = name
        self.previous = previous


class Payload:
    protocol = PROTOCOL_NAME
    version = PROTOCOL_VERSION

    def __init__(self, event: Event, object: Optional[EventObject] = None, **kwargs):
        self.event = event
        self.object = object
        self.extra = kwargs
