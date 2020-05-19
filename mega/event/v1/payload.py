from datetime import datetime
from typing import Optional

from mega.event.v1 import PROTOCOL_NAME, PROTOCOL_VERSION


class Event:
    DEFAULT_VERSION = 1

    def __init__(
            self,
            name: str,
            timestamp: Optional[datetime] = None,
            version: int = DEFAULT_VERSION,
            domain: str = None,
            subject: str = None,
            publisher: str = None,
            attributes: dict = None,
            **kwargs
    ):
        self.name = name
        self.timestamp = timestamp if timestamp else datetime.utcnow()
        self.version = version if version else self.DEFAULT_VERSION
        self.domain = domain
        self.subject = subject
        self.publisher = publisher

        self.attributes = {}
        self.attributes.update(attributes or {})
        self.attributes.update(kwargs or {})


class EventObject:
    DEFAULT_VERSION = 1

    def __init__(
            self,
            current: dict,
            type: str = None,
            id: str = None,
            version: int = DEFAULT_VERSION,
            previous: dict = None
    ):
        self.current = current
        self.type = type
        self.id = id
        self.version = version if version else self.DEFAULT_VERSION
        self.previous = previous


class MegaPayload:
    protocol = PROTOCOL_NAME
    version = PROTOCOL_VERSION

    def __init__(
            self,
            event: Event,
            object: Optional[EventObject] = None,
            extra: dict = None,
            **kwargs
    ):
        self.event = event
        self.object = object

        self.extra = {}
        self.extra.update(extra or {})
        self.extra.update(kwargs or {})
