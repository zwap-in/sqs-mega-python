from datetime import datetime
from typing import Optional

from mega.event import Payload, ObjectData, Event


class PayloadBuilder:
    def __init__(self):
        self._event_kwargs = {}
        self._object_kwargs = {}
        self._extra = {}

    def with_event(
            self,
            name: str = None,
            version: Optional[int] = None,
            timestamp: Optional[datetime] = None,
            domain: Optional[str] = None,
            subject: Optional[str] = None,
            publisher: Optional[str] = None,
            attributes: Optional[dict] = None,
            **kwargs
    ):
        self._event_kwargs.update(
            name=name,
            version=version,
            timestamp=timestamp,
            domain=domain,
            subject=subject,
            publisher=publisher,
            attributes=(attributes or {})
        )
        self._event_kwargs['attributes'].update(kwargs)
        return self

    def with_object(
            self,
            current: dict = None,
            type: Optional[str] = None,
            id: Optional[str] = None,
            version: Optional[int] = None,
            previous: Optional[dict] = None
    ):
        self._object_kwargs.update(
            current=current,
            type=type,
            id=id,
            version=version,
            previous=previous
        )
        return self

    def with_extra(self, **kwargs):
        self._extra.update(kwargs)
        return self

    def build(self) -> Payload:
        return Payload(
            event=self._build_event(),
            object=self._build_object(),
            extra=self._extra,
        )

    def _build_event(self):
        return Event(**self._event_kwargs) if self._event_kwargs else None

    def _build_object(self):
        return ObjectData(**self._object_kwargs) if self._object_kwargs else None
