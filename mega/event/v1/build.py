from mega.event import MegaPayload, MegaObject, MegaEvent


class PayloadBuilder:
    def __init__(self):
        self._event_kwargs = {}
        self._object_kwargs = {}
        self._extra = {}

    def with_event(self, **kwargs):
        self._event_kwargs.update(kwargs)
        return self

    def with_object(self, **kwargs):
        self._object_kwargs.update(kwargs)
        return self

    def with_extra(self, **kwargs):
        self._extra = kwargs
        return self

    def build(self) -> MegaPayload:
        return MegaPayload(
            event=MegaEvent(**self._event_kwargs),
            object=MegaObject(**self._object_kwargs) if self._object_kwargs else None,
            extra=self._extra,
        )
