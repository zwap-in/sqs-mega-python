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
        for key in kwargs.keys():
            if key not in ('id', 'type', 'version', 'current', 'previous'):
                raise AttributeError('Unrecognized Mega object attribute: "{}"'.format(key))
        self._object_kwargs.update(kwargs)
        return self

    def with_extra(self, **kwargs):
        self._extra = kwargs
        return self

    def build(self) -> MegaPayload:
        return MegaPayload(
            event=self._build_event(),
            object=self._build_object(),
            extra=self._extra,
        )

    def _build_event(self):
        return MegaEvent(**self._event_kwargs) if self._event_kwargs else None

    def _build_object(self):
        return MegaObject(**self._object_kwargs) if self._object_kwargs else None
