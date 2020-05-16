from marshmallow import Schema, fields, post_load, EXCLUDE

from mega.event.v1 import PROTOCOL_VERSION, PROTOCOL_NAME
from mega.event.v1.payload import MegaPayload, EventObject, Event


class MegaSchemaError(Exception):
    pass


class EventSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    name = fields.String(required=True, allow_none=False)
    timestamp = fields.DateTime(format='iso', required=True, allow_none=False)
    version = fields.Integer(required=False, allow_none=True, default=1)
    domain = fields.String(required=False, allow_none=True, default=None)
    subject = fields.String(required=False, allow_none=True, default=None)
    publisher = fields.String(required=False, allow_none=True, default=None)
    attributes = fields.Dict(keys=fields.String(), required=False, allow_none=True, default={})

    @post_load
    def make_object(self, data, **_kwargs):
        return Event(**data)

    def handle_error(self, exc, data, **kwargs):
        raise MegaSchemaError("Invalid MEGA payload. Could not deserialize the 'event' section: {0}".format(exc))


class EventObjectSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=False, allow_none=True, default=None)
    id = fields.String(required=False, allow_none=True, default=None)
    version = fields.Integer(required=False, allow_none=True, default=1)
    current = fields.Dict(required=True, allow_none=False)
    previous = fields.Dict(required=False, allow_none=True, default=None)

    @post_load
    def make_object(self, data, **_kwargs):
        return EventObject(**data)

    def handle_error(self, exc, data, **kwargs):
        raise MegaSchemaError("Invalid MEGA payload. Could not deserialize the 'object' section: {0}".format(exc))


class MegaPayloadSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    protocol = fields.Constant(PROTOCOL_NAME, dump_only=True)
    version = fields.Constant(PROTOCOL_VERSION, dump_only=True)
    event = fields.Nested(EventSchema, required=True)
    object = fields.Nested(EventObjectSchema, required=False, allow_none=True, default=None)
    extra = fields.Dict(keys=fields.String(), required=False, allow_none=True, default={})

    @post_load
    def make_object(self, data, **_kwargs):
        return MegaPayload(**data)

    def handle_error(self, exc, data, **kwargs):
        raise MegaSchemaError("Invalid MEGA payload: {0}".format(exc))


def deserialize_mega_payload(data: dict):
    return MegaPayloadSchema().load(data)
