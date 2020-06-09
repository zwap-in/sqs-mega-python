from marshmallow import Schema, fields, post_load, EXCLUDE, post_dump

from mega.event.v1 import PROTOCOL_VERSION, PROTOCOL_NAME
from mega.event.v1.payload import Payload, Object, Event


class SchemaError(Exception):
    pass


class BaseSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    @post_dump
    def remove_empty_attributes(self, data, **kwargs):
        return {
            key: value for key, value in data.items()
            if value not in (None, {})
        }


class EventSchema(BaseSchema):
    name = fields.String(required=True, allow_none=False)
    timestamp = fields.DateTime(format='iso', required=True, allow_none=False)
    version = fields.Integer(required=False, allow_none=True, default=1)
    domain = fields.String(required=False, allow_none=True, default=None)
    subject = fields.String(required=False, allow_none=True, default=None)
    publisher = fields.String(required=False, allow_none=True, default=None)
    attributes = fields.Dict(keys=fields.String(), required=False, allow_none=True, default={})

    @post_load
    def build_object(self, data, **kwargs):
        return Event(**data)

    def handle_error(self, exc, data, **kwargs):
        raise SchemaError("Invalid MEGA payload. There is an error in the 'event' section: {0}".format(exc))


class ObjectSchema(BaseSchema):
    type = fields.String(required=False, allow_none=True, default=None)
    id = fields.String(required=False, allow_none=True, default=None)
    version = fields.Integer(required=False, allow_none=True, default=1)
    current = fields.Dict(required=True, allow_none=False)
    previous = fields.Dict(required=False, allow_none=True, default=None)

    @post_load
    def build_object(self, data, **kwargs):
        return Object(**data)

    def handle_error(self, exc, data, **kwargs):
        raise SchemaError("Invalid MEGA payload. There is an error in the 'object' section: {0}".format(exc))


class PayloadSchema(BaseSchema):
    protocol = fields.Constant(PROTOCOL_NAME, dump_only=True)
    version = fields.Constant(PROTOCOL_VERSION, dump_only=True)
    event = fields.Nested(EventSchema, required=True)
    object = fields.Nested(ObjectSchema, required=False, allow_none=True, default=None)
    extra = fields.Dict(keys=fields.String(), required=False, allow_none=True, default={})

    @post_load
    def build_object(self, data, **kwargs):
        return Payload(**data)

    def handle_error(self, exc, data, **kwargs):
        raise SchemaError('Invalid MEGA payload: {0}'.format(exc))


def matches_payload(data: dict) -> bool:
    if not data:
        return False

    return (
            data.get('protocol') == PROTOCOL_NAME and
            data.get('version') == PROTOCOL_VERSION
    )


def deserialize_payload(data: dict) -> Payload:
    return PayloadSchema().load(data)


def serialize_payload(payload: Payload) -> dict:
    schema = PayloadSchema()
    data = schema.dump(payload)
    schema.validate(data)
    return data
