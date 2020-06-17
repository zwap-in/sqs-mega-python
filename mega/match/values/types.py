from datetime import date, datetime
from decimal import Decimal
from typing import Union, Dict

NullType = None
StringType = str
BooleanType = bool
NumberType = Union[int, float, Decimal]
DateTimeType = Union[date, datetime]
CollectionType = Union[list, tuple, set]
MappingType = Dict
ScalarType = Union[NullType, StringType, NumberType, BooleanType, DateTimeType]
ValueType = Union[ScalarType, CollectionType, MappingType]


def is_string(value: ValueType) -> bool:
    return isinstance(value, str)


def is_number(value: ValueType) -> bool:
    return type(value) in (int, float, Decimal)


def is_boolean(value: ValueType) -> bool:
    return type(value) is bool


def is_datetime(value: ValueType) -> bool:
    return type(value) in (date, datetime)


def is_scalar(value: ValueType) -> bool:
    return value is None or \
           is_string(value) or \
           is_number(value) or \
           is_boolean(value) or \
           is_datetime(value)


def is_collection(value: ValueType) -> bool:
    return isinstance(value, list) or \
           isinstance(value, tuple) or \
           isinstance(value, set)


def is_mapping(value: ValueType) -> bool:
    return isinstance(value, dict)
