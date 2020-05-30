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
Scalar = Union[NullType, StringType, NumberType, BooleanType, DateTimeType]
Value = Union[Scalar, CollectionType, MappingType]


def is_string(value: Value) -> bool:
    return isinstance(value, str)


def is_number(value: Value) -> bool:
    return isinstance(value, int) or \
           isinstance(value, float) or \
           isinstance(value, Decimal)


def is_boolean(value: Value) -> bool:
    return isinstance(value, bool)


def is_datetime(value: Value) -> bool:
    return isinstance(value, date) or \
           isinstance(value, datetime)


def is_scalar(value: Value) -> bool:
    return value is None or \
           is_string(value) or \
           is_number(value) or \
           is_boolean(value) or \
           is_datetime(value)


def is_collection(value: Value) -> bool:
    return isinstance(value, list) or \
           isinstance(value, tuple) or \
           isinstance(value, set)


def is_mapping(value: Value) -> bool:
    return isinstance(value, dict)
