import inspect
import types
from abc import ABC, abstractmethod
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
ComparableType = Union[NumberType, DateTimeType]
ScalarType = Union[NullType, StringType, NumberType, BooleanType, DateTimeType]
ValueType = Union[ScalarType, CollectionType, MappingType]
FunctionType = types.FunctionType


class RightHandSideValue(ABC):
    @abstractmethod
    def equal(self, lhs: ValueType) -> bool:
        pass

    @abstractmethod
    def match(self, lhs: ValueType) -> bool:
        pass


class ComparableRightHandSideValue(RightHandSideValue, ABC):
    @abstractmethod
    def less_than(self, lhs: ValueType) -> bool:
        pass

    @abstractmethod
    def less_than_or_equal(self, lhs: ValueType) -> bool:
        pass

    @abstractmethod
    def greater_than(self, lhs: ValueType) -> bool:
        pass

    @abstractmethod
    def greater_than_or_equal(self, lhs: ValueType) -> bool:
        pass


class CollectionRightHandSideValue(RightHandSideValue, ABC):
    @abstractmethod
    def contains(self, lhs: ValueType) -> bool:
        pass


class RightHandSideFunction(ABC):
    @abstractmethod
    def evaluate(self, lhs: ValueType) -> bool:
        pass


RightHandSideValueType = Union[RightHandSideValue, ValueType]
ComparableRightHandSideValueType = Union[ComparableRightHandSideValue, NumberType, DateTimeType]
CollectionRightHandSideValueType = Union[CollectionRightHandSideValue, CollectionType]
RightHandSideFunctionType = Union[RightHandSideFunction, FunctionType]
RightHandSideType = Union[RightHandSideValueType, RightHandSideFunctionType]


def is_string(value) -> bool:
    return isinstance(value, str)


def is_number(value) -> bool:
    return type(value) in (int, float, Decimal)


def is_boolean(value) -> bool:
    return type(value) is bool


def is_datetime(value) -> bool:
    return type(value) in (date, datetime)


def is_scalar(value) -> bool:
    return value is None or \
           is_string(value) or \
           is_number(value) or \
           is_boolean(value) or \
           is_datetime(value)


def is_collection(value) -> bool:
    return isinstance(value, list) or \
           isinstance(value, tuple) or \
           isinstance(value, set)


def is_mapping(value) -> bool:
    return isinstance(value, dict)


def is_function(function) -> bool:
    return inspect.isfunction(function)
