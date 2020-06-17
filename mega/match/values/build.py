from typing import Union

from mega.match.values.collection import Collection
from mega.match.values.mapping import Mapping
from mega.match.values.boolean import Boolean
from mega.match.values.datetime import DateTime
from mega.match.values.number import Number
from mega.match.values.string import String
from mega.match.values.null import Null
from mega.match.values.types import ValueType
from mega.match.values.base import RightHandSideValue, ComparableValue


def value(rhs: Union[ValueType, RightHandSideValue]) -> RightHandSideValue:
    if isinstance(rhs, RightHandSideValue):
        return rhs

    if Null.accepts_rhs(rhs):
        return Null()
    if String.accepts_rhs(rhs):
        return String(rhs)
    if Number.accepts_rhs(rhs):
        return Number(rhs)
    if DateTime.accepts_rhs(rhs):
        return DateTime(rhs)
    if Boolean.accepts_rhs(rhs):
        return Boolean(rhs)
    if Collection.accepts_rhs(rhs):
        return Collection(rhs)
    if Mapping.accepts_rhs(rhs):
        return Mapping(rhs)

    raise TypeError('Right-hand side value type is not supported: {}'.format(type(rhs).__name__))


def comparable_value(rhs) -> ComparableValue:
    comparable = value(rhs)
    if not isinstance(comparable, ComparableValue):
        raise TypeError
    return comparable


def collection_value(rhs) -> Collection:
    collection = value(rhs)
    if not isinstance(collection, Collection):
        raise TypeError
    return collection
