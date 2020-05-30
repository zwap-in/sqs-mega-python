from typing import Union

from mega.data.match.values.collection import Collection
from mega.data.match.values.mapping import Mapping
from mega.data.match.values.types import Value
from mega.data.match.values.scalars import Null, String, Number, DateTime, Boolean
from mega.data.match.values.value import RightHandSideValue, ComparableValue


def value(rhs: Union[Value, RightHandSideValue]) -> RightHandSideValue:
    if isinstance(rhs, RightHandSideValue):
        return rhs

    if Null.accepts_rhs(rhs):
        return Null(rhs)
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
