from sqs_mega_python_zwap.match.types import RightHandSideValueType, RightHandSideValue, ComparableRightHandSideValue, \
    CollectionRightHandSideValue
from sqs_mega_python_zwap.match.values.boolean import Boolean
from sqs_mega_python_zwap.match.values.collection import Collection
from sqs_mega_python_zwap.match.values.datetime import DateTime
from sqs_mega_python_zwap.match.values.mapping import Mapping
from sqs_mega_python_zwap.match.values.null import Null
from sqs_mega_python_zwap.match.values.number import Number
from sqs_mega_python_zwap.match.values.string import String


def value(rhs: RightHandSideValueType) -> RightHandSideValue:
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


def comparable_value(rhs) -> ComparableRightHandSideValue:
    comparable = value(rhs)
    if not isinstance(comparable, ComparableRightHandSideValue):
        raise TypeError
    return comparable


def collection_value(rhs) -> CollectionRightHandSideValue:
    collection = value(rhs)
    if not isinstance(collection, CollectionRightHandSideValue):
        raise TypeError
    return collection
